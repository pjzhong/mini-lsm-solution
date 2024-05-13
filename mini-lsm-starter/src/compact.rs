#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut iter = {
                    let state = self.state.read();
                    let mut tables: Vec<Arc<SsTable>> = vec![];
                    tables.append(
                        &mut l0_sstables
                            .iter()
                            .flat_map(|id| state.sstables.get(id))
                            .cloned()
                            .collect(),
                    );
                    tables.append(
                        &mut l1_sstables
                            .iter()
                            .flat_map(|id| state.sstables.get(id))
                            .cloned()
                            .collect(),
                    );

                    let mut iters = vec![];
                    for table in tables {
                        iters.push(SsTableIterator::create_and_seek_to_first(table)?.into());
                    }
                    MergeIterator::create(iters)
                };

                let mut builder = SsTableBuilder::new(self.options.block_size);
                let mut sst_tables = vec![];
                while iter.is_valid() {
                    let key = iter.key();
                    let val = iter.value();
                    if val.is_empty() {
                        iter.next()?;
                    } else {
                        builder.add(key, val);

                        if self.options.target_sst_size <= builder.estimated_size() {
                            let builder = std::mem::replace(
                                &mut builder,
                                SsTableBuilder::new(self.options.block_size),
                            );

                            let sst_id = self.next_sst_id();
                            let sst = builder.build(
                                sst_id,
                                Some(self.block_cache.clone()),
                                self.path_of_sst(sst_id),
                            )?;

                            sst_tables.push(sst.into());
                        }
                        iter.next()?;
                    }
                }

                if !builder.is_empty() {
                    let sst_id = self.next_sst_id();
                    let sst = builder.build(
                        sst_id,
                        Some(self.block_cache.clone()),
                        self.path_of_sst(sst_id),
                    )?;

                    sst_tables.push(sst.into());
                }

                Ok(sst_tables)
            }
            _ => unimplemented!(),
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        //First week merge all sst tables, only l0_sstables and stat.levels[0] have sst tables
        let (l0_sstables, l1_sstables) = {
            let stat = self.state.read();
            (stat.l0_sstables.clone(), stat.levels[0].1.clone())
        };

        let new_ssts = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        })?;

        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();

            snapshot
                .l0_sstables
                .retain(|sst_id| !l0_sstables.contains(sst_id));
            snapshot.levels[0].1 = new_ssts.iter().map(|sst| sst.sst_id()).collect();
            for sst in new_ssts {
                snapshot.sstables.insert(sst.sst_id(), sst);
            }
            for id in &l0_sstables {
                snapshot.sstables.remove(id);
            }
            for id in &l1_sstables {
                snapshot.sstables.remove(id);
            }

            *state = Arc::new(snapshot);
        }

        for id in l0_sstables {
            let path = self.path_of_sst(id);
            if path.exists() {
                std::fs::remove_file(path)?;
            }
        }

        for id in l1_sstables {
            let path = self.path_of_sst(id);
            if path.exists() {
                std::fs::remove_file(path)?;
            }
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let mem_table_size = self.state.read().imm_memtables.len() + 1;

        if self.options.num_memtable_limit <= mem_table_size {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
