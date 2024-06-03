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

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest;
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
            CompactionTask::Leveled(task) => self.leveled_compaction(task),
            CompactionTask::Tiered(task) => self.tiered_compaction(task),
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => self.full_compaction(l0_sstables, l1_sstables),
            CompactionTask::Simple(task) => self.simple_leveled_compaction(task),
        }
    }

    fn sst_ids_to_tables(snapshot: &LsmStorageState, ids: &[usize]) -> Vec<Arc<SsTable>> {
        ids.iter()
            .flat_map(|id| snapshot.sstables.get(id))
            .cloned()
            .collect::<Vec<_>>()
    }

    fn leveled_compaction(&self, task: &LeveledCompactionTask) -> Result<Vec<Arc<SsTable>>> {
        if task.upper_level.is_some() {
            let iter = {
                let read = self.state.read();
                let upper_level_tables = Self::sst_ids_to_tables(&read, &task.upper_level_sst_ids);
                let lower_level_tables = Self::sst_ids_to_tables(&read, &task.lower_level_sst_ids);
                drop(read);

                let upper_sst_conact_iter =
                    SstConcatIterator::create_and_seek_to_first(upper_level_tables)?;
                let lower_sst_conact_iter =
                    SstConcatIterator::create_and_seek_to_first(lower_level_tables)?;

                TwoMergeIterator::create(upper_sst_conact_iter, lower_sst_conact_iter)?
            };

            self.iter_compaction(iter)
        } else {
            let iter = {
                let read = self.state.read();
                let l0_tables = Self::sst_ids_to_tables(&read, &task.upper_level_sst_ids);
                let lower_level_tables = Self::sst_ids_to_tables(&read, &task.lower_level_sst_ids);
                drop(read);

                let merge_iter = {
                    let mut iter = Vec::with_capacity(l0_tables.len());
                    for table in l0_tables {
                        iter.push(SsTableIterator::create_and_seek_to_first(table)?.into());
                    }

                    MergeIterator::create(iter)
                };

                let sst_conact_iter =
                    SstConcatIterator::create_and_seek_to_first(lower_level_tables)?;

                TwoMergeIterator::create(merge_iter, sst_conact_iter)?
            };

            self.iter_compaction(iter)
        }
    }

    fn full_compaction(
        &self,
        l0_sstables: &[usize],
        l1_sstables: &[usize],
    ) -> Result<Vec<Arc<SsTable>>> {
        let iter = {
            let (l0, levels) = {
                let state = self.state.read();
                (
                    l0_sstables
                        .iter()
                        .flat_map(|id| state.sstables.get(id))
                        .cloned()
                        .collect::<Vec<_>>(),
                    l1_sstables
                        .iter()
                        .flat_map(|id| state.sstables.get(id))
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            };

            let mut l0_iters = vec![];
            for table in l0 {
                l0_iters.push(SsTableIterator::create_and_seek_to_first(table)?.into());
            }
            TwoMergeIterator::create(
                MergeIterator::create(l0_iters),
                SstConcatIterator::create_and_seek_to_first(levels)?,
            )?
        };

        self.iter_compaction(iter)
    }

    fn simple_leveled_compaction(
        &self,
        task: &SimpleLeveledCompactionTask,
    ) -> Result<Vec<Arc<SsTable>>> {
        if task.upper_level.is_some() {
            let iter = {
                let state = self.state.read();
                let upper_level_iter = task
                    .upper_level_sst_ids
                    .iter()
                    .flat_map(|id| state.sstables.get(id))
                    .cloned()
                    .collect::<Vec<_>>();
                let lower_level_iter = task
                    .lower_level_sst_ids
                    .iter()
                    .flat_map(|id| state.sstables.get(id))
                    .cloned()
                    .collect::<Vec<_>>();
                drop(state);

                TwoMergeIterator::create(
                    SstConcatIterator::create_and_seek_to_first(upper_level_iter)?,
                    SstConcatIterator::create_and_seek_to_first(lower_level_iter)?,
                )?
            };

            self.iter_compaction(iter)
        } else {
            self.full_compaction(&task.upper_level_sst_ids, &task.lower_level_sst_ids)
        }
    }

    fn tiered_compaction(&self, task: &TieredCompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let tiered_sst = {
            let guard = self.state.read();
            let mut tiered_sst = vec![];
            for (_, sst) in &task.tiers {
                tiered_sst.push(
                    sst.iter()
                        .flat_map(|id| guard.sstables.get(id))
                        .cloned()
                        .collect::<Vec<_>>(),
                );
            }
            tiered_sst
        };

        let mut iter = vec![];
        for ssts in tiered_sst {
            iter.push(SstConcatIterator::create_and_seek_to_first(ssts)?.into())
        }

        let iter = MergeIterator::create(iter);
        self.iter_compaction(iter)
    }

    pub fn iter_compaction(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut sst_builders = vec![];
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
                    sst_builders.push(builder);
                }
                iter.next()?;
            }
        }

        sst_builders.push(builder);
        let mut sst_tables = vec![];
        for builder in sst_builders {
            if builder.is_empty() {
                continue;
            }

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

    pub fn force_full_compaction(&self) -> Result<()> {
        //First week merge all sst tables, only l0_sstables and stat.levels[0] have sst tables
        let (l0_sstables, l1_sstables) = {
            let stat = self.state.read();
            (stat.l0_sstables.clone(), stat.levels[0].1.clone())
        };

        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };
        let new_ssts = self.compact(&compaction_task)?;
        let mut ids = Vec::with_capacity(new_ssts.len());
        println!("force full compaction: {:?}", compaction_task);

        {
            let state_lock = self.state_lock.lock();
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();

            snapshot
                .l0_sstables
                .retain(|sst_id| !l0_sstables.contains(sst_id));

            //先刪除在放入，減少擴容的機會
            for id in l0_sstables.iter().chain(l1_sstables.iter()) {
                snapshot.sstables.remove(id);
            }

            for sst in new_ssts {
                ids.push(sst.sst_id());
                snapshot.sstables.insert(sst.sst_id(), sst);
            }
            snapshot.levels[0].1.clone_from(&ids);

            *state = Arc::new(snapshot);
            drop(state);

            if let Some(manifest) = self.manifest.as_ref() {
                manifest.add_record(
                    &state_lock,
                    manifest::ManifestRecord::Compaction(compaction_task, ids.clone()),
                )?;
            }
        }

        for id in l0_sstables.iter().chain(l0_sstables.iter()) {
            let path = self.path_of_sst(*id);
            if path.exists() {
                std::fs::remove_file(path)?;
            }
        }

        println!("force full compaction done, new SSTs: {:?}", ids);

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let task = {
            let snapshot = self.state.read();
            match self
                .compaction_controller
                .generate_compaction_task(&snapshot)
            {
                Some(task) => task,
                None => return Ok(()),
            }
        };

        let sst_tables = self.compact(&task)?;
        let ids = sst_tables
            .iter()
            .map(|sst| sst.sst_id())
            .collect::<Vec<_>>();

        let old_sst_ids = {
            let state_lock = self.state_lock.lock();
            let mut state = self.state.write();

            let mut snapshost = state.as_ref().clone();
            for sst_table in sst_tables {
                let prev = snapshost.sstables.insert(sst_table.sst_id(), sst_table);
                assert!(prev.is_none());
            }

            let (mut snapshost, old_sst_ids) = self
                .compaction_controller
                .apply_compaction_result(&snapshost, &task, &ids);

            for id in &old_sst_ids {
                snapshost.sstables.remove(id);
            }

            *state = snapshost.into();
            drop(state);

            if let Some(manifest) = self.manifest.as_ref() {
                manifest.add_record(
                    &state_lock,
                    manifest::ManifestRecord::Compaction(task, ids.clone()),
                )?;
            }

            old_sst_ids
        };

        for sst_id in old_sst_ids {
            let path = self.path_of_sst(sst_id);
            if path.exists() {
                std::fs::remove_file(path)?;
            }
        }

        Ok(())
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
