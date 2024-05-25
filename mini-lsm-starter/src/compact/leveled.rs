use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        unimplemented!()
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let (compact_level, target_size) = {
            let mut taget_size = vec![0; self.options.max_levels];
            let mut lower_level_size_mb = self.options.base_level_size_mb as u64 * 1024 * 1024;
            let mut compact_level = self.options.max_levels - 1;
            for (level, ids) in snapshot.levels.iter().rev() {
                let size = ids
                    .iter()
                    .flat_map(|id| snapshot.sstables.get(id))
                    .map(|table| table.table_size())
                    .sum::<u64>();

                compact_level = level - 1;
                taget_size[compact_level] = size;
                if size < lower_level_size_mb {
                    break;
                } else {
                    lower_level_size_mb = size / self.options.level_size_multiplier as u64;
                }
            }

            (compact_level, taget_size)
        };

        println!(
            "{:?}_{:?}_{:?}",
            compact_level, target_size, self.options.base_level_size_mb
        );

        if self.options.level0_file_num_compaction_trigger <= snapshot.l0_sstables.len() {
            LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: compact_level,
                lower_level_sst_ids: snapshot.levels[compact_level].1.clone(),
                is_lower_level_bottom_level: false,
            }
            .into()
        } else {
            None
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        if let Some(_upper_level) = task.upper_level {
            todo!("还没实现")
        } else {
            let mut new_snapshot = snapshot.clone();

            new_snapshot
                .l0_sstables
                .retain(|id| !task.upper_level_sst_ids.contains(id));
            new_snapshot.levels[task.lower_level].1 = output.to_vec();

            let remove_sst_ids = task
                .upper_level_sst_ids
                .iter()
                .chain(task.lower_level_sst_ids.iter())
                .cloned()
                .collect();

            (new_snapshot, remove_sst_ids)
        }
    }
}
