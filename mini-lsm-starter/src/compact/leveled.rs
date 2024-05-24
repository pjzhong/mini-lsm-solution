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
            let mut lower_level_size = self.options.base_level_size_mb as u64;
            let mut compact_level = self.options.max_levels - 1;
            for (level, ids) in snapshot.levels.iter().rev() {
                let size = ids
                    .iter()
                    .flat_map(|id| snapshot.sstables.get(id))
                    .map(|table| table.table_size())
                    .sum::<u64>();

                taget_size[level - 1] = size;
                if size < lower_level_size {
                    compact_level = level - 1;
                    break;
                } else {
                    lower_level_size = size / self.options.level_size_multiplier as u64;
                }
            }

            (compact_level, taget_size)
        };

        //println!("{:?}_{:?}", compact_level, target_size);

        None
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        unimplemented!()
    }
}
