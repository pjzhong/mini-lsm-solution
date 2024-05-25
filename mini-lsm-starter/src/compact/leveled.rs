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
        let (compact_level, size, taget_size) = {
            let base_level_size_b = self.options.base_level_size_mb as u64 * 1024 * 1024;
            let mut size = vec![(0u64, 0.0); self.options.max_levels];
            let mut taget_size = vec![0u64; self.options.max_levels];
            let mut compact_level_idx = 0;
            for (level, ids) in snapshot.levels.iter().rev() {
                let level_size = ids
                    .iter()
                    .flat_map(|id| snapshot.sstables.get(id))
                    .map(|table| table.table_size())
                    .sum::<u64>();

                let idx = level - 1;
                if *level < self.options.max_levels {
                    taget_size[idx] =
                        taget_size[idx + 1] / self.options.level_size_multiplier as u64;
                    size[idx] = (level_size, level_size as f64 / taget_size[idx] as f64);
                } else {
                    // the last level no need to calc the ration
                    size[idx] = (level_size, 0.0);
                    taget_size[idx] = level_size;
                }

                if level_size < base_level_size_b {
                    compact_level_idx = idx;
                    break;
                }
            }

            (compact_level_idx, size, taget_size)
        };

        println!("{:?}_{:?}_{:?}", compact_level, size, taget_size);

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
            let mut compact_idx = 0;
            let mut max_ration = 0.0;
            for (idx, (_, ration)) in size.iter().enumerate() {
                let ration = *ration;
                if ration <= 1.0 {
                    continue;
                }

                if max_ration < ration {
                    compact_idx = idx;
                    max_ration = ration;
                }
            }

            if max_ration <= 1.0 {
                None
            } else {
                LeveledCompactionTask {
                    upper_level: Some(compact_idx),
                    upper_level_sst_ids: snapshot.levels[compact_idx].1.clone(),
                    lower_level: compact_idx + 1,
                    lower_level_sst_ids: snapshot.levels[compact_idx + 1].1.clone(),
                    is_lower_level_bottom_level: false,
                }
                .into()
            }
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        if let Some(upper_level) = task.upper_level {
            let mut new_snapshot = snapshot.clone();

            new_snapshot.levels[upper_level]
                .1
                .retain(|id| !task.upper_level_sst_ids.contains(id));
            new_snapshot.levels[task.lower_level].1 = output.to_vec();

            let remove_sst_ids = task
                .upper_level_sst_ids
                .iter()
                .chain(task.lower_level_sst_ids.iter())
                .cloned()
                .collect();

            (new_snapshot, remove_sst_ids)
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
