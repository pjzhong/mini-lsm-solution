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
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let first_key = sst_ids
            .iter()
            .flat_map(|id| snapshot.sstables.get(id))
            .map(|table| table.first_key())
            .min();
        let last_key = sst_ids
            .iter()
            .flat_map(|id| snapshot.sstables.get(id))
            .map(|table| table.last_key())
            .max();
        let first_key =
            std::ops::Bound::Included(first_key.map(|k| k.raw_ref()).unwrap_or_default());
        let last_key = std::ops::Bound::Included(last_key.map(|k| k.raw_ref()).unwrap_or_default());

        let mut sst_ids = vec![];
        for sst_id in &snapshot.levels[in_level].1 {
            let table = match snapshot.sstables.get(sst_id) {
                Some(table) => table,
                None => continue,
            };

            if table.range_overlap(first_key, last_key) {
                sst_ids.push(table.sst_id())
            }
        }

        sst_ids
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let size = {
            let base_level_size_b = self.options.base_level_size_mb as u64 * 1024 * 1024;
            // (size, target_size)
            let mut size = vec![(0u64, 0); self.options.max_levels];
            for (level, ids) in snapshot.levels.iter().rev() {
                let level_size = ids
                    .iter()
                    .flat_map(|id| snapshot.sstables.get(id))
                    .map(|table| table.table_size())
                    .sum::<u64>();

                let idx = level - 1;
                if *level < self.options.max_levels {
                    let taget_size = size[*level].1 / self.options.level_size_multiplier as u64;
                    size[idx] = (level_size, taget_size);
                } else {
                    size[idx] = (level_size, level_size.max(base_level_size_b));
                }

                if level_size < base_level_size_b {
                    break;
                }
            }

            size
        };

        if self.options.level0_file_num_compaction_trigger <= snapshot.l0_sstables.len() {
            let compact_level = size
                .iter()
                .enumerate()
                .find(|(_, (_, size))| *size > 0)
                .map(|(idx, _)| idx)
                .unwrap_or_else(|| snapshot.levels.len() - 1);

            LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: compact_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    compact_level,
                ),
                is_lower_level_bottom_level: size.len() - 1 <= compact_level,
            }
            .into()
        } else {
            let mut compact_level = 0;
            let mut max_ration = 0.0;
            for (idx, (size, target_size)) in size[..size.len() - 1].iter().enumerate() {
                let ration = *size as f64 / *target_size.max(&1) as f64;
                if ration <= 1.0 {
                    continue;
                }

                if max_ration < ration {
                    compact_level = idx;
                    max_ration = ration;
                }
            }

            if max_ration <= 1.0 {
                return None;
            }

            let min = match snapshot.levels[compact_level].1.iter().min() {
                Some(max) => *max,
                None => return None,
            };

            LeveledCompactionTask {
                upper_level: Some(compact_level),
                upper_level_sst_ids: vec![min],
                lower_level: compact_level + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &[min],
                    compact_level + 1,
                ),
                is_lower_level_bottom_level: size.len() - 1 <= compact_level,
            }
            .into()
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        is_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_snapshot = snapshot.clone();
        if let Some(upper_level) = task.upper_level {
            new_snapshot.levels[upper_level]
                .1
                .retain(|id| !task.upper_level_sst_ids.contains(id));
        } else {
            new_snapshot
                .l0_sstables
                .retain(|id| !task.upper_level_sst_ids.contains(id));
        }

        let lower_level_sst_ids = &mut new_snapshot.levels[task.lower_level].1;
        lower_level_sst_ids.retain(|id| !task.lower_level_sst_ids.contains(id));
        lower_level_sst_ids.extend_from_slice(output);
        // It doesn't work on recvoery, because sst_table is not load at this monent
        if !is_recovery {
            lower_level_sst_ids.sort_by_key(|id| {
                snapshot
                    .sstables
                    .get(id)
                    .map(|table| table.first_key())
                    .unwrap()
            });
        }

        let remove_sst_ids = task
            .upper_level_sst_ids
            .iter()
            .chain(task.lower_level_sst_ids.iter())
            .cloned()
            .collect();

        (new_snapshot, remove_sst_ids)
    }
}
