use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if self.options.level0_file_num_compaction_trigger <= snapshot.l0_sstables.len() {
            Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot
                    .levels
                    .first()
                    .map(|(_, ids)| ids.clone())
                    .unwrap_or_default(),
                is_lower_level_bottom_level: false,
            })
        } else {
            for level in 1..self.options.max_levels {
                let (upper_level, upper_level_sst_ids) = match snapshot.levels.get(level - 1) {
                    Some((upper_level, upper_level_sst_ids)) if !upper_level_sst_ids.is_empty() => {
                        (*upper_level, upper_level_sst_ids)
                    }
                    _ => continue,
                };

                let (lower_level, lower_level_sst_ids) = match snapshot.levels.get(level) {
                    Some((lower_level, lower_level_sst_ids)) => (*lower_level, lower_level_sst_ids),
                    None => continue,
                };

                let rations =
                    (lower_level_sst_ids.len() as f32 / upper_level_sst_ids.len() as f32) * 100.0;
                if rations < self.options.size_ratio_percent as f32 {
                    return Some(SimpleLeveledCompactionTask {
                        upper_level: Some(upper_level),
                        upper_level_sst_ids: upper_level_sst_ids.clone(),
                        lower_level,
                        lower_level_sst_ids: lower_level_sst_ids.clone(),
                        is_lower_level_bottom_level: snapshot.levels.len() - 1 <= lower_level,
                    });
                }
            }
            None
        }
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = snapshot.clone();
        if let Some(upper) = &task.upper_level {
            new_state.levels[*upper - 1]
                .1
                .retain(|id| !task.upper_level_sst_ids.contains(id))
        } else {
            new_state
                .l0_sstables
                .retain(|id| !task.upper_level_sst_ids.contains(id));
        }

        new_state.levels[task.lower_level - 1].1 = output.to_vec();
        let old_sst_ids = task
            .upper_level_sst_ids
            .iter()
            .chain(task.lower_level_sst_ids.iter())
            .cloned()
            .collect::<Vec<_>>();

        (new_state, old_sst_ids)
    }
}
