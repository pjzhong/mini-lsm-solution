use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        //当层数小于设定层数时，不执行压缩
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        //全部层级(除了最后一层)的数量/最后一层的数量的比列
        let last = snapshot
            .levels
            .last()
            .map(|(_, ids)| ids.len())
            .unwrap_or_default() as f32;
        let all_upper = snapshot
            .levels
            .iter()
            .map(|(_, ids)| ids.len() as f32)
            .sum::<f32>()
            - last;

        if (self.options.max_size_amplification_percent as f32 / 100.0) <= all_upper / last {
            //如果整体层数过高
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        } else if self.options.min_merge_width <= snapshot.levels.len() {
            //前N层过高
            let size_ration = (self.options.size_ratio as f32 + 100.0) / 100.0;
            let mut count = 0;

            for (idx, (_, ids)) in snapshot.levels.iter().enumerate() {
                if size_ration <= count as f32 / ids.len() as f32 {
                    return Some(TieredCompactionTask {
                        tiers: snapshot.levels[..idx + 1].to_vec(),
                        bottom_tier_included: false,
                    });
                } else {
                    count += ids.len()
                }
            }
        }

        //直接合并最高层，尽量只剩下self.options.num_tiers
        //the top most
        Some(TieredCompactionTask {
            tiers: snapshot.levels
                [..snapshot.levels.len() - self.options.num_tiers + (self.options.num_tiers - 1)]
                .to_vec(),
            bottom_tier_included: false,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();

        let mut old_sst_ids = vec![];
        let mut old_tiered = HashSet::new();
        if let Some(index) = snapshot
            .levels
            .iter()
            .enumerate()
            .find(|(_, (id, _))| id == &task.tiers[0].0)
            .map(|(index, _)| index)
        {
            snapshot.levels.insert(index, (output[0], output.to_vec()))
        } else {
            snapshot.levels.insert(0, (output[0], output.to_vec()))
        }

        for (tiered_id, ids) in &task.tiers {
            old_tiered.insert(*tiered_id);
            old_sst_ids.extend_from_slice(ids);
        }

        snapshot.levels.retain(|(id, _)| !old_tiered.contains(id));

        (snapshot, old_sst_ids)
    }
}
