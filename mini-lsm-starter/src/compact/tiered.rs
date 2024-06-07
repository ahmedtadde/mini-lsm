use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize, Clone)]
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
        assert!(
            self.options.num_tiers > 0,
            "The number of tiers should be greater than 0"
        );
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        //max_size_amplification_percent trigger check
        {
            let (upper_levels, lower_levels) = snapshot.levels.split_at(snapshot.levels.len() - 1);
            let upper_levels_size: usize = upper_levels.iter().map(|level| level.1.len()).sum();
            let lower_levels_size: usize = lower_levels.iter().map(|level| level.1.len()).sum();
            if (lower_levels_size > 0)
                && (upper_levels_size as f64 / lower_levels_size as f64) * 100.0
                    >= (self.options.max_size_amplification_percent as f64)
            {
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels.clone(),
                    bottom_tier_included: true,
                });
            }
        }

        // size_ratio trigger check
        if snapshot.levels.len() >= self.options.min_merge_width {
            let size_ratio = (self.options.size_ratio as f64 + 100.0) / 100.0;
            let (target_tier_idx, task_tiers) = snapshot
                .levels
                .iter()
                .enumerate()
                .position(|(i, x)| {
                    let prefix_size = snapshot
                        .levels
                        .iter()
                        .take(i)
                        .map(|(_, ssts)| ssts.len())
                        .sum::<usize>();
                    let current_size = x.1.len();
                    current_size > 0 && (prefix_size as f64 / current_size as f64) >= size_ratio
                })
                .map_or((None, vec![]), |i| {
                    (
                        Some(i),
                        snapshot
                            .levels
                            .iter()
                            .take(i + 1)
                            .map(|(level, ssts)| (*level, ssts.to_vec()))
                            .collect(),
                    )
                });

            if let (Some(idx), true) = (target_tier_idx, task_tiers.len() > 1) {
                return Some(TieredCompactionTask {
                    tiers: task_tiers,
                    bottom_tier_included: idx == snapshot.levels.len() - 1,
                });
            }
        }

        // num_tiers trigger
        let (task_tiers, _) = snapshot.levels.split_at(self.options.num_tiers - 1);
        if task_tiers.len() > 1 {
            return Some(TieredCompactionTask {
                bottom_tier_included: false,
                tiers: task_tiers.to_vec(),
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            task.tiers.len() >= 2,
            "The number of tiers in the task should be at least 2"
        );
        let mut output = output.to_vec();
        assert!(
            !output.is_empty(),
            "compaction sst ids output should not be empty"
        );
        output.sort_unstable();
        let mut new_snapshot = snapshot.clone();

        // find the index of the first tier in the task
        let task_first_tier_idx = new_snapshot
            .levels
            .iter()
            .position(|(level, _)| *level == task.tiers[0].0)
            .expect("The first tier in the task should exist in the snapshot");

        // replace levels[task_first_tier_idx] with the output
        new_snapshot
            .levels
            .insert(task_first_tier_idx, (output[0], output));

        // remove old tiers
        let old_tiers = task
            .tiers
            .iter()
            .map(|(tier, _)| tier)
            .collect::<HashSet<_>>();

        new_snapshot
            .levels
            .retain(|(level, _)| !old_tiers.contains(level));

        (
            new_snapshot,
            // the old sst ids
            task.tiers
                .iter()
                .flat_map(|(_, ssts)| ssts.iter().cloned())
                .collect(),
        )
    }
}
