use serde::{Deserialize, Serialize};
use std::collections::HashSet;

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

    fn compute_target_sizes(&self, snapshot: &LsmStorageState) -> Vec<usize> {
        let levels_count = snapshot.levels.len().min(self.options.max_levels);
        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;
        assert!(
            levels_count > 0,
            "The number of levels should be greater than 0"
        );
        assert!(
            self.options.level_size_multiplier > 0,
            "The level size multiplier should be greater than 0"
        );

        let level_sizes = snapshot
            .levels
            .iter()
            .take(levels_count)
            .map(|level| {
                level
                    .1
                    .iter()
                    .filter_map(|sst_id| snapshot.sstables.get(sst_id))
                    .map(|sst| sst.table_size() as usize)
                    .sum::<usize>()
            })
            .collect::<Vec<usize>>();
        let mut target_level_sizes = vec![0usize; levels_count];
        *target_level_sizes.last_mut().unwrap() =
            base_level_size_bytes.max(level_sizes.last().copied().unwrap_or(0));

        for level_idx in (0..levels_count).rev() {
            // If the last level is smaller than the target base level size, we are done.
            if level_idx == levels_count - 1 && level_sizes[level_idx] <= base_level_size_bytes {
                break;
            }

            if level_idx == levels_count - 1 {
                continue;
            }

            target_level_sizes[level_idx] =
                target_level_sizes[level_idx + 1] / self.options.level_size_multiplier;

            if target_level_sizes[level_idx] < base_level_size_bytes {
                break;
            }
        }

        target_level_sizes
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let lower_bound_key = sst_ids
            .iter()
            .filter_map(|id| snapshot.sstables.get(id))
            .map(|sst| sst.first_key())
            .min()
            .expect("sstable should have a first key");

        let upper_bound_key = sst_ids
            .iter()
            .filter_map(|id| snapshot.sstables.get(id))
            .map(|sst| sst.last_key())
            .max()
            .expect("sstable should have a last key");

        if let Some((_, ssts)) = snapshot.levels.get(in_level - 1) {
            ssts.iter()
                .filter(|sst_id| {
                    let sst = snapshot.sstables.get(sst_id).expect("sstable should exist");
                    sst.first_key() <= upper_bound_key && sst.last_key() >= lower_bound_key
                })
                .copied()
                .collect()
        } else {
            vec![]
        }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        assert!(
            self.options.max_levels > 0,
            "The maximum number of levels should be greater than 0"
        );
        assert!(snapshot.levels.len() <= self.options.max_levels,
            "The number of levels in the snapshot is greater than the maximum number of levels allowed by the compaction controller"
        );

        let levels_count = self.options.max_levels.min(snapshot.levels.len());

        assert!(
            levels_count > 0,
            "The number of levels should be greater than 0"
        );

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let lower_level_idx = self
                .compute_target_sizes(snapshot)
                .iter()
                .take(levels_count)
                .position(|&size| size > 0)
                .filter(|idx| *idx < snapshot.levels.len())
                .unwrap_or(0);

            assert!(
                lower_level_idx < snapshot.levels.len(),
                "The lower level index is out of bounds"
            );

            println!(
                "compaction triggered => level 0 has {} files (vs threshold={}); will compact to level {}",
                snapshot.l0_sstables.len(),
                self.options.level0_file_num_compaction_trigger,
                lower_level_idx + 1
            );

            let lower_level_sst_ids =
                self.find_overlapping_ssts(snapshot, &snapshot.l0_sstables, lower_level_idx + 1);

            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: lower_level_idx + 1,
                lower_level_sst_ids,
                is_lower_level_bottom_level: self.options.max_levels == lower_level_idx + 1,
            });
        }

        {
            let mut compaction_candidate_levels = snapshot
                .levels
                .iter()
                .zip(self.compute_target_sizes(snapshot))
                .take(levels_count)
                .enumerate()
                .filter_map(|(level_idx, (level, target_size))| {
                    let level_size = level
                        .1
                        .iter()
                        .filter_map(|sst_id| snapshot.sstables.get(sst_id))
                        .map(|sst| sst.table_size() as usize)
                        .sum::<usize>();

                    let ratio = level_size as f64 / target_size as f64;

                    if ratio > 1.0 && level_idx != levels_count - 1 {
                        return Some((ratio, level_idx, level.1.to_vec()));
                    }

                    None
                })
                .collect::<Vec<_>>();

            compaction_candidate_levels.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());

            if let Some((ratio, upper_level_idx, upper_level_sst_ids)) =
                compaction_candidate_levels.first()
            {
                assert!(
                    !upper_level_sst_ids.is_empty(),
                    "The number of sstables in the upper level should be greater than 0"
                );
                let lower_level_idx = *upper_level_idx + 1;
                println!(
                    "compaction triggered => level {} and level {}  where the size ratio is {} which is greater than the threshold 1.0",
                    upper_level_idx + 1,
                    lower_level_idx + 1,
                    ratio,
                );

                let selected_upper_level_sst_id = upper_level_sst_ids
                    .iter()
                    .min()
                    .copied()
                    .expect("sst id should exist");

                let lower_level_sst_ids = self.find_overlapping_ssts(
                    snapshot,
                    &[selected_upper_level_sst_id],
                    lower_level_idx + 1,
                );

                return Some(LeveledCompactionTask {
                    upper_level: Some(*upper_level_idx + 1),
                    upper_level_sst_ids: vec![selected_upper_level_sst_id],
                    lower_level: lower_level_idx + 1,
                    lower_level_sst_ids,
                    is_lower_level_bottom_level: self.options.max_levels == lower_level_idx + 1,
                });
            }
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();

        match task.upper_level {
            Some(upper_level) => {
                assert!(
                    upper_level > 0
                        && upper_level <= snapshot.levels.len().min(self.options.max_levels)
                );
                let upper_level_sst_ids = task
                    .upper_level_sst_ids
                    .iter()
                    .cloned()
                    .collect::<HashSet<_>>();
                snapshot.levels[upper_level - 1]
                    .1
                    .retain(|id| !upper_level_sst_ids.contains(id));
            }
            None => {
                // clear the current L0 sstables
                let l0_sst_ids = task
                    .upper_level_sst_ids
                    .iter()
                    .cloned()
                    .collect::<HashSet<_>>();
                snapshot.l0_sstables.retain(|id| !l0_sst_ids.contains(id));
            }
        }

        assert!(
            task.lower_level > 0
                && task.lower_level <= snapshot.levels.len().min(self.options.max_levels)
        );

        let lower_level_sst_ids = task
            .lower_level_sst_ids
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        snapshot.levels[task.lower_level - 1]
            .1
            .retain(|id| !lower_level_sst_ids.contains(id));
        snapshot.levels[task.lower_level - 1]
            .1
            .extend_from_slice(output);

        snapshot.levels[task.lower_level - 1].1.sort_by_key(|id| {
            snapshot
                .sstables
                .get(id)
                .expect("sstable should exist")
                .first_key()
        });

        (
            snapshot,
            task.upper_level_sst_ids
                .iter()
                .chain(task.lower_level_sst_ids.iter())
                .cloned()
                .collect::<Vec<_>>(),
        )
    }
}
