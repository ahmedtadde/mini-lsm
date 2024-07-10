use std::collections::HashSet;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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
        assert!(options.max_levels > 0);
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        assert!(
            self.options.max_levels > 0,
            "The maximum number of levels should be greater than 0"
        );
        assert!(snapshot.levels.len() <= self.options.max_levels,
            "The number of levels in the snapshot is greater than the maximum number of levels allowed by the compaction controller"
        );

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!(
                "compaction triggered at level 0 which has {} files and the threshold is {}",
                snapshot.l0_sstables.len(),
                self.options.level0_file_num_compaction_trigger
            );

            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot
                    .levels
                    .first()
                    .map_or(vec![], |ssts| ssts.1.to_vec()),
                is_lower_level_bottom_level: self.options.max_levels == 1,
            });
        }

        snapshot
            .levels
            .iter()
            .take(self.options.max_levels)
            .tuple_windows()
            .find_map(|(upper_lvl, lower_lvl)| {
                // let upper_lvl_size = upper_lvl
                //     .1
                //     .iter()
                //     .map(|sst| {
                //         snapshot
                //             .sstables
                //             .get(sst)
                //             .map_or(0, |v| v.table_size() as usize)
                //     })
                //     .sum::<usize>();
                // let lower_lvl_size = lower_lvl
                //     .1
                //     .iter()
                //     .map(|sst| {
                //         snapshot
                //             .sstables
                //             .get(sst)
                //             .map_or(0, |v| v.table_size() as usize)
                //     })
                //     .sum::<usize>();

                let upper_lvl_size = upper_lvl.1.len();
                let lower_lvl_size = lower_lvl.1.len();

                if upper_lvl_size == 0 {
                    return None;
                }

                if (lower_lvl_size as f64 / upper_lvl_size as f64) * 100.0
                    < self.options.size_ratio_percent as f64
                {
                    println!(
                        "compaction triggered at level {} (size={}) and level {} (size={}) where the size ratio is {} which is less than the threshold {}",
                        upper_lvl.0,
                        upper_lvl_size,
                        lower_lvl.0,
                        lower_lvl_size,
                        (lower_lvl_size as f64 / upper_lvl_size as f64) * 100.0,
                        self.options.size_ratio_percent
                    );
                    return Some(SimpleLeveledCompactionTask {
                        upper_level: Some(upper_lvl.0),
                        upper_level_sst_ids: upper_lvl.1.to_vec(),
                        lower_level: lower_lvl.0,
                        lower_level_sst_ids: lower_lvl.1.to_vec(),
                        is_lower_level_bottom_level: lower_lvl.0 == self.options.max_levels,
                    });
                }

                None
            })
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
        let mut snapshot = snapshot.clone();

        match task.upper_level {
            Some(upper_level) => {
                assert!(upper_level > 0 && upper_level <= snapshot.levels.len());
                // clear the current upper (non L0) level sstables
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
                assert!(task.lower_level == 1);
                // clear the current L0 sstables
                let l0_sst_ids = task
                    .upper_level_sst_ids
                    .iter()
                    .cloned()
                    .collect::<HashSet<_>>();
                snapshot.l0_sstables.retain(|id| !l0_sst_ids.contains(id));
            }
        }

        assert!(task.lower_level > 0 && task.lower_level <= snapshot.levels.len());
        snapshot.levels[task.lower_level - 1].1 = output.to_vec();

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
