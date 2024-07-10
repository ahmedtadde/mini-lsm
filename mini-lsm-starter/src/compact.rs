#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
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
use crate::lsm_iterator::FusedIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize, Clone)]
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
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
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
            CompactionTask::Simple(task) => self.simple_leveled_compaction(task),
            CompactionTask::Leveled(task) => self.leveled_compaction(task),
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => self.full_compaction(l0_sstables, l1_sstables, true),
            CompactionTask::Tiered(task) => self.tiered_compaction(task),
        }
    }

    fn ssts_from_compact_iter(
        &self,
        mut sstable_iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    ) -> Result<Vec<Arc<SsTable>>> {
        assert!(
            self.options.target_sst_size > 0,
            "target_sst_size must be greater than 0"
        );
        assert!(
            self.options.block_size > 0,
            "block_size must be greater than 0"
        );
        assert!(
            self.options.target_sst_size >= self.options.block_size,
            "target_sst_size must be greater than or equal to block_size"
        );
        // println!("iambatman/ssts_from_compact_iter: invoked");
        let mut new_sstables = Vec::new();
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);

        while sstable_iter.is_valid() {
            let key = sstable_iter.key();
            // println!(
            //     "iambatman/ssts_from_compact_iter: iter key: {:?}, {:?}",
            //     key.key_ref(),
            //     key.ts()
            // );

            let value = sstable_iter.value();

            let last_inserted_key = sst_builder.last_inserted_key();
            // println!(
            //     "iambatman/ssts_from_compact_iter: last inserted key: {:?}, {:?}",
            //     last_inserted_key.key_ref(),
            //     last_inserted_key.ts()
            // );

            // println!(
            //     "iambatman/ssts_from_compact_iter: estimated size {} vs target size {}",
            //     sst_builder.estimated_size(),
            //     self.options.target_sst_size
            // );

            if sst_builder.estimated_size() >= self.options.target_sst_size
                && key.key_ref() != last_inserted_key.key_ref()
            {
                // println!("iambatman/ssts_from_compact_iter: provisoning new sstable with size",);
                let builder = std::mem::replace(
                    &mut sst_builder,
                    SsTableBuilder::new(self.options.block_size),
                );
                let sstable_id = self.next_sst_id();
                let sstable = builder.build(
                    sstable_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sstable_id),
                )?;

                new_sstables.push(Arc::new(sstable));
            }

            sst_builder.add(key, value);
            sstable_iter.next()?;
        }

        if !sst_builder.is_empty() {
            let sstable_id = self.next_sst_id();
            let sstable = sst_builder.build(
                sstable_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sstable_id),
            )?;
            new_sstables.push(Arc::new(sstable));
        }

        // println!(
        //     "iambatman/ssts_from_compact_iter: returning new sstables {:?}",
        //     new_sstables.len()
        // );
        Ok(new_sstables)
    }

    fn tiered_compaction(&self, task: &TieredCompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let sstables = {
            let reader = self.state.read();
            MergeIterator::create(
                task.tiers
                    .iter()
                    .filter_map(|(_, sst_ids)| {
                        SstConcatIterator::create_and_seek_to_first(
                            sst_ids
                                .iter()
                                .filter_map(|id| reader.sstables.get(id))
                                .cloned()
                                .collect(),
                        )
                        .ok()
                    })
                    .map(Box::new)
                    .collect::<Vec<Box<SstConcatIterator>>>(),
            )
        };

        self.ssts_from_compact_iter(FusedIterator::new(sstables))
    }

    fn simple_leveled_compaction(
        &self,
        task: &SimpleLeveledCompactionTask,
    ) -> Result<Vec<Arc<SsTable>>> {
        if task.upper_level.is_none() {
            assert!(task.lower_level == 1);
            return self.full_compaction(
                task.upper_level_sst_ids.as_slice(),
                task.lower_level_sst_ids.as_slice(),
                task.is_lower_level_bottom_level,
            );
        }

        let sstables = {
            let reader = self.state.read();
            let upper_level = task.upper_level.expect("upper level should be present");
            assert!(upper_level <= reader.levels.len());

            let upper_level_iter = SstConcatIterator::create_and_seek_to_first(
                task.upper_level_sst_ids
                    .iter()
                    .flat_map(|id| reader.sstables.get(id))
                    .cloned()
                    .collect(),
            )?;

            let lower_level_iter = SstConcatIterator::create_and_seek_to_first(
                task.lower_level_sst_ids
                    .iter()
                    .flat_map(|id| reader.sstables.get(id))
                    .cloned()
                    .collect(),
            )?;

            TwoMergeIterator::create(upper_level_iter, lower_level_iter)?
        };

        self.ssts_from_compact_iter(FusedIterator::new(sstables))
    }

    fn leveled_compaction(&self, task: &LeveledCompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task.upper_level {
            Some(_) => {
                let reader = self.state.read();
                let upper_level_iter = SstConcatIterator::create_and_seek_to_first(
                    task.upper_level_sst_ids
                        .iter()
                        .flat_map(|id| reader.sstables.get(id))
                        .cloned()
                        .collect(),
                )?;

                let lower_level_iter = SstConcatIterator::create_and_seek_to_first(
                    task.lower_level_sst_ids
                        .iter()
                        .flat_map(|id| reader.sstables.get(id))
                        .cloned()
                        .collect(),
                )?;

                drop(reader);

                self.ssts_from_compact_iter(FusedIterator::new(TwoMergeIterator::create(
                    upper_level_iter,
                    lower_level_iter,
                )?))
            }
            None => {
                let reader = self.state.read();
                let upper_level_iter = MergeIterator::create(
                    task.upper_level_sst_ids
                        .iter()
                        .flat_map(|id| reader.sstables.get(id))
                        .flat_map(|s| SsTableIterator::create_and_seek_to_first(s.clone()).ok())
                        .map(Box::new)
                        .collect(),
                );

                let lower_level_iter = SstConcatIterator::create_and_seek_to_first(
                    task.lower_level_sst_ids
                        .iter()
                        .flat_map(|id| reader.sstables.get(id))
                        .cloned()
                        .collect(),
                )?;

                drop(reader);

                self.ssts_from_compact_iter(FusedIterator::new(TwoMergeIterator::create(
                    upper_level_iter,
                    lower_level_iter,
                )?))
            }
        }
    }

    fn full_compaction(
        &self,
        l0_sstables: &[usize],
        l1_sstables: &[usize],
        _is_lower_level_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let sstables = {
            let reader = {
                let guard = self.state.read();
                Arc::clone(&guard)
            };

            let l0_sstables_iter = l0_sstables
                .iter()
                .filter_map(|id| reader.sstables.get(id))
                .filter_map(|sst| SsTableIterator::create_and_seek_to_first(sst.clone()).ok())
                .map(Box::new)
                .collect::<Vec<Box<SsTableIterator>>>();

            let l1_sstables_iter = SstConcatIterator::create_and_seek_to_first(
                l1_sstables
                    .iter()
                    .flat_map(|id| reader.sstables.get(id))
                    .cloned()
                    .collect(),
            )?;

            TwoMergeIterator::create(MergeIterator::create(l0_sstables_iter), l1_sstables_iter)
        }?;

        self.ssts_from_compact_iter(FusedIterator::new(sstables))
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let reader = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let l0_sstables = reader.l0_sstables.clone();
        let l1_sstables = reader.levels.first().map_or(vec![], |ssts| ssts.1.to_vec());

        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };

        drop(reader);

        let sstables_to_cleanup_from_fs = {
            let new_sstables = self.compact(&task)?;
            let state_lock = self.state_lock.lock();
            if let Some(manifest) = self.manifest.as_ref() {
                manifest.add_record(
                    &state_lock,
                    ManifestRecord::Compaction(
                        task.clone(),
                        new_sstables.iter().map(|s| s.sst_id()).collect(),
                    ),
                )?;

                // self.sync_dir_with_state_lock_observer(&state_lock)?;
                self.sync_dir()?;
            }
            let mut guard = self.state.write();
            let mut writer = guard.as_ref().clone();

            writer
                .l0_sstables
                .truncate(writer.l0_sstables.len() - l0_sstables.len());

            let old_sst_ids = l0_sstables
                .iter()
                .chain(l1_sstables.iter())
                .cloned()
                .collect::<HashSet<_>>();
            writer.sstables.retain(|id, _| !old_sst_ids.contains(id));

            match writer.levels.first_mut() {
                Some((_, level)) => {
                    level.clear();
                    level.extend(new_sstables.iter().map(|s| s.sst_id()));
                    level.sort_unstable();
                    level.reverse();
                }
                None => {
                    let mut sstables = new_sstables.iter().map(|s| s.sst_id()).collect::<Vec<_>>();
                    sstables.sort_unstable();
                    sstables.reverse();
                    writer.levels.push((1, sstables));
                }
            }

            writer
                .sstables
                .extend(new_sstables.into_iter().map(|s| (s.sst_id(), s)));

            *guard = Arc::new(writer);

            old_sst_ids
        };

        for sst in sstables_to_cleanup_from_fs {
            std::fs::remove_file(self.path_of_sst(sst))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let reader = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        if let Some(task) = self.compaction_controller.generate_compaction_task(&reader) {
            drop(reader);
            let new_sstables = self.compact(&task)?;

            let state_lock = self.state_lock.lock();

            if let Some(manifest) = self.manifest.as_ref() {
                manifest.add_record(
                    &state_lock,
                    ManifestRecord::Compaction(
                        task.clone(),
                        new_sstables.iter().map(|s| s.sst_id()).collect(),
                    ),
                )?;

                // self.sync_dir_with_state_lock_observer(&state_lock)?;
                self.sync_dir()?;
            }

            let mut writer = self.state.write();
            let mut snapshot = writer.as_ref().clone();
            for sst in new_sstables.iter() {
                snapshot.sstables.insert(sst.sst_id(), sst.clone());
            }

            let (mut snapshot, old_sst_ids) = self.compaction_controller.apply_compaction_result(
                &snapshot,
                &task,
                &new_sstables.iter().map(|s| s.sst_id()).collect::<Vec<_>>(),
                false,
            );

            {
                for sst_id in &old_sst_ids {
                    snapshot.sstables.remove(sst_id);
                }

                *writer = Arc::new(snapshot);
            }

            drop(writer);

            for sst_id in old_sst_ids {
                let path = self.path_of_sst(sst_id);
                if path.exists() {
                    std::fs::remove_file(path)?;
                }
            }
        };

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
        let memtables_count = {
            let guard = self.state.read();
            let state = guard.as_ref();
            state.imm_memtables.len()
        };

        if memtables_count >= self.options.num_memtable_limit {
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
