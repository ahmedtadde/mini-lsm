#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{anyhow, Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.compaction_notifier.send(())?;
        self.flush_notifier.send(())?;
        if let Some(handle) = self.compaction_thread.lock().take() {
            handle.join().map_err(|e| {
                anyhow!(
                    "there was an error shutting down compaction thread: {:?}",
                    e
                )
            })?;
        }
        if let Some(handle) = self.flush_thread.lock().take() {
            handle
                .join()
                .map_err(|e| anyhow!("there was an error shutting down flush thread: {:?}", e))?;
        }
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if path.is_file() {
            return Err(anyhow!("{:?} is not a directory", path));
        }

        fs::create_dir_all(path)?;
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let reader = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        {
            if let Some(value) = reader.memtable.get(key) {
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }

            for imm_memtable in reader.imm_memtables.iter() {
                if let Some(value) = imm_memtable.get(key) {
                    if value.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(value));
                }
            }
        }

        {
            let sstables = {
                let l0_sstables_iter = MergeIterator::create(
                    reader
                        .l0_sstables
                        .iter()
                        .filter_map(|id| {
                            let sst = reader.sstables.get(id).unwrap();
                            sst.bloom.as_ref().and_then(|bloom| {
                                if !bloom.may_contain(farmhash::fingerprint32(key)) {
                                    None
                                } else {
                                    SsTableIterator::create_and_seek_to_key(
                                        sst.clone(),
                                        KeySlice::from_slice(key),
                                    )
                                    .ok()
                                }
                            })
                        })
                        .map(Box::new)
                        .collect::<Vec<Box<SsTableIterator>>>(),
                );

                let other_sstables_iter = MergeIterator::create(
                    reader
                        .levels
                        .iter()
                        .filter_map(|(_, sstables)| {
                            let parsed_sstables = sstables
                                .iter()
                                .filter_map(|id| reader.sstables.get(id))
                                .filter_map(|sst| {
                                    sst.bloom.as_ref().and_then(|bloom| {
                                        if !bloom.may_contain(farmhash::fingerprint32(key)) {
                                            None
                                        } else {
                                            Some(sst.clone())
                                        }
                                    })
                                });

                            SstConcatIterator::create_and_seek_to_key(
                                parsed_sstables.collect(),
                                KeySlice::from_slice(key),
                            )
                            .ok()
                        })
                        .map(Box::new)
                        .collect::<Vec<Box<SstConcatIterator>>>(),
                );

                TwoMergeIterator::create(l0_sstables_iter, other_sstables_iter)
            }?;

            let iter = FusedIterator::new(sstables);
            if iter.is_valid() && iter.key().raw_ref() == key && !iter.value().is_empty() {
                return Ok(Some(Bytes::copy_from_slice(iter.value())));
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "key is empty");
        assert!(key.len() <= 1 << 20, "key is too large");
        assert!(!value.is_empty(), "value is empty");
        assert!(value.len() <= 1 << 20, "value is too large");

        let memtable_capacity = {
            let guard = self.state.read();
            let writer = guard.as_ref().memtable.clone();

            // write the key-value pair to the memtable
            writer.put(key, value)?;
            writer.approximate_size()
        };

        if memtable_capacity >= self.options.target_sst_size {
            let state_lock_observer = self.state_lock.lock();
            if memtable_capacity >= self.options.target_sst_size {
                self.force_freeze_memtable(&state_lock_observer)?;
            }
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let guard = self.state.read();
        let writer = guard.as_ref().memtable.clone();
        writer.put(key, &[])?;
        if writer.approximate_size() >= self.options.target_sst_size {
            let state_lock_observer = self.state_lock.lock();
            if writer.approximate_size() >= self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&state_lock_observer)?;
            }
        }

        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_memtable = {
            let id = self.next_sst_id();
            Arc::new(MemTable::create(id))
        };

        {
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();
            let old_memtable = std::mem::replace(&mut state.memtable, new_memtable);
            state.imm_memtables.insert(0, old_memtable);
            *guard = Arc::new(state);
        }

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let imm_memtable = {
            let guard = self.state.read();
            let state = guard.as_ref().clone();
            if state.imm_memtables.is_empty() {
                None
            } else {
                state.imm_memtables.last().cloned()
            }
        };

        if imm_memtable.is_none() {
            return Ok(());
        }

        let imm_memtable = imm_memtable.unwrap();
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        imm_memtable.flush(&mut sst_builder)?;

        let sst = sst_builder.build(
            imm_memtable.id(),
            Some(self.block_cache.clone()),
            self.path_of_sst(imm_memtable.id()),
        )?;

        {
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();
            state.sstables.insert(imm_memtable.id(), Arc::new(sst));
            state.l0_sstables.insert(0, imm_memtable.id());
            state.imm_memtables.pop();
            *guard = Arc::new(state);
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let reader = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let mut iters = Vec::with_capacity(1 + reader.imm_memtables.len());
        iters.push(Box::new(reader.memtable.scan(lower, upper)));
        for imm_memtable in reader.imm_memtables.iter() {
            iters.push(Box::new(imm_memtable.scan(lower, upper)));
        }
        let memtables_merge_iter = MergeIterator::create(iters);

        let l0_sstables = reader
            .l0_sstables
            .iter()
            .filter_map(|id| {
                let sst = reader.sstables.get(id).unwrap();
                let skip = match (lower, upper) {
                    (Bound::Included(lower), _) => sst.last_key().raw_ref() < lower,
                    (Bound::Excluded(lower), _) => sst.last_key().raw_ref() <= lower,
                    (_, Bound::Included(upper)) => sst.first_key().raw_ref() > upper,
                    (_, Bound::Excluded(upper)) => sst.first_key().raw_ref() >= upper,
                    _ => false,
                };

                if skip {
                    return None;
                }

                match lower {
                    Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                        sst.clone(),
                        KeySlice::from_slice(key),
                    )
                    .ok(),
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            sst.clone(),
                            KeySlice::from_slice(key),
                        )
                        .unwrap();

                        if iter.is_valid() && iter.key() == KeySlice::from_slice(key) {
                            iter.next().unwrap();
                        };

                        Some(iter)
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst.clone()).ok(),
                }
            })
            .map(Box::new)
            .collect::<Vec<Box<SsTableIterator>>>();

        let l0_sstables_merge_iter = MergeIterator::create(l0_sstables);

        let mentables_and_l0_sstables_merge_iter =
            TwoMergeIterator::create(memtables_merge_iter, l0_sstables_merge_iter)?;

        let other_sstables = reader
            .levels
            .iter()
            .filter_map(|(_, sstables)| {
                let parsed_sstables = sstables
                    .iter()
                    .filter_map(|id| reader.sstables.get(id))
                    .filter_map(|sst| {
                        let skip = match (lower, upper) {
                            (Bound::Included(lower), _) => sst.last_key().raw_ref() < lower,
                            (Bound::Excluded(lower), _) => sst.last_key().raw_ref() <= lower,
                            (_, Bound::Included(upper)) => sst.first_key().raw_ref() > upper,
                            (_, Bound::Excluded(upper)) => sst.first_key().raw_ref() >= upper,
                            _ => false,
                        };

                        if skip {
                            return None;
                        }

                        Some(sst.clone())
                    })
                    .collect::<Vec<Arc<SsTable>>>();

                match (lower, upper) {
                    (Bound::Included(key), _) => SstConcatIterator::create_and_seek_to_key(
                        parsed_sstables,
                        KeySlice::from_slice(key),
                    )
                    .ok(),
                    (Bound::Excluded(key), _) => {
                        let iter = SstConcatIterator::create_and_seek_to_key(
                            parsed_sstables,
                            KeySlice::from_slice(key),
                        );

                        if iter.is_err() {
                            return None;
                        }

                        let mut iter = iter.unwrap();

                        if iter.is_valid() && iter.key() == KeySlice::from_slice(key) {
                            _ = iter.next();
                        };
                        Some(iter)
                    }
                    (Bound::Unbounded, _) => {
                        SstConcatIterator::create_and_seek_to_first(parsed_sstables).ok()
                    }
                }
            })
            .map(Box::new)
            .collect::<Vec<Box<SstConcatIterator>>>();

        let other_tables_merge_iter = MergeIterator::create(other_sstables);

        let merge_iter = TwoMergeIterator::create(
            mentables_and_l0_sstables_merge_iter,
            other_tables_merge_iter,
        )?;

        let lsm_iter = match upper {
            Bound::Included(key) => LsmIterator::with_upper_bound(
                merge_iter,
                Bound::Included(Bytes::copy_from_slice(key)),
            )?,
            Bound::Excluded(key) => LsmIterator::with_upper_bound(
                merge_iter,
                Bound::Excluded(Bytes::copy_from_slice(key)),
            )?,
            Bound::Unbounded => LsmIterator::new(merge_iter)?,
        };
        Ok(FusedIterator::new(lsm_iter))
    }
}
