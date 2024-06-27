#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
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
use crate::key::{KeySlice, TS_DEFAULT, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord, MANIFEST_FILE_PATH_SUFFIX};
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

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
        self.close().ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner
            .sync_dir_with_state_lock_observer(&self.inner.state_lock.lock())?;

        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
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

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner
                .sync_dir_with_state_lock_observer(&self.inner.state_lock.lock())?;
            return Ok(());
        }

        self.inner
            .force_freeze_memtable(&self.inner.state_lock.lock())?;

        loop {
            let snapshot = self.inner.state.read();

            if snapshot.imm_memtables.is_empty() {
                break;
            }

            drop(snapshot);

            self.inner.force_flush_next_imm_memtable()?;
        }

        self.inner
            .sync_dir_with_state_lock_observer(&self.inner.state_lock.lock())?;

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

        let mut state = LsmStorageState::create(&options);

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

        let block_cache = Arc::new(BlockCache::new(1 << 20)); // 4GB block cache,

        let mut latest_sst_id = state.memtable.id();
        let manifest_path = path.join(MANIFEST_FILE_PATH_SUFFIX);
        let mut memtables = BTreeSet::new();

        let manifest = if !manifest_path.exists() {
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    latest_sst_id,
                    Self::path_of_wal_static(path, latest_sst_id),
                )?)
            }
            let manifest = Manifest::create(&manifest_path)?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(latest_sst_id))?;
            manifest
        } else {
            let (manifest, records) = Manifest::recover(&manifest_path)?;
            for record in records {
                match record {
                    ManifestRecord::Flush(id) => {
                        assert!(
                            memtables.remove(&id),
                            "flushed SSTable ID must be in the memtables set"
                        );

                        //cleanup WAL file
                        if options.enable_wal {
                            let mem_path = Self::path_of_wal_static(path, id);
                            if mem_path.exists() {
                                std::fs::remove_file(mem_path)?;
                            }
                        }

                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, id);
                        } else {
                            state.levels.insert(0, (id, vec![id]));
                        }
                    }
                    ManifestRecord::Compaction(task, output) => {
                        let (new_state, old_sst_ids) =
                            compaction_controller.apply_compaction_result(&state, &task, &output);

                        // cleanup old sstables
                        for sst_id in &old_sst_ids {
                            let sst_path = Self::path_of_sst_static(path, *sst_id);
                            if sst_path.exists() {
                                std::fs::remove_file(sst_path)?;
                            }
                        }

                        state = new_state;
                        latest_sst_id =
                            latest_sst_id.max(output.iter().cloned().max().unwrap_or(0));
                    }
                    ManifestRecord::NewMemtable(id) => {
                        assert!(id >= latest_sst_id, "memtable ID must be greater than all previous SST or memtable ID in the manifest file");
                        memtables.insert(id);
                        latest_sst_id = latest_sst_id.max(id);
                    }
                }
            }

            manifest
        };

        state
            .l0_sstables
            .iter()
            .chain(state.levels.iter().flat_map(|(_, ssts)| ssts.iter()))
            .for_each(|id| {
                let sst_path = Self::path_of_sst_static(path, *id);
                let sst = Arc::new(
                    SsTable::open(
                        *id,
                        Some(block_cache.clone()),
                        FileObject::open(&sst_path).expect("Failed to open SST file"),
                    )
                    .expect("Failed to create SSTable"),
                );

                state.sstables.insert(*id, sst);
            });

        if options.enable_wal && !memtables.is_empty() {
            memtables.iter().for_each(|id| {
                let memtable = Arc::new(
                    MemTable::recover_from_wal(*id, Self::path_of_wal_static(path, *id))
                        .expect("Failed to recover memtable from WAL"),
                );
                if !memtable.is_empty() {
                    state.imm_memtables.insert(0, memtable);
                }
            });
        }

        if !state.imm_memtables.is_empty() {
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    latest_sst_id + 1,
                    Self::path_of_wal_static(path, latest_sst_id + 1),
                )?);
                latest_sst_id += 1;
            } else {
                state.memtable = Arc::new(MemTable::create(latest_sst_id + 1));
                latest_sst_id += 1;
            }

            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(latest_sst_id + 1),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(LsmMvccInner::new(TS_DEFAULT)),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        storage.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        //println!("Syncing memtable to WAL");
        let state_reader = self.state.write();
        state_reader.memtable.sync_wal()
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

        let memtables_merge_iter = {
            let mut iters = Vec::with_capacity(1 + reader.imm_memtables.len());
            iters.push(Box::new(reader.memtable.scan(
                Bound::Included(KeySlice::from_slice(key, TS_RANGE_BEGIN)),
                Bound::Included(KeySlice::from_slice(key, TS_RANGE_END)),
            )));

            for imm_memtable in reader.imm_memtables.iter() {
                iters.push(Box::new(imm_memtable.scan(
                    Bound::Included(KeySlice::from_slice(key, TS_RANGE_BEGIN)),
                    Bound::Included(KeySlice::from_slice(key, TS_RANGE_END)),
                )));
            }
            MergeIterator::create(iters)
        };

        let sstables = {
            let l0_sstables_merge_iter = MergeIterator::create(
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
                                    KeySlice::from_slice(key, TS_RANGE_BEGIN),
                                )
                                .ok()
                            }
                        })
                    })
                    .map(Box::new)
                    .collect::<Vec<Box<SsTableIterator>>>(),
            );

            let mentables_and_l0_sstables_merge_iter =
                TwoMergeIterator::create(memtables_merge_iter, l0_sstables_merge_iter)?;

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
                            KeySlice::from_slice(key, TS_RANGE_BEGIN),
                        )
                        .ok()
                    })
                    .map(Box::new)
                    .collect::<Vec<Box<SstConcatIterator>>>(),
            );

            TwoMergeIterator::create(mentables_and_l0_sstables_merge_iter, other_sstables_iter)
        }?;

        let iter = FusedIterator::new(sstables);
        if iter.is_valid() && iter.key().key_ref() == key && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        let memtable_capacity = {
            let guard = self.state.read();
            let writer = guard.as_ref().memtable.clone();
            let mvcc_handle = self.mvcc.as_ref();
            let mvcc_handle_lock = mvcc_handle.map(|mvcc_handle| mvcc_handle.write_lock.lock());
            // println!(
            //     "iambatman/write_batch:memtable_capacity:handles are present? {:?} && {:?}",
            //     mvcc_handle.is_some(),
            //     mvcc_handle_lock.is_some()
            // );
            let ts = {
                if mvcc_handle.is_none() || mvcc_handle_lock.is_none() {
                    TS_DEFAULT
                } else {
                    mvcc_handle.map_or(TS_DEFAULT, |mvcc_handle| mvcc_handle.latest_commit_ts() + 1)
                }
            };

            let mut deleted_keys = HashSet::new();
            for record in batch {
                match record {
                    WriteBatchRecord::Put(key, value) => {
                        assert!(!key.as_ref().is_empty(), "key is empty");
                        assert!(key.as_ref().len() <= 1 << 20, "key is too large");
                        assert!(!value.as_ref().is_empty(), "value is empty");
                        assert!(value.as_ref().len() <= 1 << 20, "value is too large");
                        assert!(
                            !deleted_keys.contains(key.as_ref()),
                            "cant perform put on a deleted key"
                        );
                    }
                    WriteBatchRecord::Del(key) => {
                        assert!(!key.as_ref().is_empty(), "key is empty");
                        assert!(key.as_ref().len() <= 1 << 20, "key is too large");
                        deleted_keys.insert(key.as_ref());
                    }
                }
            }

            for record in batch {
                match record {
                    WriteBatchRecord::Put(key, value) => {
                        // println!(
                        //     "iambatman/write_batch:memtable_capacity:putting key: ({:?}, {:?})",
                        //     key.as_ref(),
                        //     ts
                        // );
                        writer.put(KeySlice::from_slice(key.as_ref(), ts), value.as_ref())?;
                    }
                    WriteBatchRecord::Del(key) => {
                        // println!(
                        //     "iambatman/write_batch:memtable_capacity:deleting key: ({:?}, {:?})",
                        //     key.as_ref(),
                        //     ts
                        // );
                        writer.put(KeySlice::from_slice(key.as_ref(), ts), &[])?;
                    }
                }
            }

            if let Some(handle) = mvcc_handle {
                if mvcc_handle_lock.is_some() {
                    handle.update_commit_ts(ts);
                    // println!(
                    //     "iambatman/write_batch:memtable_capacity:updated commit ts to {:?}",
                    //     ts
                    // );
                }
            }

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

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
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
        let state_lock_observer = self.state_lock.lock();
        self.sync_dir_with_state_lock_observer(&state_lock_observer)
    }

    pub(crate) fn sync_dir_with_state_lock_observer(
        &self,
        _state_lock_observer: &MutexGuard<'_, ()>,
    ) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        if self.state.read().memtable.is_empty() {
            return Ok(());
        }

        let new_memtable = {
            let id = self.next_sst_id();
            if self.options.enable_wal {
                Arc::new(MemTable::create_with_wal(id, self.path_of_wal(id))?)
            } else {
                Arc::new(MemTable::create(id))
            }
        };

        let new_memtable_id = new_memtable.id();

        {
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();
            let old_memtable = std::mem::replace(&mut state.memtable, new_memtable);
            state.imm_memtables.insert(0, old_memtable.clone());
            *guard = Arc::new(state);
            drop(guard);
            old_memtable.sync_wal()?;
        }

        {
            if let Some(Err(e)) = self.manifest.as_ref().map(|manifest| {
                manifest.add_record(
                    state_lock_observer,
                    ManifestRecord::NewMemtable(new_memtable_id),
                )?;
                self.sync_dir_with_state_lock_observer(state_lock_observer)
            }) {
                // clean up the wal file if the manifest record fails
                let _ = fs::remove_file(self.path_of_wal(new_memtable_id));
                return Err(e);
            }
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
            {
                if self.compaction_controller.flush_to_l0() {
                    state.l0_sstables.insert(0, imm_memtable.id());
                } else {
                    state
                        .levels
                        .insert(0, (imm_memtable.id(), vec![imm_memtable.id()]));
                }
            }
            state.imm_memtables.pop();

            *guard = Arc::new(state);
        }

        {
            if let Some(Err(e)) = self.manifest.as_ref().map(|manifest| {
                let state_lock_observer = self.state_lock.lock();
                manifest.add_record(
                    &state_lock_observer,
                    ManifestRecord::Flush(imm_memtable.id()),
                )?;
                self.sync_dir_with_state_lock_observer(&state_lock_observer)
            }) {
                // clean up the sst file if the manifest record fails
                let _ = fs::remove_file(self.path_of_sst(imm_memtable.id()));
                return Err(e);
            }

            if self.options.enable_wal {
                fs::remove_file(self.path_of_wal(imm_memtable.id())).unwrap_or_else(|_| {
                    panic!(
                        "failed to remove wal for flushed imm_memtable (id={})",
                        imm_memtable.id()
                    )
                });
            }
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
        iters.push(Box::new(reader.memtable.scan(
            lower.map(|k| KeySlice::from_slice(k, TS_RANGE_BEGIN)),
            upper.map(|k| KeySlice::from_slice(k, TS_RANGE_END)),
        )));
        for imm_memtable in reader.imm_memtables.iter() {
            iters.push(Box::new(imm_memtable.scan(
                lower.map(|k| KeySlice::from_slice(k, TS_RANGE_BEGIN)),
                upper.map(|k| KeySlice::from_slice(k, TS_RANGE_END)),
            )));
        }

        // println!(
        //     "iambatman/scan:memtables_iters: {:?} (imm_memtables count is {:?})...also memtable size is {:?} , number of active iterators are {:?} and first iter is valid {:?}",
        //     iters.len(),
        //     reader.imm_memtables.len(),
        //     reader.memtable.approximate_size(),
        //     iters.iter().map(|iter| iter.num_active_iterators()).collect::<Vec<_>>(),
        //     iters.first().unwrap().is_valid()
        // );
        let memtables_merge_iter = MergeIterator::create(iters);
        // println!(
        //     "iambatman/scan:memtables_merge_iter: number of active iterators are {:?}",
        //     memtables_merge_iter.num_active_iterators()
        // );

        let l0_sstables = reader
            .l0_sstables
            .iter()
            .filter_map(|id| {
                let sst = reader.sstables.get(id).unwrap();
                let skip = match (lower, upper) {
                    (Bound::Included(lower), _) => sst.last_key().key_ref() < lower,
                    (Bound::Excluded(lower), _) => sst.last_key().key_ref() <= lower,
                    (_, Bound::Included(upper)) => sst.first_key().key_ref() > upper,
                    (_, Bound::Excluded(upper)) => sst.first_key().key_ref() >= upper,
                    _ => false,
                };

                if skip {
                    return None;
                }

                match lower {
                    Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                        sst.clone(),
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )
                    .ok(),
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            sst.clone(),
                            KeySlice::from_slice(key, TS_RANGE_END),
                        )
                        .unwrap();

                        if iter.is_valid() && iter.key().key_ref() == key {
                            iter.next().unwrap();
                        };

                        Some(iter)
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst.clone()).ok(),
                }
            })
            .map(Box::new)
            .collect::<Vec<Box<SsTableIterator>>>();

        // println!("iambatman/scan:l0_sstables: {:?}", l0_sstables.len());

        let l0_sstables_merge_iter = MergeIterator::create(l0_sstables);
        // println!(
        //     "iambatman/scan:l0_sst merge_iter: number of active iterators are {:?}",
        //     l0_sstables_merge_iter.num_active_iterators()
        // );

        let mentables_and_l0_sstables_merge_iter =
            TwoMergeIterator::create(memtables_merge_iter, l0_sstables_merge_iter)?;

        // println!(
        //     "iambatman/scan:mentables_and_l0_sstables_merge_iter: number of active iterators are {:?}",
        //     mentables_and_l0_sstables_merge_iter.num_active_iterators()
        // );

        let other_sstables = reader
            .levels
            .iter()
            .filter_map(|(_, sstables)| {
                let parsed_sstables = sstables
                    .iter()
                    .filter_map(|id| reader.sstables.get(id))
                    .filter_map(|sst| {
                        let skip = match (lower, upper) {
                            (Bound::Included(lower), _) => sst.last_key().key_ref() < lower,
                            (Bound::Excluded(lower), _) => sst.last_key().key_ref() <= lower,
                            (_, Bound::Included(upper)) => sst.first_key().key_ref() > upper,
                            (_, Bound::Excluded(upper)) => sst.first_key().key_ref() >= upper,
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
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )
                    .ok(),
                    (Bound::Excluded(key), _) => {
                        let iter = SstConcatIterator::create_and_seek_to_key(
                            parsed_sstables,
                            KeySlice::from_slice(key, TS_RANGE_END),
                        );

                        if iter.is_err() {
                            return None;
                        }

                        let mut iter = iter.unwrap();

                        if iter.is_valid() && iter.key().key_ref() == key {
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
        // println!(
        //     "iambatman/scan:other_sstables count {:?} and valid count {:?}",
        //     other_sstables.len(),
        //     other_sstables.iter().filter(|iter| iter.is_valid()).count()
        // );
        let other_tables_merge_iter = MergeIterator::create(other_sstables);
        // println!(
        //     "iambatman/scan:other_tables_merge_iter: number of active iterators are {:?}",
        //     other_tables_merge_iter.num_active_iterators()
        // );

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
