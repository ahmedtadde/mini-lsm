#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
};

use super::CommittedTxnData;

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        assert!(
            !self.committed.load(Ordering::SeqCst),
            "Transaction already committed"
        );
        let value = {
            if let Some(entry) = self.local_storage.get(&Bytes::copy_from_slice(key)) {
                if entry.value().is_empty() {
                    return Ok(None);
                }

                return Ok(Some(entry.value().clone()));
            }
            if let Some(value) = self.inner.get_with_ts(key, self.read_ts)? {
                value
            } else {
                return Ok(None);
            }
        };

        if let Some(key_hashes) = self.key_hashes.as_ref() {
            key_hashes.lock().1.insert(farmhash::fingerprint32(key));
        }

        if value.is_empty() {
            return Ok(None);
        }

        Ok(Some(value))
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        assert!(
            !self.committed.load(Ordering::SeqCst),
            "Transaction already committed"
        );
        TxnIterator::create(
            self.clone(),
            self.inner.scan_with_ts(lower, upper, self.read_ts)?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        assert!(
            !self.committed.load(Ordering::SeqCst),
            "Transaction already committed"
        );
        assert!(
            !key.is_empty(),
            "Attempting a PUT operation with an empty key (key len = 0)"
        );
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));

        if let Some(key_hashes) = self.key_hashes.as_ref() {
            key_hashes.lock().0.insert(farmhash::fingerprint32(key));
        }
    }

    pub fn delete(&self, key: &[u8]) {
        self.put(key, &[]);
    }

    pub fn commit(&self) -> Result<()> {
        //take commit lock
        let _lock = self.inner.mvcc().commit_lock.lock();
        self.committed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .expect("Attempting to commit a transaction that has already been committed");

        // check for serializable conflicts
        let serializable = {
            if let Some(key_hashes) = self.key_hashes.as_ref() {
                let key_hashes_lock = key_hashes.lock();
                let read_set = &key_hashes_lock.1;
                let write_set = &key_hashes_lock.0;

                if !write_set.is_empty() {
                    let committed_txns = self.inner.mvcc().committed_txns.lock();
                    for committed_txn in committed_txns.range(self.read_ts + 1..) {
                        let committed_txn_write_set = &committed_txn.1.key_hashes;
                        if read_set
                            .intersection(committed_txn_write_set)
                            .next()
                            .is_some()
                        {
                            return Err(anyhow::anyhow!("Serializable conflict detected"));
                        };
                    }
                }

                true
            } else {
                false
            }
        };

        // write to storage
        let entries = self
            .local_storage
            .iter()
            .map(|entry| {
                if entry.value().is_empty() {
                    WriteBatchRecord::Del(entry.key().clone())
                } else {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                }
            })
            .collect::<Vec<_>>();

        let commit_ts = self.inner.write_batch_inner(&entries)?;

        // sanity check
        assert_eq!(
            commit_ts,
            self.inner.mvcc().latest_commit_ts(),
            "txn commit_ts mismatch"
        );

        //log the latest commited txn
        if serializable {
            if let Some(key_hashes) = self.key_hashes.as_ref() {
                let key_hashes_lock = key_hashes.lock();
                let write_set = &key_hashes_lock.0;

                let mut committed_txns = self.inner.mvcc().committed_txns.lock();
                //check if committed_txns already has an entry with `commit_ts`
                if let Some(committed_txn) = committed_txns.get(&commit_ts) {
                    //this is bad
                    panic!("Trying to mark a transaction as committed after it has already been committed");
                }

                committed_txns.insert(
                    commit_ts,
                    CommittedTxnData {
                        key_hashes: write_set.clone(),
                        read_ts: self.read_ts,
                        commit_ts,
                    },
                );

                let watermark = self.inner.mvcc().watermark();
                committed_txns.retain(|k, _| *k >= watermark);
            }
        }

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts)
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> &[u8] {
        let key = &self.borrow_item().0;
        &key[..]
    }

    fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|fields| {
            if let Some(entry) = fields.iter.next() {
                *fields.item = (entry.key().clone(), entry.value().clone());
            } else {
                *fields.item = (Bytes::from_static(&[]), Bytes::from_static(&[]));
            }

            Ok(())
        })
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(txn: Arc<Transaction>, iter: FusedIterator<LsmIterator>) -> Result<Self> {
        let mut local_iter = TxnLocalIteratorBuilder {
            map: txn.local_storage.clone(),
            iter_builder: |map| map.range((Bound::Unbounded, Bound::Unbounded)),
            item: (Bytes::from_static(&[]), Bytes::from_static(&[])),
        }
        .build();

        local_iter.with_mut(|fields| {
            if let Some(entry) = fields.iter.next() {
                *fields.item = (entry.key().clone(), entry.value().clone());
            }
        });

        let mut iter = Self {
            txn,
            iter: TwoMergeIterator::create(local_iter, iter)?,
        };

        while iter.is_valid() && iter.value().is_empty() {
            iter.next()?;
        }

        if iter.is_valid() {
            if let Some(key_hashes) = iter.txn.key_hashes.as_ref() {
                key_hashes
                    .lock()
                    .1
                    .insert(farmhash::fingerprint32(iter.key()));
            }
        }
        Ok(iter)
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        while self.is_valid() && self.value().is_empty() {
            self.iter.next()?;
        }

        if self.is_valid() {
            if let Some(key_hashes) = self.txn.key_hashes.as_ref() {
                key_hashes
                    .lock()
                    .1
                    .insert(farmhash::fingerprint32(self.key()));
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
