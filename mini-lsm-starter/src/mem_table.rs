#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(key) => Bound::Included(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(key.key_ref()),
            key.ts(),
        )),
        Bound::Excluded(x) => Bound::Excluded(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        MemTable {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        //println!("Creating MemTable with id {} and wal file", id,);
        let wal = Wal::create(path)?;
        Ok(MemTable {
            map: Arc::new(SkipMap::new()),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        // println!("Recovering MemTable {} from wal", id);
        let map: Arc<SkipMap<KeyBytes, Bytes>> = Arc::new(SkipMap::new());
        let wal = Wal::recover(path, &map)?;
        Ok(MemTable {
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(
                map.iter()
                    .map(|entry| entry.key().raw_len() + entry.value().len())
                    .sum(),
            )),
            map,
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::from_slice(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(
            lower.map(|x| KeySlice::from_slice(x, TS_DEFAULT)),
            upper.map(|x| KeySlice::from_slice(x, TS_DEFAULT)),
        )
    }

    /// Get a value by key.
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        let key = KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(key.key_ref()), key.ts());
        self.map.get(&key).map(|v| v.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let insert_key =
            KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(key.key_ref()), key.ts());
        let insert_value = Bytes::copy_from_slice(value);
        self.map.insert(insert_key, insert_value);

        if let Some(ref wal) = self.wal {
            wal.put(&key, value)?;
            wal.sync()?;
        }

        self.approximate_size.fetch_add(
            key.raw_len() + value.len(),
            std::sync::atomic::Ordering::Relaxed,
        );
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
            //println!("Memtable::sync_wal sync successful");
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (KeyBytes::new(), Bytes::new()),
        }
        .build();

        iter.with_mut(|fields| {
            if let Some(entry) = fields.iter.next() {
                *fields.item = (entry.key().clone(), entry.value().clone());
            } else {
                *fields.item = (KeyBytes::new(), Bytes::from_static(&[]));
            }
        });

        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        self.map.iter().for_each(|entry| {
            builder.add(
                KeySlice::from_slice(entry.key().key_ref(), entry.key().ts()),
                entry.value(),
            );
        });

        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
///
/// immutably borrowed field: a field which is immutably borrowed by at least one other field.
/// mutably borrowed field: a field which is mutably borrowed by exactly one other field.
/// self-referencing field: a field which borrows at least one other field.
/// head field: a field which does not borrow any other fields, I.E. not self-referencing. This does not include fields with empty borrows annotations (#[borrows()].)
/// tail field: a field which is not borrowed by any other fields.

#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    /// this is a self-referencing field and a tail field.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> KeySlice {
        let key = &self.borrow_item().0;
        KeySlice::from_slice(key.key_ref(), key.ts())
    }

    fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|fields| {
            if let Some(entry) = fields.iter.next() {
                *fields.item = (entry.key().clone(), entry.value().clone());
            } else {
                *fields.item = (KeyBytes::new(), Bytes::from_static(&[]));
            }

            Ok(())
        })
    }
}
