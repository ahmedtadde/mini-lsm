#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;

use anyhow::{bail, Result};
use bytes::Bytes;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    prev_key: Option<Vec<u8>>,
    read_ts: u64,
    upper_bound: Option<Bound<Bytes>>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, read_ts: u64) -> Result<Self> {
        let mut iterator = Self {
            inner: iter,
            upper_bound: None,
            prev_key: None,
            read_ts,
        };

        while iterator.is_valid() && iterator.inner.key().ts() > read_ts {
            iterator.inner.next()?;
        }

        while iterator.is_valid() && iterator.value().is_empty() {
            let deleted_key = iterator.key().to_vec();
            while iterator.is_valid()
                && (iterator.inner.key().ts() > iterator.read_ts || iterator.key() == deleted_key)
            {
                iterator.inner.next()?;
            }
        }

        Ok(iterator)
    }

    pub(crate) fn with_upper_bound(
        iter: LsmIteratorInner,
        upper_bound: Bound<Bytes>,
        read_ts: u64,
    ) -> Result<Self> {
        let mut iterator = Self {
            inner: iter,
            upper_bound: Some(upper_bound),
            prev_key: None,
            read_ts,
        };
        while iterator.is_valid() && iterator.inner.key().ts() > read_ts {
            iterator.inner.next()?;
        }

        while iterator.is_valid() && iterator.value().is_empty() {
            let deleted_key = iterator.key().to_vec();
            while iterator.is_valid()
                && (iterator.inner.key().ts() > iterator.read_ts || iterator.key() == deleted_key)
            {
                iterator.inner.next()?;
            }
        }

        Ok(iterator)
    }

    pub(crate) fn upper_bound(&self) -> Option<&Bound<Bytes>> {
        self.upper_bound.as_ref()
    }

    fn is_before_upper_bound(&self) -> bool {
        self.upper_bound.as_ref().map_or(true, |bound| match bound {
            Bound::Included(key) => self.inner.key().key_ref() <= key.as_ref(),
            Bound::Excluded(key) => self.inner.key().key_ref() < key.as_ref(),
            Bound::Unbounded => true,
        })
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid() && !self.key().is_empty() && self.is_before_upper_bound()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }

    fn next(&mut self) -> Result<()> {
        self.prev_key = Some(self.key().to_vec());
        self.inner.next()?;

        while self.is_valid()
            && (self.inner.key().ts() > self.read_ts
                || self.key() == self.prev_key.as_deref().unwrap_or(&[]))
        {
            self.inner.next()?;
        }

        while self.is_valid() && self.value().is_empty() {
            let deleted_key = self.key().to_vec();
            while self.is_valid()
                && (self.inner.key().ts() > self.read_ts || self.key() == deleted_key)
            {
                self.inner.next()?;
            }
        }

        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("Iterator has already errored");
        }

        if !self.is_valid() {
            return Ok(());
        }

        match self.iter.next() {
            Ok(_) => Ok(()),
            Err(e) => {
                self.has_errored = true;
                Err(e)
            }
        }
    }
}
