#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let mut sstables = sstables;
        sstables.sort_by(|a, b| a.first_key().cmp(b.first_key()));
        let mut iter = SstConcatIterator {
            current: None,
            next_sst_idx: 0,
            sstables,
        };

        iter.next()?;

        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut next_sst_idx = 0;
        let mut sstables = sstables;
        sstables.sort_by(|a, b| a.first_key().cmp(b.first_key()));

        let mut left = 0;
        let mut right = sstables.len();
        while left < right {
            let mid = left + (right - left) / 2;
            if sstables[mid].first_key().key_ref() <= key.key_ref()
                && key.key_ref() <= sstables[mid].last_key().key_ref()
            {
                next_sst_idx = mid;
                break;
            } else if sstables[mid].first_key().key_ref() > key.key_ref() {
                right = mid;
            } else {
                left = mid + 1;
            }
        }

        if next_sst_idx >= sstables.len() {
            return Ok(SstConcatIterator {
                current: None,
                next_sst_idx,
                sstables,
            });
        }

        let sst_iter =
            SsTableIterator::create_and_seek_to_key(sstables[next_sst_idx].clone(), key)?;

        let iter = SstConcatIterator {
            current: Some(sst_iter),
            next_sst_idx: next_sst_idx + 1,
            sstables,
        };

        Ok(iter)
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        if let Some(iter) = &self.current {
            iter.key()
        } else {
            KeySlice::default()
        }
    }

    fn value(&self) -> &[u8] {
        if let Some(iter) = &self.current {
            iter.value()
        } else {
            &[]
        }
    }

    fn is_valid(&self) -> bool {
        if let Some(iter) = &self.current {
            iter.is_valid()
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        if let Some(iter) = &mut self.current {
            iter.next()?;
            if iter.is_valid() {
                return Ok(());
            }
        }

        while self.next_sst_idx < self.sstables.len() {
            let sst = &self.sstables[self.next_sst_idx];
            let iter = SsTableIterator::create_and_seek_to_first(sst.clone())?;
            self.next_sst_idx += 1;
            let is_valid = iter.is_valid();
            self.current = Some(iter);
            if is_valid {
                return Ok(());
            }
        }

        self.current = None;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
