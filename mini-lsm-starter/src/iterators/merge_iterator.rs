#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::{Ok, Result};

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut bheap =
            BinaryHeap::from_iter(iters.into_iter().enumerate().filter_map(|(i, iter)| {
                if iter.is_valid() {
                    Some(HeapWrapper(i, iter))
                } else {
                    None
                }
            }));

        if bheap.is_empty() {
            return MergeIterator {
                iters: bheap,
                current: None,
            };
        }

        let current = bheap.pop().unwrap();
        Self {
            iters: bheap,
            current: Some(current),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        if let Some(current) = &self.current {
            current.1.key()
        } else {
            KeySlice::default()
        }
    }

    fn value(&self) -> &[u8] {
        if let Some(current) = &self.current {
            current.1.value()
        } else {
            &[]
        }
    }

    fn is_valid(&self) -> bool {
        if let Some(current) = &self.current {
            current.1.is_valid()
        } else {
            false
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.iters
            .iter()
            .map(|x| x.1.num_active_iterators())
            .sum::<usize>()
            + self
                .current
                .as_ref()
                .map_or(0, |i| i.1.num_active_iterators())
    }

    fn next(&mut self) -> Result<()> {
        // we want to advance all iterators that have the same key as the current iterator since we only want to return one item per key across all iterators
        // and we want to keep the invariant that the current iterator has the latest value for a given key

        while let Some(mut inner_iter) = self.iters.peek_mut() {
            if inner_iter.1.key() == self.current.as_ref().unwrap().1.key() {
                if let Err(e) = inner_iter.1.next() {
                    PeekMut::pop(inner_iter);
                    return Err(e);
                }

                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            } else {
                break;
            }
        }

        if let Some(current) = &mut self.current {
            current.1.next()?;
        }

        if self.is_valid() {
            self.iters.push(self.current.take().unwrap());
        }

        if let Some(inner_iter) = self.iters.pop() {
            self.current = Some(inner_iter);
        } else {
            self.current = None;
        }

        Ok(())
    }
}
