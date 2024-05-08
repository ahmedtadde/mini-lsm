#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.next();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx == self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }

        let offset = self.block.offsets[self.idx] as usize;
        let key_len =
            u16::from_ne_bytes([self.block.data[offset], self.block.data[offset + 1]]) as usize;
        let key_start = offset + 2;
        let key_end = key_start + key_len;
        let value_len =
            u16::from_ne_bytes([self.block.data[key_end], self.block.data[key_end + 1]]) as usize;
        let value_start = key_end + 2;
        let value_end = value_start + value_len;

        self.key
            .set_from_slice(KeySlice::from_slice(&self.block.data[key_start..key_end]));
        self.value_range = (value_start, value_end);

        if self.idx == 0 {
            self.first_key.set_from_slice(self.key.as_key_slice());
        }

        self.idx += 1;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut left = 0;
        let mut right = self.block.offsets.len();
        while left < right {
            let mid = left + (right - left) / 2;
            let offset = self.block.offsets[mid] as usize;
            let key_len =
                u16::from_ne_bytes([self.block.data[offset], self.block.data[offset + 1]]) as usize;
            let key_start = offset + 2;
            let key_end = key_start + key_len;
            let key_slice = KeySlice::from_slice(&self.block.data[key_start..key_end]);
            if key_slice < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        self.idx = left;
        self.next();
    }
}
