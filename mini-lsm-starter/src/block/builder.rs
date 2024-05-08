#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};

use super::Block;

const U16_BYTES: usize = std::mem::size_of::<u16>();
const TRAILER_BYTES: usize = U16_BYTES;
const OFFSET_BYTES: usize = U16_BYTES;
const KEY_LENGTH_BYTES: usize = U16_BYTES;
const VALUE_LENGTH_BYTES: usize = U16_BYTES;
/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(
            !key.is_empty() && key.len() <= u16::MAX as usize,
            "Key is empty or too long"
        );
        // Check if the block is full
        let required_capacity =
            OFFSET_BYTES + KEY_LENGTH_BYTES + key.len() + VALUE_LENGTH_BYTES + value.len();

        if !self.is_empty()
            && (self.offsets.len() * OFFSET_BYTES)
                + self.data.len()
                + TRAILER_BYTES
                + required_capacity
                > self.block_size
        {
            return false;
        }

        if self.is_empty() {
            self.first_key = key.to_key_vec();
        }

        // Add the key-value pair to the block
        self.offsets.push(self.data.len() as u16);
        self.data
            .extend_from_slice(&(key.len() as u16).to_ne_bytes());
        self.data.extend_from_slice(key.raw_ref());
        self.data
            .extend_from_slice(&(value.len() as u16).to_ne_bytes());
        self.data.extend_from_slice(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            offsets: self.offsets,
            data: self.data,
        }
    }
}
