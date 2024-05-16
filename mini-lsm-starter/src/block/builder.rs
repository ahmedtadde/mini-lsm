use super::Block;
use crate::key::{KeySlice, KeyVec};
use bytes::{BufMut, Bytes};

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
    /// The common prefix
    prefix: Option<KeyVec>,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
            prefix: None,
        }
    }

    pub fn compressed_key(&self, key: KeySlice) -> (u16, Bytes) {
        let first_key_slice = self.first_key.raw_ref();
        let input_key_slice = key.raw_ref();

        // Find the overlap length
        let overlap_len = first_key_slice
            .iter()
            .zip(input_key_slice.iter())
            .take_while(|(a, b)| a == b)
            .count();

        // Convert overlap length to u16 (ensure it fits within u16)
        let overlap_len = overlap_len.min(u16::MAX as usize) as u16;

        // Get the rest of the key that doesn't overlap
        let rest_key = Bytes::copy_from_slice(&input_key_slice[overlap_len as usize..]);

        (overlap_len, rest_key)
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        self.add_with_prefix(key, 0, value)
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add_with_prefix(&mut self, key: KeySlice, prefix_len: usize, value: &[u8]) -> bool {
        if self.data.is_empty() {
            self.first_key = KeyVec::from_vec(key.raw_ref().to_vec());
        } else if self.is_exceed_size(key, value) {
            return false;
        }

        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(prefix_len as u16);

        let key = &key.raw_ref()[prefix_len..];
        self.data.put_u16(key.len() as u16);
        self.data.put_slice(key);

        self.data.put_u16(value.len() as u16);
        self.data.put_slice(value);

        if self.is_empty() {
            self.first_key = KeyVec::from_vec(key.to_vec());
        }

        true
    }

    pub fn is_exceed_size(&self, key: KeySlice, value: &[u8]) -> bool {
        const KEY_VAL_LEN: usize = 4;
        self.block_size
            < self.data.len() + self.offsets.len() * 2 + KEY_VAL_LEN + key.len() + value.len()
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
