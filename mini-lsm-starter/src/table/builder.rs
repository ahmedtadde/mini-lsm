#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::iter;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, Bytes};

use super::{BlockMeta, FileObject, SsTable};
use crate::block::BlockIterator;
use crate::key::{KeyBytes, KeyVec, TS_DEFAULT};
use crate::table::bloom::Bloom;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
    max_ts: u64,
}

/// see this https://users.rust-lang.org/t/how-to-find-common-prefix-of-two-byte-slices-effectively/25815/3
fn mismatch(xs: &[u8], ys: &[u8]) -> usize {
    mismatch_chunks::<128>(xs, ys)
}

fn mismatch_chunks<const N: usize>(xs: &[u8], ys: &[u8]) -> usize {
    let off = iter::zip(xs.chunks_exact(N), ys.chunks_exact(N))
        .take_while(|(x, y)| x == y)
        .count()
        * N;
    off + iter::zip(&xs[off..], &ys[off..])
        .take_while(|(x, y)| x == y)
        .count()
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: vec![],
            last_key: vec![],
            data: vec![],
            meta: vec![],
            block_size,
            key_hashes: Default::default(),
            max_ts: 0,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.key_ref().to_vec();
        }

        let prefix_len = mismatch(&self.first_key, key.key_ref());
        let ok = self.builder.add_with_prefix(key, prefix_len, value);
        if !ok {
            self.split_block();
            // println!("iambatman/sst_builder::add: split block successful");
            let _ = self.builder.add_with_prefix(key, prefix_len, value);
        }
        self.key_hashes.push(farmhash::fingerprint32(key.key_ref()));
        self.max_ts = self.max_ts.max(key.ts());
    }

    fn split_block(&mut self) {
        let block = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = Arc::new(block.build());
        let offset = self.data.len();

        let iter = BlockIterator::new_with_prefix(
            block.clone(),
            Some(KeyBytes::from_bytes_with_ts(
                Bytes::copy_from_slice(&self.first_key),
                TS_DEFAULT,
            )),
        );

        let block_meta = BlockMeta {
            offset,
            first_key: iter.first_key().unwrap_or_default().into_key_bytes(),
            last_key: iter.last_key().unwrap_or_default().into_key_bytes(),
        };
        self.meta.push(block_meta);
        let encoded_block = block.encode();
        self.data.extend_from_slice(&encoded_block);
        self.data.put_u32(crc32fast::hash(&encoded_block));
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len() + self.builder.estimated_size()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty() && self.builder.is_empty()
    }

    pub fn last_inserted_key(&self) -> KeyVec {
        self.builder.last_key()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let mut sst_builder = self;
        sst_builder.split_block();

        let mut bytes = vec![];
        bytes.extend_from_slice(&sst_builder.data);

        let block_meta_offset = bytes.len();
        let first_key = sst_builder
            .meta
            .first()
            .map(|meta| meta.first_key.clone())
            .unwrap_or_default();
        let last_key = sst_builder
            .meta
            .last()
            .map(|meta| meta.last_key.clone())
            .unwrap_or_default();
        BlockMeta::encode_block_meta(&sst_builder.meta, sst_builder.max_ts, &mut bytes);

        bytes.put_u32(block_meta_offset as u32);

        let bloom_offset = bytes.len();
        let bits_per_key = Bloom::bloom_bits_per_key(sst_builder.key_hashes.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(&sst_builder.key_hashes, bits_per_key);
        bloom.encode(&mut bytes);

        bytes.put_u32(bloom_offset as u32);

        let file = FileObject::create(path.as_ref(), bytes)?;

        assert!(
            path.as_ref().exists(),
            "path does not exists: {:?}",
            path.as_ref()
        );

        Ok(SsTable {
            file,
            block_meta: sst_builder.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: sst_builder.max_ts,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
