#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, BytesMut};

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            first_key: Vec::new(),
            last_key: Vec::new(),
            builder: BlockBuilder::new(block_size),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        let ok = self.builder.add(key, value);

        if ok {
            self.last_key = key.raw_ref().to_vec();
            if self.first_key.is_empty() {
                self.first_key = key.raw_ref().to_vec();
            }
        } else {
            let mut builder = BlockBuilder::new(self.block_size);
            let _ = builder.add(key, value);
            self.flush_and_replace_builder(builder).unwrap();
            self.first_key = key.raw_ref().to_vec();
            self.last_key = key.raw_ref().to_vec();
        }
    }

    fn flush_and_replace_builder(&mut self, mut builder: BlockBuilder) -> Result<()> {
        std::mem::swap(&mut self.builder, &mut builder);
        let block = builder.build();
        let offset = self.data.len();
        self.data.extend_from_slice(&block.encode());
        self.meta.push(BlockMeta {
            offset,
            first_key: KeyVec::from_vec(self.first_key.clone()).into_key_bytes(),
            last_key: KeyVec::from_vec(self.last_key.clone()).into_key_bytes(),
        });
        Ok(())
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // ensure the last block is flushed
        let mut this = self;
        if !this.builder.is_empty() {
            let builder = BlockBuilder::new(this.block_size);
            this.flush_and_replace_builder(builder)?;
        };

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&this.data);
        let metadata_offset = buf.len();
        let mut block_meta = Vec::new();
        BlockMeta::encode_block_meta(&this.meta, &mut block_meta);
        buf.extend_from_slice(&block_meta);
        buf.put_u32_ne(metadata_offset.try_into().unwrap());

        let file = FileObject::create(path.as_ref(), buf.to_vec())?;
        let first_key = this
            .meta
            .first()
            .map(|x| x.first_key.clone())
            .unwrap_or_default();
        let last_key = this
            .meta
            .last()
            .map(|x| x.last_key.clone())
            .unwrap_or_default();

        Ok(SsTable {
            file,
            block_meta: this.meta,
            block_meta_offset: metadata_offset,
            id,
            block_cache,
            first_key,
            last_key,
            max_ts: 0,
            bloom: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
