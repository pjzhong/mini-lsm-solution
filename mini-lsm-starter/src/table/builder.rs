#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{BlockMeta, FileObject, SsTable};
use crate::key::KeyBytes;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

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
            builder: BlockBuilder::new(block_size),
            first_key: vec![],
            last_key: vec![],
            data: vec![],
            meta: vec![],
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        let suc = self.builder.add(key, value);
        if !suc {
            self.split_block();
            let _ = self.builder.add(key, value);
        }
    }

    fn split_block(&mut self) {
        let block = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = block.build();
        let offset = self.data.len();
        let block_meta = BlockMeta {
            offset,
            first_key: KeyBytes::from_bytes(block.first_key().unwrap_or_default()),
            last_key: KeyBytes::from_bytes(block.last_key().unwrap_or_default()),
        };
        self.meta.push(block_meta);
        self.data.extend_from_slice(&block.encode());
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
        BlockMeta::encode_block_meta(&sst_builder.meta, &mut bytes);

        bytes.put_u32(block_meta_offset as u32);

        let file = FileObject::create(path.as_ref(), bytes)?;

        Ok(SsTable {
            file,
            block_meta: sst_builder.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
