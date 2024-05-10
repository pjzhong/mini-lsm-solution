use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let blk_iter = BlockIterator::new_with_prefix(
            table.read_block_cached(0)?,
            Some(table.first_key.clone()),
        );
        Ok(Self {
            table,
            blk_iter,
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        self.blk_iter = BlockIterator::new_with_prefix(
            self.table.read_block_cached(0)?,
            Some(self.table.first_key.clone()),
        );
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut iter = Self {
            table,
            blk_iter: Default::default(),
            blk_idx: Default::default(),
        };
        iter.seek_to_key(key)?;
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let idx = self.table.find_block_idx(key).saturating_sub(1);
        let block_meta = &self.table.block_meta[idx];

        if key <= block_meta.last_key.as_key_slice() {
            let mut blk_iter = BlockIterator::new_with_prefix(
                self.table.read_block_cached(idx)?,
                Some(self.table.first_key.clone()),
            );
            blk_iter.seek_to_key(key);
            self.blk_iter = blk_iter;
            self.blk_idx = idx;
        } else {
            let idx = idx + 1;
            let blk_iter = BlockIterator::new_with_prefix(
                self.table.read_block_cached(idx)?,
                Some(self.table.first_key.clone()),
            );
            self.blk_iter = blk_iter;
            self.blk_idx = idx;
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.is_valid() && self.blk_idx + 1 < self.table.block_meta.len() {
            self.blk_idx += 1;
            self.blk_iter = BlockIterator::new_with_prefix(
                self.table.read_block_cached(self.blk_idx)?,
                Some(self.table.first_key.clone()),
            );
        }
        Ok(())
    }
}
