use bytes::Buf;
use std::mem::size_of;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
#[derive(Default)]
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
    /// The common prefix
    prefix: Option<KeyBytes>,
}

impl BlockIterator {
    pub fn new_with_prefix(block: Arc<Block>, prefix: Option<KeyBytes>) -> Self {
        let mut iter = Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
            prefix,
        };
        let (value_range, key) = iter.nth_entry(iter.idx);
        iter.value_range = value_range;
        iter.key = key;
        iter.first_key = iter.key.clone();
        iter
    }

    fn nth_entry(&self, idx: usize) -> ((usize, usize), KeyVec) {
        if self.block.offsets.len() <= idx {
            ((0, 0), KeyVec::new())
        } else {
            let data = self.block.offsets[idx] as usize;
            let mut data = &self.block.data[data..];
            let prefix_len = data.get_u16() as usize;
            let key_len = data.get_u16() as usize;
            let key = {
                let mut key = vec![0; key_len + prefix_len];
                if let Some(prefix) = self
                    .prefix
                    .as_ref()
                    .map(|bytes| &bytes.raw_ref()[..prefix_len])
                {
                    key[0..prefix_len].copy_from_slice(prefix);
                }
                data.copy_to_slice(&mut key[prefix_len..]);

                key
            };

            let val_len = data.get_u16() as usize;
            ((key_len, val_len), KeyVec::from_vec(key))
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        Self::new_with_prefix(block, None)
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new_with_prefix(block, None);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        //entry_offset + prefix_len +  key len +  value length field length
        let start =
            self.block.offsets[self.idx] as usize + self.value_range.0 + size_of::<u16>() * 3;
        let end = start + self.value_range.1;
        &self.block.data[start..end]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let (value_range, key_vec) = self.nth_entry(0);

        self.value_range = value_range;
        self.key = key_vec;
        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        let (value_range, key) = self.nth_entry(self.idx);
        self.value_range = value_range;
        self.key = key;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        // Being sorted?? Binary Search?????
        for idx in 0..self.block.offsets.len() {
            let (value_range, key_vec) = self.nth_entry(idx);
            if key <= key_vec.as_key_slice() {
                self.value_range = value_range;
                self.key = key_vec;
                self.idx = idx;
                break;
            }
        }
    }

    pub fn first_key(&self) -> Option<KeyVec> {
        let (_, key) = self.nth_entry(0);
        Some(key)
    }

    pub fn last_key(&self) -> Option<KeyVec> {
        let (_, key) = self.nth_entry(self.block.offsets.len().saturating_sub(1));
        Some(key)
    }
}
