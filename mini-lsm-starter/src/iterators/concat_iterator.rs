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
        Ok(if sstables.is_empty() {
            Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            }
        } else {
            Self {
                current: Some(SsTableIterator::create_and_seek_to_first(
                    sstables[0].clone(),
                )?),
                next_sst_idx: 1,
                sstables,
            }
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        if sstables.is_empty() {
            return Self::create_and_seek_to_first(sstables);
        }

        let idx = sstables
            .binary_search_by_key(&key.to_key_vec().into_key_bytes(), |sst| {
                sst.first_key().clone()
            })
            .unwrap_or_else(|idx| idx)
            .saturating_sub(1);
        let sst = &sstables[idx];

        Ok(if key <= sst.last_key().as_key_slice() {
            Self {
                current: Some(SsTableIterator::create_and_seek_to_key(sst.clone(), key)?),
                next_sst_idx: idx + 1,
                sstables,
            }
        } else {
            let idx = idx.saturating_add(1);
            let iter = match sstables
                .get(idx)
                .map(|sst| SsTableIterator::create_and_seek_to_first(sst.clone()))
            {
                None => None,
                Some(Ok(iter)) => Some(iter),
                Some(Err(e)) => return Err(e),
            };
            Self {
                current: iter,
                next_sst_idx: idx + 1,
                sstables,
            }
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current
            .as_ref()
            .map(SsTableIterator::key)
            .unwrap_or_default()
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .map(SsTableIterator::value)
            .unwrap_or_default()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(SsTableIterator::is_valid)
            .unwrap_or_default()
    }

    fn next(&mut self) -> Result<()> {
        if let Some(current) = self.current.as_mut() {
            current.next()?;
        }

        if !self.is_valid() {
            let idx = self.next_sst_idx;
            self.next_sst_idx += 1;

            if let Some(sst) = self.sstables.get(idx) {
                self.current = Some(SsTableIterator::create_and_seek_to_first(sst.clone())?);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
