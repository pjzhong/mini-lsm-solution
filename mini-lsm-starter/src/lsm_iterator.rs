use anyhow::{anyhow, Result};
use bytes::Bytes;
use std::ops::Bound;

use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::table::SsTableIterator;
use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end: Bound<Bytes>,
}

impl LsmIterator {
    pub(crate) fn new(inner: LsmIteratorInner, end: Bound<&[u8]>) -> Result<Self> {
        let end = match end {
            Bound::Included(x) | Bound::Excluded(x) => Bound::Included(Bytes::copy_from_slice(x)),
            Bound::Unbounded => Bound::Unbounded,
        };

        let mut iter = Self { inner, end };
        iter.skip_delete_key()?;
        Ok(iter)
    }

    fn is_valid(&self) -> bool {
        if !self.inner.is_valid() {
            return false;
        };

        match &self.end {
            Bound::Included(x) => self.inner.key().raw_ref() <= x && !self.value().is_empty(),
            Bound::Excluded(x) => self.inner.key().raw_ref() < x && !self.value().is_empty(),
            Bound::Unbounded => true,
        }
    }

    fn skip_delete_key(&mut self) -> Result<()> {
        while self.is_valid() && !self.inner.key().is_empty() && self.inner.value().is_empty() {
            self.inner.next()?;
        }

        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        self.skip_delete_key()?;
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            Err(anyhow!("Invalid iterator"))
        } else {
            let res = self.iter.next();
            self.has_errored = res.is_err();
            res
        }
    }
}
