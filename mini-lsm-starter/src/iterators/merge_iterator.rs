use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

//1.整合多个Iter的结果，整合多个Iter的结果。如果遇到重复的key，下标最小的优先
/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let iters = iters
            .into_iter()
            .filter(|b| b.is_valid())
            .enumerate()
            .map(|(idx, b)| HeapWrapper(idx, b))
            .collect::<Vec<_>>();
        let mut iters = BinaryHeap::from(iters);
        let current = iters.pop();

        Self { iters, current }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        match self.current.as_ref() {
            Some(wrapper) => {
                let iter = &wrapper.1;
                iter.key()
            }
            None => KeySlice::from_slice(&[]),
        }
    }

    fn value(&self) -> &[u8] {
        match self.current.as_ref() {
            Some(wrapper) => {
                let iter = &wrapper.1;
                iter.value()
            }
            None => &[],
        }
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map_or(false, |inner_iter| inner_iter.1.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        let heap_wrapper = match self.current.as_mut() {
            None => return Ok(()),
            Some(heap_wrapper) => heap_wrapper,
        };

        let key = heap_wrapper.1.key();

        while let Some(mut wrapper) = self.iters.peek_mut() {
            if wrapper.1.key() == key {
                if let e @ Err(_) = wrapper.1.next() {
                    PeekMut::pop(wrapper);
                    return e;
                }

                if !wrapper.1.is_valid() {
                    PeekMut::pop(wrapper);
                }
            } else {
                break;
            }
        }

        heap_wrapper.1.next()?;

        if heap_wrapper.1.is_valid() {
            if let Some(mut wrapper) = self.iters.peek_mut() {
                if *heap_wrapper < *wrapper {
                    std::mem::swap(&mut *wrapper, heap_wrapper);
                }
            }
        } else {
            self.current = self.iters.pop();
        }

        Ok(())
    }

    /// Number of underlying active iterators for this iterator.
    fn num_active_iterators(&self) -> usize {
        let mut count = 0;
        for iter in &self.iters {
            count += iter.1.num_active_iterators();
        }

        if let Some(iter) = &self.current {
            count += iter.1.num_active_iterators()
        }

        count
    }
}
