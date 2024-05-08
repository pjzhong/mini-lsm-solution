use anyhow::Result;
use std::cmp::Ordering;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        Ok(Self { a, b })
    }

    fn is_a_valid(&self) -> bool {
        match (self.a.is_valid(), self.b.is_valid()) {
            (true, true) => self.a.key() <= self.b.key(),
            (true, false) => true,
            _ => false,
        }
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.is_a_valid() {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.is_a_valid() {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        match (self.a.is_valid(), self.b.is_valid()) {
            (true, true) => {
                let cmp = self.a.key().cmp(&self.b.key());
                match cmp {
                    Ordering::Less => {
                        self.a.next()?;
                    }
                    Ordering::Greater => {
                        self.b.next()?;
                    }
                    Ordering::Equal => {
                        self.a.next()?;
                        self.b.next()?;
                    }
                };
            }
            (true, false) => {
                self.a.next()?;
            }
            _ => self.b.next()?,
        }

        Ok(())
    }

    /// Number of underlying active iterators for this iterator.
    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
