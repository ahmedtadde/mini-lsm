#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        Ok(Self { a, b })
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.a.is_valid() && !self.b.is_valid() {
            return self.a.key();
        }

        if !self.a.is_valid() && self.b.is_valid() {
            return self.b.key();
        }

        if self.a.key() <= self.b.key() {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.a.is_valid() && !self.b.is_valid() {
            return self.a.value();
        }

        if !self.a.is_valid() && self.b.is_valid() {
            return self.b.value();
        }

        if self.a.key() <= self.b.key() {
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
            (false, false) => Ok(()),
            (true, false) => self.a.next(),
            (false, true) => self.b.next(),
            _ => {
                if self.a.key() < self.b.key() {
                    return self.a.next();
                }

                if self.b.key() < self.a.key() {
                    return self.b.next();
                }

                self.a.next()?;
                self.b.next()?;
                Ok(())
            }
        }
    }
}
