#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        self.readers.entry(ts).and_modify(|e| *e += 1).or_insert(1);
    }

    pub fn remove_reader(&mut self, ts: u64) {
        self.readers.entry(ts).and_modify(|e| *e -= 1);
        if self.readers[&ts] == 0 {
            self.readers.remove(&ts);
        }
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.keys().count()
    }

    pub fn readers(&self) -> Vec<u64> {
        self.readers.keys().copied().collect()
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.keys().next().copied()
    }
}
