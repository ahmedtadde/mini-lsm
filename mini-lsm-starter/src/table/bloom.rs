// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::mem::size_of;

/// Implements a bloom filter
pub struct Bloom {
    /// data of filter in bits
    pub(crate) filter: Bytes,
    /// number of hash functions
    pub(crate) k: u8,
}

pub trait BitSlice {
    fn get_bit(&self, idx: usize) -> bool;
    fn bit_len(&self) -> usize;
}

pub trait BitSliceMut {
    fn set_bit(&mut self, idx: usize, val: bool);
}

impl<T: AsRef<[u8]>> BitSlice for T {
    fn get_bit(&self, idx: usize) -> bool {
        let pos = idx / 8;
        let offset = idx % 8;
        (self.as_ref()[pos] & (1 << offset)) != 0
    }

    fn bit_len(&self) -> usize {
        self.as_ref().len() * 8
    }
}

impl<T: AsMut<[u8]>> BitSliceMut for T {
    fn set_bit(&mut self, idx: usize, val: bool) {
        let pos = idx / 8;
        let offset = idx % 8;
        if val {
            self.as_mut()[pos] |= 1 << offset;
        } else {
            self.as_mut()[pos] &= !(1 << offset);
        }
    }
}

impl Bloom {
    /// Decode a bloom filter
    pub fn decode(buffer: &[u8]) -> Result<Self> {
        const U32_SIZE: usize = size_of::<u32>();
        if buffer.len() < U32_SIZE {
            return Err(anyhow!("bloom filter too short"));
        }

        // last 4 bytes should be the checksum... so, extract it and check it
        let stored_checksum = (&buffer[buffer.len() - U32_SIZE..]).get_u32();
        let computed_checksum = crc32fast::hash(&buffer[..buffer.len() - U32_SIZE]);
        if stored_checksum != computed_checksum {
            return Err(anyhow!(
                "Checksum mismatch for bloom; stored {} vs computed {} ",
                stored_checksum,
                computed_checksum
            ));
        }
        let bloom_bytes = &buffer[..buffer.len() - U32_SIZE];
        let filter = &bloom_bytes[..bloom_bytes.len() - 1];
        let k = bloom_bytes[bloom_bytes.len() - 1];
        Ok(Self {
            filter: filter.to_vec().into(),
            k,
        })
    }

    /// Encode a bloom filter
    pub fn encode(&self, buf: &mut Vec<u8>) {
        let offset = buf.len();
        buf.extend(&self.filter);
        buf.put_u8(self.k);
        buf.put_u32(crc32fast::hash(&buf[offset..]));
    }

    /// Get bloom filter bits per key from entries count and FPR
    pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
        let size =
            -1.0 * (entries as f64) * false_positive_rate.ln() / std::f64::consts::LN_2.powi(2);
        let locs = (size / (entries as f64)).ceil();
        locs as usize
    }

    /// Build bloom filter from key hashes
    pub fn build_from_key_hashes(keys: &[u32], bits_per_key: usize) -> Self {
        let k = (bits_per_key as f64 * 0.69) as u32;
        let k = k.clamp(1, 30);
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = (nbits + 7) / 8;
        let nbits = nbytes * 8;
        let mut filter = BytesMut::with_capacity(nbytes);
        filter.resize(nbytes, 0);

        keys.iter().for_each(|key| {
            let mut h = *key;
            let delta = (h >> 17) | (h << 15);
            for _ in 0..k {
                filter.set_bit((h as usize) % nbits, true);
                h = h.wrapping_add(delta);
            }
        });

        Self {
            filter: filter.freeze(),
            k: k as u8,
        }
    }

    /// Check if a bloom filter may contain some data
    pub fn may_contain(&self, key: u32) -> bool {
        if self.k > 30 {
            // potential new encoding for short bloom filters
            true
        } else {
            let nbits = self.filter.bit_len();
            let delta = (key >> 17) | (key << 15);
            let mut key = key;
            for _ in 0..self.k {
                if !self.filter.get_bit((key as usize) % nbits) {
                    return false;
                }
                key = key.wrapping_add(delta);
            }

            true
        }
    }
}
