#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    pub fn first_key(&self) -> Vec<u8> {
        let mut buf = &self.data[..];
        buf.get_u16();
        let key_len = buf.get_u16();
        let key = &buf[..key_len as usize];
        key.to_vec()
    }
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut data = BytesMut::new();
        let num_entries = self.offsets.len();
        data.extend_from_slice(&self.data);
        data.extend_from_slice(
            &self
                .offsets
                .iter()
                .flat_map(|x| x.to_ne_bytes())
                .collect::<Vec<u8>>(),
        );
        data.extend_from_slice(&(num_entries as u16).to_ne_bytes());
        Bytes::from(data)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        if data.is_empty() {
            return Self {
                data: Vec::new(),
                offsets: Vec::new(),
            };
        }

        if data.len() < 2 {
            panic!("Invalid data length");
        }

        let mut data = Bytes::copy_from_slice(data);
        let num_entries = data.split_off(data.len() - 2).to_vec();
        let num_entries = u16::from_ne_bytes(num_entries.as_slice().try_into().unwrap());
        let mut offsets = Vec::with_capacity(num_entries as usize);
        for idx in (0..num_entries).rev() {
            let offset = data.split_off(data.len() - 2).to_vec();
            offsets.push(u16::from_ne_bytes(offset.as_slice().try_into().unwrap()));
        }

        offsets.reverse();

        Self {
            data: data.to_vec(),
            offsets,
        }
    }
}
