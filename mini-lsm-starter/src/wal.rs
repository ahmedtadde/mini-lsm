#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};
use bytes::{BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        //println!("Creating wal file at {:?}", path.as_ref());

        let file = Arc::new(Mutex::new(BufWriter::new(
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .read(true)
                .open(path)?,
        )));

        Ok(Self { file })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        assert!(path.as_ref().exists(), "wal file not found");
        assert!(path.as_ref().is_file(), "wal file is not a file");

        // println!("Recovering wal file at {:?}", path.as_ref());

        let mut file = OpenOptions::new().read(true).append(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        // get the size of a u16 and u32
        const U16_SIZE: usize = std::mem::size_of::<u16>();
        const U32_SIZE: usize = std::mem::size_of::<u32>();
        let mut cursor: usize = 0;

        while cursor < buf.len() {
            let entry_offset = cursor;
            // assung key_len is 2 bytes and value_len is 2 bytes
            // this is the kv entry layout per the memtable block -> key_len | key | value_len | value
            // use U16_SIZE to get the len values and U32_SIZE for the checksum

            // consume the U16_SIZE bytes from the buffer
            let key_len = u16::from_ne_bytes(
                (&buf[cursor..cursor + U16_SIZE])
                    .try_into()
                    .unwrap_or_else(|_| panic!("expected {} bytes for key_len", U16_SIZE)),
            ) as usize;

            cursor += U16_SIZE;
            if cursor + key_len + U16_SIZE > buf.len() {
                break;
            }

            let key = Bytes::copy_from_slice(&buf[cursor..cursor + key_len]);
            cursor += key_len;
            let value_len = u16::from_ne_bytes(
                (&buf[cursor..cursor + U16_SIZE])
                    .try_into()
                    .unwrap_or_else(|_| panic!("expected {} bytes for value_len", U16_SIZE)),
            ) as usize;
            cursor += U16_SIZE;
            let value = Bytes::copy_from_slice(&buf[cursor..cursor + value_len]);
            cursor += value_len;

            // check the crc32 checksum
            {
                let stored_checksum = u32::from_ne_bytes(
                    (&buf[cursor..cursor + U32_SIZE])
                        .try_into()
                        .unwrap_or_else(|_| panic!("expected {} bytes for checksum", U32_SIZE)),
                );

                let computed_checksum = crc32fast::hash(&buf[entry_offset..cursor]);

                if stored_checksum != computed_checksum {
                    bail!(
                        "Checksum mismatch for wal entry; stored {} vs computed {}",
                        stored_checksum,
                        computed_checksum
                    );
                }

                cursor += U32_SIZE;
            }

            skiplist.insert(key, value);
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = Vec::new();
        buf.put_u16_ne(key.len() as u16);
        buf.put(key);
        buf.put_u16_ne(value.len() as u16);
        buf.put(value);
        buf.put_u32_ne(crc32fast::hash(&buf[..]));
        file.write_all(&buf)?;

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;

        Ok(())
    }
}
