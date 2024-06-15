#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        println!("Creating wal file at {:?}", path.as_ref());

        let file = Arc::new(Mutex::new(BufWriter::new(
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .read(true)
                .open(path)?,
        )));

        Ok(Self { file })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        assert!(path.as_ref().exists(), "wal file not found");
        assert!(path.as_ref().is_file(), "wal file is not a file");

        println!("Recovering wal file at {:?}", path.as_ref());

        let mut file = OpenOptions::new().read(true).append(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        // get the size of a u16
        let u16_size = std::mem::size_of::<u16>();
        let mut cursor: usize = 0;

        while cursor < buf.len() {
            // assung key_len is 2 bytes and value_len is 2 bytes
            // this is the kv entry layout per the memtable block -> key_len | key | value_len | value
            // use u16_size to get the len values

            // consume the u16_size bytes from the buffer
            let key_len = u16::from_ne_bytes(
                (&buf[cursor..cursor + u16_size])
                    .try_into()
                    .unwrap_or_else(|_| panic!("expected {} bytes for key_len", u16_size)),
            ) as usize;
            cursor += u16_size;
            if cursor + key_len + u16_size > buf.len() {
                break;
            }
            let key = Bytes::copy_from_slice(&buf[cursor..cursor + key_len]);
            cursor += key_len;
            let value_len = u16::from_ne_bytes(
                (&buf[cursor..cursor + u16_size])
                    .try_into()
                    .unwrap_or_else(|_| panic!("expected {} bytes for value_len", u16_size)),
            ) as usize;
            cursor += u16_size;
            let value = Bytes::copy_from_slice(&buf[cursor..cursor + value_len]);
            cursor += value_len;

            skiplist.insert(key, value);
        }

        println!("Recovered {} entries from wal", skiplist.len());

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        file.write_all(&(key.len() as u16).to_ne_bytes())?;
        file.write_all(key)?;
        file.write_all(&(value.len() as u16).to_ne_bytes())?;
        file.write_all(value)?;

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;

        Ok(())
    }
}
