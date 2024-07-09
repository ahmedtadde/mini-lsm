#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{bail, Result};
use bytes::BufMut;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::compact::CompactionTask;

pub const MANIFEST_FILE_PATH_SUFFIX: &str = "MANIFEST";

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = {
            if path.as_ref().ends_with(MANIFEST_FILE_PATH_SUFFIX) {
                path.as_ref().to_path_buf()
            } else {
                path.as_ref().join(MANIFEST_FILE_PATH_SUFFIX)
            }
        };

        //println!("Creating manifest file at {:?}", path);

        let file = Arc::new(Mutex::new(
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .read(true)
                .open(path)?,
        ));
        Ok(Self { file })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let path = {
            if path.as_ref().ends_with(MANIFEST_FILE_PATH_SUFFIX) {
                path.as_ref().to_path_buf()
            } else {
                path.as_ref().join(MANIFEST_FILE_PATH_SUFFIX)
            }
        };

        let mut file = OpenOptions::new().read(true).append(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        // get the size of a u32
        const U32_SIZE: usize = std::mem::size_of::<u32>();
        let mut cursor: usize = 0;
        let mut records = Vec::new();

        while cursor < buf.len() {
            let record_len = u32::from_ne_bytes(
                (&buf[cursor..cursor + U32_SIZE])
                    .try_into()
                    .unwrap_or_else(|_| panic!("expected {} bytes for record_len", U32_SIZE)),
            ) as usize;

            cursor += U32_SIZE;
            if cursor + record_len + U32_SIZE > buf.len() {
                break;
            }

            let record = &buf[cursor..cursor + record_len];
            cursor += record_len;

            let stored_checksum = u32::from_ne_bytes(
                (&buf[cursor..cursor + U32_SIZE])
                    .try_into()
                    .unwrap_or_else(|_| panic!("expected {} bytes for checksum", U32_SIZE)),
            );
            cursor += U32_SIZE;

            let computed_checksum = crc32fast::hash(record);
            if stored_checksum != computed_checksum {
                bail!(
                    "Checksum mismatch for manifest record; stored {} vs computed {} ",
                    stored_checksum,
                    computed_checksum
                );
            }

            records.put(record);
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            Deserializer::from_slice(&records)
                .into_iter::<ManifestRecord>()
                .flat_map(|x| x.ok())
                .collect(),
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut writer = self.file.lock();
        let buf = serde_json::to_vec(&record)?;
        writer.write_all((buf.len() as u32).to_ne_bytes().as_ref())?;
        writer.write_all(&buf)?;
        writer.write_all(crc32fast::hash(&buf).to_ne_bytes().as_ref())?;
        // writer.flush()?;
        writer.sync_all()?;

        Ok(())
    }
}
