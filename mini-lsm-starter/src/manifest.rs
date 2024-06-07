#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::Result;
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

        println!("Creating manifest file at {:?}", path);

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

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            Deserializer::from_slice(&buf)
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
        writer.write_all(&buf)?;
        writer.flush()?;
        writer.sync_all()?;

        Ok(())
    }
}
