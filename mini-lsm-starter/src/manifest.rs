#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

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
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        Ok(Self {
            file: Arc::new(file.into()),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;

        let mut buf = vec![];
        file.read_to_end(&mut buf)?;

        let mut records = serde_json::Deserializer::from_slice(&buf);
        let records = Vec::<ManifestRecord>::deserialize(&mut records)?;

        Ok((
            Self {
                file: Arc::new(file.into()),
            },
            records,
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
        let bytes = serde_json::to_vec(&record)?;
        let mut file = self.file.lock();
        file.write_all(&bytes)?;
        Ok(())
    }
}
