#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Buf;
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

        let mut records = vec![];
        let mut buf = buf.as_slice();

        const LENGTH_FIELD_LEN: usize = size_of::<u32>();
        while !buf.is_empty() {
            let mut hasher = crc32fast::Hasher::new();
            let length = {
                let mut length = &buf[..LENGTH_FIELD_LEN];
                hasher.update(length);

                buf.advance(length.len());
                length.get_u32()
            };

            let record = {
                let record = &buf[..length as usize];
                hasher.update(record);
                buf.advance(record.len());
                record
            };

            let check_sum = buf.get_u32();
            if hasher.finalize() != check_sum {
                return Err(anyhow!("manifest recover, check sum error"));
            }

            let record = serde_json::from_slice::<ManifestRecord>(record)?;
            records.push(record);
        }

        // let records = if buf.is_empty() {
        //     vec![]
        // } else {
        //     let de_sers = serde_json::Deserializer::from_slice(&buf).into_iter::<ManifestRecord>();

        //     for res in de_sers {
        //         records.push(res?);
        //     }

        //     records
        // };

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
        let len = (bytes.len() as u32).to_be_bytes();
        let mut file = self.file.lock();
        file.write_all(&len)?;
        file.write_all(&bytes)?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&len);
        hasher.update(&bytes);
        file.write_all(&hasher.finalize().to_be_bytes())?;
        Ok(())
    }
}
