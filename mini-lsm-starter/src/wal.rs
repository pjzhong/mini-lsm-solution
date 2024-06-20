#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use bytes::{Buf, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let file = Arc::new(Mutex::new(BufWriter::new(file)));

        Ok(Self { file })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        const LENGTH_FIELD_LEN: usize = size_of::<u16>();
        const CHECK_SUM_FIELD_LEN: usize = size_of::<u32>();
        let read_entry = |file: &mut File| -> Result<(Bytes, Bytes)> {
            let mut buffer = vec![0; LENGTH_FIELD_LEN];
            file.read_exact(&mut buffer)?;
            let len = buffer.as_slice().get_u16() as usize;

            let mut entry = vec![0; len];
            file.read_exact(&mut entry)?;

            Ok((Bytes::from(buffer), Bytes::from(entry)))
        };

        let read_check_sum = |file: &mut File| -> Result<u32> {
            let mut buffer = [0; CHECK_SUM_FIELD_LEN];
            file.read_exact(&mut buffer)?;
            Ok(buffer.as_slice().get_u32())
        };

        let path = path.as_ref();

        let mut file = OpenOptions::new()
            .read(true)
            .open(path)
            .context(format!("error recover from wal:{:?}", path))?;
        let file_len = file.metadata()?.len() as usize;

        let file = &mut file;
        let mut count = 0usize;
        while count < file_len {
            let (key_len, key) = read_entry(file)?;
            let (val_len, value) = read_entry(file)?;
            let check_sum = read_check_sum(file)?;

            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&key_len);
            hasher.update(&key);
            hasher.update(&val_len);
            hasher.update(&value);
            if hasher.finalize() != check_sum {
                return Err(anyhow!("wal recover, check sum error"));
            }

            count += key_len.len() + val_len.len() + key.len() + value.len() + CHECK_SUM_FIELD_LEN;
            skiplist.insert(key, value);
        }
        Self::create(path)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();

        let key_len = &(key.len() as u16).to_be_bytes();
        let val_len = &(value.len() as u16).to_be_bytes();
        file.write_all(key_len)?;
        file.write_all(key)?;
        file.write_all(val_len)?;
        file.write_all(value)?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(key_len);
        hasher.update(key);
        hasher.update(val_len);
        hasher.update(value);
        file.write_all(&hasher.finalize().to_be_bytes())?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.get_mut().sync_all()?;
        Ok(())
    }
}
