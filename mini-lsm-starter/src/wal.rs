#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
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
        let read_entry = |file: &mut File, buffer: &mut BytesMut| -> Result<Bytes> {
            let ken_len = size_of::<u16>();
            file.read_exact(&mut buffer[0..ken_len])?;
            let len = buffer.get_u16() as usize;

            buffer.reserve(len);
            file.read_exact(&mut buffer[0..len])?;

            Ok(Bytes::copy_from_slice(&buffer[0..len]))
        };

        let mut file = OpenOptions::new().read(true).open(&path)?;
        let file_len = file.metadata()?.len() as usize;
        if file_len == 0 {
            Self::create(path)
        } else {
            let length_field_len = size_of::<u16>();
            let mut buffer = BytesMut::with_capacity(size_of::<u16>());
            let mut count = 0usize;
            while count < file_len {
                buffer.clear();
                let key = read_entry(&mut file, &mut buffer)?;

                buffer.clear();
                let value = read_entry(&mut file, &mut buffer)?;

                count += length_field_len * 2 + key.len() + value.len();
                skiplist.insert(key, value);
            }
            Self::create(path)
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buffer = BytesMut::with_capacity(key.len() + value.len());
        buffer.put_u16(key.len() as u16);
        buffer.put_slice(key);
        buffer.put_u16(value.len() as u16);
        buffer.put_slice(value);
        file.write_all(&buffer)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.get_mut().sync_all()?;
        Ok(())
    }
}
