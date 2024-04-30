#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;
use std::mem::size_of;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut bytes = BytesMut::new();

        bytes.put_slice(&self.data);
        for offset in &self.offsets {
            bytes.put_u16(*offset);
        }
        bytes.put_u16(self.offsets.len() as u16);

        bytes.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let mut data = data;
        let num = (&data[data.len() - size_of::<u16>()..]).get_u16();

        let mut entries = vec![];

        for _ in 0..num {
            let key_len = data.get_u16() as usize;
            entries.put_u16(key_len as u16);
            entries.extend_from_slice(&data[..key_len]);
            data.advance(key_len);

            let val_len = data.get_u16() as usize;
            entries.put_u16(val_len as u16);
            entries.extend_from_slice(&data[..val_len]);
            data.advance(val_len);
        }

        let mut offsets = vec![];
        for _ in 0..num {
            offsets.push(data.get_u16());
        }

        Self {
            data: entries,
            offsets,
        }
    }
}
