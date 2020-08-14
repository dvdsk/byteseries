use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Error, Read};
use std::path::Path;

use crate::util::open_and_check;

pub struct Header {
    pub file: File,

    pub data: BTreeMap<i64, u64>,
    pub last_timestamp: i64,
    pub last_timestamp_numb: i64,
}

impl Header {
    pub fn open<P: AsRef<Path>>(name: P) -> Result<Header, Error> {
        let (mut file, _) = open_and_check(name.as_ref().with_extension("h"), 16)?;

        //read in the entire file
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let mut data = BTreeMap::new();

        let last_timestamp = if bytes.is_empty() {
            0 as i64
        } else {
            let mut numbers = vec![0u64; bytes.len() / 8];
            LittleEndian::read_u64_into(&bytes, numbers.as_mut_slice());

            for i in (0..numbers.len()).step_by(2) {
                data.insert(numbers[i] as i64, numbers[i + 1]);
            }

            numbers[numbers.len() - 2] as i64
        };

        log::trace!("last_timestamp: {}", last_timestamp);
        Ok(Header {
            file,
            data,
            last_timestamp,
            last_timestamp_numb: last_timestamp / (u16::max_value() as i64),
        })
    }

    pub fn update(
        &mut self,
        timestamp: i64,
        line_start: u64,
        new_timestamp_numb: i64,
    ) -> Result<(), Error> {
        let ts = timestamp as u64;
        self.file.write_u64::<LittleEndian>(ts)?;
        self.file.write_u64::<LittleEndian>(line_start)?;
        log::trace!("wrote headerline: {}, {}", ts, line_start);

        self.data.insert(timestamp, line_start);
        self.last_timestamp_numb = new_timestamp_numb;
        Ok(())
    }
}
