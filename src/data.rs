use byteorder::{ByteOrder, LittleEndian};
use chrono::{DateTime, Utc};
use num_traits::cast::FromPrimitive;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::header::Header;
use crate::util::open_and_check;
use crate::Error;

#[derive(Debug)]
pub struct ByteSeries {
    pub data: File,
    pub header: Header,

    pub line_size: usize,
    pub full_line_size: usize,
    timestamp: i64,

    pub first_time_in_data: Option<i64>,
    pub last_time_in_data: Option<i64>,

    pub data_size: u64,
}

// ---------------------------------------------------------------------
// -- we store only the last 4 bytes of the timestamp ------------------
// ---------------------------------------------------------------------
#[derive(Debug)]
pub struct FullTime {
    pub curr: i64,
    pub next: Option<i64>,
    pub next_pos: Option<u64>,
}

impl ByteSeries {
    pub fn open<P: AsRef<Path>>(name: P, line_size: usize) -> Result<ByteSeries, Error> {
        let full_line_size = line_size + 2; //+2 accounts for u16 timestamp
        let (mut data, size) = open_and_check(name.as_ref().with_extension("dat"), line_size + 2)?;
        let header = Header::open(name)?;

        let first_time = header.first_time_in_data();
        let last_time = Self::get_last_time_in_data(&mut data, &header, full_line_size);

        Ok(ByteSeries {
            data,
            header, // add triple headers

            line_size,
            full_line_size, //+2 accounts for u16 timestamp
            timestamp: 0,

            first_time_in_data: first_time,
            last_time_in_data: last_time,

            //these are set during: set_read_start, set_read_end then read is
            //bound by these points
            data_size: size,
        })
    }

    fn get_last_time_in_data(
        data: &mut File,
        header: &Header,
        full_line_size: usize,
    ) -> Option<i64> {
        let mut buf = [0u8; 2]; //rewrite to use bufferd data
        if data.seek(SeekFrom::End(-(full_line_size as i64))).is_ok() {
            data.read_exact(&mut buf).unwrap();
            let timestamp_low = dbg!(LittleEndian::read_u16(&buf) as i64);
            let timestamp_high = dbg!(header.last_timestamp) & !0b1111_1111_1111_11111;
            dbg!(timestamp_high);
            let timestamp = timestamp_high | timestamp_low;
            dbg!(Some(timestamp))
        } else {
            log::warn!("file is empty");
            None
        }
    }

    pub fn append(&mut self, time: DateTime<Utc>, line: &[u8]) -> Result<(), Error> {
        self.append_fast(time, line)?;
        self.force_write_to_disk();
        Ok(())
    }

    pub fn append_fast(&mut self, time: DateTime<Utc>, line: &[u8]) -> Result<(), Error> {
        //TODO decide if a lock is needed here
        //write 16 bit timestamp and then the line to file
        let timestamp = self.time_to_line_timestamp(time);
        self.data.write_all(&timestamp)?;
        self.data.write_all(&line[..self.line_size])?;

        //write 64 bit timestamp to header
        //(needed no more then once every 18 hours)
        self.update_header()?;
        self.data_size += self.full_line_size as u64;

        self.last_time_in_data = Some(time.timestamp());
        self.first_time_in_data.get_or_insert(time.timestamp());
        Ok(())
    }

    fn update_header(&mut self) -> Result<(), Error> {
        let new_timestamp_numb = self.timestamp / 2i64.pow(16);
        //println!("inserting; ts_low: {}, ts_numb: {}",self.timestamp as u16, new_timestamp_numb);
        if new_timestamp_numb > self.header.last_timestamp_numb {
            log::info!("updating file header");
            //println!("updating header");
            let line_start = self.data.metadata().unwrap().len() - self.full_line_size as u64;
            self.header
                .update(self.timestamp, line_start, new_timestamp_numb)?;
        }
        Ok(())
    }

    pub fn force_write_to_disk(&mut self) {
        self.data.sync_data().unwrap();
        self.header.file.sync_data().unwrap();
    }

    fn time_to_line_timestamp(&mut self, time: DateTime<Utc>) -> [u8; 2] {
        //for now no support for sign bit since data will always be after 0 (1970)
        self.timestamp = time.timestamp().abs();

        //we store the timestamp in little endian Signed magnitude representation
        //(least significant (lowest value) byte at lowest adress)
        //for the line timestamp we use only the 2 lower bytes
        let mut line_timestamp = [0; 2];
        LittleEndian::write_u16(&mut line_timestamp, self.timestamp as u16);
        line_timestamp
    }

    pub fn get_timestamp<T>(&mut self, line: &[u8], pos: u64, full_ts: &mut FullTime) -> T
    where
        T: FromPrimitive,
    {
        //update full timestamp when needed
        if pos + 1 > full_ts.next_pos.unwrap() {
            log::debug!(
                "updating ts, pos: {:?}, next ts pos: {:?}",
                pos,
                full_ts.next_pos
            );
            //update current timestamp
            full_ts.curr = full_ts.next.unwrap();

            //set next timestamp and timestamp pos
            //minimum in map greater then current timestamp
            if let Some(next) = self.header.next_full_timestamp(full_ts.curr) {
                full_ts.next = Some(next.timestamp);
                full_ts.next_pos = Some(next.pos);
            } else {
                //TODO handle edge case, last full timestamp
                log::debug!(
                    "loaded last timestamp in header, no next TS, current pos: {}",
                    pos
                );
                full_ts.next = None;
                full_ts.next_pos = None;
            }
        }
        let timestamp_low = LittleEndian::read_u16(line) as u64;
        let timestamp_high = (full_ts.curr as u64 >> 16) << 16;
        let timestamp = timestamp_high | timestamp_low;

        T::from_u64(timestamp).unwrap()
    }

    pub fn read(
        &mut self,
        buf: &mut [u8],
        start_byte: &mut u64,
        stop_byte: u64,
    ) -> Result<usize, Error> {
        dbg!(&start_byte,&stop_byte,buf.len());
        self.data.seek(SeekFrom::Start(*start_byte))?;
        let mut nread = self.data.read(buf)?;
        dbg!(nread);

        nread = if (*start_byte + nread as u64) >= stop_byte {
            (stop_byte - *start_byte) as usize
        } else {
            nread
        };
        *start_byte += nread as u64; //todo move to seek?
        Ok(nread - nread % self.full_line_size)
    }

    pub fn decode_time(
        &mut self,
        lines_to_read: usize,
        start_byte: &mut u64,
        stop_byte: u64,
        full_ts: &mut FullTime,
    ) -> Result<(Vec<u64>, Vec<u8>), Error> {
        //let mut buf = Vec::with_capacity(lines_to_read*self.full_line_size);
        let mut buf = vec![0; lines_to_read * self.full_line_size];

        let mut timestamps: Vec<u64> = Vec::with_capacity(lines_to_read);
        let mut line_data: Vec<u8> = Vec::with_capacity(lines_to_read);
        //save file pos indicator before read call moves it around
        let mut file_pos = *start_byte;
        let n_read = self.read(&mut buf, start_byte, stop_byte)? as usize;
        log::trace!("read: {} bytes", n_read);
        for line in buf[..n_read].chunks(self.full_line_size) {
            timestamps.push(self.get_timestamp::<u64>(line, file_pos, full_ts));
            file_pos += self.full_line_size as u64;
            line_data.extend_from_slice(&line[2..]);
        }
        Ok((timestamps, line_data))
    }
    pub fn decode_last_line(&mut self) -> Result<(i64, Vec<u8>), Error> {
        if self.data_size < self.full_line_size as u64 {
            return Err(Error::NoData);
        }

        let mut start_byte = self.data_size - self.full_line_size as u64;
        let stop_byte = self.data_size;

        let mut full_line = vec![0; self.full_line_size];
        let nread = self.read(&mut full_line, &mut start_byte, stop_byte)?;

        if nread < self.full_line_size {
            return Err(Error::PartialLine);
        }

        full_line.remove(0);
        full_line.remove(0);
        let line = full_line;
        let time = self.last_time_in_data.ok_or(Error::NoData)?;
        Ok((time, line))
    }
}
