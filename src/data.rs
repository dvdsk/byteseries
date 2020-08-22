use byteorder::{ByteOrder, LittleEndian};
use chrono::{DateTime, NaiveDateTime, Utc};
use num_traits::cast::FromPrimitive;
use std::fs::File;
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::header::Header;
use crate::util::open_and_check;

pub struct ByteSeries {
    pub data: File,
    pub header: Header,

    pub line_size: usize,
    pub full_line_size: usize,
    timestamp: i64,

    pub first_time_in_data: Option<DateTime<Utc>>,
    pub last_time_in_data: Option<DateTime<Utc>>,

    pub data_size: u64,
}

// ---------------------------------------------------------------------
// -- we store only the last 4 bytes of the timestamp ------------------
// ---------------------------------------------------------------------

pub struct FullTime {
    curr: i64,
    next: i64,
    next_pos: u64,
}

impl ByteSeries {
    pub fn open<P: AsRef<Path>>(name: P, line_size: usize) -> Result<ByteSeries, Error> {
        let full_line_size = line_size + 2; //+2 accounts for u16 timestamp
        let (mut data, size) = open_and_check(name.as_ref().with_extension("dat"), line_size + 2)?;
        let header = Header::open(name)?;

        let first_time = Self::get_first_time_in_data(&header);
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

    fn get_first_time_in_data(header: &Header) -> Option<DateTime<Utc>> {
        let timestamp;
        if let Some(first_header_entry) = header.data.range(0..).next() {
            timestamp = *first_header_entry.0;
            Some(DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(timestamp, 0),
                Utc,
            ))
        } else {
            None
        }
    }

    fn get_last_time_in_data(
        data: &mut File,
        header: &Header,
        full_line_size: usize,
    ) -> Option<DateTime<Utc>> {
        let mut buf = [0u8; 2]; //rewrite to use bufferd data
        if data.seek(SeekFrom::End(-(full_line_size as i64))).is_ok() {
            data.read_exact(&mut buf).unwrap();
            let timestamp_low = LittleEndian::read_u16(&buf) as i64;
            let timestamp_high = header.last_timestamp & (!0b1111_1111_1111_1111);
            let timestamp = timestamp_high | timestamp_low;
            Some(DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(timestamp, 0),
                Utc,
            ))
        } else {
            log::warn!("file is empty");
            None
        }
    }

    pub fn append(&mut self, time: DateTime<Utc>, line: &[u8]) -> Result<(), Error> {
        //TODO decide if a lock is needed here
        //write 16 bit timestamp and then the line to file
        let timestamp = self.time_to_line_timestamp(time);
        self.data.write_all(&timestamp)?;
        self.data.write_all(&line[..self.line_size])?;

        //write 64 bit timestamp to header
        //(needed no more then once every 18 hours)
        self.update_header()?;
        self.force_write_to_disk(); //FIXME should this be exposed to the user?
        self.data_size += self.full_line_size as u64;
        self.last_time_in_data = Some(time);
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
        self.last_time_in_data = Some(time);
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
        if pos + 1 > full_ts.next_pos {
            log::debug!(
                "updating ts, pos: {}, next ts pos: {}",
                pos,
                full_ts.next_pos
            );
            //update current timestamp
            full_ts.curr = full_ts.next;

            //set next timestamp and timestamp pos
            //minimum in map greater then current timestamp
            if let Some(next) = self
                .header
                .data
                .range(full_ts.curr + 1..)
                .next()
            {
                full_ts.next = *next.0;
                full_ts.next_pos = *next.1;
            } else {
                //TODO handle edge case, last full timestamp
                log::debug!(
                    "loaded last timestamp in header, no next TS, current pos: {}",
                    pos
                );
                full_ts.next = 0;
                full_ts.next_pos = u64::max_value();
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
        self.data.seek(SeekFrom::Start(*start_byte))?;
        let mut nread = self.data.read(buf)?;

        nread = if (*start_byte + nread as u64) >= stop_byte {
            log::trace!(
                "diff: {}, {}, {}",
                *start_byte as i64,
                stop_byte as i64,
                stop_byte as i64 - *start_byte as i64
            );
            (stop_byte - *start_byte) as usize
        } else {
            log::trace!("nread: {}, {}, {}", nread, start_byte, stop_byte);
            nread
        };
        *start_byte += nread as u64;
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
    pub fn decode_last_line(&mut self) -> Result<(DateTime<Utc>, Vec<u8>), Error> {
        if self.data_size < self.full_line_size as u64 {
            return Err(Error::new(ErrorKind::UnexpectedEof, "No data in file"));
        }

        let mut start_byte = self.data_size - self.full_line_size as u64;
        let stop_byte = self.data_size;

        let mut full_line = vec![0; self.full_line_size];
        let nread = self.read(&mut full_line, &mut start_byte, stop_byte)?;

        if nread < self.full_line_size {
            dbg!(nread);
            let custom_error = Error::new(ErrorKind::Other, "could not read a full line!");
            return Err(custom_error);
        }

        full_line.remove(0);
        full_line.remove(0);
        let line = full_line;
        Ok((self.last_time_in_data.unwrap(), line))
    }
}
