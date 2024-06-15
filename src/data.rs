use byteorder::{ByteOrder, LittleEndian};
use num_traits::cast::FromPrimitive;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use time::OffsetDateTime;

use crate::header::Header;
use crate::util::open_and_check;
use crate::{Decoder, Error};

#[derive(Debug)]
pub struct ByteSeries {
    pub(crate) data: File,
    pub(crate) header: Header,

    pub(crate) line_size: usize,
    pub(crate) full_line_size: usize,
    timestamp: i64,

    pub(crate) first_time_in_data: Option<i64>,
    pub(crate) last_time_in_data: Option<i64>,

    pub(crate) data_size: u64,
}

// ---------------------------------------------------------------------
// -- we store only the last 4 bytes of the timestamp ------------------
// ---------------------------------------------------------------------
#[derive(Debug)]
pub(crate) struct FullTime {
    pub(crate) curr: i64,
    pub(crate) next: Option<i64>,
    pub(crate) next_pos: Option<u64>,
}

impl ByteSeries {
    pub fn open<P: AsRef<Path>>(name: P, line_size: usize) -> Result<ByteSeries, Error> {
        let full_line_size = line_size + 2; //+2 accounts for u16 timestamp
        let (mut data, size) = open_and_check(name.as_ref().with_extension("dat"), line_size + 2)?;
        let header = Header::open(name)?;

        let first_time = header.first_time_in_data();
        let last_time = Self::load_last_time_in_data(&mut data, &header, full_line_size);

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

    pub fn last_line<'a, T: std::fmt::Debug + std::clone::Clone>(
        &mut self,
        decoder: &'a mut (dyn Decoder<T> + 'a),
    ) -> Result<(OffsetDateTime, Vec<T>), Error> {
        let (time, data) = self.last_line_raw()?;
        let data = decoder.decoded(&data);
        Ok((time, data))
    }

    pub fn last_line_raw(&mut self) -> Result<(OffsetDateTime, Vec<u8>), Error> {
        let (time, bytes) = self.decode_last_line()?;
        let time = OffsetDateTime::from_unix_timestamp(time)
            .expect("only current timestamps are written to file");
        Ok((time, bytes))
    }

    fn load_last_time_in_data(
        data: &mut File,
        header: &Header,
        full_line_size: usize,
    ) -> Option<i64> {
        let mut buf = [0u8; 2]; //rewrite to use bufferd data
        if data.seek(SeekFrom::End(-(full_line_size as i64))).is_ok() {
            data.read_exact(&mut buf).unwrap();
            let timestamp_low = LittleEndian::read_u16(&buf) as i64;
            let timestamp_high = header.last_timestamp & !0b1_1111_1111_1111_1111;
            let timestamp = timestamp_high | timestamp_low;
            Some(timestamp)
        } else {
            tracing::warn!("file is empty");
            None
        }
    }

    /// Append the line and force a flush to disk.
    /// When this returns all data is safely stored
    pub fn append_flush(&mut self, time: OffsetDateTime, line: &[u8]) -> Result<(), Error> {
        self.append_fast(time, line)?;
        self.force_write_to_disk();
        Ok(())
    }

    /// Append data to disk but do not flush, a crash can still lead to the data being lost
    pub fn append_fast(&mut self, time: OffsetDateTime, line: &[u8]) -> Result<(), Error> {
        //TODO decide if a lock is needed here
        //write 16 bit timestamp and then the line to file
        let timestamp = self.time_to_line_timestamp(time);
        self.data.write_all(&timestamp)?;
        self.data.write_all(&line[..self.line_size])?;

        //write 64 bit timestamp to header
        //(needed no more then once every 18 hours)
        self.update_header()?;
        self.data_size += self.full_line_size as u64;

        self.last_time_in_data = Some(time.unix_timestamp());
        self.first_time_in_data.get_or_insert(time.unix_timestamp());
        Ok(())
    }

    // needs to be called before self.data_size is increased
    fn update_header(&mut self) -> Result<(), Error> {
        let new_timestamp_numb = self.timestamp / 2i64.pow(16);
        if new_timestamp_numb > self.header.last_timestamp_numb {
            tracing::info!("updating file header");
            let line_start = self.data_size;
            self.header
                .update(self.timestamp, line_start, new_timestamp_numb)?;
        }
        Ok(())
    }

    /// asks the os to write its buffers out before
    /// continuing
    pub(crate) fn force_write_to_disk(&mut self) {
        self.data.sync_data().unwrap();
        self.header.file.sync_data().unwrap();
    }

    fn time_to_line_timestamp(&mut self, time: OffsetDateTime) -> [u8; 2] {
        //for now no support for sign bit since data will always be after 0 (1970)
        self.timestamp = time.unix_timestamp();
        if self.timestamp < 0 {
            panic!("dates before 1970 are not supported")
        }

        //we store the timestamp in little endian Signed magnitude representation
        //(least significant (lowest value) byte at lowest address)
        //for the line timestamp we use only the 2 lower bytes
        let mut line_timestamp = [0; 2];
        LittleEndian::write_u16(&mut line_timestamp, self.timestamp as u16);
        line_timestamp
    }

    pub(crate) fn get_timestamp<T>(&mut self, line: &[u8], pos: u64, full_ts: &mut FullTime) -> T
    where
        T: FromPrimitive,
    {
        //update full timestamp when needed
        if full_ts
            .next_pos
            .map_or(false, |next_pos| pos + 1 > next_pos)
        {
            tracing::debug!(
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
                tracing::debug!(
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
    pub(crate) fn read(
        &mut self,
        buf: &mut [u8],
        start_byte: u64,
        stop_byte: u64,
    ) -> Result<usize, Error> {
        self.data.seek(SeekFrom::Start(start_byte))?;
        let nread = self.data.read(buf)?;
        let left = stop_byte + self.full_line_size as u64 - start_byte;
        let nread = nread.min(left as usize);
        let nread = nread - nread % self.full_line_size;
        Ok(nread)
    }

    pub(crate) fn decode_last_line(&mut self) -> Result<(i64, Vec<u8>), Error> {
        if self.data_size < self.full_line_size as u64 {
            return Err(Error::NoData);
        }

        let start_byte = self.data_size - self.full_line_size as u64;
        let stop_byte = self.data_size;

        let mut full_line = vec![0; self.full_line_size];
        let nread = self.read(&mut full_line, start_byte, stop_byte)?;

        if nread < self.full_line_size {
            return Err(Error::PartialLine);
        }

        full_line.remove(0);
        full_line.remove(0);
        let line = full_line;
        let time = self.last_time_in_data.ok_or(Error::NoData)?;
        Ok((time, line))
    }

    pub(crate) fn last_time_in_data(&self) -> Option<OffsetDateTime> {
        self.last_time_in_data
            .map(OffsetDateTime::from_unix_timestamp)
            .map(|res| res.expect("only current timestamps are used"))
    }
}
