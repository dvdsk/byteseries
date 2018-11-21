#![allow(dead_code)]

extern crate byteorder;
extern crate chrono;
extern crate ndarray;
extern crate num_traits;

use self::num_traits::cast::FromPrimitive;

use self::chrono::{DateTime, NaiveDateTime, Utc};

use std::fs::{File, OpenOptions};
use std::io::{Error, Read, Seek, SeekFrom, Write};

use self::byteorder::{ByteOrder, LittleEndian, WriteBytesExt};

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

struct Header {
    file: File,

    data: BTreeMap<i64, u64>,
    last_timestamp: i64,
    last_timestamp_numb: i64,

    current_timestamp: i64,

    next_timestamp: i64,
    next_timestamp_pos: u64,
}

impl Header {
    fn open<P: AsRef<Path>>(name: P) -> Result<Header, Error> {
        let mut file = open_and_check(name.as_ref().with_extension("h"), 16)?;

        //read in the entire file
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let mut data = BTreeMap::new();

        let last_timestamp = if bytes.len() == 0 {
            let timestamp = 0 as i64;
            //data.insert(timestamp, 0);
            //file.write_u64::<LittleEndian>(timestamp as u64)?;
            //file.write_u64::<LittleEndian>(0)?;

            timestamp
        } else {
            let mut numbers = vec![0u64; bytes.len() / 8];
            LittleEndian::read_u64_into(&bytes, numbers.as_mut_slice());

            for i in (0..numbers.len()).step_by(2) {
                data.insert(numbers[i] as i64, numbers[i + 1]);
            }

            numbers[numbers.len() - 2] as i64
        };

        trace!("last_timestamp: {}", last_timestamp);
        Ok(Header {
            file: file,
            data: data,
            last_timestamp: last_timestamp,
            last_timestamp_numb: last_timestamp / (u16::max_value() as i64),

            current_timestamp: 0, //FIXME init properly

            next_timestamp: 0,
            next_timestamp_pos: 0,
        })
    }

    fn update(
        &mut self,
        timestamp: i64,
        line_start: u64,
        new_timestamp_numb: i64,
    ) -> Result<(), Error> {
        let ts = timestamp as u64;
        self.file.write_u64::<LittleEndian>(ts)?;
        self.file.write_u64::<LittleEndian>(line_start)?;

        self.data.insert(timestamp, line_start);
        self.last_timestamp_numb = new_timestamp_numb;
        Ok(())
    }
}

enum DecodeOptions {
    Interleaved,
    Sequential(usize),
}

//rewrite using cont generics when availible
//https://github.com/rust-lang/rfcs/blob/master/text/2000-const-generics.md
pub struct Timeseries {
    data: File,
    header: Header, // add triple headers

    pub line_size: usize,
    full_line_size: usize,
    timestamp: i64,

    first_time_in_data: DateTime<Utc>,
    last_time_in_data: DateTime<Utc>,

    pub start_byte: u64,
    pub stop_byte: u64,

    decode_option: DecodeOptions,
}

// ---------------------------------------------------------------------
// -- we store only the last 4 bytes of the timestamp ------------------
// ---------------------------------------------------------------------

impl Timeseries {
    pub fn open<P: AsRef<Path>>(name: P, line_size: usize) -> Result<Timeseries, Error> {
        let mut data = open_and_check(name.as_ref().with_extension("dat"), line_size)?;
        let header = Header::open(name)?;

        let first_time = Self::get_first_time_in_data(&mut data);
        let last_time = Self::get_last_time_in_data();

        Ok(Timeseries {
            data: data,
            header: header, // add triple headers

            line_size: line_size,
            full_line_size: line_size + 2, //+2 accounts for u16 timestamp
            timestamp: 0,

            first_time_in_data: first_time,
            last_time_in_data: last_time,

            //these are set during: set_read_start, set_read_end then read is
            //bound by these points
            start_byte: 0,
            stop_byte: i64::max_value() as u64,

            decode_option: DecodeOptions::Sequential(8_000),
        })
    }

    fn get_first_time_in_data(data: &mut File) -> DateTime<Utc> {
        let mut buf = [0; 8]; //rewrite to use bufferd data
        if data.seek(SeekFrom::Start(0)).is_err() {
            data.read(&mut buf).unwrap();
            let timestamp = LittleEndian::read_u64(&buf) as i64;
            DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc)
        } else {
            warn!("file is empty");
            Utc::now()
        }
    }
    fn get_last_time_in_data() -> DateTime<Utc> {
        //let mut buf = [0; 8]; //rewrite to use bufferd data
        //let last_timestamp = if !file.seek(SeekFrom::End(-16)).is_err(){;
        //file.read(&mut buf);
        //LittleEndian::read_u64(&buf) as i64
        //} else {warn!("header is empty"); 0};
        Utc::now()
    }

    pub fn append(&mut self, time: DateTime<Utc>, line: &[u8]) -> Result<(), Error> {
        //TODO decide if a lock is needed here
        //write 16 bit timestamp and then the line to file
        let timestamp = self.time_to_line_timestamp(time);
        self.data.write(&timestamp)?;
        self.data.write(&line[..self.line_size])?;

        //write 64 bit timestamp to header
        //(needed no more then once every 18 hours)
        self.update_header()?;
        self.force_write_to_disk(); //FIXME should this be exposed to the user?

        self.last_time_in_data = time;
        Ok(())
    }

    fn update_header(&mut self) -> Result<(), Error> {
        let new_timestamp_numb = self.timestamp / (u16::max_value() as i64);
        if new_timestamp_numb > self.header.last_timestamp_numb {
            info!("updating file header");
            println!("{}", self.data.metadata().unwrap().len());
            let line_start = self.data.metadata().unwrap().len() - self.full_line_size as u64;
            self.header
                .update(self.timestamp, line_start, new_timestamp_numb)?;
        }
        Ok(())
    }

    fn force_write_to_disk(&mut self) {
        self.data.sync_data().unwrap();
        self.header.file.sync_data().unwrap();
    }

    fn get_search_bounds_and_timestampInfo(
        &mut self,
        start_time: DateTime<Utc>,
    ) -> (u64, u64, i64, i64, u64) {
        println!("header data {:?}", self.header.data);
        //maximum in map less/equeal then/to needed timestamp
        let (start_search, current_timestamp) = if let Some(header_line) = self
            .header
            .data
            .range(..(start_time.timestamp() + 1))
            .next_back()
        {
            (*header_line.1, *header_line.0)
        } else {
            //not found -> search till the end of file
            unimplemented!();
            #[allow(unreachable_code)]
            (0, 0)
        };
        //minimum in map greater then needed timestamp
        let (stop_search, next_timestamp, next_timestamp_pos) = if let Some(header_line) =
            self.header.data.range(start_time.timestamp() + 1..).next()
        {
            (*header_line.1, *header_line.0, *header_line.1)
        } else {
            //no minimum greater then needed timestamp -> search till the end of file
            let end_of_file = self.data.metadata().unwrap().len();
            //-1 prevents overflow
            //+2 makes sure we never switch to the next timestamp
            (end_of_file, i64::max_value() - 1, end_of_file + 2)
        };

        (
            start_search,
            stop_search,
            current_timestamp,
            next_timestamp,
            next_timestamp_pos,
        )
    }

    fn get_search_bounds(&mut self, start_time: DateTime<Utc>) -> (u64, u64, i64) {
        //maximum in map less/equeal then/to needed timestamp
        println!("header: {:?}",self.header.data);
        let (start_search, current_timestamp) = 
        if let Some(header_line) = self
            .header
            .data
            .range(..(start_time.timestamp() + 1))
            .next_back()
        {
            (*header_line.1, *header_line.0)
        } else {
            //not found -> search till the end of file
            unimplemented!();
            #[allow(unreachable_code)]
            (0, 0)
        };
        //minimum in map greater then needed timestamp
        let stop_search = if let Some(header_line) =
            self.header.data.range(start_time.timestamp() + 1..).next()
        {
            *header_line.1
        } else {
            //no minimum greater then needed timestamp -> search till the end of file
            let end_of_file = self.data.metadata().unwrap().len();
            end_of_file
        };

        (start_search, stop_search, current_timestamp)
    }

    pub fn set_bounds(
        &mut self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<(), Error> {
        self.set_read_stop(end_time)?;
        //setting the read start also positions the file seek
        //it is thus called last
        self.set_read_start(start_time)?;

        //set filepointer so read operations start at the right place
        self.data.seek(SeekFrom::Start(self.start_byte))?;
        Ok(())
    }

    fn set_read_start(&mut self, start_time: DateTime<Utc>) -> Result<(), Error> {
        //maximum in map less/equeal then/to needed timestamp
        let (start_search, stop_search, current_timestamp, next_timestamp, next_timestamp_pos) =
            self.get_search_bounds_and_timestampInfo(start_time);
        println!("start_search: {}", start_search);

        //set start full timestamp
        self.header.current_timestamp = current_timestamp;
        trace!("set first full ts to: {}", current_timestamp);
        //set next full timestamp
        self.header.next_timestamp = next_timestamp;
        self.header.next_timestamp_pos = next_timestamp_pos;
        trace!("set next full ts to: {}", next_timestamp);

        //compare partial (16 bit) timestamps in between the bounds
        let mut buf = vec![0u8; (stop_search - start_search) as usize];
        self.data.seek(SeekFrom::Start(start_search))?;
        self.data.read_exact(&mut buf)?;

        self.start_byte = start_search; //if we do not find anything pass the end of the search area
        for line_start in (0..buf.len()).step_by(self.full_line_size) {
            if LittleEndian::read_u16(&buf[line_start..line_start + 2])
                >= start_time.timestamp() as u16
            {
                println!("setting start_byte from liniar search, pos: {}", line_start);
                self.start_byte = start_search + line_start as u64;
                break;
            }
        }

        let line = &buf[(self.start_byte-start_search) as usize..(self.start_byte-start_search + 2) as usize];
        let timestamp_low = LittleEndian::read_u16(line) as u64;
        let timestamp_high = (self.header.current_timestamp as u64 >> 16) << 16;
        let timestamp = timestamp_high | timestamp_low;
        println!("starting read at ts: {}", timestamp);

        Ok(())
    }

    fn set_read_stop(&mut self, end_time: DateTime<Utc>) -> Result<(), Error> {
        let (start_search, stop_search, current_timestamp) = self.get_search_bounds(end_time);
        println!("stop_search {}", stop_search);
        println!("start_search {}", start_search);
        //compare partial (16 bit) timestamps in between these bounds
        let mut buf = vec![0u8; (stop_search - start_search) as usize];
        self.data.seek(SeekFrom::Start(start_search))?;
        self.data.read_exact(&mut buf)?;

        //
        if (current_timestamp as u64 >> 16) << 16 != (end_time.timestamp() as u64 >> 16) << 16 {
            warn!("no data younger then requested ts, returning most recent data");
            self.stop_byte = stop_search;
            return Ok(());
        }

        for line_start in (0..buf.len() - self.full_line_size + 1)
            .rev()
            .step_by(self.full_line_size)
        {
            println!("{}", line_start);
            if LittleEndian::read_u16(&buf[line_start..line_start + 2])
                <= end_time.timestamp() as u16
            {
                trace!("setting start_byte from liniar search, start of search area");
                self.stop_byte = start_search + line_start as u64;
                break;
            } else {
                trace!("could not find data younger then requested ts, returning most recent data");
                self.stop_byte = stop_search;
            }
        }

        let line = &buf[(self.stop_byte-start_search) as usize..(self.stop_byte -start_search + 2) as usize]; // panicked at 'index 160 out of range for slice of length 4'
        let timestamp_low = LittleEndian::read_u16(line) as u64;
        let timestamp_high = (current_timestamp as u64 >> 16) << 16;
        let timestamp = timestamp_high | timestamp_low;

        println!("stopping read after ts: {}", timestamp);

        Ok(())
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

    pub fn get_timestamp<T>(&mut self, line: &[u8], pos: u64) -> T
    where
        T: FromPrimitive,
    {
        //update full timestamp when needed
        //println!("self.header.current_timestamp: {}, ",self.header.current_timestamp);
        if pos + 1 > self.header.next_timestamp_pos {
            //println!("next ts pos: {}", self.header.next_timestamp_pos);
            //update current timestamp
            self.header.current_timestamp = self.header.next_timestamp;

            //set next timestamp and timestamp pos
            //minimum in map greater then current timestamp
            if let Some(next) = self
                .header
                .data
                .range(self.header.current_timestamp + 1..)
                .next()
            {
                self.header.next_timestamp = *next.0;
                self.header.next_timestamp_pos = *next.1;
            } else {
                //TODO handle edge case, last full timestamp
                self.header.next_timestamp = 0;
                self.header.next_timestamp_pos = u64::max_value();
            }
        }
        let timestamp_low = LittleEndian::read_u16(line) as u64;
        let timestamp_high = (self.header.current_timestamp as u64 >> 16) << 16;
        let timestamp = timestamp_high | timestamp_low;
        //print!("decoded TS: {}, ",timestamp);
        T::from_u64(timestamp).unwrap()
    }
}

impl Read for Timeseries {
    //guarantees we always discrete lines
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        let mut nread = self.data.read(buf)?;

        nread = if self.start_byte + nread as u64 >= self.stop_byte {
            println!(
                "diff: {}, {}, {}",
                self.start_byte as i64,
                self.stop_byte as i64,
                self.stop_byte as i64 - self.start_byte as i64
            );
            (self.stop_byte - self.start_byte) as usize
        } else {
            nread
        };
        self.start_byte += nread as u64;
        Ok(nread - nread % self.full_line_size)
    }
}

impl Timeseries {
    pub fn decode_sequential_time_only(
        &mut self,
        lines_to_read: usize,
    ) -> Result<(Vec<u64>, Vec<u8>), Error> {
        //let mut buf = Vec::with_capacity(lines_to_read*self.full_line_size);
        let mut buf = vec![0; lines_to_read * self.full_line_size];

        let mut timestamps: Vec<u64> = Vec::with_capacity(lines_to_read);
        let mut decoded: Vec<u8> = Vec::with_capacity(lines_to_read);

        let n_read = self.data.read(&mut buf).unwrap() as usize;
        println!("read: {} bytes", n_read);
        let mut file_pos = self.start_byte;
        for (_i, line) in buf[..n_read].chunks(self.full_line_size).enumerate() {
            file_pos += self.full_line_size as u64;
            timestamps.push(self.get_timestamp::<u64>(line, file_pos));
            decoded.extend_from_slice(&line[2..]);
        }
        Ok((timestamps, decoded))
    }
}

//open file and check if it has the right lenght
//(an interger multiple of the line lenght) if it
//has not warn and repair by truncating
fn open_and_check(path: PathBuf, full_line_size: usize) -> Result<File, Error> {
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(path)?;
    let metadata = file.metadata()?;

    let rest = metadata.len() % (full_line_size as u64);
    if rest > 0 {
        warn!("Last write incomplete, truncating to largest multiple of the line size");
        file.set_len(metadata.len() - rest)?;
    }
    Ok(file)
}

//----------------------------------------------------------------------
//----------------------------------------------------------------------
#[macro_use]
extern crate log;
extern crate fxhash;

#[cfg(test)]
mod tests {
    use super::byteorder::NativeEndian;
    use super::chrono::{DateTime, NaiveDateTime, Utc};
    use super::*;

    fn insert_uniform_arrays(
        data: &mut Timeseries,
        n_to_insert: u32,
        _step: i64,
        line_size: usize,
        time: DateTime<Utc>,
    ) {
        let mut timestamp = time.timestamp();
        for i in 0..n_to_insert {
            let buffer = vec![i as u8; line_size];

            let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
            data.append(dt, buffer.as_slice()).unwrap();
            timestamp += 5;
        }
    }

    mod appending {
        use super::fxhash::hash64;
        use super::*;
        use std::fs;
        use std::path::Path;

        fn insert_timestamp_hashes(
            data: &mut Timeseries,
            n_to_insert: u32,
            step: i64,
            time: DateTime<Utc>,
        ) {
            let mut timestamp = time.timestamp();

            for _ in 0..n_to_insert {
                let hash = hash64::<i64>(&timestamp);

                let mut buffer = Vec::with_capacity(8);
                &buffer.write_u64::<NativeEndian>(hash).unwrap();

                let dt =
                    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
                data.append(dt, buffer.as_slice()).unwrap();
                timestamp += step;
            }
        }

        fn insert_timestamp_arrays(
            data: &mut Timeseries,
            n_to_insert: u32,
            step: i64,
            time: DateTime<Utc>,
        ) {
            let mut timestamp = time.timestamp();

            for _ in 0..n_to_insert {
                let mut buffer = Vec::with_capacity(8);

                let dt =
                    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
                &buffer.write_i64::<NativeEndian>(timestamp).unwrap();

                data.append(dt, buffer.as_slice()).unwrap();
                timestamp += step;
            }
        }

        #[test]
        fn basic() {
            if Path::new("test_append.h").exists() {
                fs::remove_file("test_append.h").unwrap();
            }
            if Path::new("test_append.data").exists() {
                fs::remove_file("test_append.data").unwrap();
            }
            const LINE_SIZE: usize = 10;
            const STEP: i64 = 5;
            const N_TO_INSERT: u32 = 100;

            let time = Utc::now();

            let mut data = Timeseries::open("test_append", LINE_SIZE).unwrap();
            insert_uniform_arrays(&mut data, N_TO_INSERT, STEP, LINE_SIZE, time);

            assert_eq!(
                data.data.metadata().unwrap().len(),
                ((LINE_SIZE + 2) as u32 * N_TO_INSERT) as u64
            );
            assert_eq!(data.header.file.metadata().unwrap().len(), 16);
        }

        #[test]
        fn test_set_read() {
            if Path::new("test_set_read.h").exists() {
                fs::remove_file("test_set_read.h").unwrap();
            }
            if Path::new("test_set_read.data").exists() {
                fs::remove_file("test_set_read.data").unwrap();
            }
            const LINE_SIZE: usize = 10;
            const STEP: i64 = 5;
            const N_TO_INSERT: u32 = 100;

            let time = Utc::now();
            let timestamp = time.timestamp();
            //println!("now: {}, u8_ts: {}", time, timestamp as u8 );

            let mut data = Timeseries::open("test_set_read", LINE_SIZE).unwrap();
            insert_uniform_arrays(&mut data, N_TO_INSERT, STEP, LINE_SIZE, time);
            //println!("data length: {}",data.data.metadata().unwrap().len());

            let t1 = DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(timestamp + 2 * STEP, 0),
                Utc,
            );

            data.set_read_start(t1).unwrap(); //println!("start_byte: {}",data.start_byte);
            assert_eq!(data.start_byte, ((data.line_size + 2) * 2) as u64);
        }

        #[test]
        fn hashes_then_verify() {
            const NUMBER_TO_INSERT: i64 = 8_00;
            const PERIOD: i64 = 5;

            if Path::new("test_append_hashes_then_verify.h").exists() {
                fs::remove_file("test_append_hashes_then_verify.h").unwrap();
            }
            if Path::new("test_append_hashes_then_verify.data").exists() {
                fs::remove_file("test_append_hashes_then_verify.data").unwrap();
            }

            let time = Utc::now();

            let mut data = Timeseries::open("test_append_hashes_then_verify", 8).unwrap();
            insert_timestamp_hashes(&mut data, NUMBER_TO_INSERT as u32, PERIOD, time);
            //println!("inserted test data");

            let timestamp = time.timestamp();
            let t1 = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
            let t2 = DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(timestamp + NUMBER_TO_INSERT * PERIOD, 0),
                Utc,
            );

            data.set_read_start(t1).unwrap();
            data.set_read_stop(t2);
            assert_eq!(
                (data.stop_byte - data.start_byte) / ((data.line_size + 2) as u64),
                (NUMBER_TO_INSERT) as u64
            );

            let n = 8_000;
            let loops_to_check_everything =
                NUMBER_TO_INSERT / n + if NUMBER_TO_INSERT % n > 0 { 1 } else { 0 };
            for _ in 0..loops_to_check_everything {
                //println!("loop, {} at the time",n);
                let (timestamps, decoded) = data.decode_sequential_time_only(n as usize).unwrap();
                //println!("timestamps: {:?}", timestamps);
                for (timestamp, decoded) in timestamps.iter().zip(decoded.chunks(data.line_size)) {
                    let hash1 = hash64::<i64>(&(*timestamp as i64));
                    let hash2 = NativeEndian::read_u64(decoded);
                    assert_eq!(hash1, hash2);
                }
            }
        }

        #[test]
        fn timestamps_then_verify() {
            const NUMBER_TO_INSERT: i64 = 8_00;
            const PERIOD: i64 = 5;

            if Path::new("test_append_timestamps_then_verify.h").exists() {
                fs::remove_file("test_append_timestamps_then_verify.h").unwrap();
            }
            if Path::new("test_append_timestamps_then_verify.data").exists() {
                fs::remove_file("test_append_timestamps_then_verify.data").unwrap();
            }

            let time = Utc::now();

            let mut data = Timeseries::open("test_append_timestamps_then_verify", 8).unwrap();
            insert_timestamp_arrays(&mut data, NUMBER_TO_INSERT as u32, PERIOD, time);
            //println!("inserted test data");

            let timestamp = time.timestamp();
            let t1 = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
            let t2 = DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(timestamp + NUMBER_TO_INSERT * PERIOD, 0),
                Utc,
            );

            data.set_read_start(t1).unwrap();
            data.set_read_stop(t2);
            assert_eq!(
                (data.stop_byte - data.start_byte) / ((data.line_size + 2) as u64),
                (NUMBER_TO_INSERT) as u64
            );

            let n = 8_000;
            let loops_to_check_everything =
                NUMBER_TO_INSERT / n + if NUMBER_TO_INSERT % n > 0 { 1 } else { 0 };
            for _ in 0..loops_to_check_everything {
                //println!("loop, {} at the time",n);
                let (timestamps, decoded) = data.decode_sequential_time_only(n as usize).unwrap();
                //println!("timestamps: {:?}", timestamps);
                for (timestamp, decoded) in timestamps.iter().zip(decoded.chunks(data.line_size)) {
                    let ts_from_decode = *timestamp as i64;
                    let ts_from_data = NativeEndian::read_i64(decoded);

                    //println!("ts_from_decode: {}, ts_from_data: {}",ts_from_decode,ts_from_data);
                    assert_eq!(ts_from_decode, ts_from_data);
                }
            }
            assert_eq!(2 + 2, 4);
        }

        /*#[test]
        fn multiple_hashes_then_verify() {
            const NUMBER_TO_INSERT: i64 = 8_00; 
            const PERIOD: i64 = 5;
            
            if Path::new("test_multiple_append_hashes_then_verify.h").exists()    {fs::remove_file("test_multiple_append_hashes_then_verify.h").unwrap();   }
            if Path::new("test_multiple_append_hashes_then_verify.data").exists() {fs::remove_file("test_multiple_append_hashes_then_verify.data").unwrap();}
            let time = Utc::now();
                    
            let mut data = Timeseries::open("test_multiple_append_hashes_then_verify", 8).unwrap();
            insert_timestamp_arrays(&mut data, NUMBER_TO_INSERT as u32 /3 , PERIOD, time);
            let last_time_in_data = data.last_time_in_data;
            insert_timestamp_arrays(&mut data, NUMBER_TO_INSERT as u32 /3 , PERIOD, last_time_in_data);
            //println!("inserted test data");

            let timestamp = time.timestamp();
            let t1 = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
            let t2 = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp + (2*NUMBER_TO_INSERT/3-20)*PERIOD, 0), Utc);
            
            data.set_read_start(t1).unwrap();
            data.set_read_stop( t2);
            assert_eq!((data.stop_byte-data.start_byte)/((data.line_size+2) as u64), (2*NUMBER_TO_INSERT/3) as u64);

            let n = 8_000;
            let loops_to_check_everything = 2*NUMBER_TO_INSERT/3/n + if 2*NUMBER_TO_INSERT/3 % n > 0 {1} else {0};
            for _ in 0..loops_to_check_everything {
                //println!("loop, {} at the time",n);
                let (timestamps, decoded) = data.decode_sequential_time_only(n as usize).unwrap();
                //println!("timestamps: {:?}", timestamps);
                for (timestamp, decoded) in timestamps.iter().zip(decoded.chunks(data.line_size)){
                    let ts_from_decode = *timestamp as i64;
                    let ts_from_data = NativeEndian::read_i64(decoded);
                    
                    //println!("ts_from_decode: {}, ts_from_data: {}",ts_from_decode,ts_from_data);
                    assert_eq!(ts_from_decode, ts_from_data);
                }
            }

            let last_time_in_data = data.last_time_in_data;        
            insert_timestamp_arrays(&mut data, NUMBER_TO_INSERT as u32 /3, PERIOD, last_time_in_data);

            data.set_read_start(t2).unwrap();
            let t3 = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp + (3*NUMBER_TO_INSERT)*PERIOD+1000, 0), Utc);
            data.set_read_stop( t3);
            assert_eq!((data.stop_byte-data.start_byte)/((data.line_size+2) as u64), (NUMBER_TO_INSERT) as u64);

            let n = 8_000;
            let loops_to_check_everything = NUMBER_TO_INSERT/3/n + if NUMBER_TO_INSERT/3 % n > 0 {1} else {0};
            for _ in 0..loops_to_check_everything {
                //println!("loop, {} at the time",n);
                let (timestamps, decoded) = data.decode_sequential_time_only(n as usize).unwrap();
                //println!("timestamps: {:?}", timestamps);
                for (timestamp, decoded) in timestamps.iter().zip(decoded.chunks(data.line_size)){
                    let ts_from_decode = *timestamp as i64;
                    let ts_from_data = NativeEndian::read_i64(decoded);
                    
                    //println!("ts_from_decode: {}, ts_from_data: {}",ts_from_decode,ts_from_data);
                    assert_eq!(ts_from_decode, ts_from_data);
                }
            }
        }*/
    }

    mod seek {
        use super::*;
        use std::fs;
        use std::path::Path;

        #[test]
        fn varing_length() {
            if Path::new("test_varing_length.h").exists() {
                fs::remove_file("test_varing_length.h").unwrap();
            }
            if Path::new("test_varing_length.data").exists() {
                fs::remove_file("test_varing_length.data").unwrap();
            }
            const LINE_SIZE: usize = 8;
            const STEP: i64 = 5;
            const N_TO_INSERT: u32 = 100;
            let start_read_inlines = 10;
            let read_length_inlines = 10;

            //let time = Utc::now();
            let time = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(1539180000, 0), Utc);
            let timestamp = time.timestamp();
            println!("start timestamp {}", timestamp);
            let mut data = Timeseries::open("test_varing_length", LINE_SIZE).unwrap();

            insert_uniform_arrays(&mut data, N_TO_INSERT, STEP, LINE_SIZE, time);

            //TODO for loop this over random sizes
            let t1 = DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(timestamp + start_read_inlines * STEP, 0),
                Utc,
            );
            let t2 = DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(
                    timestamp + (start_read_inlines + read_length_inlines) * STEP,
                    0,
                ),
                Utc,
            );

            data.set_bounds(t1, t2);
            println!(
                "t1: {}, t2: {}, start_byte: {}, stop_byte: {}",
                t1.timestamp(),
                t2.timestamp(),
                data.start_byte,
                data.stop_byte
            );
            let (timestamps, decoded) = data
                .decode_sequential_time_only(read_length_inlines as usize)
                .unwrap();
            //println!("timestamps: {:?}", timestamps);

            assert_eq!(
                data.start_byte,
                (start_read_inlines * (LINE_SIZE as i64 + 2)) as u64
            );
            assert_eq!(
                (data.stop_byte - data.start_byte) / ((LINE_SIZE as u64 + 2) as u64),
                read_length_inlines as u64
            );

            let (timestamps, decoded) = data.decode_sequential_time_only(10).unwrap();
            assert!(
                timestamps[0] as i64 >= timestamp + 10 * STEP
                    && timestamps[0] as i64 <= timestamp + 20 * STEP
            );
        }

        #[test]
        fn correct_data() {}

    }
}
