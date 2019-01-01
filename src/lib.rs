#![allow(dead_code)]

extern crate byteorder;
extern crate chrono;
extern crate ndarray;
extern crate num_traits;

use self::num_traits::cast::FromPrimitive;

use self::chrono::{DateTime, NaiveDateTime, Utc};

use std::fs::{File, OpenOptions};
use std::io::{Error, Read, Seek, SeekFrom, Write, ErrorKind};

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
        let (mut file, _) = open_and_check(name.as_ref().with_extension("h"), 16)?;

        //read in the entire file
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let mut data = BTreeMap::new();

        let last_timestamp = if bytes.len() == 0 {
            0 as i64
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
        trace!("wrote headerline: {}, {}", ts, line_start);

        self.data.insert(timestamp, line_start);
        self.last_timestamp_numb = new_timestamp_numb;
        Ok(())
    }
}

enum DecodeOptions {
  Interleaved,
  Sequential(usize),
}

struct SearchBounds {
  start: u64,
  stop: u64,
}

pub struct DecodeParams {
  current_timestamp: i64,
  next_timestamp: i64,
  next_timestamp_pos: u64,
}

enum SbResult {
	Clipped,
	Bounded(SearchBounds),
}

pub enum BoundResult {
	IoError(Error),
	NoData,
	Ok((u64, u64, DecodeParams)),// read_params, start_byte, stop_byte
}

//TODO implement this to use the ? operator in get_bounds
// impl std::ops::Try for BoundResult{

// }

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

		pub data_size: u64,

    decode_option: DecodeOptions,
}

// ---------------------------------------------------------------------
// -- we store only the last 4 bytes of the timestamp ------------------
// ---------------------------------------------------------------------

impl Timeseries {
    pub fn open<P: AsRef<Path>>(name: P, line_size: usize) -> Result<Timeseries, Error> {
        let full_line_size = line_size + 2; //+2 accounts for u16 timestamp
        let (mut data, size) = open_and_check(name.as_ref().with_extension("dat"), line_size+2)?;
        let header = Header::open(name)?;

        let first_time = Self::get_first_time_in_data(&mut data);
        let last_time = Self::get_last_time_in_data();

        Ok(Timeseries {
            data: data,
            header: header, // add triple headers

            line_size: line_size,
            full_line_size: full_line_size, //+2 accounts for u16 timestamp
            timestamp: 0,

            first_time_in_data: first_time,
            last_time_in_data: last_time,

            //these are set during: set_read_start, set_read_end then read is
            //bound by these points
            data_size: size,

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
				self.data_size += self.full_line_size as u64;
        self.last_time_in_data = time;
        Ok(())
    }

    pub fn append_fast(&mut self, time: DateTime<Utc>, line: &[u8]) -> Result<(), Error> {
        //TODO decide if a lock is needed here
        //write 16 bit timestamp and then the line to file
        let timestamp = self.time_to_line_timestamp(time);
        self.data.write(&timestamp)?;
        self.data.write(&line[..self.line_size])?;

        //write 64 bit timestamp to header
        //(needed no more then once every 18 hours)
        self.update_header()?;
				self.data_size += self.full_line_size as u64;
        self.last_time_in_data = time;
        Ok(())
    }

    fn update_header(&mut self) -> Result<(), Error> {
        let new_timestamp_numb = self.timestamp / 2i64.pow(16);
        //println!("inserting; ts_low: {}, ts_numb: {}",self.timestamp as u16, new_timestamp_numb);
        if new_timestamp_numb > self.header.last_timestamp_numb {
            info!("updating file header");
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

		//Search for start bounds
		//
		// Finds the requested TS, depending on if it is in the data do the following:
		//
		// case_A -- start_of_data -- case_B -- ?case_C?-- end_of_data -- ?case_C?--  case_D
		//
		// case A: requested TS before start of data
		//	-> CLIP [set read to start of file?]
		// case B: requested TS guaranteed within data
		//	-> SEARCH [largest header before B time, entry after B or EOF]
		// case C: requested TS might be within data or after
		//	-> SEARCH [largest header before B time, EOF]
		// case D: requested TS guaranteed outside of data
		//	-> ERROR
		//
    fn startread_search_bound( &mut self,start_time: DateTime<Utc>, ) -> Option<(SbResult, DecodeParams)> {
        debug!("header data {:?}", self.header.data);
        //get header timestamp =< timestamp, marks begin of search area
        if let Some(header_line) = self
            .header
            .data
            .range(..(start_time.timestamp() + 1))
            .next_back()
        {
						//Case B, C or D
            let start_search = *header_line.1;
            let start_timestamp = *header_line.0;

            //timestamp in header >= then sought timestamp, marks end of search area
				    if let Some(header_line) = self.header.data.range(start_time.timestamp() + 1..).next(){
				    		//Case B -> return search area
				        let next_timestamp = *header_line.0;
				        let next_timestamp_pos = *header_line.1;
				        let stop_search = next_timestamp_pos;
						    return Some((
						    	SbResult::Bounded(SearchBounds {start: start_search, stop: stop_search }),
									DecodeParams {current_timestamp: start_timestamp, next_timestamp, next_timestamp_pos }
								));
				    } else {
				        //Case C or D -> determine which
				        if start_time <= self.last_time_in_data {
									//Case C ->return search area clipped at EOF
						      let next_timestamp = i64::max_value() - 1;
						      //search at the most to the end of the file
									let end_of_file = self.data.metadata().unwrap().len();
						      let stop_search = end_of_file.saturating_sub(self.full_line_size as u64);
						      //never switch to a new full timestamp as there are non
						      let next_timestamp_pos = end_of_file + 2; //TODO refactor try stop_search = next timestamp pos
						      return Some((
						      	SbResult::Bounded(SearchBounds {start: start_search, stop: stop_search }),
										DecodeParams {current_timestamp: start_timestamp, next_timestamp, next_timestamp_pos }
									));
				        } else {
				        	debug!("start_time: {}, last_in_data: {}", start_time, self.last_time_in_data);
				        	//Case D -> no data within user requested interval
						      return None;
				        }
				    };
        } else {
            //Case A -> clip to start of file
            warn!("start TS earlier then start of data -> start_byte = 0");
						//there should always be a header in a non empty file, thus if start_time results in
						//Case A then the following cant fail.
						let header_line = self.header.data.range(start_time.timestamp() + 1..).next()
						.expect("no header found, these should always be one header! datafile is corrupt");
						//get the start timestamp from this header
		        let start_timestamp = *header_line.0;

						//check if there is another header
						let decode_params = if let Some(header_line) = self.header.data.range(start_timestamp + 1..).next(){
							let next_timestamp = *header_line.0;
				      let next_timestamp_pos = *header_line.1;
            	DecodeParams {current_timestamp: start_timestamp, next_timestamp, next_timestamp_pos}
						} else {
							//use safe defaults
							let end_of_file = self.data.metadata().unwrap().len();
							let next_timestamp = i64::max_value() - 1; //-1 prevents overflow
				      let next_timestamp_pos = end_of_file + 2;  //+2 makes sure we never switch to the next timestamp
            	DecodeParams {current_timestamp: start_timestamp, next_timestamp, next_timestamp_pos}
						};

            return Some((
            	SbResult::Clipped,
            	decode_params,
            ));
        };
    }

    fn find_read_start(&mut self, start_time: DateTime<Utc>, search_params: SearchBounds) -> Result<u64, Error> {
        //compare partial (16 bit) timestamps in between the bounds
        let mut buf = vec![0u8; (search_params.stop - search_params.start) as usize];
        self.data.seek(SeekFrom::Start(search_params.start))?;
        self.data.read_exact(&mut buf)?;

        for line_start in (0..buf.len().saturating_sub(2)).step_by(self.full_line_size) {
            if LittleEndian::read_u16(&buf[line_start..line_start + 2])
                >= start_time.timestamp() as u16
            {
                debug!("setting start_byte from liniar search, pos: {}", line_start);
                let start_byte = search_params.start + line_start as u64;
                return Ok(start_byte);
            }
        }

				//no data more recent then start time within bounds, return location of most recent data
        Ok(search_params.stop)
    }


		//Search for stop bounds
		//
		// Finds the requested TS, depending on if it is in the data do the following:
		//
		// case_A -- start_of_data -- case_B -- ?case_C?-- end_of_data -- ?case_C?--  case_D
		//
		// case A: requested TS before start of data
		//	-> ERROR, no data can possibly be read now
		// case B: requested TS guaranteed within data
		//	-> SEARCH [largest header before B time, entry after B or EOF]
		// case C: requested TS might be within data or after
		//	-> SEARCH [largest header before B time, EOF]
		// case D: requested TS guaranteed outside of data
		//	-> CLIP, clipping to end
		//
    fn stopread_search_bounds(&mut self, start_time: DateTime<Utc>) -> Option<SbResult> {
        debug!("header data {:?}", self.header.data);
        //get header timestamp =< timestamp, marks begin of search area
        if let Some(header_line) = self
            .header
            .data
            .range(..(start_time.timestamp() + 1))
            .next_back()
        {
						//Case B, C or D
            let start_search = *header_line.1;

            //timestamp in header >= then sought timestamp, marks end of search area
				    if let Some(header_line) = self.header.data.range(start_time.timestamp() + 1..).next(){
				    		//Case B -> return search area
				    		let next_timestamp_pos = *header_line.1;
				        let stop_search = next_timestamp_pos;
						    return Some(SbResult::Bounded(SearchBounds {start: start_search, stop: stop_search}));
				    } else {
				        //Case D or C -> determine which
				        if start_time <= self.last_time_in_data {
									//Case D ->return search area
						      //search at the most to the end of the file
									let end_of_file = self.data.metadata().unwrap().len();
						      let stop_search = end_of_file.saturating_sub(self.full_line_size as u64);
						      return Some(SbResult::Bounded(SearchBounds {start: start_search, stop: stop_search}));
				        } else {
				        	//Case D
						      return Some(SbResult::Clipped);
				        }
				    };
        } else {
            //Case A -> ERROR
            warn!("start TS earlier then start of data -> start_byte = 0");
            return None;
        };
    }

    pub fn get_bounds(
        &mut self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> BoundResult {
    		//check if the datafile isnt empty

    		if self.data_size == 0 {
          return BoundResult::NoData;
    		}

				let start_bounds = self.startread_search_bound(start_time);
				if start_bounds.is_none() {
					let error = Error::new(ErrorKind::NotFound, "start_time TS more recent then last data");
					return BoundResult::IoError(error);
				}

				let stop_bounds = self.stopread_search_bounds(end_time);
				if stop_bounds.is_none() {
					let error = Error::new(ErrorKind::NotFound, "end_time older then oldest data");
					return BoundResult::IoError(error);
				}

				//must be a solvable request
				let (case, decode_params) = start_bounds.unwrap();
				let start_byte = match case {
					SbResult::Bounded(search_bounds) => {
						//TODO change to use ? operator
						let start_byte = self.find_read_start(start_time, search_bounds);
						if let Err(err) = start_byte {return BoundResult::IoError(err);}
						start_byte.unwrap()
					}
					SbResult::Clipped => 0,
				};

				let case = stop_bounds.unwrap();
				let stop_byte = match case {
					SbResult::Bounded(search_bounds) => {
						//TODO change to use ? operator
						let stop_byte = self.find_read_stop(end_time, search_bounds);
						if let Err(err) = stop_byte {return BoundResult::IoError(err);}
						stop_byte.unwrap()
					}
					SbResult::Clipped => {
						let end_of_file = self.data.metadata().unwrap().len();
						end_of_file.saturating_sub(self.full_line_size as u64)
					},
				};

				debug!("start time: {}, {}; end_time: {}, {}",
					start_time, start_time.timestamp(), end_time, end_time.timestamp());
				debug!("start_byte: {}", start_byte);

        BoundResult::Ok((start_byte, stop_byte, decode_params))
    }

    fn find_read_stop(&mut self, end_time: DateTime<Utc>, search_params: SearchBounds) -> Result<u64, Error> {
        //compare partial (16 bit) timestamps in between these bounds
        let mut buf = vec![0u8; (search_params.stop - search_params.start) as usize];
        self.data.seek(SeekFrom::Start(search_params.start))?;
        self.data.read_exact(&mut buf)?;

				trace!("buf.len(): {}",buf.len());
        for line_start in (0..buf.len() - self.full_line_size + 1)
           .rev()
           .step_by(self.full_line_size)
        {
            //trace!("line: {}, {}", line_start, LittleEndian::read_u16(&buf[line_start..line_start + 2]));
            if LittleEndian::read_u16(&buf[line_start..line_start + 2]) <= end_time.timestamp() as u16
            {
                debug!("setting start_byte from liniar search, start of search area");
                let stop_byte = search_params.start + line_start as u64;
                return Ok(stop_byte);
            }
        }
        Ok(search_params.stop)
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

    pub fn get_timestamp<T>(&mut self, line: &[u8], pos: u64, decode_params: &mut DecodeParams) -> T
    where
        T: FromPrimitive,
    {
        //update full timestamp when needed
        if pos + 1 > decode_params.next_timestamp_pos {
            debug!("updating ts, pos: {}, next ts pos: {}", pos, decode_params.next_timestamp_pos);
            //update current timestamp
            decode_params.current_timestamp = decode_params.next_timestamp;

            //set next timestamp and timestamp pos
            //minimum in map greater then current timestamp
            if let Some(next) = self
                .header
                .data
                .range(decode_params.current_timestamp + 1..)
                .next()
            {
                decode_params.next_timestamp = *next.0;
                decode_params.next_timestamp_pos = *next.1;
            } else {
                //TODO handle edge case, last full timestamp
                debug!("loaded last timestamp in header, no next TS, current pos: {}",pos);
                decode_params.next_timestamp = 0;
                decode_params.next_timestamp_pos = u64::max_value();
            }
        }
        let timestamp_low = LittleEndian::read_u16(line) as u64;
        let timestamp_high = (decode_params.current_timestamp as u64 >> 16) << 16;
        let timestamp = timestamp_high | timestamp_low;

        T::from_u64(timestamp).unwrap()
    }
}


impl Timeseries {
		fn read(&mut self, buf: &mut [u8], start_byte: &mut u64, stop_byte: u64) -> Result<usize, Error> {
        self.data.seek(SeekFrom::Start(*start_byte))?;
        let mut nread = self.data.read(buf)?;

        nread = if (*start_byte + nread as u64) >= stop_byte {
            trace!("diff: {}, {}, {}",
                *start_byte as i64,
                stop_byte as i64,
                stop_byte as i64 - *start_byte as i64
            );
            (stop_byte - *start_byte) as usize
        } else {
						trace!("nread: {}, {}, {}", nread, start_byte, stop_byte);
            nread
        };
        *start_byte += nread as u64;
        Ok(nread - nread % self.full_line_size)
    }
}

impl Timeseries {
    pub fn decode_sequential_time_only(
        &mut self,
        lines_to_read: usize,
				start_byte: &mut u64,
				stop_byte: u64,
				decode_params: &mut DecodeParams,
    ) -> Result<(Vec<u64>, Vec<u8>), Error> {
        //let mut buf = Vec::with_capacity(lines_to_read*self.full_line_size);
        let mut buf = vec![0; lines_to_read * self.full_line_size];

        let mut timestamps: Vec<u64> = Vec::with_capacity(lines_to_read);
        let mut decoded: Vec<u8> = Vec::with_capacity(lines_to_read);
				//save file pos indicator before read call moves it around
        let mut file_pos = *start_byte;
        let n_read = self.read(&mut buf, start_byte, stop_byte)? as usize;
        trace!("read: {} bytes", n_read);
        for line in buf[..n_read].chunks(self.full_line_size) {
            timestamps.push(self.get_timestamp::<u64>(line, file_pos, decode_params));
            file_pos += self.full_line_size as u64;
            decoded.extend_from_slice(&line[2..]);
        }
        Ok((timestamps, decoded))
    }
}

//open file and check if it has the right lenght
//(an interger multiple of the line lenght) if it
//has not warn and repair by truncating
fn open_and_check(path: PathBuf, full_line_size: usize) -> Result<(File,u64), Error> {
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
    Ok((file, metadata.len()))
}

//----------------------------------------------------------------------
//----------------------------------------------------------------------
#[macro_use]
extern crate log;
extern crate fxhash;

#[cfg(test)]
mod tests {

		extern crate fern;
		use tests::fern::colors::{Color, ColoredLevelConfig};

    use super::byteorder::NativeEndian;
    use super::chrono::{DateTime, NaiveDateTime, Utc};
    use super::*;

		fn setup_debug_logging(verbosity: u8) -> Result<(), fern::InitError> {
			let mut base_config = fern::Dispatch::new();
			let colors = ColoredLevelConfig::new()
						       .info(Color::Green)
						       .debug(Color::Yellow)
						       .warn(Color::Magenta);

			base_config = match verbosity {
				0 =>
					// Let's say we depend on something which whose "info" level messages are too
					// verbose to include in end-user output. If we don't need them,
					// let's not include them.
					base_config
					.level(log::LevelFilter::Info),
				1 => base_config
					.level(log::LevelFilter::Debug),
				2 => base_config
					.level(log::LevelFilter::Trace),
				_3_or_more => base_config.level(log::LevelFilter::Trace),
			};

			let stdout_config = fern::Dispatch::new()
				.format(move |out, message, record| {
						out.finish(format_args!(
								"[{}][{}][{}] {}",
							chrono::Local::now().format("%H:%M"),
							record.target(),
							colors.color(record.level()),
							message
						))
				})
				.chain(std::io::stdout());

			base_config.chain(stdout_config).apply()?;
			Ok(())
		}

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
            if Path::new("test_append.dat").exists() {
                fs::remove_file("test_append.dat").unwrap();
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
            if Path::new("test_set_read.dat").exists() {
                fs::remove_file("test_set_read.dat").unwrap();
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

            let bound_result = data.get_bounds(t1, Utc::now());
            match bound_result {
							BoundResult::IoError(_err) => panic!(),
							BoundResult::NoData => panic!(),
							BoundResult::Ok((start_byte, _stop_byte, _decode_params)) => {
            		assert_eq!(start_byte, ((data.line_size + 2) * 2) as u64);
							},
            }
        }

        #[test]
        fn hashes_then_verify() {
          const NUMBER_TO_INSERT: i64 = 1_000;
          const PERIOD: i64 = 24*3600/NUMBER_TO_INSERT;

          if Path::new("test_append_hashes_then_verify.h").exists() {
              fs::remove_file("test_append_hashes_then_verify.h").unwrap();
          }
          if Path::new("test_append_hashes_then_verify.dat").exists() {
              fs::remove_file("test_append_hashes_then_verify.dat").unwrap();
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

          if let BoundResult::Ok((mut start_byte, stop_byte, mut decode_params)) = data.get_bounds(t1,t2){
	          println!("stop: {}, start: {}",stop_byte, start_byte);
	          let lines_in_range = (stop_byte - start_byte) / ((data.line_size + 2) as u64) +1;
          	assert_eq!(lines_in_range, (NUMBER_TO_INSERT) as u64);

	          let n = 8_000;
	          let loops_to_check_everything =
	              NUMBER_TO_INSERT / n + if NUMBER_TO_INSERT % n > 0 { 1 } else { 0 };

	          for _ in 0..loops_to_check_everything {
              //println!("loop, {} at the time",n);
              let (timestamps, decoded) = data.decode_sequential_time_only(
              	n as usize,
              	&mut start_byte,
              	stop_byte,
              	&mut decode_params,
              ).unwrap();
              //println!("timestamps: {:?}", timestamps);
              for (timestamp, decoded) in timestamps.iter().zip(decoded.chunks(data.line_size)) {
                let hash1 = hash64::<i64>(&(*timestamp as i64));
                let hash2 = NativeEndian::read_u64(decoded);
                assert_eq!(hash1, hash2);
		          }
		        }
        	} else {panic!(); }
        }

        #[test]
        fn timestamps_then_verify() {
          const NUMBER_TO_INSERT: i64 = 10_000;
          const PERIOD: i64 = 24*3600/NUMBER_TO_INSERT;

					//setup_debug_logging(2).unwrap();

          if Path::new("test_append_timestamps_then_verify.h").exists() {
              fs::remove_file("test_append_timestamps_then_verify.h").unwrap();
          }
          if Path::new("test_append_timestamps_then_verify.dat").exists() {
              fs::remove_file("test_append_timestamps_then_verify.dat").unwrap();
          }

          let time = Utc::now();

          let mut data = Timeseries::open("test_append_timestamps_then_verify", 8).unwrap();
          insert_timestamp_arrays(&mut data, NUMBER_TO_INSERT as u32, PERIOD, time);
          //println!("inserted test data");

          let timestamp = time.timestamp();
          let t1 = time;
          let t2 = DateTime::<Utc>::from_utc(
              NaiveDateTime::from_timestamp(timestamp + NUMBER_TO_INSERT * PERIOD, 0), Utc, );

        	if let BoundResult::Ok((mut start_byte, stop_byte, mut decode_params)) = data.get_bounds(t1,t2){
	          let lines_in_range = (stop_byte - start_byte) / ((data.line_size + 2) as u64) +1;
	          assert_eq!(
	              lines_in_range,
	              (NUMBER_TO_INSERT) as u64
	          );

	          let n = 8_000;
	          let loops_to_check_everything =
	              NUMBER_TO_INSERT / n + if NUMBER_TO_INSERT % n > 0 { 1 } else { 0 };
	          for _ in 0..loops_to_check_everything {
              let (timestamps, decoded) = data.decode_sequential_time_only(
              	n as usize,
              	&mut start_byte,
              	stop_byte,
              	&mut decode_params,
              ).unwrap();
              for (i, (timestamp, decoded)) in timestamps.iter().zip(decoded.chunks(data.line_size)).enumerate() {
                  let ts_from_decode = *timestamp as i64;
                  let ts_from_data = NativeEndian::read_i64(decoded);

                  //println!("ts_from_decode: {}, ts_from_data: {}",ts_from_decode,ts_from_data);
                  assert_eq!(ts_from_decode, ts_from_data,
                  "failed on element: {}, which should have ts: {}, but has been given {},
                   prev element has ts: {}, the step is: {}",
                  i, ts_from_data, ts_from_decode, timestamps[i-1], PERIOD);
              }
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
            if Path::new("test_varing_length.dat").exists() {
                fs::remove_file("test_varing_length.dat").unwrap();
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

        		if let BoundResult::Ok((mut start_byte, stop_byte, mut decode_params)) = data.get_bounds(t1,t2){
		          println!(
		              "t1: {}, t2: {}, start_byte: {}, stop_byte: {}",
		              t1.timestamp(),
		              t2.timestamp(),
		              start_byte,
		              stop_byte
		          );

		          assert_eq!(
		              start_byte,
		              (start_read_inlines * (LINE_SIZE as i64 + 2)) as u64
		          );
		          assert_eq!(
		              (stop_byte - start_byte) / ((LINE_SIZE as u64 + 2) as u64),
		              read_length_inlines as u64
		          );

		          //let (timestamps, decoded) = data
		              //.decode_sequential_time_only(read_length_inlines as usize)
		              //.unwrap();
		          //println!("timestamps: {:?}", timestamps);

		          let (timestamps, _decoded) = data.decode_sequential_time_only(
              	10 as usize,
              	&mut start_byte,
              	stop_byte,
              	&mut decode_params,
              ).unwrap();
		          assert!(
		              timestamps[0] as i64 >= timestamp + 10 * STEP
		                  && timestamps[0] as i64 <= timestamp + 20 * STEP
		          );
            }
        }

        #[test]
        fn beyond_range() {
            if Path::new("test_beyond_range.h").exists() {
                fs::remove_file("test_beyond_range.h").unwrap();
            }
            if Path::new("test_beyond_range.dat").exists() {
                fs::remove_file("test_beyond_range.dat").unwrap();
            }
            const LINE_SIZE: usize = 8;
            const STEP: i64 = 5;
            const N_TO_INSERT: u32 = 100;
            let start_read_inlines = 101;
            let read_length_inlines = 10;

            //let time = Utc::now();
            let time = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(1539180000, 0), Utc);
            let timestamp = time.timestamp();
            println!("start timestamp {}", timestamp);
            let mut data = Timeseries::open("test_beyond_range", LINE_SIZE).unwrap();

            insert_uniform_arrays(&mut data, N_TO_INSERT, STEP, LINE_SIZE, time);

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


            if let BoundResult::IoError(error) = data.get_bounds(t1,t2){
							assert_eq!(error.kind(), ErrorKind::NotFound);
						} else {
							panic!();
						}
				}
    }
}
