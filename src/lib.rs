#![allow(dead_code)]

#[macro_use]
extern crate log;
extern crate chrono;
extern crate byteorder;
extern crate ndarray;

use chrono::{DateTime, NaiveDateTime, Utc};

use std::fs::{OpenOptions, File};
use std::io::{Write, Read, Error, SeekFrom, Seek};

use byteorder::{ByteOrder, LittleEndian};

use ndarray::{Array, Axis};




struct Header {
	file: File,
	data: ndarray::ArrayBase<ndarray::OwnedRepr<u64>, ndarray::Dim<[usize; 2]>>,
	
	last_timestamp: i64,
	last_timestamp_numb: i64,
}

impl Header {
	fn open(name: &str) -> Result<Header, Error> {
		let mut file = open_and_check(name.to_owned()+".h1", 16)?;
		
		//read in the entire file
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    
		let mut numbers = vec![0u64; bytes.len()/8];
		LittleEndian::read_u64_into(&bytes, numbers.as_mut_slice());
    
    let a = Array::from_shape_vec((numbers.len()/2,2), numbers).unwrap();
    let last_timestamp = a[[a.len()/2-1,0]] as i64;

		//let mut buf = [0; 8]; //rewrite to use bufferd data
		//let last_timestamp = if !file.seek(SeekFrom::End(-16)).is_err(){;
			//file.read(&mut buf);
			//LittleEndian::read_u64(&buf) as i64
		//} else {warn!("header is empty"); 0};
		println!("last_timestamp: {}",last_timestamp);
		
		Ok(Header {
			file: file,
			data: a,
			last_timestamp: last_timestamp as i64,
			last_timestamp_numb: last_timestamp / (u16::max_value() as i64),
		})
	}
}

//rewrite using cont generics when availible 
//https://github.com/rust-lang/rfcs/blob/master/text/2000-const-generics.md
pub struct Timeseries {
	data: File,
	header: Header, // add triple headers
	
	line_size: usize,
	time_buffer: [u8; 8],
	
	first_time_in_data: DateTime<Utc>,
	last_time_in_data: DateTime<Utc>,
	
	start_byte: usize,
	stop_byte: usize,
}

// ---------------------------------------------------------------------
// -- we store only the last 4 bytes of the timestamp ------------------
// ---------------------------------------------------------------------

impl Timeseries {
	pub fn open(name: &str, line_size: usize) -> Result<Timeseries, Error> {
		
		let mut data = open_and_check(name.to_owned()+".data", line_size)?;
		let header = Header::open(name)?;
		
		let first_time = Self::get_first_time_in_data(&mut data);
		let last_time = Self::get_last_time_in_data();
		
		Ok(Timeseries {
			data: data,
			header: header, // add triple headers
			
			line_size: line_size,
			time_buffer: [0; 8],
			
			first_time_in_data: first_time,
			last_time_in_data: last_time,
			
			//these are set during: set_read_start, set_read_end then read is
			//bound by these points
			start_byte: 0,
			stop_byte: i64::max_value() as usize,
		})
	}
	
	fn get_first_time_in_data(data: &mut File)-> DateTime<Utc> {
		let mut buf = [0; 8]; //rewrite to use bufferd data
		if data.seek(SeekFrom::Start(0)).is_err(){
			data.read(&mut buf).unwrap();
			let timestamp = LittleEndian::read_u64(&buf) as i64;
			DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc)
		} else {warn!("file is empty"); Utc::now()}
		
	}
	fn get_last_time_in_data() -> DateTime<Utc> {
		//let mut buf = [0; 8]; //rewrite to use bufferd data
		//let last_timestamp = if !file.seek(SeekFrom::End(-16)).is_err(){;
			//file.read(&mut buf);
			//LittleEndian::read_u64(&buf) as i64
		//} else {warn!("header is empty"); 0};
		Utc::now()
	}
	
	pub fn append(&mut self, time: DateTime<Utc>, line: &[u8]) -> Result<(),Error>{
		//TODO decide if a lock is needed here
		
		//write 16 bit timestamp and then the line to file
		let timestamp = time.timestamp();
		self.i64_timestamp_to_u8_array(timestamp);
		
		self.data.write(&self.time_buffer[0..2] )?;
		self.data.write(line)?;
		
		//write 64 bit timestamp to header
		//(happens no more then once every 18 hours)
		self.update_header(timestamp)?;
		
		self.force_write_to_disk();
		Ok(())
	}
	
	fn update_header(&mut self, timestamp: i64) -> Result<(),Error>{
		let timestamp_numb = timestamp / (u16::max_value() as i64);
		if timestamp_numb > self.header.last_timestamp_numb {
			println!("updating header");
			
			let line_start = self.data.metadata().unwrap().len() - self.line_size as u64;
			let mut line_start_buf = [0u8; 8];
			LittleEndian::write_u64(&mut line_start_buf, line_start);
			
			self.header.file.write(&self.time_buffer)?;
			self.header.file.write(&line_start_buf)?;
			self.header.data.
			
			self.header.last_timestamp_numb = timestamp_numb;
		}
		Ok(())
	}
	
	fn force_write_to_disk(&mut self){
    self.data.sync_data().unwrap();
    self.header.file.sync_data().unwrap();
	}
	
	fn set_read_start(&mut self, _start_time: DateTime<Utc>){
		//self.header.data
		self.start_byte = 0;
	}
	fn set_read_end(&mut self, _end_time: DateTime<Utc>){
		self.stop_byte = 0;
		unimplemented!();
	}

	fn i64_timestamp_to_u8_array(&mut self, x: i64){
		//no support for sign bit since data will always be after 0 (1970)
		let x = if x<0 {0} else {x as u64};
		//we store the number in little endian Signed magnitude representation
		//(least significant (lowest value) byte at lowest adress)	
		LittleEndian::write_u64(&mut self.time_buffer, x);
	}
}

impl Read for Timeseries {
    fn read(&mut self, _buf: &mut [u8]) -> Result<usize, Error> {
      unimplemented!();
    }
}

//open file and check if it has the right lenght
//(an interger multiple of the line lenght) if it 
//has not warn and repair by truncating
fn open_and_check(path: String, line_size: usize) -> Result<File, Error> {
	let file = OpenOptions::new().read(true).append(true).create(true).open(path)?;
	let metadata = file.metadata()?;
	
	let rest = metadata.len() % (line_size as u64);
	if rest > 0 {
		warn!("Last write incomplete, truncating to largest multiple of the line size");
		file.set_len(metadata.len() - rest)?;
	}	
	Ok(file)
}

//----------------------------------------------------------------------
//----------------------------------------------------------------------

#[cfg(test)]
mod tests {
	use super::*;
	use chrono::{DateTime, NaiveDateTime, Utc};


	#[test]
	fn append() {
		let now = Utc::now().timestamp();
		let buffer = [0; 10];
		let mut data = Timeseries::open("test",10).unwrap();
		
		for _ in 0..10 {
			let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(now+5, 0), Utc);
			data.append(dt, &buffer).unwrap();
		}
		assert_eq!(2 + 2, 4);
	}

	#[test]
	fn read() {
		let _buffer = [0; 10];
		let now = Utc::now();
		
		let mut data = Timeseries::open("test",10).unwrap();
		
		data.set_read_start(now);
		
		let mut buffer = Vec::new();
    // read the whole file
    data.read_to_end(&mut buffer).unwrap();
		println!("{:?}",buffer);
		assert_eq!(2 + 2, 4);
	}

}
