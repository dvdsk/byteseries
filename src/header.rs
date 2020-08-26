use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::Error;
use crate::util::open_and_check;
use crate::data::FullTime;

#[derive(Debug)]
pub struct Entry {
    pub timestamp: i64,
    pub pos: u64,
}

#[derive(Debug)]
pub struct Header {
    pub file: File,

    pub data: Vec<Entry>,
    pub last_timestamp: i64,
    pub last_timestamp_numb: i64,
}

#[derive(Debug)]
pub enum SearchBounds {
    Found(u64),
    Clipped,
    TillEnd(u64),
    Window(u64,u64),
}

impl Header {
    pub fn open<P: AsRef<Path>>(name: P) -> Result<Header, Error> {
        let (mut file, _) = open_and_check(name.as_ref().with_extension("h"), 16)?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let mut numbers = vec![0u64; bytes.len() / 8];
        LittleEndian::read_u64_into(&bytes, numbers.as_mut_slice());

        let mut data = Vec::new();
        for i in (0..numbers.len()).step_by(2) {
            data.push(Entry {
                timestamp: numbers[i] as i64,
                pos: numbers[i + 1],
            });
        }

        let last_timestamp = numbers.get(numbers.len() - 2)
            .map(|n| *n as i64)
            .unwrap_or(0);
        
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

        self.data.push(Entry{timestamp, pos: line_start});
        self.last_timestamp_numb = new_timestamp_numb;
        Ok(())
    }

    pub fn search_bounds(&self, start: i64, stop: i64)
        -> (SearchBounds, SearchBounds, FullTime) {
        let idx = self.data.binary_search_by_key(&start, |e| e.timestamp);
        let (start_bound, full_time) = match idx {
            Ok(i) => (SearchBounds::Found(self.data[i].pos),
                FullTime {
                    curr: start,
                    next: self.data.get(i+1).map(|e| e.timestamp),
                    next_pos: self.data.get(i+1).map(|e| e.pos)
                }),
            Err(end) => {
                if end == 0 { //start lies before file
                    (SearchBounds::Clipped, 
                     FullTime {
                        curr: self.data[0].timestamp,
                        next: self.data.get(1).map(|e| e.timestamp),
                        next_pos: self.data.get(1).map(|e| e.pos),
                    })
                } else if end == self.data.len() {
                    (SearchBounds::TillEnd(self.data.last().unwrap().pos), 
                     FullTime {
                        curr: self.data.last().unwrap().timestamp,
                        next: None, //there is no full timestamp beyond the end
                        next_pos: None,
                    })
                } else { //end is not 0 or 1 thus data[end] and data[end-1] exist 
                    (SearchBounds::Window(self.data[end-1].pos, 
                        self.data[end].pos),
                    FullTime {
                        curr: self.data[end-1].timestamp,
                        next: Some(self.data[end].timestamp),
                        next_pos: Some(self.data[end].pos),
                    })
                }
            }
        };
        let idx = self.data.binary_search_by_key(&stop, |e| e.timestamp);
        let stop_bound = match idx {
            Ok(i) => SearchBounds::Found(self.data[i].pos),
            Err(end) => {
                if end == 0 { //stop lies before file
                    panic!("stop lying before start of data should be caught
                        before calling search_bounds. We should never reach
                        this")
                } else if end == self.data.len() {
                    SearchBounds::TillEnd(self.data.last().unwrap().pos)
                } else { //end is not 0 or 1 thus data[end] and data[end-1] exist 
                    SearchBounds::Window(self.data[end-1].pos, self.data[end].pos)
                }
            }
        };
        (start_bound, stop_bound, full_time)
    }
    pub fn first_time_in_data(&self) -> Option<i64> {
        self.data.first().map(|e| e.timestamp)
    }

    pub fn next_full_timestamp(&self, curr: i64) -> Option<&Entry> {
        let i = self.data.binary_search_by_key(&(curr+1), |e| e.timestamp);
        let i = unwrap_result(i);
        self.data.get(i)
    }
}

fn unwrap_result<T>(res: Result<T,T>) -> T {
    match res {
        Ok(v) => v,
        Err(v) => v,
    }
}
// https://rust-algo.club/doc/src/rust_algorithm_club/searching/interpolation_search/mod.rs.html#16-69
