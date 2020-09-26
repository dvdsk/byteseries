use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::data::FullTime;
use crate::util::open_and_check;
use crate::Error;

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
    Window(u64, u64),
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

        let last_timestamp = numbers
            .get(numbers.len().saturating_sub(2))
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

        self.data.push(Entry {
            timestamp,
            pos: line_start,
        });
        self.last_timestamp_numb = new_timestamp_numb;
        Ok(())
    }

    pub fn search_bounds(&self, start: i64, stop: i64) -> (SearchBounds, SearchBounds, FullTime) {
        let idx = self.data.binary_search_by_key(&start, |e| e.timestamp);
        let (start_bound, full_time) = match idx {
            Ok(i) => (
                SearchBounds::Found(self.data[i].pos),
                FullTime {
                    curr: start,
                    next: self.data.get(i + 1).map(|e| e.timestamp),
                    next_pos: self.data.get(i + 1).map(|e| e.pos),
                },
            ),
            Err(end) => {
                if end == 0 {
                    //start lies before file
                    (
                        SearchBounds::Clipped,
                        FullTime {
                            curr: self.data[0].timestamp,
                            next: self.data.get(1).map(|e| e.timestamp),
                            next_pos: self.data.get(1).map(|e| e.pos),
                        },
                    )
                } else if end == self.data.len() {
                    (
                        SearchBounds::TillEnd(self.data.last().unwrap().pos),
                        FullTime {
                            curr: self.data.last().unwrap().timestamp,
                            next: None, //there is no full timestamp beyond the end
                            next_pos: None,
                        },
                    )
                } else {
                    //end is not 0 or 1 thus data[end] and data[end-1] exist
                    dbg!(self.data.iter().map(|e| e.pos % 105).collect::<Vec<_>>());
                    (
                        SearchBounds::Window(self.data[end - 1].pos, self.data[end].pos),
                        FullTime {
                            curr: self.data[end - 1].timestamp,
                            next: Some(self.data[end].timestamp),
                            next_pos: Some(self.data[end].pos),
                        },
                    )
                }
            }
        };
        let idx = self.data.binary_search_by_key(&stop, |e| e.timestamp);
        let stop_bound = match idx {
            Ok(i) => SearchBounds::Found(self.data[i].pos),
            Err(end) => {
                if end == 0 {
                    //stop lies before file
                    panic!(
                        "stop lying before start of data should be caught
                        before calling search_bounds. We should never reach
                        this"
                    )
                } else if end == self.data.len() {
                    SearchBounds::TillEnd(self.data.last().unwrap().pos)
                } else {
                    //end is not 0 or 1 thus data[end] and data[end-1] exist
                    SearchBounds::Window(self.data[end - 1].pos, self.data[end].pos)
                }
            }
        };
        (start_bound, stop_bound, full_time)
    }
    pub fn first_time_in_data(&self) -> Option<i64> {
        self.data.first().map(|e| e.timestamp)
    }

    pub fn next_full_timestamp(&self, curr: i64) -> Option<&Entry> {
        let i = self.data.binary_search_by_key(&(curr + 1), |e| e.timestamp);
        let i = unwrap_result(i);
        self.data.get(i)
    }
}

fn unwrap_result<T>(res: Result<T, T>) -> T {
    match res {
        Ok(v) => v,
        Err(v) => v,
    }
}
// https://rust-algo.club/doc/src/rust_algorithm_club/searching/interpolation_search/mod.rs.html#16-69
//
#[cfg(test)]
mod tests {
    use super::*;

    fn test_header(n: usize) -> Header {
        let path = format!("/tmp/test_header_{}.h", n);
        let mut path = std::path::PathBuf::from(path);
        if path.exists() {
            std::fs::remove_file(&path).unwrap();
        }
        path.set_extension("");
        Header::open(path).unwrap()
    }
    fn fill_header(h: &mut Header) {
        for i in 20..24 {
            let ts = i * 2i64.pow(16);
            let new_timestamp_numb = ts / 2i64.pow(16);
            h.update(ts, i as u64, new_timestamp_numb).unwrap();
        }
    }

    #[test]
    fn start_found() {
        let mut h = test_header(0);
        fill_header(&mut h);
        let start = 22 * 2i64.pow(16);
        let stop = 23 * 2i64.pow(16);
        let (start, _stop, ft) = h.search_bounds(start, stop);
        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&SearchBounds::Found(0))
        );
        assert!(ft.next.is_some());
        assert!(ft.next_pos.is_some());
    }
    #[test]
    fn start_clipped() {
        let mut h = test_header(1);
        fill_header(&mut h);
        let start = 12342;
        let stop = 23 * 2i64.pow(16);
        let (start, _stop, ft) = h.search_bounds(start, stop);

        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&SearchBounds::Clipped)
        );
        assert!(ft.next.is_some());
        assert!(ft.next_pos.is_some());
    }
    #[test]
    fn start_window() {
        let mut h = test_header(2);
        fill_header(&mut h);
        let start = 22 * 2i64.pow(16) + 400;
        let stop = 23 * 2i64.pow(16);
        let (start, _stop, ft) = h.search_bounds(start, stop);

        // dbg!(&start);
        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&SearchBounds::Window(0, 0))
        );
        assert!(ft.next.is_some());
        assert!(ft.next_pos.is_some());
    }
    #[test]
    fn start_till_end() {
        let mut h = test_header(3);
        fill_header(&mut h);
        let start = 24 * 2i64.pow(16) + 400;
        let stop = 25 * 2i64.pow(16);
        let (start, _stop, ft) = h.search_bounds(start, stop);

        // dbg!(&start);
        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&SearchBounds::TillEnd(0))
        );
        assert!(ft.next.is_none());
        assert!(ft.next_pos.is_none());
    }
}
