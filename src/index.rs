use byteorder::{ByteOrder, LittleEndian};
use core::fmt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{Read, Seek, Write};
use std::path::Path;
use tracing::instrument;

use crate::data::FullTime;
use crate::util::{FileWithHeader, OffsetFile};
use crate::Error;

pub(crate) mod restore;

#[derive(Debug)]
pub struct Entry {
    pub timestamp: i64,
    pub line_start: u64,
}

#[derive(Debug)]
pub struct Index {
    pub file: OffsetFile,

    pub entries: Vec<Entry>,
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

impl Index {
    #[instrument]
    pub fn new<H>(name: impl AsRef<Path> + fmt::Debug, user_header: H) -> Result<Index, Error>
    where
        H: DeserializeOwned + Serialize + Eq + fmt::Debug + 'static + Clone,
    {
        let file: FileWithHeader<H> = FileWithHeader::new(
            name.as_ref().with_extension("byteseries_index"),
            user_header,
        )?;

        Ok(Index {
            file: file.split_off_header().0,

            entries: Vec::new(),
            last_timestamp: 0,
            last_timestamp_numb: 0,
        })
    }
    #[instrument]
    pub fn open_existing<H>(
        name: impl AsRef<Path> + fmt::Debug,
        user_header: &H,
    ) -> Result<Index, Error>
    where
        H: DeserializeOwned + Serialize + Eq + fmt::Debug + 'static + Clone,
    {
        let mut file: FileWithHeader<H> =
            FileWithHeader::open_existing(name.as_ref().with_extension("byteseries_index"), 16)?;

        if *user_header != file.user_header {
            return Err(Error::IndexAndDataHeaderDifferent);
        }

        let mut bytes = Vec::new();
        file.handle
            .seek(std::io::SeekFrom::Start(file.data_offset))?;
        file.handle.read_to_end(&mut bytes)?;
        let mut numbers = vec![0u64; bytes.len() / 8];
        LittleEndian::read_u64_into(&bytes, numbers.as_mut_slice());

        let mut data = Vec::new();
        for i in (0..numbers.len()).step_by(2) {
            data.push(Entry {
                timestamp: numbers[i] as i64,
                line_start: numbers[i + 1],
            });
        }

        let last_timestamp = numbers
            .get(numbers.len().saturating_sub(2))
            .map(|n| *n as i64)
            .unwrap_or(0);

        tracing::trace!("last_timestamp: {}", last_timestamp);
        Ok(Index {
            file: file.split_off_header().0,

            entries: data,
            last_timestamp,
            last_timestamp_numb: last_timestamp / (u16::max_value() as i64),
        })
    }
    pub fn update(&mut self, timestamp: i64, line_start: u64) -> Result<(), std::io::Error> {
        let ts = timestamp as u64;
        self.file.write_all(&ts.to_le_bytes())?;
        self.file.write_all(&line_start.to_le_bytes())?;
        tracing::trace!(
            "wrote headerline: {ts}, {line_start} as line: {:?} {:?}",
            ts.to_le_bytes(),
            line_start.to_le_bytes()
        );

        self.entries.push(Entry {
            timestamp,
            line_start,
        });
        let new_timestamp_numb = timestamp / 2i64.pow(16);
        self.last_timestamp_numb = new_timestamp_numb;
        Ok(())
    }

    pub fn search_bounds(&self, start: i64, stop: i64) -> (SearchBounds, SearchBounds, FullTime) {
        let idx = self.entries.binary_search_by_key(&start, |e| e.timestamp);
        let (start_bound, full_time) = match idx {
            Ok(i) => (
                SearchBounds::Found(self.entries[i].line_start),
                FullTime {
                    curr: start,
                    next: self.entries.get(i + 1).map(|e| e.timestamp),
                    next_pos: self.entries.get(i + 1).map(|e| e.line_start),
                },
            ),
            Err(end) => {
                if end == 0 {
                    //start lies before file
                    (
                        SearchBounds::Clipped,
                        FullTime {
                            curr: self.entries[0].timestamp,
                            next: self.entries.get(1).map(|e| e.timestamp),
                            next_pos: self.entries.get(1).map(|e| e.line_start),
                        },
                    )
                } else if end == self.entries.len() {
                    (
                        SearchBounds::TillEnd(self.entries.last().unwrap().line_start),
                        FullTime {
                            curr: self.entries.last().unwrap().timestamp,
                            next: None, //there is no full timestamp beyond the end
                            next_pos: None,
                        },
                    )
                } else {
                    //end is not 0 or 1 thus data[end] and data[end-1] exist
                    (
                        SearchBounds::Window(
                            self.entries[end - 1].line_start,
                            self.entries[end].line_start,
                        ),
                        FullTime {
                            curr: self.entries[end - 1].timestamp,
                            next: Some(self.entries[end].timestamp),
                            next_pos: Some(self.entries[end].line_start),
                        },
                    )
                }
            }
        };
        let idx = self.entries.binary_search_by_key(&stop, |e| e.timestamp);
        let stop_bound = match idx {
            Ok(i) => SearchBounds::Found(self.entries[i].line_start),
            Err(end) => {
                if end == 0 {
                    //stop lies before file
                    panic!(
                        "stop lying before start of data should be caught
                        before calling search_bounds. We should never reach
                        this"
                    )
                } else if end == self.entries.len() {
                    SearchBounds::TillEnd(self.entries.last().unwrap().line_start)
                } else {
                    //end is not 0 or 1 thus data[end] and data[end-1] exist
                    SearchBounds::Window(
                        self.entries[end - 1].line_start,
                        self.entries[end].line_start,
                    )
                }
            }
        };
        (start_bound, stop_bound, full_time)
    }
    pub fn first_time_in_data(&self) -> Option<i64> {
        self.entries.first().map(|e| e.timestamp)
    }

    pub fn next_full_timestamp(&self, curr: i64) -> Option<&Entry> {
        let i = self
            .entries
            .binary_search_by_key(&(curr + 1), |e| e.timestamp);
        let i = unwrap_result(i);
        self.entries.get(i)
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
    use temp_dir::TempDir;

    use super::*;

    fn test_index() -> Index {
        let test_dir = TempDir::new().unwrap();
        let test_path = test_dir.child("test.byteseries_index");
        Index::new(test_path, ()).unwrap()
    }
    fn fill_index(h: &mut Index) {
        for i in 20..24 {
            let ts = i * 2i64.pow(16);
            h.update(ts, i as u64).unwrap();
        }
    }

    #[test]
    fn start_found() {
        let mut h = test_index();
        fill_index(&mut h);
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
        let mut h = test_index();
        fill_index(&mut h);
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
        let mut h = test_index();
        fill_index(&mut h);
        let start = 22 * 2i64.pow(16) + 400;
        let stop = 23 * 2i64.pow(16);
        let (start, _stop, ft) = h.search_bounds(start, stop);

        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&SearchBounds::Window(0, 0))
        );
        assert!(ft.next.is_some());
        assert!(ft.next_pos.is_some());
    }
    #[test]
    fn start_till_end() {
        let mut h = test_index();
        fill_index(&mut h);
        let start = 24 * 2i64.pow(16) + 400;
        let stop = 25 * 2i64.pow(16);
        let (start, _stop, ft) = h.search_bounds(start, stop);

        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&SearchBounds::TillEnd(0))
        );
        assert!(ft.next.is_none());
        assert!(ft.next_pos.is_none());
    }
}
