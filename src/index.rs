use byteorder::{ByteOrder, LittleEndian};
use core::fmt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{Read, Seek, Write};
use std::path::Path;
use tracing::instrument;

use crate::data::Timestamp;
use crate::util::{FileWithHeader, OffsetFile};
use crate::Error;

pub(crate) mod restore;

#[derive(Debug)]
pub struct Entry {
    pub timestamp: Timestamp,
    pub line_start: u64,
}

#[derive(Debug)]
pub struct Index {
    pub file: OffsetFile,

    pub entries: Vec<Entry>,
    pub last_timestamp: Timestamp,
}

#[derive(Debug)]
pub enum SearchBounds {
    Found(u64),
    Clipped,
    TillEnd(u64),
    Window(u64, u64),
}

impl Drop for Index {
    fn drop(&mut self) {
        dbg!(&self.entries);
    }
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
                timestamp: numbers[i] as Timestamp,
                line_start: numbers[i + 1],
            });
        }

        Ok(Index {
            file: file.split_off_header().0,
            last_timestamp: data
                .last()
                .map(|Entry { timestamp, .. }| timestamp)
                .copied()
                .unwrap_or(0),
            entries: data,
        })
    }
    pub fn update(&mut self, timestamp: u64, line_start: u64) -> Result<(), std::io::Error> {
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
        self.last_timestamp = timestamp;
        Ok(())
    }

    pub fn search_bounds(&self, start: Timestamp, stop: Timestamp) -> (SearchBounds, SearchBounds, Timestamp) {
        let idx = self.entries.binary_search_by_key(&start, |e| e.timestamp);
        let (start_bound, full_time) = match idx {
            Ok(i) => (SearchBounds::Found(self.entries[i].line_start), start),
            Err(end) => {
                if end == 0 {
                    //start lies before file
                    (SearchBounds::Clipped, self.entries[0].timestamp)
                } else if end == self.entries.len() {
                    (
                        SearchBounds::TillEnd(self.entries.last().unwrap().line_start),
                        self.entries.last().unwrap().timestamp,
                    )
                } else {
                    //end is not 0 or 1 thus data[end] and data[end-1] exist
                    (
                        SearchBounds::Window(
                            self.entries[end - 1].line_start,
                            self.entries[end].line_start,
                        ),
                        self.entries[end - 1].timestamp,
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
    pub fn first_time_in_data(&self) -> Option<Timestamp> {
        self.entries.first().map(|e| e.timestamp)
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
            let ts = i * 2u64.pow(16);
            h.update(ts, i as u64).unwrap();
        }
    }

    #[test]
    fn start_found() {
        let mut h = test_index();
        fill_index(&mut h);
        let start = 22 * 2u64.pow(16);
        let stop = 23 * 2u64.pow(16);
        let (start, _, ft) = h.search_bounds(start, stop);
        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&SearchBounds::Found(0))
        );
        assert_eq!(ft, 22 * 2u64.pow(16))
    }
    #[test]
    fn start_clipped() {
        let mut h = test_index();
        fill_index(&mut h);
        let start = 12342;
        let stop = 23 * 2u64.pow(16);
        let (start, _, _) = h.search_bounds(start, stop);

        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&SearchBounds::Clipped)
        );
    }
    #[test]
    fn start_window() {
        let mut h = test_index();
        fill_index(&mut h);
        let start = 22 * 2u64.pow(16) + 400;
        let stop = 23 * 2u64.pow(16);
        let (start, _, _) = h.search_bounds(start, stop);

        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&SearchBounds::Window(0, 0))
        );
    }
    #[test]
    fn start_till_end() {
        let mut h = test_index();
        fill_index(&mut h);
        let start = 24 * 2u64.pow(16) + 400;
        let stop = 25 * 2u64.pow(16);
        let (start, _, _) = h.search_bounds(start, stop);

        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&SearchBounds::TillEnd(0))
        );
    }
}
