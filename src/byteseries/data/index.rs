use core::fmt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{Read, Seek, Write};
use std::path::Path;
use tracing::instrument;

use crate::util::{self, FileWithHeader, OffsetFile};
use crate::Timestamp;

use super::inline_meta;

pub(crate) mod restore;

#[derive(Debug)]
pub(crate) struct Entry {
    pub timestamp: Timestamp,
    pub line_start: u64,
}

#[derive(Debug)]
pub(crate) struct Index {
    pub(crate) file: OffsetFile,

    entries: Vec<Entry>,
    last_timestamp: Option<Timestamp>,
}

#[derive(Debug, Clone)]
pub enum StartArea {
    Found(u64),
    /// start lies before first timestamp or end lies after last timestamp
    Clipped,
    /// start timestamp lies in section from this position till the end of the data
    TillEnd(u64),
    Window(u64, u64),
}

#[derive(Debug, Clone)]
pub enum EndArea {
    Found(u64),
    /// end timestamp lies in section from this position till the end of the data
    TillEnd(u64),
    /// end timestamp lies in between the first and second position
    Window(u64, u64),
}

impl StartArea {
    pub(crate) fn map(&self, mut op: impl FnMut(u64) -> u64) -> Self {
        match self.clone() {
            Self::Found(pos) => Self::Found(op(pos)),
            Self::Clipped => Self::Clipped,
            Self::TillEnd(x) => Self::TillEnd(op(x)),
            Self::Window(x, y) => Self::Window(op(x), op(y)),
        }
    }
}

impl EndArea {
    pub(crate) fn map(&self, mut op: impl FnMut(u64) -> u64) -> Self {
        match self.clone() {
            Self::Found(pos) => Self::Found(op(pos)),
            Self::TillEnd(x) => Self::TillEnd(op(x)),
            Self::Window(x, y) => Self::Window(op(x), op(y)),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("")]
    File(util::OpenError),
    #[error("The header in the index and byteseries are different")]
    IndexAndDataHeaderDifferent,
    #[error("reading in")]
    Reading(std::io::Error),
}

impl Index {
    #[instrument]
    pub fn new<H>(
        name: impl AsRef<Path> + fmt::Debug,
        user_header: H,
    ) -> Result<Index, util::OpenError>
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
            last_timestamp: None,
        })
    }
    #[instrument]
    pub fn open_existing<H>(
        name: impl AsRef<Path> + fmt::Debug,
        user_header: &H,
    ) -> Result<Index, OpenError>
    where
        H: DeserializeOwned + Serialize + Eq + fmt::Debug + 'static + Clone,
    {
        let mut file: FileWithHeader<H> =
            FileWithHeader::open_existing(name.as_ref().with_extension("byteseries_index"), 16)
                .map_err(OpenError::File)?;

        if *user_header != file.user_header {
            return Err(OpenError::IndexAndDataHeaderDifferent);
        }

        let mut bytes = Vec::new();
        file.handle
            .seek(std::io::SeekFrom::Start(file.data_offset))
            .map_err(OpenError::Reading)?;
        file.handle
            .read_to_end(&mut bytes)
            .map_err(OpenError::Reading)?;

        let entries: Vec<_> = bytes
            .chunks_exact(16)
            .map(|line| {
                let timestamp: [u8; 8] = line[0..8].try_into().expect("line is 2*8 bytes");
                let timestamp = u64::from_le_bytes(timestamp);
                let line_start: [u8; 8] = line[8..].try_into().expect("line is 2*8 bytes");
                let line_start = u64::from_le_bytes(line_start);
                Entry {
                    timestamp,
                    line_start,
                }
            })
            .collect();

        Ok(Index {
            file: file.split_off_header().0,
            last_timestamp: entries
                .last()
                .map(|Entry { timestamp, .. }| timestamp)
                .copied(),
            entries,
        })
    }

    #[instrument(level = "trace", skip(self), ret)]
    pub fn update(&mut self, timestamp: u64, line_start: u64) -> Result<(), std::io::Error> {
        let ts = timestamp;
        self.file.write_all(&ts.to_le_bytes())?;
        self.file.write_all(&line_start.to_le_bytes())?;

        self.entries.push(Entry {
            timestamp,
            line_start,
        });
        self.last_timestamp = Some(timestamp);
        Ok(())
    }

    pub fn start_search_bounds(
        &self,
        start: Timestamp,
        payload_size: usize,
    ) -> (StartArea, Timestamp) {
        let idx = self.entries.binary_search_by_key(&start, |e| e.timestamp);
        match idx {
            Ok(i) => (StartArea::Found(self.entries[i].line_start), start),
            Err(end) => {
                if end == 0 {
                    (StartArea::Clipped, self.entries[0].timestamp)
                } else if end == self.entries.len() {
                    (
                        StartArea::TillEnd(self.entries.last().unwrap().line_start),
                        self.entries.last().unwrap().timestamp,
                    )
                } else {
                    //end is not 0 or 1 thus data[end] and data[end-1] exist
                    (
                        StartArea::Window(
                            self.entries[end - 1].line_start,
                            self.entries[end].line_start
                                - inline_meta::bytes_per_metainfo(payload_size) as u64,
                        ),
                        self.entries[end - 1].timestamp,
                    )
                }
            }
        }
    }
    pub fn end_search_bounds(&self, stop: Timestamp, payload_len: usize) -> (EndArea, Timestamp) {
        dbg!(&self.entries);
        let idx = self.entries.binary_search_by_key(&stop, |e| e.timestamp);
        match idx {
            Ok(i) => (
                EndArea::Found(self.entries[i].line_start),
                self.entries[i].timestamp,
            ),
            Err(end) => {
                if end == 0 {
                    //stop lies before file
                    panic!(
                        "end lying before start of data should be caught
                        before calling search_bounds. We should never reach
                        this"
                    )
                } else if end == self.entries.len() {
                    let last = self
                        .entries
                        .last()
                        .expect("Index always has one entry when the byteseries is not empty");
                    (EndArea::TillEnd(last.line_start), last.timestamp)
                } else {
                    //end is not 0 or 1 thus data[end] and data[end-1] exist
                    (
                        EndArea::Window(
                            self.entries[end - 1].line_start,
                            self.entries[end].line_start
                                - inline_meta::bytes_per_metainfo(payload_len) as u64,
                        ),
                        self.entries[end - 1].timestamp,
                    )
                }
            }
        }
    }
    pub fn first_time_in_data(&self) -> Option<Timestamp> {
        self.entries.first().map(|e| e.timestamp)
    }

    pub fn last_timestamp(&self) -> Option<Timestamp> {
        self.last_timestamp
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
            h.update(ts, i).unwrap();
        }
    }

    #[test]
    fn start_found() {
        let mut h = test_index();
        fill_index(&mut h);
        let start = 22 * 2u64.pow(16);
        let (start, ft) = h.start_search_bounds(start, 0);
        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&StartArea::Found(0))
        );
        assert_eq!(ft, 22 * 2u64.pow(16))
    }
    #[test]
    fn start_clipped() {
        let mut h = test_index();
        fill_index(&mut h);
        let start = 12342;
        let (start, _) = h.start_search_bounds(start, 0);

        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&StartArea::Clipped)
        );
    }
    #[test]
    fn start_window() {
        let mut h = test_index();
        fill_index(&mut h);
        let start = 22 * 2u64.pow(16) + 400;
        let (start, _) = h.start_search_bounds(start, 0);

        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&StartArea::Window(0, 0))
        );
    }
    #[test]
    fn start_till_end() {
        let mut h = test_index();
        fill_index(&mut h);
        let start = 24 * 2u64.pow(16) + 400;
        let (start, _) = h.start_search_bounds(start, 0);

        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&StartArea::TillEnd(0))
        );
    }
}
