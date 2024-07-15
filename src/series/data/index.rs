use core::fmt;
use num_traits::Saturating;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{Read, Seek, Write};
use std::path::Path;
use tracing::instrument;

use crate::file::{self, FileWithHeader, OffsetFile};
use crate::Timestamp;

use super::inline_meta::{self, SetLen};

pub(crate) mod create;

#[derive(Debug)]
pub(crate) struct Entry {
    pub timestamp: Timestamp,
    pub line_start: u64,
}

#[derive(Debug)]
pub(crate) struct Index {
    pub(crate) file: OffsetFile,

    entries: Vec<Entry>,
    /// time for next point is 1 larger the this
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
    /// Pos at which the end line ends
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
    File(file::OpenError),
    #[error("The header in the index and byteseries are different")]
    IndexAndDataHeaderDifferent,
    #[error("reading in index: {0}")]
    Reading(std::io::Error),
    #[error("Could not check or repair the index: {0}")]
    CheckOrRepair(#[from] CheckAndRepairError),
}

impl Index {
    #[instrument]
    pub fn new<H>(
        name: impl AsRef<Path> + fmt::Debug,
        user_header: H,
    ) -> Result<Index, file::OpenError>
    where
        H: DeserializeOwned + Serialize + fmt::Debug + 'static + Clone,
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
        last_line_in_data_start: Option<u64>,
        last_full_ts_in_data: Option<Timestamp>,
    ) -> Result<Index, OpenError>
    where
        H: DeserializeOwned + Serialize + fmt::Debug + PartialEq + 'static + Clone,
    {
        let file: FileWithHeader<H> =
            FileWithHeader::open_existing(name.as_ref().with_extension("byteseries_index"), 16)
                .map_err(OpenError::File)?;

        let (mut file, header) = file.split_off_header();
        if *user_header != header {
            return Err(OpenError::IndexAndDataHeaderDifferent);
        }

        check_and_repair(&mut file, last_line_in_data_start, last_full_ts_in_data)?;
        let mut bytes = Vec::new();
        file.seek(std::io::SeekFrom::Start(0))
            .map_err(OpenError::Reading)?;
        file.read_to_end(&mut bytes).map_err(OpenError::Reading)?;

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
            file,
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
        let idx = self.entries.binary_search_by_key(&stop, |e| e.timestamp);
        match idx {
            Ok(i) => (
                // entry marks start of end line, need end
                EndArea::Found(self.entries[i].line_start + payload_len as u64 + 2),
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
    pub fn first_meta_timestamp(&self) -> Option<Timestamp> {
        self.entries.first().map(|e| e.timestamp)
    }

    pub fn last_timestamp(&self) -> Option<Timestamp> {
        self.last_timestamp
    }

    #[instrument]
    pub(crate) fn meta_ts_for(&self, line_start: u64) -> u64 {
        match self
            .entries
            .binary_search_by_key(&line_start, |entry| entry.line_start)
        {
            Ok(idx) => self.entries[idx].timestamp,
            // inserting at idx would keep the list sorted, so the full timestamp
            // before start lies at idx - 1
            Err(idx) => self.entries[idx - 1].timestamp,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CheckAndRepairError {
    #[error("The index is missing items")]
    IndexMissesItems,
    #[error("Could not repair the index by truncating it: {0}")]
    Truncate(std::io::Error),
    #[error("Could not check the index, failed to get its length: {0}")]
    GetLength(std::io::Error),
    #[error("Could not check the index, failed to seek in it: {0}")]
    Seek(std::io::Error),
    #[error("Could not check the index, failed to read it: {0}")]
    Read(std::io::Error),
}

#[instrument(err)]
fn check_and_repair(
    file: &mut OffsetFile,
    last_line_in_data_start: Option<u64>,
    last_full_ts_in_data: Option<Timestamp>,
) -> Result<(), CheckAndRepairError> {
    let len = file.len().map_err(CheckAndRepairError::GetLength)?;
    let Some(last_line_in_data_start) = last_line_in_data_start else {
        file.set_len(0).map_err(CheckAndRepairError::Truncate)?;
        return Ok(());
    };
    let rest = len % 16;
    let uncorrupted_len = len - rest;
    file.set_len(uncorrupted_len)
        .map_err(CheckAndRepairError::Truncate)?;

    file.seek(std::io::SeekFrom::End(-16))
        .map_err(CheckAndRepairError::Seek)?;
    let mut last_entry = vec![0u8; 16];
    file.read_exact(&mut last_entry)
        .map_err(CheckAndRepairError::Read)?;
    let last_full_ts: [u8; 8] = last_entry[0..8].try_into().expect("just read 16 bytes");
    let last_full_ts = u64::from_le_bytes(last_full_ts);
    let last_line_start: [u8; 8] = last_entry[8..].try_into().expect("just read 16 bytes");
    let last_line_start = u64::from_le_bytes(last_line_start);

    let len = file.len().map_err(CheckAndRepairError::GetLength)?;
    if last_line_start > last_line_in_data_start {
        file.set_len(len - 16)
            .map_err(CheckAndRepairError::Truncate)?;
    }

    let last_full_ts_in_data = last_full_ts_in_data.expect(
        "last time in the data since last_line_in_data_start is not None \
         so there is at least one time in the data",
    );
    if last_full_ts_in_data != last_full_ts {
        Err(CheckAndRepairError::IndexMissesItems)
    } else {
        Ok(())
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
