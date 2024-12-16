use core::fmt;
use std::io::{Read, Seek, Write};
use std::ops::Sub;
use std::path::Path;
use tracing::instrument;

use crate::file::{self, FileWithHeader, OffsetFile};
use crate::Timestamp;

use super::inline_meta::SetLen;
use super::MAX_SMALL_TS;

pub(crate) mod create;

/// An offset from the start where a metaposition starts
#[derive(Debug, Clone, Copy)]
pub(crate) struct MetaPos(pub(crate) u64);

impl MetaPos {
    pub(crate) const ZERO: Self = Self(0);
    pub(crate) fn line_start(&self, payload_size: PayloadSize) -> LinePos {
        LinePos(self.0 + payload_size.metainfo_size() as u64)
    }
    pub(crate) fn to_le_bytes(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    pub(crate) fn raw_offset(&self) -> u64 {
        self.0
    }
}

impl Sub<MetaPos> for MetaPos {
    type Output = u64;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0 - rhs.0
    }
}

/// Start of a line
#[derive(Debug, Clone, Copy)]
pub(crate) struct LinePos(pub(crate) u64);
impl LinePos {
    pub(crate) fn raw_offset(&self) -> u64 {
        self.0
    }
    pub(crate) fn next_line_start(&self, payload_size: PayloadSize) -> Self {
        Self(self.0 + payload_size.line_size() as u64)
    }
}

impl Sub<LinePos> for LinePos {
    type Output = u64;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0 - rhs.0
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PayloadSize(usize);

impl PayloadSize {
    pub(crate) fn metainfo_size(&self) -> usize {
        super::inline_meta::meta::lines_per_metainfo(self.0) * (self.line_size())
    }
    pub(crate) fn line_size(&self) -> usize {
        self.0 + 2
    }
    pub(crate) fn raw(&self) -> usize {
        self.0
    }
    pub(crate) fn from_raw(raw: usize) -> Self {
        Self(raw)
    }
}

#[derive(Debug)]
pub(crate) struct Entry {
    pub timestamp: Timestamp,
    /// the offset from the start where the meta section with the same timestamp
    /// starts in the file
    pub meta_start: MetaPos,
}

pub(crate) struct Index {
    pub(crate) file: OffsetFile,

    entries: Vec<Entry>,
    /// time for next point is 1 larger the this
    last_timestamp: Option<Timestamp>,
}

impl fmt::Debug for Index {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Index")
            .field("file", &self.file)
            .field("# entries", &self.entries.len())
            .field("last_timestamp", &self.last_timestamp)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub(crate) enum StartArea {
    /// This line has the same time as the start time
    Found(LinePos),
    /// start lies before first timestamp in data
    Clipped,
    /// start timestamp lies in section from this position till the end of the data
    TillEnd(LinePos),
    /// start timestamp lies in between the first and second position
    Window(LinePos, MetaPos),
    /// start ts is in a data gap. We again have data starting after the end of
    /// the gap (`stops`)
    Gap { stops: LinePos },
}

#[derive(Debug, Clone)]
pub(crate) enum EndArea {
    /// This line has the same time as the sought after end time
    Found(LinePos),
    /// end timestamp lies in section from this position till the end of the data
    TillEnd(LinePos),
    /// end timestamp lies in between the first and second position
    Window(LinePos, MetaPos),
    /// end lies before this time/point however we have no data between here and it
    Gap { start: MetaPos },
}

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("Could not open index")]
    File(#[source] file::OpenError),
    #[error("The header in the index and byteseries are different")]
    IndexAndDataHeaderDifferent,
    #[error("reading in index: {0}")]
    Reading(std::io::Error),
    #[error("Could not check or repair the index")]
    CheckOrRepair(
        #[source]
        #[from]
        CheckAndRepairError,
    ),
}

impl Index {
    #[instrument]
    pub(crate) fn new(
        name: impl AsRef<Path> + fmt::Debug,
    ) -> Result<Index, file::OpenError> {
        let file =
            FileWithHeader::new(name.as_ref().with_extension("byteseries_index"), &[])?;

        Ok(Index {
            file: file.split_off_header().0,

            entries: Vec::new(),
            last_timestamp: None,
        })
    }
    #[instrument]
    pub(crate) fn open_existing(
        name: impl AsRef<Path> + fmt::Debug,
        last_line_in_data_start: Option<u64>,
        last_full_ts_in_data: Option<Timestamp>,
    ) -> Result<Index, OpenError> {
        let file = FileWithHeader::open_existing(
            name.as_ref().with_extension("byteseries_index"),
        )
        .map_err(OpenError::File)?;

        let (mut file, _) = file.split_off_header();
        check_and_repair(&mut file, last_line_in_data_start, last_full_ts_in_data)?;
        let mut bytes = Vec::new();
        file.seek(std::io::SeekFrom::Start(0))
            .map_err(OpenError::Reading)?;
        file.read_to_end(&mut bytes).map_err(OpenError::Reading)?;

        let entries: Vec<_> = bytes
            .chunks_exact(16)
            .map(|line| {
                let timestamp: [u8; 8] =
                    line[0..8].try_into().expect("line is 2*8 bytes");
                let timestamp = u64::from_le_bytes(timestamp);
                let line_start: [u8; 8] =
                    line[8..].try_into().expect("line is 2*8 bytes");
                let line_start = u64::from_le_bytes(line_start);
                Entry {
                    timestamp,
                    meta_start: MetaPos(line_start),
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

    /// `line_start` points to the start of the meta section in the data file
    #[instrument(level = "trace", skip(self), ret)]
    pub(crate) fn update(
        &mut self,
        timestamp: u64,
        meta_start: MetaPos,
    ) -> Result<(), std::io::Error> {
        let ts = timestamp;
        self.file.write_all(&ts.to_le_bytes())?;
        self.file.write_all(&meta_start.to_le_bytes())?;

        self.entries.push(Entry {
            timestamp,
            meta_start,
        });
        self.last_timestamp = Some(timestamp);
        Ok(())
    }

    #[instrument]
    pub(crate) fn start_search_bounds(
        &self,
        start_ts: Timestamp,
        payload_size: PayloadSize,
    ) -> (StartArea, Timestamp) {
        let idx = self
            .entries
            .binary_search_by_key(&start_ts, |e| e.timestamp);
        let end = match idx {
            Ok(i) => {
                let next_line_start = self.entries[i].meta_start.line_start(payload_size);
                return (StartArea::Found(next_line_start), start_ts);
            }
            Err(end) => end,
        };

        if end == 0 {
            return (StartArea::Clipped, self.entries[0].timestamp);
        }

        if end == self.entries.len() {
            let next_line_start = self
                .entries
                .last()
                .unwrap()
                .meta_start
                .line_start(payload_size);
            return (
                StartArea::TillEnd(next_line_start),
                self.entries.last().unwrap().timestamp,
            );
        }

        // End is not 0 or 1 thus data[end] and data[end-1] exist
        if in_gap(start_ts, self.entries[end - 1].timestamp) {
            return (
                StartArea::Gap {
                    stops: self.entries[end].meta_start.line_start(payload_size),
                },
                self.entries[end].timestamp,
            );
        }

        if start_ts >= self.entries[end].timestamp {
            let stop = self.entries[end].meta_start.line_start(payload_size);
            (StartArea::Gap { stops: stop }, self.entries[end].timestamp)
        } else {
            let start = self.entries[end - 1].meta_start.line_start(payload_size);
            let stop = self.entries[end].meta_start;
            (
                StartArea::Window(start, stop),
                self.entries[end - 1].timestamp,
            )
        }
    }

    #[instrument(level = "debug", ret)]
    pub(crate) fn end_search_bounds(
        &self,
        end_ts: Timestamp,
        payload_size: PayloadSize,
    ) -> (EndArea, Timestamp) {
        let idx = self.entries.binary_search_by_key(&end_ts, |e| e.timestamp);
        let end = match idx {
            Ok(i) => {
                let pos = self.entries[i].meta_start.line_start(payload_size);
                return (EndArea::Found(pos), self.entries[i].timestamp);
            }
            Err(end) => end,
        };

        assert!(end > 0, "checked in check_range");

        if end == self.entries.len() {
            let last = self
                .entries
                .last()
                .expect("Index always has one entry when the byteseries is not empty");
            let start = last.meta_start.line_start(payload_size);
            return (EndArea::TillEnd(start), last.timestamp);
        }

        // End is not 0 or 1 thus data[end] and data[end-1] exist
        if in_gap(end_ts, self.entries[end - 1].timestamp) {
            return (
                EndArea::Gap {
                    start: self.entries[end - 1].meta_start,
                },
                self.entries[end - 1].timestamp,
            );
        }

        let start = self.entries[end - 1].meta_start.line_start(payload_size);
        let stop = self.entries[end].meta_start;
        (
            EndArea::Window(start, stop),
            self.entries[end - 1].timestamp,
        )
    }
    pub(crate) fn first_meta_timestamp(&self) -> Option<Timestamp> {
        self.entries.first().map(|e| e.timestamp)
    }

    pub(crate) fn last_timestamp(&self) -> Option<Timestamp> {
        self.last_timestamp
    }

    #[instrument]
    pub(crate) fn meta_ts_for(&self, line_start: LinePos) -> u64 {
        match self
            .entries
            .binary_search_by_key(&line_start.0, |entry| entry.meta_start.0)
        {
            Ok(idx) => self.entries[idx].timestamp,
            // inserting at idx would keep the list sorted, so the full timestamp
            // before start lies at idx - 1
            Err(idx) => self.entries[idx - 1].timestamp,
        }
    }

    pub(crate) fn clear(&mut self) -> Result<(), std::io::Error> {
        self.file.set_len(0)?;
        self.entries.clear();
        self.last_timestamp = None;
        Ok(())
    }
}

fn in_gap(val: Timestamp, gap_start: Timestamp) -> bool {
    let reach = MAX_SMALL_TS;
    val > gap_start + reach
}

#[derive(Debug, thiserror::Error)]
pub enum CheckAndRepairError {
    #[error(
        "The last item in the index does not match the last in the file.\
        last timestamp in data: {last_ts_in_data}, in index: {last_ts_in_index}"
    )]
    IndexLastTimeMismatch {
        last_ts_in_index: Timestamp,
        last_ts_in_data: Timestamp,
    },
    #[error("Could not repair the index by truncating it: {0}")]
    Truncate(std::io::Error),
    #[error("Could not check the index, failed to get its length: {0}")]
    GetLength(std::io::Error),
    #[error("Could not check the index, failed to seek in it: {0}")]
    Seek(std::io::Error),
    #[error("Could not check the index, failed to read it: {0}")]
    Read(std::io::Error),
}

/// repairs only failed writes not user induced damage
/// such as purposefully truncating files
#[instrument]
pub(crate) fn check_and_repair(
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
    let last_line_start: [u8; 8] =
        last_entry[8..].try_into().expect("just read 16 bytes");
    let last_line_start = u64::from_le_bytes(last_line_start);

    let len = file.len().map_err(CheckAndRepairError::GetLength)?;
    if last_line_start > last_line_in_data_start {
        // can only be caused by a failed write in data with a
        // succeed one in the index. Taking off that succeeded line
        // in the index is enough to restore it.
        file.set_len(len - 16)
            .map_err(CheckAndRepairError::Truncate)?;
    }

    let last_full_ts_in_data = last_full_ts_in_data.expect(
        "last time in the data since last_line_in_data_start is not None \
         so there is at least one time in the data",
    );

    if last_full_ts_in_data == last_full_ts {
        Ok(())
    } else {
        Err(CheckAndRepairError::IndexLastTimeMismatch {
            last_ts_in_index: last_full_ts,
            last_ts_in_data: last_full_ts_in_data,
        })
    }
}
