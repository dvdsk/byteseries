use std::io::{Read, Seek, SeekFrom};
use std::ops::Bound;

use crate::byteseries::data::index::SearchArea;
use crate::byteseries::data::Data;
use crate::ByteSeries;
use crate::Timestamp;

#[derive(thiserror::Error, Debug)]
pub enum SeekError {
    #[error("could not find timestamp in this series")]
    NotFound,
    #[error("data file is empty")]
    EmptyFile,
    #[error("no data to return as the start time is after the last time in the data")]
    StartAfterData,
    #[error("no data to return as the stop time is before the data")]
    StopBeforeData,
    #[error("error while searching through data")]
    Io(#[from] std::io::Error),
}

#[derive(Debug)]
pub struct RoughSeekPos {
    start_ts: Timestamp,
    /// area where to search for the start time
    start_search_area: SearchArea,
    end_ts: Timestamp,
    /// area where to search for the end time
    end_search_area: SearchArea,
    /// 64 bit timestamp that should be added to the small time
    /// for the search for the start timestamp.
    start_section_full_ts: Timestamp,
    /// 64 bit timestamp that should be added to the small time
    /// during the search for the end timestamp.
    end_section_full_ts: Timestamp,
}

impl RoughSeekPos {
    pub(crate) fn new(data: &Data, start: Bound<Timestamp>, end: Bound<Timestamp>) -> Self {
        let start_ts = match start {
            Bound::Included(ts) => ts,
            Bound::Excluded(ts) => ts - 1,
            Bound::Unbounded => data.index.first_time_in_data().expect("data_len > 0"),
        };

        let (start_search_area, start_section_full_ts) = match start {
            Bound::Included(ts) => data.index.start_search_bounds(ts, data.payload_size()),
            Bound::Excluded(ts) => data.index.start_search_bounds(ts - 1, data.payload_size()),
            Bound::Unbounded => (
                SearchArea::Found(0),
                data.index.first_time_in_data().expect("data_len > 0"),
            ),
        };

        let end_ts = match end {
            Bound::Included(ts) => ts,
            Bound::Excluded(ts) => ts - 1,
            Bound::Unbounded => data.index.last_timestamp().expect("data_len > 0"),
        };

        let (end_search_area, end_section_full_ts) = match end {
            Bound::Included(ts) => data.index.end_search_bounds(ts, data.payload_size()),
            Bound::Excluded(ts) => data.index.end_search_bounds(ts - 1, data.payload_size()),
            Bound::Unbounded => (
                SearchArea::Found(data.last_line_start()),
                data.index.last_timestamp().expect("data_len > 0"),
            ),
        };

        Self {
            start_ts,
            start_search_area,
            end_ts,
            end_search_area,
            start_section_full_ts,
            end_section_full_ts,
        }
    }

    pub(crate) fn refine(self, data: &mut Data) -> Result<SeekPos, SeekError> {
        let start_time: u16 = self
            .start_ts
            .checked_sub(self.start_section_full_ts)
            .expect(
                "search_bounds should be such that requested_start_time falls within \
                start_full_time..start_full_time+u16::MAX",
            )
            .try_into()
            .expect("search range should be smaller then u16::MAX");
        let start_byte = match self.start_search_area {
            SearchArea::Found(pos) => pos,
            SearchArea::Clipped => 0,
            SearchArea::TillEnd(start) => {
                let end = data.data_len;
                find_read_start(data, start_time, start, end)?
            }
            SearchArea::Window(start, stop) => find_read_start(data, start_time, start, stop)?,
        };

        let end_time: u16 = self
            .end_ts
            .checked_sub(self.end_section_full_ts)
            .expect(
                "search_bounds should be such that requested_end_time falls within \
                end_full_time..end_full_time+u16::MAX",
            )
            .try_into()
            .expect("search range should be smaller then u16::MAX");
        let end_byte = match self.end_search_area {
            SearchArea::Found(pos) => pos,
            SearchArea::TillEnd(pos) => {
                let end = data.data_len;
                find_read_end(data, end_time, pos, end)?
            }
            SearchArea::Window(start, end) => find_read_end(data, end_time, start, end)?,
            SearchArea::Clipped => panic!("should never occur"),
        };

        Ok(SeekPos {
            start: start_byte,
            end: end_byte,
            first_full_ts: self.start_section_full_ts,
        })
    }
}

#[derive(Debug)]
pub struct SeekPos {
    /// start of the first line that should be read
    pub start: u64,
    /// start of the last line that should be read
    pub end: u64,
    /// 64 bit timestamp that should be added to the small time
    /// for the first section.
    pub first_full_ts: Timestamp,
}


impl SeekPos {
    pub fn lines(&self, series: &ByteSeries) -> u64 {
        (self.end - self.start) / (series.payload_size() + 2) as u64
    }
}

/// returns the offset from the start of the file where the first line starts
fn find_read_start(
    data: &mut Data,
    start_time: u16,
    start: u64,
    stop: u64,
) -> Result<u64, SeekError> {
    assert!(stop >= start + 2);

    let mut buf = vec![0u8; (stop - start) as usize];
    data.file_handle.seek(SeekFrom::Start(start))?;
    data.file_handle.file_handle.read_exact(&mut buf)?;

    if let Some(start_line) = buf
        .chunks_exact(data.payload_size() + 2)
        .map(|line| {
            line[0..2]
                .try_into()
                .expect("start and stop at least 2 apart")
        })
        .map(u16::from_le_bytes)
        .position(|line_ts| line_ts >= start_time)
    {
        let start_byte = start + start_line as u64 * (data.payload_size() + 2) as u64;
        Ok(start_byte)
    } else {
        Ok(stop)
    }
}

/// returns the offset from the start of the file where last line **stops**
fn find_read_end(data: &mut Data, end_time: u16, start: u64, stop: u64) -> Result<u64, SeekError> {
    //compare partial (16 bit) timestamps in between these bounds
    let mut buf = vec![0u8; (stop - start) as usize];
    data.file_handle.seek(SeekFrom::Start(start))?;
    data.file_handle.file_handle.read_exact(&mut buf)?;

    if let Some(stop_line) = buf
        .chunks_exact(data.payload_size() + 2)
        .map(|line| line[..2].try_into().expect("chunks are at least 2 long"))
        .map(u16::from_le_bytes)
        .rposition(|line_ts| line_ts <= end_time)
    {
        let stop_byte = start + stop_line as u64 * (data.payload_size() + 2) as u64;
        Ok(stop_byte)
    } else {
        Ok(stop)
    }
}
