use std::io::{Read, Seek, SeekFrom};

use crate::byteseries::data::index::SearchBounds;
use crate::ByteSeries;
use crate::Error;
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
pub struct TimeSeek {
    /// position from start in bytes
    pub start: u64,
    /// position from start in bytes
    pub stop: u64,
    pub first_full_ts: Timestamp,
}

impl TimeSeek {
    pub fn new(series: &mut ByteSeries, start: Timestamp, stop: Timestamp) -> Result<Self, Error> {
        let (start, stop, first_full_ts) = series.get_bounds(start, stop)?;

        Ok(TimeSeek {
            start,
            stop,
            first_full_ts,
        })
    }
    pub fn lines(&self, series: &ByteSeries) -> u64 {
        (self.stop - self.start) / (series.payload_size() + 2) as u64
    }
}

impl ByteSeries {
    /// returns start, stop and full timestamp for first line
    fn get_bounds(
        &mut self,
        requested_start_time: Timestamp,
        requested_end_time: Timestamp,
    ) -> Result<(u64, u64, Timestamp), SeekError> {
        if self.data.data_len == 0 {
            return Err(SeekError::EmptyFile);
        }
        if requested_start_time >= self.last_time_in_data.unwrap() {
            return Err(SeekError::StartAfterData);
        }
        if requested_end_time <= self.first_time_in_data.unwrap() {
            return Err(SeekError::StopBeforeData);
        }

        let (start_bound, start_full_time) = self
            .data
            .index
            .start_search_bounds(requested_start_time, self.payload_size());
        let start_time: u16 = requested_start_time
            .checked_sub(start_full_time)
            .expect(
                "search_bounds should be such that requested_start_time falls within \
                start_full_time..start_full_time+u16::MAX",
            )
            .try_into()
            .expect("search range should be smaller then u16::MAX");
        let start_byte = match start_bound {
            SearchBounds::Found(pos) => pos,
            SearchBounds::Clipped => 0,
            SearchBounds::TillEnd(start) => {
                let end = self.data.data_len;
                self.find_read_start(start_time, start, end)?
            }
            SearchBounds::Window(start, stop) => self.find_read_start(start_time, start, stop)?,
        };

        let (end_bound, end_full_time) = self
            .data
            .index
            .end_search_bounds(requested_end_time, self.payload_size());
        let end_time: u16 = requested_end_time
            .checked_sub(end_full_time)
            .expect(
                "search_bounds should be such that requested_end_time falls within \
                end_full_time..end_full_time+u16::MAX",
            )
            .try_into()
            .expect("search range should be smaller then u16::MAX");
        let end_byte = match end_bound {
            SearchBounds::Found(pos) => pos,
            SearchBounds::TillEnd(pos) => {
                let end = self.data.data_len;
                self.find_read_end(end_time, pos, end)?
            }
            SearchBounds::Window(start, end) => self.find_read_end(end_time, start, end)?,
            SearchBounds::Clipped => panic!("should never occur"),
        };

        Ok((start_byte, end_byte, start_full_time))
    }

    /// returns the offset from the start of the file where the first line starts
    fn find_read_start(
        &mut self,
        start_time: u16,
        start: u64,
        stop: u64,
    ) -> Result<u64, SeekError> {
        assert!(stop >= start + 2);

        let mut buf = vec![0u8; (stop - start) as usize];
        self.data.file_handle.seek(SeekFrom::Start(start))?;
        self.data.file_handle.file_handle.read_exact(&mut buf)?;

        if let Some(start_line) = buf
            .chunks_exact(self.payload_size() + 2)
            .map(|line| {
                line[0..2]
                    .try_into()
                    .expect("start and stop at least 2 apart")
            })
            .map(u16::from_le_bytes)
            .position(|line_ts| line_ts >= start_time)
        {
            let start_byte = start + start_line as u64 * (self.payload_size() + 2) as u64;
            Ok(start_byte)
        } else {
            Ok(stop)
        }
    }

    /// returns the offset from the start of the file where last line **stops**
    fn find_read_end(&mut self, end_time: u16, start: u64, stop: u64) -> Result<u64, SeekError> {
        //compare partial (16 bit) timestamps in between these bounds
        let mut buf = vec![0u8; (stop - start) as usize];
        self.data.file_handle.seek(SeekFrom::Start(start))?;
        self.data.file_handle.file_handle.read_exact(&mut buf)?;

        if let Some(stop_line) = buf
            .chunks_exact(self.payload_size() + 2)
            .map(|line| line[..2].try_into().expect("chunks are at least 2 long"))
            .map(u16::from_le_bytes)
            .rposition(|line_ts| line_ts <= end_time)
        {
            let stop_byte = start + stop_line as u64 * (self.payload_size() + 2) as u64;
            Ok(stop_byte)
        } else {
            Ok(stop)
        }
    }
}
