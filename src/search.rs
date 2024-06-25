use std::io::{Read, Seek, SeekFrom};

use crate::data::Timestamp;
use crate::index::SearchBounds;
use crate::ByteSeries;
use crate::Error;

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
    ///returns the offset from the start of the file where the first line starts
    fn find_read_start(
        &mut self,
        start_time: Timestamp,
        start: u64,
        stop: u64,
    ) -> Result<u64, SeekError> {
        assert!(stop >= start + 2);

        //compare partial (16 bit) timestamps in between the bounds
        let mut buf = vec![0u8; (stop - start) as usize];
        self.data.file_handle.seek(SeekFrom::Start(start))?;
        self.data.file_handle.read_exact(&mut buf)?;

        if let Some(start_line) = buf
            .chunks_exact(self.payload_size() + 2)
            .map(|line| {
                line[0..2]
                    .try_into()
                    .expect("start and stop at least 2 apart")
            })
            .map(u16::from_le_bytes)
            .position(|line_ts| line_ts > start_time as u16)
        {
            let start_byte = start + start_line as u64 * (self.payload_size() + 2) as u64;
            Ok(start_byte)
        } else {
            Ok(stop)
        }
    }

    /// returns start, stop and full timestamp for first line
    fn get_bounds(
        &mut self,
        start_time: Timestamp,
        end_time: Timestamp,
    ) -> Result<(u64, u64, Timestamp), SeekError> {
        if self.data.data_len == 0 {
            return Err(SeekError::EmptyFile);
        }
        if start_time >= self.last_time_in_data.unwrap() {
            return Err(SeekError::StartAfterData);
        }
        if end_time <= self.first_time_in_data.unwrap() {
            return Err(SeekError::StopBeforeData);
        }

        let (start_bound, stop_bound, full_time) =
            self.data.index.search_bounds(start_time, end_time);

        //must be a solvable request
        let start_byte = match start_bound {
            SearchBounds::Found(pos) => pos,
            SearchBounds::Clipped => 0,
            SearchBounds::TillEnd(start) => {
                let end = self.data.data_len;
                self.find_read_start(start_time, start, end)?
            }
            SearchBounds::Window(start, stop) => self.find_read_start(start_time, start, stop)?,
        };

        let stop_byte = match stop_bound {
            SearchBounds::Found(pos) => pos,
            SearchBounds::TillEnd(pos) => {
                let end = self.data.data_len;
                self.find_read_stop(end_time, pos, end)?
            }
            SearchBounds::Window(start, stop) => self.find_read_stop(end_time, start, stop)?,
            SearchBounds::Clipped => panic!("should never occur"),
        };

        Ok((start_byte, stop_byte, full_time))
    }

    ///returns the offset from the start of the file where last line **stops**
    fn find_read_stop(
        &mut self,
        end_time: Timestamp,
        start: u64,
        stop: u64,
    ) -> Result<u64, SeekError> {
        //compare partial (16 bit) timestamps in between these bounds
        let mut buf = vec![0u8; (stop - start) as usize];
        self.data.file_handle.seek(SeekFrom::Start(start))?;
        self.data.file_handle.read_exact(&mut buf)?;

        if let Some(stop_line) = buf
            .chunks_exact(self.payload_size() + 2)
            .map(|line| line[..2].try_into().expect("chunks are at least 2 long"))
            .map(u16::from_le_bytes)
            .rposition(|ts_small| ts_small <= end_time as u16)
        {
            let stop_byte = start + stop_line as u64 * (self.payload_size() + 2) as u64;
            Ok(stop_byte)
        } else {
            Ok(stop)
        }
    }
}
