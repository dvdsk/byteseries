use byteorder::{ByteOrder, LittleEndian};
use chrono::{DateTime, Utc};
use std::io::{Read, Seek, SeekFrom};

use crate::data::FullTime;
use crate::header::SearchBounds;
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
    pub start: u64,
    pub stop: u64,
    pub curr: u64,
    pub full_time: FullTime,
}

impl TimeSeek {
    pub fn new(
        series: &mut ByteSeries,
        start: chrono::DateTime<Utc>,
        stop: chrono::DateTime<Utc>,
    ) -> Result<Self, Error> {
        let (start, stop, full_time) = series.get_bounds(start, stop)?;

        Ok(TimeSeek {
            start,
            stop,
            curr: start,
            full_time,
        })
    }
    pub fn lines(&self, series: &ByteSeries) -> u64 {
        (self.stop - self.start) / (series.full_line_size as u64)
    }
}

impl ByteSeries {
    fn find_read_start(
        &mut self,
        start_time: DateTime<Utc>,
        start: u64,
        stop: u64,
    ) -> Result<u64, SeekError> {
        //compare partial (16 bit) timestamps in between the bounds
        let mut buf = vec![0u8; (stop - start) as usize];
        self.data.seek(SeekFrom::Start(start))?;
        self.data.read_exact(&mut buf)?;

        for line_start in (0..buf.len().saturating_sub(2)).step_by(self.full_line_size) {
            if LittleEndian::read_u16(&buf[line_start..line_start + 2])
                >= start_time.timestamp() as u16
            {
                log::debug!("setting start_byte from liniar search, pos: {}", line_start);
                let start_byte = start + line_start as u64;
                return Ok(start_byte);
            }
        }

        //no data more recent then start time within bounds, return location of most recent data
        Ok(stop)
    }

    pub fn get_bounds(
        &mut self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<(u64, u64, FullTime), SeekError> {
        //check if the datafile isnt empty

        if self.data_size == 0 {
            return Err(SeekError::EmptyFile);
        }
        if start_time.timestamp() >= self.last_time_in_data.unwrap() {
            return Err(SeekError::StartAfterData);
        }
        if end_time.timestamp() <= self.first_time_in_data.unwrap() {
            return Err(SeekError::StopBeforeData);
        }

        let (start_bound, stop_bound, full_time) = self
            .header
            .search_bounds(start_time.timestamp(), end_time.timestamp());

        //must be a solvable request
        let start_byte = match start_bound {
            SearchBounds::Found(pos) => pos,
            SearchBounds::Clipped => 0,
            SearchBounds::TillEnd(start) => {
                let end = self.data_size;
                self.find_read_start(start_time, start, end)?
            }
            SearchBounds::Window(start, stop) => self.find_read_start(start_time, start, stop)?,
        };

        let stop_byte = match stop_bound {
            SearchBounds::Found(pos) => pos,
            SearchBounds::TillEnd(pos) => {
                let end = self.data_size;
                self.find_read_stop(end_time, pos, end)?
            }
            SearchBounds::Window(start, stop) => self.find_read_stop(end_time, start, stop)?,
            SearchBounds::Clipped => panic!("should never occur"),
        };

        log::debug!(
            "start time: {}, {}; end_time: {}, {}",
            start_time,
            start_time.timestamp(),
            end_time,
            end_time.timestamp()
        );
        log::debug!("start_byte: {}", start_byte);

        Ok((start_byte, stop_byte, full_time))
    }

    fn find_read_stop(
        &mut self,
        end_time: DateTime<Utc>,
        start: u64,
        stop: u64,
    ) -> Result<u64, SeekError> {
        //compare partial (16 bit) timestamps in between these bounds
        let mut buf = vec![0u8; (stop - start) as usize];
        self.data.seek(SeekFrom::Start(start))?;
        self.data.read_exact(&mut buf)?;

        log::trace!("buf.len(): {}", buf.len());
        for line_start in (0..buf.len() - self.full_line_size + 1)
            .rev()
            .step_by(self.full_line_size)
        {
            //trace!("line: {}, {}", line_start, LittleEndian::read_u16(&buf[line_start..line_start + 2]));
            if LittleEndian::read_u16(&buf[line_start..line_start + 2])
                <= end_time.timestamp() as u16
            {
                log::debug!("setting start_byte from liniar search, start of search area");
                let stop_byte = start + line_start as u64;
                return Ok(stop_byte);
            }
        }
        Ok(stop)
    }
}
