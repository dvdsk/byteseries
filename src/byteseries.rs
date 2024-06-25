pub mod data;
use core::fmt;
use std::path::Path;

use data::Data;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::instrument;

use crate::{Decoder, Error, Timestamp};

#[derive(Debug)]
pub struct ByteSeries {
    pub(crate) data: Data,

    pub(crate) first_time_in_data: Option<Timestamp>,
    pub(crate) last_time_in_data: Option<Timestamp>,
}

impl ByteSeries {
    #[instrument]
    pub fn new<H>(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: usize,
        header: H,
    ) -> Result<ByteSeries, Error>
    where
        H: DeserializeOwned + Serialize + Eq + fmt::Debug + 'static + Clone,
    {
        Ok(ByteSeries {
            first_time_in_data: None,
            last_time_in_data: None,
            data: Data::new(name, payload_size, header)?,
        })
    }

    /// line size in bytes, path is *without* any extension
    #[instrument]
    pub fn open_existing<H>(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: usize,
    ) -> Result<(ByteSeries, H), Error>
    where
        H: DeserializeOwned + Serialize + Eq + fmt::Debug + 'static + Clone,
    {
        let (mut data, header) = Data::open_existing(name, payload_size)?;

        let bs = ByteSeries {
            first_time_in_data: data.first_time(),
            last_time_in_data: data.last_time(),
            data,
        };

        Ok((bs, header))
    }

    pub(crate) fn payload_size(&self) -> usize {
        self.data.payload_size()
    }

    pub fn push_line(&mut self, ts: Timestamp, line: impl AsRef<[u8]>) -> Result<(), Error> {
        //write 16 bit timestamp and then the line to file
        //for now no support for sign bit since data will always be after 0 (1970)
        if ts <= self.last_time_in_data.unwrap_or(0) {
            return Err(Error::NewLineBeforePrevious {
                new: ts,
                prev: self.last_time_in_data,
            });
        }
        self.data.push_data(ts, line.as_ref())?;
        self.last_time_in_data = Some(ts);
        self.first_time_in_data.get_or_insert(ts);
        Ok(())
    }

    pub fn flush_to_disk(&mut self) {
        self.data.flush_to_disk()
    }

    pub fn read_to_data<D: Decoder>(
        &mut self,
        start_byte: u64,
        stop_byte: u64,
        first_full_ts: Timestamp,
        decoder: &mut D,
        timestamps: &mut Vec<Timestamp>,
        data: &mut Vec<D::Item>,
    ) -> Result<(), Error> {
        self.data.read_to_data(
            start_byte,
            stop_byte,
            first_full_ts,
            decoder,
            timestamps,
            data,
        )
    }
}
