use core::fmt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::Write;
use std::path::Path;
use time::OffsetDateTime;
use tracing::instrument;

use crate::index::Index;
use crate::util::{FileWithHeader, OffsetFile};
use crate::Error;

pub(crate) mod inline_meta;
use inline_meta::FileWithInlineMeta;

use self::inline_meta::write_meta;

#[derive(Debug)]
pub struct Data {
    pub(crate) file_handle: FileWithInlineMeta<OffsetFile>,
    pub(crate) index: Index,

    payload_size: usize,
    /// current length of the data file in bytes
    pub(crate) data_len: u64,
}

pub trait Decoder: core::fmt::Debug {
    type Item: core::fmt::Debug + Clone;
    fn decode_line(&mut self, line: &[u8]) -> Self::Item;
}

#[derive(Debug)]
struct EmptyDecoder;
impl Decoder for EmptyDecoder {
    type Item = ();
    fn decode_line(&mut self, _: &[u8]) -> Self::Item {
        ()
    }
}

pub type Timestamp = u64;

impl Data {
    pub fn new<H>(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: usize,
        header: H,
    ) -> Result<Self, Error>
    where
        H: DeserializeOwned + Serialize + Eq + fmt::Debug + 'static + Clone,
    {
        let file = FileWithHeader::new(name.as_ref().with_extension("byteseries"), header.clone())?;
        let index = Index::new(name, header)?;
        let (file_handle, _) = file.split_off_header();
        let data_len = file_handle.data_len()?;
        let file_handle = FileWithInlineMeta {
            file_handle,
            line_size: payload_size + 2,
        };
        Ok(Self {
            file_handle,
            index,
            payload_size,
            data_len,
        })
    }

    #[instrument]
    pub fn open_existing<H>(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: usize,
    ) -> Result<(Data, H), Error>
    where
        H: DeserializeOwned + Serialize + Eq + fmt::Debug + 'static + Clone,
    {
        // TODO, check for zero pattern at the end
        // a single u16 time of zeros
        // may only exists with a full timestamp in front
        let file: FileWithHeader<H> = FileWithHeader::open_existing(
            name.as_ref().with_extension("byteseries"),
            payload_size + 2,
        )?;
        let (mut file_handle, header) = file.split_off_header();
        let index = match Index::open_existing(&name, &header) {
            Ok(index) => index,
            Err(_) => {
                Index::restore_from_byteseries(&mut file_handle, payload_size, name, header.clone())
                    .map_err(Error::RestoringIndex)?
            }
        };

        let data_len = file_handle.data_len()?;
        let file_handle = FileWithInlineMeta {
            file_handle,
            line_size: payload_size + 2,
        };
        let data = Self {
            file_handle,
            index,
            payload_size,
            data_len,
        };
        Ok((data, header))
    }

    pub fn last_line<'a, T: std::fmt::Debug + std::clone::Clone>(
        &mut self,
        decoder: &mut impl Decoder<Item = T>,
    ) -> Result<(Timestamp, T), Error> {
        let start = self.data_len - (self.payload_size + 2) as u64;
        let end = self.data_len;

        let mut timestamps = Vec::new();
        let mut data = Vec::new();
        self.file_handle.read2(
            decoder,
            &mut timestamps,
            &mut data,
            start,
            end,
            self.index.last_timestamp,
        )?;

        let ts = timestamps.pop().ok_or(Error::NoData)?;
        let item = data.pop().ok_or(Error::NoData)?;

        Ok((ts, item))
    }

    pub(crate) fn first_time(&mut self) -> Option<Timestamp> {
        self.index.first_time_in_data()
    }

    pub(crate) fn last_time(&mut self) -> Option<Timestamp> {
        self.last_line(&mut EmptyDecoder).map(|(ts, _)| ts).ok()
    }

    /// Append data to disk but do not flush, a crash can still lead to the data being lost
    pub fn push_data(&mut self, ts: Timestamp, line: &[u8]) -> Result<(), Error> {
        //we store the timestamp - the last recorded full timestamp as u16. If
        //that overflows a new timestamp will be inserted. The 16 bit small
        //timestamp is stored little endian
        let diff = ts - self.index.last_timestamp;
        let small_ts = if let Ok(small_ts) = TryInto::<u16>::try_into(diff) {
            small_ts
        } else {
            let meta = ts.to_le_bytes();
            let written = write_meta(&mut self.file_handle, meta, self.payload_size)?;
            self.data_len += written;
            self.index.update(ts, self.data_len)?;
            0
        };

        self.file_handle.write_all(&small_ts.to_le_bytes())?;
        self.file_handle.write_all(&line[..self.payload_size])?;
        self.data_len += (self.payload_size + 2) as u64;
        Ok(())
    }

    /// asks the os to write its buffers and block till its done
    pub(crate) fn flush_to_disk(&mut self) {
        self.file_handle.inner_mut().sync_data().unwrap();
        self.index.file.sync_data().unwrap();
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
        self.file_handle.read2(
            decoder,
            timestamps,
            data,
            start_byte,
            stop_byte,
            first_full_ts,
        )
    }
}

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
        self.data.payload_size
    }

    pub fn push_line(&mut self, time: OffsetDateTime, line: impl AsRef<[u8]>) -> Result<(), Error> {
        //write 16 bit timestamp and then the line to file
        //for now no support for sign bit since data will always be after 0 (1970)
        let ts = time.unix_timestamp() as Timestamp;
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
