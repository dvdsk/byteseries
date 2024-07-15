use core::fmt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::Write;
use std::path::Path;
use tracing::{instrument, warn};

use crate::file::{self, FileWithHeader, OffsetFile};
use crate::{Decoder, SeekPos, Timestamp};

pub(crate) mod inline_meta;
use inline_meta::FileWithInlineMeta;
pub mod index;
use index::Index;

use self::index::create::{self, last_meta_timestamp, ExtractingTsError};
use self::inline_meta::write_meta;

pub(crate) const MAX_SMALL_TS: u64 = (u16::MAX - 1) as u64;

#[derive(Debug)]
pub struct Data {
    pub(crate) file_handle: FileWithInlineMeta<OffsetFile>,
    pub(crate) index: Index,

    payload_size: usize,
    /// current length of the data file in bytes
    pub(crate) data_len: u64,
}

#[derive(Debug)]
struct EmptyDecoder;
impl Decoder for EmptyDecoder {
    type Item = ();
    fn decode_payload(&mut self, _: &[u8]) -> Self::Item {}
}

#[derive(Debug, thiserror::Error)]
pub enum CreateError {
    #[error("{0}")]
    File(file::OpenError),
    #[error("Could not check file for integrity or repair it: {0}")]
    CheckOrRepair(std::io::Error),
    #[error("{0}")]
    Index(file::OpenError),
    #[error("{0}")]
    GetLength(std::io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("{0}")]
    File(file::OpenError),
    #[error("Could not check file for integrity or repair it: {0}")]
    CheckOrRepair(std::io::Error),
    #[error("{0}")]
    Index(#[from] create::Error),
    #[error("{0}")]
    GetLength(std::io::Error),
    #[error(
        "Could not find last full time in data, needed to check\
        index integrity: {0}"
    )]
    GetLastMeta(ExtractingTsError),
}

#[derive(Debug, thiserror::Error)]
pub enum PushError {
    #[error("{0}")]
    File(file::OpenError),
    #[error("Could not insert meta section {0}")]
    Meta(std::io::Error),
    #[error("Failed to update index {0}")]
    Index(std::io::Error),
    #[error("Could not append new data to file")]
    Write(std::io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("The file is empty")]
    NoData,
    #[error("{0}")]
    Reading(std::io::Error),
}

impl Data {
    /// # Errors
    ///
    /// See the [`CreateError`] docs for an exhaustive list of everything that can go wrong.
    pub fn new<H>(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: usize,
        header: H,
    ) -> Result<Self, CreateError>
    where
        H: DeserializeOwned + Serialize + fmt::Debug + 'static + Clone,
    {
        let file = FileWithHeader::new(name.as_ref().with_extension("byteseries"), header.clone())
            .map_err(CreateError::File)?;
        let (file_handle, _) = file.split_off_header();
        let data_len = file_handle.data_len().map_err(CreateError::GetLength)?;
        let file_handle = FileWithInlineMeta::new(file_handle, payload_size)
            .map_err(CreateError::CheckOrRepair)?;
        let index = Index::new(name, header).map_err(CreateError::Index)?;
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
    ) -> Result<(Data, H), OpenError>
    where
        H: DeserializeOwned + Serialize + fmt::Debug + PartialEq + 'static + Clone,
    {
        let file: FileWithHeader<H> = FileWithHeader::open_existing(
            name.as_ref().with_extension("byteseries"),
            payload_size + 2,
        )
        .map_err(OpenError::File)?;
        let (file_handle, header) = file.split_off_header();

        let mut file_handle =
            FileWithInlineMeta::new(file_handle, payload_size).map_err(OpenError::CheckOrRepair)?;
        let data_len = file_handle
            .file_handle
            .data_len()
            .map_err(OpenError::GetLength)?;
        let last_line_starts = data_len.checked_sub((payload_size + 2) as u64);
        let last_full_ts_in_data = last_meta_timestamp(file_handle.inner_mut(), payload_size)
            .map_err(OpenError::GetLastMeta)?;
        let index =
            match Index::open_existing(&name, &header, last_line_starts, last_full_ts_in_data) {
                Ok(index) => index,
                Err(e) => {
                    warn!("Creating new index since existing could not be used: {e}");
                    Index::create_from_byteseries(
                        file_handle.inner_mut(),
                        payload_size,
                        name,
                        header.clone(),
                    )?
                }
            };

        let data = Self {
            file_handle,
            index,
            payload_size,
            data_len,
        };
        Ok((data, header))
    }

    /// # Errors
    ///
    /// See the [`ReadError`] docs for an exhaustive list of everything that can go wrong.
    pub fn last_line<T: std::fmt::Debug + std::clone::Clone>(
        &mut self,
        decoder: &mut impl Decoder<Item = T>,
    ) -> Result<(Timestamp, T), ReadError> {
        let mut timestamps = Vec::new();
        let mut data = Vec::new();
        let seek = SeekPos {
            first_full_ts: self.index.last_timestamp().ok_or(ReadError::NoData)?,
            start: self.data_len - (self.payload_size + 2) as u64,
            end: self.data_len,
        };
        self.file_handle
            .read(decoder, &mut timestamps, &mut data, seek)
            .map_err(ReadError::Reading)?;

        let ts = timestamps.pop().ok_or(ReadError::NoData)?;
        let item = data.pop().ok_or(ReadError::NoData)?;

        Ok((ts, item))
    }

    #[instrument]
    pub(crate) fn first_time(&mut self) -> Option<Timestamp> {
        self.index.first_meta_timestamp()
    }

    #[instrument]
    pub(crate) fn last_time(&mut self) -> Result<Option<Timestamp>, ReadError> {
        match self.last_line(&mut EmptyDecoder) {
            Ok((ts, _)) => Ok(Some(ts)),
            Err(ReadError::NoData) => Ok(None),
            Err(other) => Err(other),
        }
    }

    /// Append data to disk but do not flush, a crash can still lead to the data being lost
    #[instrument(skip(self, line), level = "trace")]
    pub fn push_data(&mut self, ts: Timestamp, line: &[u8]) -> Result<(), PushError> {
        //we store the timestamp - the last recorded full timestamp as u16. If
        //that overflows a new timestamp will be inserted. The 16 bit small
        //timestamp is stored little endian

        tracing::trace!("{}, {:?}", ts, self.index.last_timestamp());
        let small_ts = self
            .index
            .last_timestamp()
            .map(|last_timestamp| {
                ts.checked_sub(last_timestamp).expect(
                    "impossible for last_timestamp to be later (bigger) then new, \
                    since new timestamp is verified to be later then the last \
                    in Byteseries::push_line",
                )
            })
            .map(|diff| {
                if diff > MAX_SMALL_TS {
                    None
                } else {
                    Some(u16::try_from(diff).expect("MAX_SMALL_TS < u16::MAX"))
                }
            })
            .flatten();

        let small_ts = if let Some(small_ts) = small_ts {
            small_ts
        } else {
            tracing::debug!(
                "inserting full timestamp via and updating index\
                , timestamp: {ts}"
            );
            self.index
                .update(ts, self.data_len)
                .map_err(PushError::Index)?;
            let meta = ts.to_le_bytes();
            let written = write_meta(&mut self.file_handle, meta, self.payload_size)
                .map_err(PushError::Meta)?;
            self.data_len += written;
            0 // value does not matter, full timestamp just ahead is used
        };

        self.file_handle
            .write_all(&small_ts.to_le_bytes())
            .map_err(PushError::Write)?;
        self.file_handle
            .write_all(&line[..self.payload_size])
            .map_err(PushError::Write)?;
        self.data_len += (self.payload_size + 2) as u64;
        Ok(())
    }

    /// asks the os to write its buffers and block till its done
    pub(crate) fn flush_to_disk(&mut self) {
        self.file_handle.inner_mut().sync_data().unwrap();
        self.index.file.sync_data().unwrap();
    }

    /// # Errors
    ///
    /// See the [`ReadError`] docs for an exhaustive list of everything that can go wrong.
    pub fn read_all<D: Decoder>(
        &mut self,
        seek: SeekPos,
        decoder: &mut D,
        timestamps: &mut Vec<Timestamp>,
        data: &mut Vec<D::Item>,
    ) -> Result<(), ReadError> {
        self.file_handle
            .read(decoder, timestamps, data, seek)
            .map_err(ReadError::Reading)
    }

    #[instrument(skip(self, resampler, timestamps, data), err)]
    pub(crate) fn read_resampling<R: crate::Resampler>(
        &mut self,
        seek: SeekPos,
        resampler: &mut R,
        bucket_size: usize,
        timestamps: &mut Vec<u64>,
        data: &mut Vec<<R as Decoder>::Item>,
    ) -> Result<(), ReadError> {
        self.file_handle
            .read_resampling(resampler, bucket_size, timestamps, data, seek)
            .map_err(ReadError::Reading)
    }

    pub(crate) fn payload_size(&self) -> usize {
        self.payload_size
    }

    pub(crate) fn last_line_start(&self) -> u64 {
        self.data_len - (self.payload_size as u64 + 2)
    }
}
