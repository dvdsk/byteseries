use core::fmt;
use inline_meta::meta::lines_per_metainfo;
use std::io::Write;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use tracing::{instrument, warn};

use crate::file::{self, FileWithHeader, OffsetFile};
use crate::{Decoder, Pos, Timestamp};

pub(crate) mod inline_meta;
use inline_meta::FileWithInlineMeta;
pub mod index;
use index::{Index, LinePos, PayloadSize};

use self::index::create::{self, last_meta_timestamp, ExtractingTsError};
use self::inline_meta::{meta, SetLen};

/// largest small timestamp that can be stored. This corresponds to
/// [254, 255] (little endian). The pattern [255, 255] indicates a meta timestamp.
pub(crate) const MAX_SMALL_TS: u64 = (u16::MAX - 1) as u64;

#[derive(Debug)]
pub(crate) struct Data {
    pub(crate) file_handle: FileWithInlineMeta<OffsetFile>,
    pub(crate) index: Index,

    payload_size: PayloadSize,
    /// current length of the data file in bytes
    pub(crate) data_len: u64,
    /// last timestamp in the data
    last_time: Option<Timestamp>,
}

#[derive(Debug)]
struct EmptyDecoder;
impl Decoder for EmptyDecoder {
    type Item = ();
    fn decode_payload(&mut self, _: &[u8]) -> Self::Item {}
}

#[derive(Debug, thiserror::Error)]
pub enum CreateError {
    #[error("Could not create the data file at path: {path:?}")]
    File {
        #[source]
        source: file::OpenError,
        path: PathBuf,
    },
    #[error("Could not check file for integrity or repair it: {0}")]
    CheckOrRepair(std::io::Error),
    #[error("Could not create the index file")]
    Index(#[source] file::OpenError),
    #[error("Failed to get the length of the data: {0}")]
    GetLength(std::io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("Can not open, path: {path}")]
    File {
        #[source]
        source: file::OpenError,
        path: PathBuf,
    },
    #[error("Could not check file for integrity or repair it: {0}")]
    CheckOrRepair(std::io::Error),
    #[error("{0}")]
    Index(#[from] create::Error),
    #[error("{0}")]
    GetLength(std::io::Error),
    #[error(
        "Could not find last full time in data, needed to check\
        index integrity"
    )]
    GetLastMeta(#[source] ExtractingTsError),
    #[error("Could not read the last line to get the last time in Data")]
    ReadLastTime(#[source] ReadError),
}

#[derive(Debug, thiserror::Error)]
pub enum PushError {
    #[error("Error opening file")]
    File(#[source] file::OpenError),
    #[error("Could not insert meta section {0}")]
    Meta(std::io::Error),
    #[error("Failed to update index {0}")]
    Index(std::io::Error),
    #[error("Could not append new data to file")]
    Write(std::io::Error),
    #[error("Can only append items newer then the last")]
    OutOfOrder { last: Timestamp, item: Timestamp },
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
    /// Will return an error if there already is a file
    pub(crate) fn new(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: PayloadSize,
        header: &[u8],
    ) -> Result<Self, CreateError> {
        let path = name.as_ref().with_extension("byteseries");
        let file = FileWithHeader::new(&path, header)
            .map_err(|source| CreateError::File { source, path })?;
        let (file_handle, _) = file.split_off_header();
        let data_len = file_handle
            .data_len_bytes()
            .map_err(CreateError::GetLength)?;
        let file_handle = FileWithInlineMeta::new(file_handle, payload_size)
            .map_err(CreateError::CheckOrRepair)?;
        let index = Index::new(name).map_err(CreateError::Index)?;
        Ok(Self {
            file_handle,
            index,
            payload_size,
            data_len,
            last_time: None,
        })
    }

    #[instrument]
    pub(crate) fn open_existing(
        name: impl AsRef<Path> + fmt::Debug,
        file: OffsetFile,
        payload_size: PayloadSize,
    ) -> Result<Data, OpenError> {
        let mut file = FileWithInlineMeta::new(file, payload_size)
            .map_err(OpenError::CheckOrRepair)?;
        let data_len = file
            .file_handle
            .data_len_bytes()
            .map_err(OpenError::GetLength)?;
        let last_line_starts = data_len.checked_sub((payload_size.line_size()) as u64);
        let last_full_ts_in_data = last_meta_timestamp(file.inner_mut(), payload_size)
            .map_err(OpenError::GetLastMeta)?;
        let index =
            match Index::open_existing(&name, last_line_starts, last_full_ts_in_data) {
                Ok(index) => index,
                Err(e) => {
                    warn!("Creating new index, existing is broken: {e}");
                    Index::create_from_byteseries(file.inner_mut(), payload_size, name)?
                }
            };

        let last_time =
            match last_line(&index, data_len, payload_size, &mut file, &mut EmptyDecoder)
            {
                Ok((time, _)) => Some(time),
                Err(ReadError::NoData) => None,
                Err(other) => return Err(OpenError::ReadLastTime(other)),
            };

        let data = Self {
            file_handle: file,
            index,
            payload_size,
            data_len,
            last_time,
        };
        Ok(data)
    }

    /// # Errors
    ///
    /// See the [`ReadError`] docs for an exhaustive list of everything
    /// that can go wrong.
    pub(crate) fn last_line<T: std::fmt::Debug + std::clone::Clone>(
        &mut self,
        decoder: &mut impl Decoder<Item = T>,
    ) -> Result<(Timestamp, T), ReadError> {
        last_line(
            &self.index,
            self.data_len,
            self.payload_size,
            &mut self.file_handle,
            decoder,
        )
    }

    #[instrument]
    pub(crate) fn first_meta_timestamp(&self) -> Option<Timestamp> {
        self.index.first_meta_timestamp()
    }

    #[instrument]
    pub(crate) fn last_time(&self) -> Option<Timestamp> {
        self.last_time
    }

    pub(crate) fn range(&self) -> Option<RangeInclusive<Timestamp>> {
        self.first_meta_timestamp()
            .map(|f| f..=self.last_time.expect("first time is Some"))
    }

    /// Append data to disk but do not flush, a crash can still lead to the data
    /// being lost
    #[instrument(skip(self, line), level = "trace")]
    pub(crate) fn push_data(
        &mut self,
        ts: Timestamp,
        line: &[u8],
    ) -> Result<(), PushError> {
        //we store the timestamp - the last recorded full timestamp as u16. If
        //that overflows a new timestamp will be inserted. The 16 bit small
        //timestamp is stored little endian
        let small_ts = self
            .index
            .last_timestamp()
            .map(|last_timestamp| {
                ts.checked_sub(last_timestamp).ok_or(PushError::OutOfOrder {
                    last: last_timestamp,
                    item: ts,
                })
            })
            .transpose()?
            .and_then(|diff| {
                if diff > MAX_SMALL_TS {
                    None
                } else {
                    Some(u16::try_from(diff).expect("MAX_SMALL_TS < u16::MAX"))
                }
            });

        let small_ts = if let Some(small_ts) = small_ts {
            small_ts
        } else {
            tracing::debug!(
                "inserting full timestamp and updating index\
                , timestamp: {ts}"
            );
            self.index
                .update(ts, index::MetaPos(self.data_len))
                .map_err(PushError::Index)?;
            let meta = ts.to_le_bytes();
            let written = meta::write(&mut self.file_handle, meta, self.payload_size)
                .map_err(PushError::Meta)?;
            self.data_len += written;
            0 // value does not matter, full timestamp just ahead is used
        };

        self.file_handle
            .write_all(&small_ts.to_le_bytes())
            .map_err(PushError::Write)?;
        self.file_handle
            .write_all(&line[..self.payload_size.raw()])
            .map_err(PushError::Write)?;
        self.data_len += self.payload_size.line_size() as u64;
        self.last_time = Some(ts);
        Ok(())
    }

    /// asks the OS to write its buffers and block till its done
    pub(crate) fn flush_to_disk(&mut self) -> std::io::Result<()> {
        self.file_handle.inner_mut().sync_data()?;
        self.index.file.sync_data()?;
        Ok(())
    }

    /// # Errors
    ///
    /// See the [`ReadError`] docs for an exhaustive list of everything that can go wrong.
    pub(crate) fn read_all<D: Decoder>(
        &mut self,
        seek: Pos,
        decoder: &mut D,
        timestamps: &mut Vec<Timestamp>,
        data: &mut Vec<D::Item>,
    ) -> Result<(), ReadError> {
        self.file_handle
            .read(decoder, timestamps, data, seek)
            .map_err(ReadError::Reading)
    }

    /// # Errors
    ///
    /// See the [`ReadError`] docs for an exhaustive list of everything that can go wrong.
    pub(crate) fn read_first_n<D: Decoder>(
        &mut self,
        n: usize,
        seek: Pos,
        decoder: &mut D,
        timestamps: &mut Vec<Timestamp>,
        data: &mut Vec<D::Item>,
    ) -> Result<(), ReadError> {
        self.file_handle
            .read_first_n(n, decoder, timestamps, data, seek)
            .map_err(ReadError::Reading)
    }

    #[instrument(skip(self, resampler, timestamps, data), err)]
    pub(crate) fn read_resampling<R: crate::Resampler>(
        &mut self,
        seek: Pos,
        resampler: &mut R,
        bucket_size: usize,
        timestamps: &mut Vec<u64>,
        data: &mut Vec<<R as Decoder>::Item>,
    ) -> Result<(), ReadError> {
        self.file_handle
            .read_resampling(resampler, bucket_size, timestamps, data, seek)
            .map_err(ReadError::Reading)
    }

    pub(crate) fn payload_size(&self) -> PayloadSize {
        self.payload_size
    }

    pub(crate) fn last_line_start(&self) -> LinePos {
        // any metasection is written at the
        // same time and before a line. (they are 'atomic')
        LinePos(self.data_len - self.payload_size.line_size() as u64)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data_len == 0
    }

    pub(crate) fn clear(&mut self) -> Result<(), std::io::Error> {
        self.file_handle.file_handle.set_len(0)?;
        self.index.clear()?;
        self.data_len = 0;
        Ok(())
    }

    /// number of entries/samples/pushed lines in the file.
    pub(crate) fn len(&self) -> u64 {
        let lines = self.data_len / self.payload_size().line_size() as u64;
        let meta_sections = self.index.len() as u64;
        let meta_lines =
            meta_sections * lines_per_metainfo(self.payload_size().raw()) as u64;
        lines - meta_lines
    }
}

// not member of Data since we need it for Data's initialization
fn last_line<T>(
    index: &Index,
    data_len: u64,
    payload_size: PayloadSize,
    file_handle: &mut FileWithInlineMeta<OffsetFile>,
    decoder: &mut impl Decoder<Item = T>,
) -> Result<(Timestamp, T), ReadError> {
    let mut timestamps = Vec::new();
    let mut data = Vec::new();
    let seek = Pos {
        first_full_ts: index.last_timestamp().ok_or(ReadError::NoData)?,
        // repair will have removed any trialing meta section, thus this
        // will always read an actual line and not part of metadata.
        start: LinePos(data_len - payload_size.line_size() as u64),
        end: data_len,
    };
    file_handle
        .read(decoder, &mut timestamps, &mut data, seek)
        .map_err(ReadError::Reading)?;

    let ts = timestamps.pop().ok_or(ReadError::NoData)?;
    let item = data.pop().ok_or(ReadError::NoData)?;

    Ok((ts, item))
}
