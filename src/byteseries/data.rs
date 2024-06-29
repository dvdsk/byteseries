use core::fmt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::Write;
use std::path::Path;
use tracing::instrument;

use crate::util::{FileWithHeader, OffsetFile};
use crate::{Decoder, Error, SeekPos, Timestamp};

pub(crate) mod inline_meta;
use inline_meta::FileWithInlineMeta;
pub mod index;
use index::Index;

use self::inline_meta::write_meta;

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
    fn decode_line(&mut self, _: &[u8]) -> Self::Item {}
}

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
            self.index.last_timestamp().ok_or(Error::NoData)?,
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
        //
        let small_ts = self
            .index
            .last_timestamp()
            .map(|last_timestamp| ts - last_timestamp)
            .map(TryInto::<u16>::try_into)
            .and_then(Result::ok);

        let small_ts = if let Some(small_ts) = small_ts {
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

    pub fn read_all<D: Decoder>(
        &mut self,
        seek: SeekPos,
        decoder: &mut D,
        timestamps: &mut Vec<Timestamp>,
        data: &mut Vec<D::Item>,
    ) -> Result<(), Error> {
        self.file_handle.read2(
            decoder,
            timestamps,
            data,
            seek.start,
            seek.end,
            seek.first_full_ts,
        )
    }

    pub(crate) fn read_resampling<R: crate::Resampler>(
        &mut self,
        seek: SeekPos,
        resampler: &mut R,
        bucket_size: usize,
        timestamps: &mut Vec<u64>,
        data: &mut Vec<<R as Decoder>::Item>,
    ) -> Result<(), Error> {
        self.file_handle.read2_resampling(
            resampler,
            bucket_size,
            timestamps,
            data,
            seek.start,
            seek.end,
            seek.first_full_ts,
        )
    }

    pub(crate) fn payload_size(&self) -> usize {
        self.payload_size
    }

    pub(crate) fn last_line_start(&self) -> u64 {
        self.data_len - (self.payload_size as u64 + 2)
    }
}
