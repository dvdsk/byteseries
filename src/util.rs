use core::fmt;
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::{self, SeekFrom};
use std::io::{Read, Seek, Write};
use std::path::PathBuf;

use byteorder::{ReadBytesExt, WriteBytesExt};
use ron::ser::PrettyConfig;
use serde::de::DeserializeOwned;
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("Could not open file on disk io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Can not create new file, one already exists")]
    AlreadyExists,
    #[error("Failed to deserialize header: {0}")]
    CorruptHeader(#[from] ron::error::SpannedError),
    #[error("Could not serialize the header to a ron encoded string: {0}")]
    SerializingHeader(ron::Error),
    #[error("Max size for a header is around 2^16, the provided header is too large")]
    HeaderTooLarge,
}

pub(crate) struct FileWithHeader<T> {
    pub(crate) handle: File,
    pub(crate) len: u64,
    pub(crate) header: T,
    /// data starts at this offset from the start
    pub(crate) data_offset: u64,
}

/// size comes from the u16 encoded length of the
/// header followed by 2 line ends.
pub(crate) const EMPTY_HEADER_SIZE: usize = 4;

/// open file and check if it has the right length
/// (an integer multiple of the line length) if it
/// has not warn and repair by truncating to a multiple
///
/// takes care to disregard the header for this

impl<H> FileWithHeader<H>
where
    H: DeserializeOwned + Serialize + fmt::Debug + 'static + Clone,
{
    pub fn new(path: PathBuf, header: H) -> Result<FileWithHeader<H>, OpenError> {
        let mut file = match OpenOptions::new()
            .read(true)
            .append(true)
            .create_new(true)
            .open(path)
        {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                return Err(OpenError::AlreadyExists)
            }
            Err(err) => return Err(err)?,
        };
        let encoded_header = ron::ser::to_string_pretty(&header, PrettyConfig::new())
            .map_err(OpenError::SerializingHeader)?;
        let encoded_len = encoded_header
            .len()
            .try_into()
            .map_err(|_| OpenError::HeaderTooLarge)?;
        file.write_u16::<byteorder::LittleEndian>(encoded_len)?;
        file.write(b"\n\n")?;
        file.write(encoded_header.as_bytes())?;
        return Ok(FileWithHeader {
            handle: file,
            len: 2 + 2 + encoded_len as u64,
            header,
            data_offset: encoded_len as u64 + EMPTY_HEADER_SIZE as u64,
        });
    }

    pub fn open_existing(
        path: PathBuf,
        full_line_size: usize,
    ) -> Result<FileWithHeader<H>, OpenError>
    where
        H: DeserializeOwned + Serialize + fmt::Debug + 'static + Clone,
    {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(false)
            .open(path)?;
        let metadata = file.metadata()?;

        let header_len = file.read_u16::<byteorder::LittleEndian>()?;
        let mut header = vec![0; header_len as usize + 2];
        file.seek(std::io::SeekFrom::Start(EMPTY_HEADER_SIZE as u64))?;
        file.read_exact(&mut header)?;
        let header = ron::de::from_bytes(&header)?;

        let len_without_header = metadata.len() - header_len as u64;
        let rest = len_without_header % (full_line_size as u64);
        if rest > 0 {
            tracing::warn!(
                "Last write incomplete, truncating to largest multiple of the line size"
            );
            file.set_len(metadata.len() - rest)?;
        }

        Ok(FileWithHeader {
            handle: file,
            len: metadata.len(),
            data_offset: 2u64 + header_len as u64,
            header,
        })
    }

    pub fn split_off_header(self) -> (OffsetFile, H) {
        (
            OffsetFile {
                handle: self.handle,
                offset: self.data_offset,
            },
            self.header,
        )
    }
}

/// The files have headers, instead of take these into account
/// and complicating all algorithms we use this. It forwards corrected
/// file seeks. We can use this as if the header does not exist.
#[derive(Debug)]
pub(crate) struct OffsetFile {
    handle: File,
    offset: u64,
}

impl OffsetFile {
    pub fn sync_data(&self) -> std::io::Result<()> {
        self.handle.sync_data()
    }
}

impl Seek for OffsetFile {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let offset_pos = match pos {
            SeekFrom::Start(p) => SeekFrom::Start(p + self.offset),
            SeekFrom::End(p) => SeekFrom::End(p),
            SeekFrom::Current(p) => SeekFrom::Current(p),
        };
        self.handle.seek(offset_pos)
    }
}

impl Read for OffsetFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.handle.read(buf)
    }
}

impl Write for OffsetFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.handle.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.handle.flush()
    }
}
