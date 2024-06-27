use core::{fmt, mem};
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::{self, SeekFrom};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use ron::ser::PrettyConfig;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::instrument;

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
    pub(crate) user_header: T,
    /// data starts at this offset from the start
    pub(crate) data_offset: u64,
}

/// size comes from the u16 encoded length of the
/// header followed by 2 line ends.
const LINE_ENDS: &[u8; 2] = b"\n\n";
pub(crate) const USER_HEADER_STARTS: usize = LINE_ENDS.len() + mem::size_of::<u16>();

/// open file and check if it has the right length
/// (an integer multiple of the line length) if it
/// has not warn and repair by truncating to a multiple
///
/// takes care to disregard the header for this

impl<H> FileWithHeader<H>
where
    H: DeserializeOwned + Serialize + fmt::Debug + 'static + Clone,
{
    pub fn new(path: impl AsRef<Path>, user_header: H) -> Result<FileWithHeader<H>, OpenError> {
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
        let config = PrettyConfig::new();
        let encoded_user_header = ron::ser::to_string_pretty(&user_header, config)
            .map_err(OpenError::SerializingHeader)?;
        let user_header_len: u16 = encoded_user_header
            .len()
            .try_into()
            .map_err(|_| OpenError::HeaderTooLarge)?;
        file.write_all(&user_header_len.to_le_bytes())?;
        file.write_all(LINE_ENDS)?;
        file.write_all(encoded_user_header.as_bytes())?;

        let len = LINE_ENDS.len() as u64
            + mem::size_of_val(&user_header_len) as u64
            + user_header_len as u64;
        Ok(FileWithHeader {
            handle: file,
            user_header,
            data_offset: len,
        })
    }

    #[instrument(fields(file_len, user_header_len, header_len))]
    pub fn open_existing(
        path: PathBuf,
        line_size: usize,
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

        let mut user_header_len = [0u8, 2];
        file.read_exact(&mut user_header_len)?;
        let user_header_len = u16::from_le_bytes(user_header_len);
        let mut user_header = vec![0; user_header_len as usize];
        file.seek(std::io::SeekFrom::Start(USER_HEADER_STARTS as u64))?;
        file.read_exact(&mut user_header)?;
        let user_header = ron::de::from_bytes(&user_header)?;
        let header_len =
            user_header_len as usize + LINE_ENDS.len() + mem::size_of_val(&user_header_len);

        tracing::Span::current()
            .record("file_len", metadata.len())
            .record("user_header_len", user_header_len)
            .record("header_len", header_len);

        let len_without_header = metadata.len() - header_len as u64;
        let rest = len_without_header % (line_size as u64);
        if rest > 0 {
            tracing::warn!(
                "Last write incomplete, truncating to largest multiple of the line size"
            );
            file.set_len(metadata.len() - rest)?;
        }

        Ok(FileWithHeader {
            handle: file,
            data_offset: LINE_ENDS.len() as u64
                + mem::size_of_val(&user_header_len) as u64
                + user_header_len as u64,
            user_header,
        })
    }

    pub fn split_off_header(self) -> (OffsetFile, H) {
        (
            OffsetFile {
                handle: self.handle,
                offset: self.data_offset,
            },
            self.user_header,
        )
    }
}

/// The files have headers, instead of take these into account
/// and complicating all algorithms we use this. It forwards corrected
/// file seeks. We can use this as if the header does not exist.
#[derive(Debug)]
pub(crate) struct OffsetFile {
    pub(crate) handle: File,
    offset: u64,
}

impl OffsetFile {
    pub fn sync_data(&self) -> std::io::Result<()> {
        self.handle.sync_data()
    }

    /// length needed to read the entire file without the header.
    /// You can use this as input for read_exact though you might
    /// want to spread the read.
    pub fn data_len(&self) -> std::io::Result<u64> {
        self.handle
            .metadata()
            .map(|m| m.len() - self.offset)
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

    fn stream_position(&mut self) -> io::Result<u64> {
        self.handle.stream_position().map(|p| p - self.offset)
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
