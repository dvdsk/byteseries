use core::mem;
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::{self, SeekFrom};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use tracing::instrument;

use crate::series::data::inline_meta::SetLen;

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("Os returned IO-error")]
    Io(#[from] std::io::Error),
    #[error("Can not create new file, one already exists")]
    AlreadyExists,
    #[error("Could not serialize the header to a ron encoded string")]
    SerializingHeader(#[source] ron::Error),
    #[error("Max size for a header is around 2^16, the provided header is too large")]
    HeaderTooLarge,
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[error("Failed to deserialize header: {error}")]
pub struct HeaderDeserErr {
    error: ron::error::SpannedError,
    header: Vec<u8>,
}

pub(crate) struct FileWithHeader {
    pub(crate) handle: File,
    pub(crate) header: Vec<u8>,
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
impl FileWithHeader {
    /// Will return an error if the file already exists
    pub(crate) fn new(
        path: impl AsRef<Path>,
        user_header: &[u8],
    ) -> Result<FileWithHeader, OpenError> {
        let mut file = match OpenOptions::new()
            .read(true)
            .append(true)
            .create_new(true)
            .open(path)
        {
            Ok(file) => file,
            Err(err) => return Err(err)?,
        };
        let user_header_len: u16 = user_header
            .len()
            .try_into()
            .map_err(|_| OpenError::HeaderTooLarge)?;
        file.write_all(&user_header_len.to_le_bytes())?;
        file.write_all(LINE_ENDS)?;
        file.write_all(user_header)?;

        let len = LINE_ENDS.len() as u64
            + mem::size_of_val(&user_header_len) as u64
            + u64::from(user_header_len);
        Ok(FileWithHeader {
            handle: file,
            header: user_header.to_vec(),
            data_offset: len,
        })
    }

    /// # Panics
    /// If the path does not have the extension byteseries or byteseries_index.
    #[instrument(fields(file_len, user_header_len, header_len))]
    pub(crate) fn open_existing(path: PathBuf) -> Result<FileWithHeader, OpenError> {
        assert!(
            path.extension().is_some_and(|e| e == "byteseries")
                || path.extension().is_some_and(|e| e == "byteseries_index"),
            "Path extension ({:?}) must be 'byteseries' or 'byteseries_index'",
            path.extension()
        );
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(false)
            .open(path)?;
        let metadata = file.metadata()?;

        let mut header_len = [0u8, 2];
        file.read_exact(&mut header_len)?;
        let header_len = u16::from_le_bytes(header_len);
        let mut header = vec![0; header_len as usize];
        file.seek(std::io::SeekFrom::Start(USER_HEADER_STARTS as u64))?;
        file.read_exact(&mut header)?;
        let header_len =
            header_len as usize + LINE_ENDS.len() + mem::size_of_val(&header_len);

        tracing::Span::current()
            .record("file_len", metadata.len())
            .record("user_header_len", header_len)
            .record("header_len", header_len);

        Ok(FileWithHeader {
            handle: file,
            data_offset: header_len as u64,
            header,
        })
    }

    pub(crate) fn split_off_header(self) -> (OffsetFile, Vec<u8>) {
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
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub(crate) struct OffsetFile {
    pub(crate) handle: File,
    offset: u64,
}

impl OffsetFile {
    pub(crate) fn sync_data(&self) -> std::io::Result<()> {
        self.handle.sync_data()
    }

    /// length needed to read the entire file without the header.
    /// You can use this as input for `read_exact` though you might
    /// want to spread the read.
    ///
    /// # Errors
    /// Returns an error if the underlying file returned an io error.
    pub(crate) fn data_len(&self) -> std::io::Result<u64> {
        self.handle.metadata().map(|m| m.len() - self.offset)
    }
}

impl SetLen for OffsetFile {
    fn len(&self) -> Result<u64, std::io::Error> {
        self.data_len()
    }

    fn set_len(&mut self, len: u64) -> Result<(), std::io::Error> {
        self.handle.set_len(len + self.offset)
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
