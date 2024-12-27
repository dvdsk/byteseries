use core::fmt;
use std::io::{Read, Seek};
use std::path::Path;

use tracing::instrument;

use crate::file::{FileWithHeader, OffsetFile, OpenError};
use crate::series::data::inline_meta::meta;
use crate::Timestamp;

use super::{Entry, Index, PayloadSize};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("ran into io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("could not create a new index file")]
    Open(
        #[from]
        #[source]
        OpenError,
    ),
    #[error("could not extract timestamps from byteseries data")]
    ExtractingTimestamps(
        #[from]
        #[source]
        ExtractingTsError,
    ),
    #[error("appending of a index element failed: {0}")]
    Appending(std::io::Error),
    #[error("could not remove the temporary `.part` extension to the now fully recoverd `byteseries_index` file: {0}")]
    Moving(std::io::Error),
}

impl Index {
    #[instrument]
    pub(crate) fn create_from_byteseries(
        byteseries: &mut OffsetFile,
        payload_size: PayloadSize,
        name: impl AsRef<Path> + fmt::Debug,
    ) -> Result<Self, Error> {
        let temp_path = name.as_ref().with_extension("byteseries_index.part");
        let index_file = FileWithHeader::new(&temp_path, &[])?;
        let entries = extract_entries(byteseries, payload_size)?;

        let mut index = Self {
            last_timestamp: entries.last().map(|Entry { timestamp, .. }| *timestamp),
            file: index_file.split_off_header().0,
            entries: Vec::new(),
        };

        for entry in entries {
            index
                .update(entry.timestamp, entry.meta_start)
                .map_err(Error::Appending)?;
        }

        // its fine on linux to move a file that is opened
        let final_path = name.as_ref().with_extension("byteseries_index");
        std::fs::rename(temp_path, final_path).map_err(Error::Moving)?;

        Ok(index)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ExtractingTsError {
    #[error("Could not get length of byteseries data file")]
    GetDataLength(std::io::Error),
    #[error("Could not read middle or start of data")]
    ReadChunk(std::io::Error),
    #[error("Could not read last part of data")]
    ReadFinalChunk(std::io::Error),
    #[error("Could not seek to start of byteseries data")]
    Seek(std::io::Error),
}

pub(crate) fn extract_entries(
    file: &mut OffsetFile,
    payload_size: PayloadSize,
) -> Result<Vec<Entry>, ExtractingTsError> {
    let data_len = file.data_len_bytes().map_err(ExtractingTsError::GetDataLength)?;
    extract_entries_inner(file, payload_size, 0, data_len)
}

#[instrument]
pub(crate) fn extract_entries_inner(
    file: &mut OffsetFile,
    payload_size: PayloadSize,
    start: u64,
    end: u64,
) -> Result<Vec<Entry>, ExtractingTsError> {
    let mut entries = Vec::new();

    let chunk_size = 16384usize.next_multiple_of(payload_size.line_size());
    let overlap = payload_size.metainfo_size();

    // do not init with zero or the initially empty overlap
    // will be seen as a full timestamp
    let mut buffer = vec![1u8; chunk_size + overlap];
    file.seek(std::io::SeekFrom::Start(start))
        .map_err(ExtractingTsError::Seek)?;

    let mut to_read = end - start;
    let mut previously_read = 0;

    while to_read > 0 {
        let read_size = chunk_size.min(usize::try_from(to_read).unwrap_or(usize::MAX));
        file.read_exact(&mut buffer[overlap..overlap + read_size])
            .map_err(ExtractingTsError::ReadChunk)?;
        to_read -= read_size as u64;

        entries.extend(
            meta(
                &buffer[..overlap + read_size],
                payload_size.line_size(),
                overlap,
            )
            .into_iter()
            .map(|(pos, timestamp)| Entry {
                timestamp,
                meta_start: super::MetaPos(previously_read + pos as u64),
            }),
        );
        previously_read += read_size as u64;
    }

    Ok(entries)
}

#[instrument(level = "debug", skip_all, ret)]
pub(crate) fn last_meta_timestamp(
    file: &mut OffsetFile,
    payload_size: PayloadSize,
) -> Result<Option<Timestamp>, ExtractingTsError> {
    let data_bytes = file.data_len_bytes().map_err(ExtractingTsError::GetDataLength)?;

    let window = 10_000u64.next_multiple_of(payload_size.line_size() as u64);
    let overlap = payload_size.metainfo_size();
    let mut start = data_bytes.saturating_sub(window);

    loop {
        let end = (start + window).min(data_bytes);
        if start == end {
            return Ok(None);
        };
        let mut list = extract_entries_inner(file, payload_size, start, end)?;

        if let Some(Entry { timestamp, .. }) = list.pop() {
            return Ok(Some(timestamp));
        }

        assert!(
            start > 0,
            "Should have found meta timestamp in data when its not empty. \
                repair guarantees the file is either empty or \
                contain at least one full timestamp metadata section.\
                file is: {data_bytes} bytes, window: {window}"
        );

        start = (start + overlap as u64).saturating_sub(window);
    }
}

#[instrument(skip(buf))]
pub(crate) fn meta(buf: &[u8], line_size: usize, overlap: usize) -> Vec<(usize, u64)> {
    let mut chunks = buf.chunks_exact(line_size).enumerate();
    let mut res = Vec::new();
    loop {
        let Some((idx, chunk)) = chunks.next() else {
            return res;
        };
        if chunk[..2] != meta::PREAMBLE {
            continue;
        }

        let Some((_, next_chunk)) = chunks.next() else {
            return res;
        };
        if next_chunk[..2] != meta::PREAMBLE {
            continue;
        }

        let chunks = chunks.by_ref().map(|(_, chunk)| chunk);
        let meta::Result::Meta { meta, .. } = meta::read(chunks, chunk, next_chunk)
        else {
            return res;
        };
        let index_of_meta = idx * line_size - overlap;
        let ts = u64::from_le_bytes(meta);
        res.push((index_of_meta, ts));
    }
}

/// returns None if not enough data was left to decode a u64
#[instrument(level = "trace", skip(chunks), ret)]
fn read_timestamp<'a>(
    mut chunks: impl Iterator<Item = &'a [u8]>,
    first_chunk: &'a [u8],
    next_chunk: &'a [u8],
    payload_size: PayloadSize,
) -> Option<u64> {
    let mut result = 0u64.to_le_bytes();
    match payload_size.raw() {
        0 => {
            result[0..2].copy_from_slice(chunks.next()?);
            result[2..4].copy_from_slice(chunks.next()?);
            result[4..6].copy_from_slice(chunks.next()?);
            result[6..8].copy_from_slice(chunks.next()?);
        }
        1 => {
            result[0] = first_chunk[2];
            result[1] = next_chunk[2];
            result[2..5].copy_from_slice(chunks.next()?);
            result[5..8].copy_from_slice(chunks.next()?);
        }
        2 => {
            result[0..2].copy_from_slice(&first_chunk[2..]);
            result[2..4].copy_from_slice(&next_chunk[2..]);
            result[4..8].copy_from_slice(chunks.next()?);
        }
        3 => {
            result[0..3].copy_from_slice(&first_chunk[2..]);
            result[3..6].copy_from_slice(&next_chunk[2..]);
            result[6..8].copy_from_slice(&chunks.next()?[0..2]);
        }
        4.. => {
            result[0..4].copy_from_slice(&first_chunk[2..6]);
            result[4..8].copy_from_slice(&next_chunk[2..6]);
        }
    }

    Some(u64::from_le_bytes(result))
}
