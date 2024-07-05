use core::fmt;
use std::io::Read;
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::instrument;

use crate::series::data::inline_meta::{read_meta, MetaResult};
use crate::util::{FileWithHeader, OffsetFile, OpenError};

use super::{Entry, Index};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("ran into io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("could not create a new index file: {0}")]
    Open(#[from] OpenError),
    #[error("could not extract timestamps from byteseries data: {0}")]
    ExtractingTimestamps(#[from] ExtractingTsError),
    #[error("appending of a index element failed")]
    Appending(std::io::Error),
    #[error("could not remove the temporary `.part` extension to the now fully recoverd `byteseries_index` file")]
    Moving(std::io::Error),
}

impl Index {
    #[instrument]
    pub fn restore_from_byteseries<H>(
        byteseries: &mut OffsetFile,
        payload_size: usize,
        name: impl AsRef<Path> + fmt::Debug,
        header: H,
    ) -> Result<Self, Error>
    where
        H: DeserializeOwned + Serialize + fmt::Debug + 'static + Clone,
    {
        let temp_path = name.as_ref().with_extension("byteseries_index.part");
        let index_file: FileWithHeader<H> = FileWithHeader::new(&temp_path, header)?;
        let entries = extract_entries(byteseries, payload_size)?;

        let mut index = Self {
            last_timestamp: entries.last().map(|Entry { timestamp, .. }| *timestamp),
            file: index_file.split_off_header().0,
            entries: Vec::new(),
        };

        for entry in entries {
            index
                .update(entry.timestamp, entry.line_start)
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
}

pub(crate) fn extract_entries(
    file: &mut OffsetFile,
    payload_size: usize,
) -> Result<Vec<Entry>, ExtractingTsError> {
    let mut entries = Vec::new();

    let data_len = file.data_len().map_err(ExtractingTsError::GetDataLength)?;
    let chunk_size = 16384usize.next_multiple_of(payload_size + 2);

    // max size of the metadata section.
    let overlap = 5 * (payload_size + 2);

    // do not init with zero or the initially empty overlap
    // will be seen as a full timestamp
    let mut buffer = vec![1u8; chunk_size + overlap];
    for i in 0..(data_len / chunk_size as u64) {
        file.read_exact(&mut buffer[overlap..])
            .map_err(ExtractingTsError::ReadChunk)?;
        entries.extend(
            meta(&buffer, payload_size, overlap)
                .into_iter()
                .map(|(pos, timestamp)| Entry {
                    timestamp,
                    line_start: i * (chunk_size as u64) + pos as u64,
                }),
        );
    }

    // mod chunk_size (was usize) is within usize
    #[allow(clippy::cast_possible_truncation)]
    let left = (data_len % (chunk_size as u64)) as usize;
    file.read_exact(&mut buffer[overlap..overlap + left])
        .map_err(ExtractingTsError::ReadFinalChunk)?;
    entries.extend(
        meta(&buffer, payload_size, overlap)
            .into_iter()
            .map(|(pos, timestamp)| Entry {
                timestamp,
                line_start: data_len - left as u64 + pos as u64,
            }),
    );

    Ok(entries)
}

pub(crate) fn meta(buf: &[u8], payload_size: usize, overlap: usize) -> Vec<(usize, u64)> {
    let mut chunks = buf.chunks_exact(2 + payload_size).enumerate();
    let mut res = Vec::new();
    loop {
        let Some((idx, chunk)) = chunks.next() else {
            return res;
        };
        if chunk[..2] != [0, 0] {
            continue;
        }

        let Some((_, next_chunk)) = chunks.next() else {
            return res;
        };
        if next_chunk[..2] != [0, 0] {
            continue;
        }

        let chunks = chunks.by_ref().map(|(_, chunk)| chunk);
        let MetaResult::Meta(meta) = read_meta(chunks, chunk, next_chunk) else {
            return res;
        };
        let index_of_meta = idx * (2 + payload_size) - overlap;
        let index_of_line = index_of_meta + 2 * (2 + payload_size);
        let ts = u64::from_le_bytes(meta);
        res.push((index_of_line, ts));
    }
}

/// returns None if not enough data was left to decode a u64
#[instrument(level = "trace", skip(chunks), ret)]
fn read_timestamp<'a>(
    mut chunks: impl Iterator<Item = &'a [u8]>,
    first_chunk: &'a [u8],
    next_chunk: &'a [u8],
    payload_size: usize,
) -> Option<u64> {
    let mut result = 0u64.to_le_bytes();
    match payload_size {
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
