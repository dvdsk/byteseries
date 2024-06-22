use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use core::fmt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{Read, Seek};
use std::path::Path;

use crate::data::FullTime;
use crate::util::{FileWithHeader, OffsetFile};
use crate::Error;

#[derive(Debug)]
pub struct Entry {
    pub timestamp: i64,
    pub pos: u64,
}

#[derive(Debug)]
pub struct Index {
    pub file: OffsetFile,

    pub entries: Vec<Entry>,
    pub last_timestamp: i64,
    pub last_timestamp_numb: i64,
}

#[derive(Debug)]
pub enum SearchBounds {
    Found(u64),
    Clipped,
    TillEnd(u64),
    Window(u64, u64),
}

impl Index {
    pub fn new<H>(name: impl AsRef<Path>, header: H) -> Result<Index, Error>
    where
        H: DeserializeOwned + Serialize + Eq + fmt::Debug + 'static + Clone,
    {
        let file: FileWithHeader<H> =
            FileWithHeader::new(name.as_ref().with_extension("byteseries_index"), header)?;

        Ok(Index {
            file: file.split_off_header().0,

            entries: Vec::new(),
            last_timestamp: 0,
            last_timestamp_numb: 0,
        })
    }
    pub fn open_existing<H>(name: impl AsRef<Path>, header: &H) -> Result<Index, Error>
    where
        H: DeserializeOwned + Serialize + Eq + fmt::Debug + 'static + Clone,
    {
        let mut file: FileWithHeader<H> =
            FileWithHeader::open_existing(name.as_ref().with_extension("byteseries_index"), 16)?;

        if *header != file.header {
            return Err(Error::IndexAndDataHeaderDifferent);
        }

        let mut bytes = Vec::new();
        file.handle
            .seek(std::io::SeekFrom::Start(file.data_offset))?;
        file.handle.read_to_end(&mut bytes)?;
        let mut numbers = vec![0u64; bytes.len() / 8];
        LittleEndian::read_u64_into(&bytes, numbers.as_mut_slice());

        let mut data = Vec::new();
        for i in (0..numbers.len()).step_by(2) {
            data.push(Entry {
                timestamp: numbers[i] as i64,
                pos: numbers[i + 1],
            });
        }

        let last_timestamp = numbers
            .get(numbers.len().saturating_sub(2))
            .map(|n| *n as i64)
            .unwrap_or(0);

        tracing::trace!("last_timestamp: {}", last_timestamp);
        Ok(Index {
            file: file.split_off_header().0,

            entries: data,
            last_timestamp,
            last_timestamp_numb: last_timestamp / (u16::max_value() as i64),
        })
    }

    pub fn create_from_byteseries<H>(
        byteseries: &mut crate::data::inline_meta::FileWithInlineMeta<OffsetFile>,
        line_size: usize,
        name: impl AsRef<Path>,
        header: H,
    ) -> Result<Self, Error>
    where
        H: DeserializeOwned + Serialize + Eq + fmt::Debug + 'static + Clone,
    {
        let entries = extract_entries(byteseries, line_size)?;
        let index_file: FileWithHeader<H> =
            FileWithHeader::new(name.as_ref().with_extension("byteseries_index"), header)?;
        let last_timestamp = entries
            .last()
            .map(|Entry { timestamp, .. }| *timestamp)
            .unwrap_or(0);

        Ok(Self {
            last_timestamp,
            last_timestamp_numb: last_timestamp / (u16::MAX as i64),
            file: index_file.split_off_header().0,
            entries,
        })
    }

    pub fn update(
        &mut self,
        timestamp: i64,
        line_start: u64,
        new_timestamp_numb: i64,
    ) -> Result<(), Error> {
        let ts = timestamp as u64;
        self.file.write_u64::<LittleEndian>(ts)?;
        self.file.write_u64::<LittleEndian>(line_start)?;
        tracing::trace!("wrote headerline: {}, {}", ts, line_start);

        self.entries.push(Entry {
            timestamp,
            pos: line_start,
        });
        self.last_timestamp_numb = new_timestamp_numb;
        Ok(())
    }

    pub fn search_bounds(&self, start: i64, stop: i64) -> (SearchBounds, SearchBounds, FullTime) {
        let idx = self.entries.binary_search_by_key(&start, |e| e.timestamp);
        let (start_bound, full_time) = match idx {
            Ok(i) => (
                SearchBounds::Found(self.entries[i].pos),
                FullTime {
                    curr: start,
                    next: self.entries.get(i + 1).map(|e| e.timestamp),
                    next_pos: self.entries.get(i + 1).map(|e| e.pos),
                },
            ),
            Err(end) => {
                if end == 0 {
                    //start lies before file
                    (
                        SearchBounds::Clipped,
                        FullTime {
                            curr: self.entries[0].timestamp,
                            next: self.entries.get(1).map(|e| e.timestamp),
                            next_pos: self.entries.get(1).map(|e| e.pos),
                        },
                    )
                } else if end == self.entries.len() {
                    (
                        SearchBounds::TillEnd(self.entries.last().unwrap().pos),
                        FullTime {
                            curr: self.entries.last().unwrap().timestamp,
                            next: None, //there is no full timestamp beyond the end
                            next_pos: None,
                        },
                    )
                } else {
                    //end is not 0 or 1 thus data[end] and data[end-1] exist
                    (
                        SearchBounds::Window(self.entries[end - 1].pos, self.entries[end].pos),
                        FullTime {
                            curr: self.entries[end - 1].timestamp,
                            next: Some(self.entries[end].timestamp),
                            next_pos: Some(self.entries[end].pos),
                        },
                    )
                }
            }
        };
        let idx = self.entries.binary_search_by_key(&stop, |e| e.timestamp);
        let stop_bound = match idx {
            Ok(i) => SearchBounds::Found(self.entries[i].pos),
            Err(end) => {
                if end == 0 {
                    //stop lies before file
                    panic!(
                        "stop lying before start of data should be caught
                        before calling search_bounds. We should never reach
                        this"
                    )
                } else if end == self.entries.len() {
                    SearchBounds::TillEnd(self.entries.last().unwrap().pos)
                } else {
                    //end is not 0 or 1 thus data[end] and data[end-1] exist
                    SearchBounds::Window(self.entries[end - 1].pos, self.entries[end].pos)
                }
            }
        };
        (start_bound, stop_bound, full_time)
    }
    pub fn first_time_in_data(&self) -> Option<i64> {
        self.entries.first().map(|e| e.timestamp)
    }

    pub fn next_full_timestamp(&self, curr: i64) -> Option<&Entry> {
        let i = self
            .entries
            .binary_search_by_key(&(curr + 1), |e| e.timestamp);
        let i = unwrap_result(i);
        self.entries.get(i)
    }
}

pub(crate) fn extract_entries(
    file: &mut crate::data::inline_meta::FileWithInlineMeta<OffsetFile>,
    line_size: usize,
) -> Result<Vec<Entry>, Error> {
    /// max metadata size when the metadata does not fit on
    /// the two line that contain the metadata pattern
    /// (first two bytes are zero).
    const MAX_META_FOR_SMALL_LINESIZE: usize = 3 * 5;
    let mut entries = Vec::new();

    let data_len = file.inner_mut().data_len()?;
    let chunk_size = 16384usize.next_multiple_of(line_size);

    // max size of the metadata section.
    let overlap = usize::max(MAX_META_FOR_SMALL_LINESIZE, 2 * (line_size + 2));

    // do not init with zero or the initially empty overlap
    // will be seen as a full timestamp
    let mut buffer = vec![1u8; chunk_size + overlap];
    for i in 0..(data_len / chunk_size as u64) {
        file.read_exact(&mut buffer[overlap..])?;
        entries.extend(
            meta(&buffer, line_size, overlap)
                .into_iter()
                .map(|(pos, timestamp)| Entry {
                    timestamp: timestamp as i64,
                    pos: i * (chunk_size as u64) + pos as u64,
                }),
        );
    }

    let left = (data_len % (chunk_size as u64)) as usize;
    file.read_exact(&mut buffer[overlap..overlap + left])?;
    entries.extend(meta(&buffer[..left], line_size, overlap).into_iter().map(
        |(pos, timestamp)| Entry {
            timestamp: timestamp as i64,
            pos: data_len - left as u64 + pos as u64,
        },
    ));

    Ok(entries)
}

pub(crate) fn meta(buf: &[u8], line_size: usize, overlap: usize) -> Vec<(usize, u64)> {
    let mut chunks = buf.chunks_exact(line_size).enumerate();
    let mut res = Vec::new();
    loop {
        let (idx, chunk) = chunks.next().unwrap();
        if chunk[..2] != [0, 0] {
            continue;
        }

        let (_, next_chunk) = chunks.next().unwrap();
        if next_chunk[..2] != [0, 0] {
            continue;
        }

        // TODO correct index for shift (was needed for overlap)
        let chunks = chunks.by_ref().map(|(_, chunk)| chunk);
        let ts = read_timestamp(chunks, chunk, next_chunk, line_size);
        let index = idx * line_size - overlap;
        res.push((index, ts));
    }
}

/// returns None if not enough data was left to decode a u64
fn read_timestamp<'a>(
    mut chunks: impl Iterator<Item = &'a [u8]>,
    first_chunk: &'a [u8],
    next_chunk: &'a [u8],
    line_size: usize,
) -> Option<u64> {
    let mut result = 0u64.to_le_bytes();
    match line_size {
        0 => {
            result[0..2].copy_from_slice(&chunks.next()?);
            result[2..4].copy_from_slice(&chunks.next()?);
            result[4..6].copy_from_slice(&chunks.next()?);
            result[6..8].copy_from_slice(&chunks.next()?);
        }
        1 => {
            result[0] = first_chunk[2];
            result[1] = next_chunk[2];
            result[2..5].copy_from_slice(&chunks.next()?);
            result[5..8].copy_from_slice(&chunks.next()?);
        }
        2 => {
            result[0..2].copy_from_slice(&first_chunk[2..]);
            result[2..4].copy_from_slice(&next_chunk[2..]);
            result[4..8].copy_from_slice(&chunks.next()?);
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

fn unwrap_result<T>(res: Result<T, T>) -> T {
    match res {
        Ok(v) => v,
        Err(v) => v,
    }
}
// https://rust-algo.club/doc/src/rust_algorithm_club/searching/interpolation_search/mod.rs.html#16-69
//
#[cfg(test)]
mod tests {
    use temp_dir::TempDir;

    use super::*;

    fn test_index() -> Index {
        let test_dir = TempDir::new().unwrap();
        let test_path = test_dir.child("test.byteseries_index");
        Index::new(test_path, ()).unwrap()
    }
    fn fill_index(h: &mut Index) {
        for i in 20..24 {
            let ts = i * 2i64.pow(16);
            let new_timestamp_numb = ts / 2i64.pow(16);
            h.update(ts, i as u64, new_timestamp_numb).unwrap();
        }
    }

    #[test]
    fn start_found() {
        let mut h = test_index();
        fill_index(&mut h);
        let start = 22 * 2i64.pow(16);
        let stop = 23 * 2i64.pow(16);
        let (start, _stop, ft) = h.search_bounds(start, stop);
        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&SearchBounds::Found(0))
        );
        assert!(ft.next.is_some());
        assert!(ft.next_pos.is_some());
    }
    #[test]
    fn start_clipped() {
        let mut h = test_index();
        fill_index(&mut h);
        let start = 12342;
        let stop = 23 * 2i64.pow(16);
        let (start, _stop, ft) = h.search_bounds(start, stop);

        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&SearchBounds::Clipped)
        );
        assert!(ft.next.is_some());
        assert!(ft.next_pos.is_some());
    }
    #[test]
    fn start_window() {
        let mut h = test_index();
        fill_index(&mut h);
        let start = 22 * 2i64.pow(16) + 400;
        let stop = 23 * 2i64.pow(16);
        let (start, _stop, ft) = h.search_bounds(start, stop);

        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&SearchBounds::Window(0, 0))
        );
        assert!(ft.next.is_some());
        assert!(ft.next_pos.is_some());
    }
    #[test]
    fn start_till_end() {
        let mut h = test_index();
        fill_index(&mut h);
        let start = 24 * 2i64.pow(16) + 400;
        let stop = 25 * 2i64.pow(16);
        let (start, _stop, ft) = h.search_bounds(start, stop);

        assert_eq!(
            std::mem::discriminant(&start),
            std::mem::discriminant(&SearchBounds::TillEnd(0))
        );
        assert!(ft.next.is_none());
        assert!(ft.next_pos.is_none());
    }
}
