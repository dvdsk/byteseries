use std::io::{Read, Seek, SeekFrom};
use std::ops::Bound;

use tracing::instrument;

use crate::series::data::index::{EndArea, LinePos, MetaPos, PayloadSize, StartArea};
use crate::series::data::{Data, MAX_SMALL_TS};
use crate::Timestamp;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("could not find timestamp in this series")]
    NotFound,
    #[error("data file is empty")]
    EmptyFile,
    #[error("no data to return as the start time is after the last time in the data")]
    StartAfterData,
    #[error("no data to return as the stop time is before the data")]
    StopBeforeData,
    #[error("error while searching through data for precise end or start: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug)]
pub struct RoughPos {
    /// Timestamp that the first line should ideally have
    start_ts: Timestamp,
    /// Area where to search for the start time
    start_search_area: StartArea,
    /// 64 bit timestamp that should be added to the small time
    /// for the search for the start timestamp.
    start_section_full_ts: Timestamp,
    /// Read lines up to and including this Timestamp
    end_ts: Timestamp,
    /// Area where to search for the end time
    end_search_area: EndArea,
    /// 64 bit timestamp that should be added to the small time
    /// during the search for the end timestamp.
    end_section_full_ts: Timestamp,
}

impl RoughPos {
    /// # Returns `None` if the data file is empty
    #[instrument(skip(data), ret)]
    pub(crate) fn new(
        data: &Data,
        start: Bound<Timestamp>,
        end: Bound<Timestamp>,
    ) -> Option<Self> {
        let first_time_in_data = data.index.first_meta_timestamp()?;
        let start_ts = match start {
            Bound::Included(ts) => ts,
            Bound::Excluded(ts) => ts - 1,
            Bound::Unbounded => first_time_in_data,
        };
        let start_ts = start_ts.max(first_time_in_data);

        let (start_search_area, start_section_full_ts) = match start {
            Bound::Included(_) | Bound::Excluded(_) => data
                .index
                .start_search_bounds(start_ts, data.payload_size()),
            Bound::Unbounded => (
                StartArea::Found(MetaPos::ZERO.line_start(data.payload_size())),
                first_time_in_data,
            ),
        };

        let end_ts = match end {
            Bound::Included(ts) => ts,
            Bound::Excluded(ts) => ts - 1,
            Bound::Unbounded => data
                .last_time()
                .expect("first time is set so last should be too"),
        };
        let last_time_in_data = data
            .last_time()
            .expect("first time is set so last should be too");
        let end_ts = end_ts.min(last_time_in_data);

        assert!(
            end_ts >= start_ts,
            "end time ({end_ts}) larger then start ({start_ts})"
        );

        let (end_search_area, end_section_full_ts) = match end {
            Bound::Included(_) | Bound::Excluded(_) => {
                data.index.end_search_bounds(end_ts, data.payload_size())
            }
            Bound::Unbounded => (
                EndArea::Found(data.last_line_start()),
                data.index
                    .last_timestamp()
                    .expect("first time is set so last should be too"),
            ),
        };

        Some(Self {
            start_ts,
            start_search_area,
            end_ts,
            end_search_area,
            start_section_full_ts,
            end_section_full_ts,
        })
    }

    /// returns None if there is no data to read
    #[tracing::instrument]
    pub(crate) fn refine(self, data: &mut Data) -> Result<Option<Pos>, Error> {
        let start_byte = match self.start_search_area {
            StartArea::Found(pos) | StartArea::Gap { stops: pos } => pos,
            StartArea::Clipped => MetaPos::ZERO.line_start(data.payload_size()),
            StartArea::TillEnd(start) => {
                let end = data.data_len;
                find_read_start(data, self.start_small_ts(), start, end)?
            }
            StartArea::Window(start, stop) => {
                find_read_start(data, self.start_small_ts(), start, stop.raw_offset())?
            }
        };

        let end_byte = match self.end_search_area {
            EndArea::Found(pos) => pos.next_line_start(data.payload_size()).raw_offset(),
            EndArea::Gap { start: pos } => pos.raw_offset(),
            EndArea::TillEnd(start) => {
                let end = data.data_len;
                find_read_end(data, self.end_small_ts(), start, end)?
            }
            EndArea::Window(start, end) => {
                find_read_end(data, self.end_small_ts(), start, end.raw_offset())?
            }
        };

        Ok(if end_byte <= start_byte.raw_offset() {
            None
        } else {
            Some(Pos {
                start: start_byte,
                end: end_byte,
                first_full_ts: self.start_section_full_ts,
            })
        })
    }

    fn end_small_ts(&self) -> u16 {
        let end_time = self.end_ts.checked_sub(self.end_section_full_ts).expect(
            "search_bounds should be such that requested_end_time falls within \
                end_full_time..end_full_time+MAX_SMALL_TS",
        );
        assert!(
            end_time <= MAX_SMALL_TS,
            "end_time must be smaller then MAX_SMALL_TS. \
            end time: {end_time}, MAX_SMALL_TS: {MAX_SMALL_TS}"
        );
        u16::try_from(end_time).expect("just asserted")
    }

    fn start_small_ts(&self) -> u16 {
        let start_time = self
            .start_ts
            .checked_sub(self.start_section_full_ts)
            .expect(
                "search_bounds should be such that requested_start_time falls within \
                start_full_time..start_full_time+u16::MAX",
            );
        assert!(
            start_time <= MAX_SMALL_TS,
            "start time: {start_time}, MAX_SMALL_TS: {MAX_SMALL_TS}"
        );
        u16::try_from(start_time).expect("just asserted")
    }

    pub(crate) fn estimate_lines(
        &self,
        payload_size: PayloadSize,
        data_len: u64,
    ) -> Estimate {
        use EndArea as End;
        use StartArea::{Clipped, Found, Gap, TillEnd, Window};

        let estimate_in_bytes =
            match (self.start_search_area.clone(), self.end_search_area.clone()) {
                (Found(start) | Gap { stops: start }, End::Found(end)) => Estimate {
                    max: end - start,
                    min: end - start,
                },
                (Found(start) | Gap { stops: start }, End::Gap { start: end }) => {
                    Estimate {
                        max: end.raw_offset() - start.raw_offset(),
                        min: end.raw_offset() - start.raw_offset(),
                    }
                }
                (Found(start) | Gap { stops: start }, End::TillEnd(end)) => Estimate {
                    max: data_len - start.0,
                    min: end - start,
                },
                (Found(start) | Gap { stops: start }, End::Window(end_min, end_max)) => {
                    Estimate {
                        max: end_max.raw_offset() - start.raw_offset(),
                        min: end_min - start,
                    }
                }

                (Clipped, End::Found(end)) => Estimate {
                    max: end.raw_offset(),
                    min: end.raw_offset(),
                },
                (Clipped, End::Gap { start: end }) => Estimate {
                    max: end.raw_offset(),
                    // incorrect, but we have no better guess for it
                    min: end.raw_offset(),
                },
                (Clipped, End::TillEnd(end)) => Estimate {
                    max: data_len,
                    min: end.raw_offset(),
                },
                (Clipped, End::Window(end_min, end_max)) => Estimate {
                    max: end_max.raw_offset(),
                    min: end_min.raw_offset(),
                },

                (TillEnd(start), End::Found(end)) => Estimate {
                    max: end - start,
                    min: 1,
                },
                (TillEnd(start), End::Gap { start: end }) => Estimate {
                    max: end.line_start(payload_size) - start,
                    min: 1,
                },
                (TillEnd(start), End::TillEnd(_)) => Estimate {
                    max: data_len - start.raw_offset(),
                    min: 1,
                },
                (TillEnd(_), End::Window(_, _)) => unreachable!(
                "The start has to lie before the end, if the end is a search area from \
                min..max then start can not be an area from start..end_of_file"
            ),

                (Window(start_min, start_max), End::Found(end)) => Estimate {
                    max: end - start_min,
                    min: end - start_max.line_start(payload_size),
                },
                (Window(start_min, start_max), End::Gap { start: end }) => Estimate {
                    max: end.raw_offset() - start_min.raw_offset(),
                    min: end - start_max,
                },
                (Window(start_min, start_max), End::TillEnd(end)) => Estimate {
                    max: data_len - start_min.raw_offset(),
                    min: end - start_max.line_start(payload_size),
                },
                (Window(start_min, start_max), End::Window(end_min, end_max)) => {
                    Estimate {
                        max: end_max.raw_offset() - start_min.raw_offset(),
                        min: end_min - start_max.line_start(payload_size),
                    }
                }
            };

        Estimate {
            max: estimate_in_bytes.max / payload_size.line_size() as u64,
            min: estimate_in_bytes.min / payload_size.line_size() as u64,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Estimate {
    pub(crate) max: u64,
    pub(crate) min: u64,
}

#[derive(Debug)]
pub struct Pos {
    /// start of the first line that should be read
    pub(crate) start: LinePos,
    /// Read up to this byte not including it.
    /// first byte after the end of the last line that should be read
    pub(crate) end: u64,
    /// 64 bit timestamp that should be added to the small time
    /// for the first section.
    pub(crate) first_full_ts: Timestamp,
}

impl Pos {
    #[must_use]
    pub(crate) fn lines(&self, series: &Data) -> u64 {
        (self.end - self.start.raw_offset()) / series.payload_size().line_size() as u64
    }
}

/// returns the offset from the start of the file where the first line starts
#[instrument(err)]
fn find_read_start(
    data: &mut Data,
    start_time: u16,
    start: LinePos,
    stop: u64,
) -> Result<LinePos, Error> {
    if stop <= start.next_line_start(data.payload_size()).raw_offset() {
        return Ok(LinePos(stop));
    }

    let buf_len =
        usize::try_from(stop - start.raw_offset()).expect("search area < u16::MAX");
    let mut buf = vec![0u8; buf_len];
    data.file_handle.seek(SeekFrom::Start(start.raw_offset()))?;
    data.file_handle.file_handle.read_exact(&mut buf)?;

    if let Some(start_line) = buf
        .chunks_exact(data.payload_size().line_size())
        .map(|line| {
            line[0..2]
                .try_into()
                .expect("start and stop at least 2 apart")
        })
        .map(u16::from_le_bytes)
        .position(|line_ts| line_ts >= start_time)
    {
        let bytes_past_start = start_line as u64 * data.payload_size().line_size() as u64;
        let start_byte = start.raw_offset() + bytes_past_start;
        Ok(LinePos(start_byte))
    } else {
        Ok(LinePos(stop))
    }
}

/// returns the offset from the start of the file where last line **stops**
#[instrument(err)]
fn find_read_end(
    data: &mut Data,
    end_time: u16,
    start: LinePos,
    stop: u64,
) -> Result<u64, Error> {
    assert!(
        stop >= start.raw_offset(),
        "stop ({stop}) must be large then start ({start:?})"
    );
    //compare partial (16 bit) timestamps in between these bounds
    let buf_len = usize::try_from(stop - start.raw_offset())
        .expect("search area is smaller the u16::MAX");
    let mut buf = vec![0u8; buf_len];
    data.file_handle.seek(SeekFrom::Start(start.raw_offset()))?;
    data.file_handle.file_handle.read_exact(&mut buf)?;

    if let Some(stop_line) = buf
        .chunks_exact(data.payload_size().line_size())
        .map(|line| line[..2].try_into().expect("chunks are at least 2 long"))
        .map(u16::from_le_bytes)
        .rposition(|line_ts| line_ts <= end_time)
    {
        let stop_byte = start.raw_offset()
            + (stop_line + 1) as u64 * data.payload_size().line_size() as u64;
        Ok(stop_byte)
    } else {
        Ok(stop)
    }
}
