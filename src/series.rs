pub mod data;
pub mod downsample;
use core::fmt;
use std::ops::{Bound, RangeBounds};
use std::path::Path;

use data::index::PayloadSize;
use data::Data;
use itertools::Itertools;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::instrument;

use crate::seek::{self, Estimate};
use crate::{Decoder, Resampler, Timestamp};

use self::downsample::DownSampledData;

trait DownSampled: fmt::Debug + Send + 'static {
    fn process(&mut self, ts: Timestamp, line: &[u8]) -> Result<(), data::PushError>;
    fn estimate_lines(
        &self,
        start: Bound<Timestamp>,
        end: Bound<Timestamp>,
    ) -> Option<Estimate>;
    fn data_mut(&mut self) -> &mut Data;
    fn data(&self) -> &Data;
}

#[derive(Debug, Clone, Default)]
pub enum TimeRange {
    #[default]
    None,
    Some(std::ops::RangeInclusive<Timestamp>),
}

impl TimeRange {
    fn first(&self) -> Option<Timestamp> {
        match self {
            Self::None => None,
            Self::Some(range) => Some(*range.start()),
        }
    }
    fn last(&self) -> Option<Timestamp> {
        match self {
            Self::None => None,
            Self::Some(range) => Some(*range.end()),
        }
    }
    fn from_data(data: &mut Data) -> Self {
        if let Some(first) = data.first_time() {
            let last = data.last_time().expect(
                "if there is a first time there is a last (can be equal to first)",
            );
            Self::Some(first..=last)
        } else {
            Self::None
        }
    }

    fn update(&mut self, new_ts: Timestamp) -> Result<(), Error> {
        let new = match self {
            Self::Some(range) if *range.end() >= new_ts => {
                return Err(Error::NewLineBeforePrevious {
                    new: new_ts,
                    prev: *range.end(),
                })
            }
            Self::Some(range) => Self::Some(*range.start()..=new_ts),
            Self::None => Self::Some(new_ts..=new_ts),
        };
        *self = new;
        Ok(())
    }
}

impl From<TimeRange> for Option<std::ops::RangeInclusive<Timestamp>> {
    fn from(val: TimeRange) -> Self {
        match val {
            TimeRange::None => None,
            TimeRange::Some(range) => Some(range.clone()),
        }
    }
}

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct ByteSeries {
    pub(crate) data: Data,
    downsampled: Vec<Box<dyn DownSampled>>,

    pub(crate) range: TimeRange,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Could not open byteseries: {0}")]
    Open(data::OpenError),
    #[error("Failed to create new byteseries: {0}")]
    Create(data::CreateError),
    #[error("Error with cached downsampled data: {0}")]
    Downsampled(downsample::Error),
    #[error("Could not push, new timestamp: {new} lies before last in data: {prev}")]
    NewLineBeforePrevious { new: u64, prev: u64 },
    #[error("Could not push to data file: {0}")]
    Pushing(data::PushError),
    #[error("Could not updated downsampled data file's metadata: {0}")]
    Downampling(data::PushError),
    #[error("Timestamps do not exist in Data: {0}")]
    InvalidRange(seek::Error),
    #[error("Error while finding start and end point in data: {0}")]
    Seeking(seek::Error),
    #[error("Could not read data: {0}")]
    Reading(data::ReadError),
    #[error("Would need to collect more then usize::MAX samples to resample.")]
    TooMuchToResample,
}

impl ByteSeries {
    #[instrument]
    pub fn new(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: usize,
        header: &[u8],
    ) -> Result<ByteSeries, Error> {
        Self::new_with_resamplers(
            name,
            payload_size,
            header,
            downsample::resample::EmptyResampler,
            Vec::new(),
        )
    }

    #[instrument]
    pub fn new_with_resamplers<R>(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: usize,
        header: &[u8],
        resampler: R,
        resample_configs: Vec<downsample::Config>,
    ) -> Result<ByteSeries, Error>
    where
        R: Resampler + Clone + Send + 'static,
        R::State: Send + 'static,
    {
        let payload_size = PayloadSize::from_raw(payload_size);
        let mut data =
            Data::new(name.as_ref(), payload_size, header).map_err(Error::Create)?;
        Ok(ByteSeries {
            range: TimeRange::None,
            downsampled: resample_configs
                .into_iter()
                .map(|config| {
                    DownSampledData::create(
                        resampler.clone(),
                        config,
                        name.as_ref(),
                        payload_size,
                        &mut data,
                    )
                    .map_err(downsample::Error::Creating)
                })
                .map_ok(Box::new)
                .map_ok(|boxed| boxed as Box<dyn DownSampled>)
                .collect::<Result<Vec<_>, downsample::Error>>()
                .map_err(Error::Downsampled)?,
            data,
        })
    }

    /// line size in bytes, path is *without* any extension
    #[instrument]
    pub fn open_existing(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: usize,
    ) -> Result<(ByteSeries, Vec<u8>), Error> {
        let payload_size = PayloadSize::from_raw(payload_size);
        let (mut data, header) =
            Data::open_existing(name, payload_size).map_err(Error::Open)?;

        let bs = ByteSeries {
            range: TimeRange::from_data(&mut data),
            downsampled: Vec::new(),
            data,
        };

        Ok((bs, header))
    }

    /// Caches one or more downsampled versions of the data
    /// line size in bytes, path is *without* any extension
    ///
    /// # Note
    /// If the data file got truncated (due to corruption/failed writes/another
    /// process) and the cache did not the library can panic. This should be
    /// exceedingly rare. Please let me know if this hits you and I'll see into
    /// fixing this behaviour.
    #[instrument]
    pub fn open_existing_with_resampler<H, R>(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: usize,
        resampler: R,
        resample_configs: Vec<downsample::Config>,
    ) -> Result<(ByteSeries, Vec<u8>), Error>
    where
        H: DeserializeOwned + Serialize + fmt::Debug + PartialEq + 'static + Clone,
        R: Resampler + Clone + Send + 'static,
        R::State: Send + 'static,
    {
        let payload_size = PayloadSize::from_raw(payload_size);
        let (mut data, header) =
            Data::open_existing(&name, payload_size).map_err(Error::Open)?;

        Ok((
            ByteSeries {
                range: TimeRange::from_data(&mut data),
                downsampled: resample_configs
                    .into_iter()
                    .map(|config| {
                        DownSampledData::open_or_create(
                            resampler.clone(),
                            config,
                            name.as_ref(),
                            payload_size,
                            &mut data,
                        )
                        .map_err(downsample::Error::OpenOrCreate)
                    })
                    .map_ok(Box::new)
                    .map_ok(|boxed| boxed as Box<dyn DownSampled>)
                    .collect::<Result<Vec<_>, downsample::Error>>()
                    .map_err(Error::Downsampled)?,
                data,
            },
            header,
        ))
    }

    #[instrument(skip(self, line), level = "trace")]
    pub fn push_line(
        &mut self,
        ts: Timestamp,
        line: impl AsRef<[u8]>,
    ) -> Result<(), Error> {
        //write 16 bit timestamp and then the line to file
        //for now no support for sign bit since data will always be after 0 (1970)
        self.range.update(ts)?;

        self.data
            .push_data(ts, line.as_ref())
            .map_err(Error::Pushing)?;

        for downsampled in &mut self.downsampled {
            downsampled
                .process(ts, line.as_ref())
                .map_err(Error::Downampling)?;
        }
        Ok(())
    }

    /// Will return zero samples if there is nothing to read.
    ///
    /// # Errors
    ///
    /// See the [`Error`] docs for an exhaustive list of everything that can go wrong.
    /// Its mostly io-errors
    ///
    /// # Panics
    pub fn read_all<D: Decoder>(
        &mut self,
        range: impl RangeBounds<Timestamp>,
        decoder: &mut D,
        timestamps: &mut Vec<Timestamp>,
        data: &mut Vec<D::Item>,
    ) -> Result<(), Error> {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        self.check_range(start, end).map_err(Error::InvalidRange)?;
        let Some(seek) = seek::RoughPos::new(
            &self.data,
            range.start_bound().cloned(),
            range.end_bound().cloned(),
        )
        .expect("no data should be caught be check range")
        .refine(&mut self.data)
        .map_err(Error::Seeking)?
        else {
            tracing::debug!(
                "No data to read within given range, probably due to \
                a gap in the data."
            );
            return Ok(());
        };

        self.data
            .read_all(seek, decoder, timestamps, data)
            .map_err(Error::Reading)
    }

    /// Will return zero if there is nothing to read between the given points.
    ///
    /// # Errors
    ///
    /// See the [`Error`] docs for an exhaustive list of everything that can go wrong.
    /// Its mostly io-errors
    ///
    /// # Panics
    pub fn n_lines_between(
        &mut self,
        range: impl RangeBounds<Timestamp>,
    ) -> Result<u64, Error> {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        self.check_range(start, end).map_err(Error::InvalidRange)?;

        Ok(seek::RoughPos::new(&self.data, start, end)
            .expect(
                "check range catches the undownsampled file missing data \
                and downsampled are not selected if their `estimate_lines` is None ",
            )
            .refine(&mut self.data)
            .map_err(Error::Seeking)?
            .map(|pos| pos.lines(&mut self.data))
            .unwrap_or(0))
    }
    /// Will return between zero and two times `n` samples
    ///
    /// This might read more but will resample down using averages.
    /// No interpolation is performed.
    ///
    /// # Errors
    ///
    /// See the [`Error`] docs for an exhaustive list of everything that can go wrong.
    /// Its mostly IO-issues.
    #[allow(clippy::missing_panics_doc)] // is bug if panic
    #[instrument(skip(self, resampler, timestamps, data),
        fields(range = format!("{:?}..{:?}", range.start_bound(), range.end_bound())))]
    pub fn read_n<R: Resampler>(
        &mut self,
        n: usize,
        range: impl RangeBounds<Timestamp>,
        resampler: &mut R,
        timestamps: &mut Vec<Timestamp>,
        data: &mut Vec<<R as Decoder>::Item>,
    ) -> Result<(), Error> {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        self.check_range(start, end).map_err(Error::InvalidRange)?;

        assert!(
            self.downsampled
                .windows(2)
                .all(|w| w[0].data().data_len >= w[1].data().data_len),
            "downsampled must be sorted in descending resolution/numb lines"
        );

        let mut optimal_data = &mut self.data;
        for downsampled in &mut self.downsampled {
            let Some(estimate) = downsampled.estimate_lines(start, end) else {
                break; // more downsampled files are empty
            };
            if estimate.max < n as u64 {
                tracing::debug!(
                    "not enough datapoints, not using next\
                    downsamled cache, estimate was: {estimate:?}"
                );
                break;
            }
            if estimate.min < n as u64 {
                tracing::debug!(
                    "possibly not enough datapoints, not using \
                    next level downsamled cache, estimate was: {estimate:?}"
                );
                break;
            }
            tracing::debug!("using downsampled data: {downsampled:?}");
            optimal_data = downsampled.data_mut();
        }

        let Some(seek) = seek::RoughPos::new(optimal_data, start, end)
            .expect(
                "check range catches the undownsampled file missing data \
                and downsampled are not selected if their `estimate_lines` is None ",
            )
            .refine(optimal_data)
            .map_err(Error::Seeking)?
        else {
            tracing::debug!(
                "No data to read within given range, probably due to \
                a gap in the data."
            );
            return Ok(());
        };

        let lines = seek.lines(optimal_data);
        let bucket_size = 1.max(lines / n as u64);
        let bucket_size =
            usize::try_from(bucket_size).map_err(|_| Error::TooMuchToResample)?;

        optimal_data
            .read_resampling(seek, resampler, bucket_size, timestamps, data)
            .map_err(Error::Reading)
    }

    fn check_range(
        &self,
        start: Bound<Timestamp>,
        end: Bound<Timestamp>,
    ) -> Result<(), seek::Error> {
        let inline_meta_size = self.data.payload_size().metainfo_size();
        if self.data.data_len <= inline_meta_size as u64 {
            return Err(seek::Error::EmptyFile);
        }

        match start {
            Bound::Included(ts) => {
                if ts > self.range.last().expect("data_len > 0") {
                    return Err(seek::Error::StartAfterData);
                }
            }
            Bound::Excluded(ts) => {
                if ts >= self.range.last().expect("data_len > 0") {
                    return Err(seek::Error::StartAfterData);
                }
            }
            Bound::Unbounded => (),
        };

        match end {
            Bound::Included(ts) => {
                if ts < self.range.first().expect("data_len > 0") {
                    return Err(seek::Error::StopBeforeData);
                }
            }
            Bound::Excluded(ts) => {
                if ts <= self.range.first().expect("data_len > 0") {
                    return Err(seek::Error::StopBeforeData);
                }
            }
            Bound::Unbounded => (),
        };

        Ok(())
    }

    /// # Errors
    /// Returns a [`ReadError`] if anything goes wrong reading
    /// the last line. That could be an io issue or the file could be empty.
    pub fn last_line<D>(
        &mut self,
        decoder: &mut D,
    ) -> Result<(u64, <D as Decoder>::Item), data::ReadError>
    where
        D: Decoder + Clone,
        <D as Decoder>::Item: Clone,
    {
        self.data.last_line(decoder)
    }

    /// # Errors
    /// When the os fails to flush files to disk the underlying
    /// io error is returned
    pub fn flush_to_disk(&mut self) -> std::io::Result<()> {
        self.data.flush_to_disk()
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)] // is bug if panic
    pub fn range(&self) -> Option<core::ops::RangeInclusive<Timestamp>> {
        self.range.clone().into()
    }
}
