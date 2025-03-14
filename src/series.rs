use core::fmt;
use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::path::Path;

use downsample::resample::EmptyResampler;
use itertools::Itertools;
use tracing::instrument;

pub mod data;
pub mod downsample;
mod file_header;

use data::index::PayloadSize;
use data::Data;

use crate::builder::PayloadSizeOption;
use crate::seek::{self, Estimate};
use crate::{builder, CorruptionCallback, Decoder, Resampler, Timestamp};

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
    fn from_data(data: &mut Data) -> Self {
        if let Some(first) = data.first_meta_timestamp() {
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
                return Err(Error::TimeNotAfterLast {
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

#[allow(clippy::module_name_repetitions)]
pub struct ByteSeries {
    pub(crate) data: Data,
    downsampled: Vec<Box<dyn DownSampled>>,
    corruption_callback: Option<CorruptionCallback>,

    pub(crate) range: TimeRange,
}

impl Debug for ByteSeries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ByteSeries")
            .field("data", &self.data)
            .field("downsampled", &self.downsampled)
            .field("corruption_callback", &self.corruption_callback.is_some())
            .field("range", &self.range)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Parameter check failed")]
    Parameters(#[from] file_header::Error),
    #[error("Could not open byteseries")]
    Open(#[source] data::OpenError),
    #[error("Failed to create new byteseries")]
    Create(#[source] data::CreateError),
    #[error("Error with cached downsampled data")]
    Downsampled(#[source] downsample::Error),
    #[error(
        "Could not push, new timestamp: {new} is the same or lies \
        before last in data: {prev}"
    )]
    TimeNotAfterLast { new: u64, prev: u64 },
    #[error("Could not push to data file")]
    Pushing(#[source] data::PushError),
    #[error("Could not updated downsampled data file's metadata")]
    Downampling(#[source] data::PushError),
    #[error("Timestamps do not exist in Data")]
    InvalidRange(#[source] seek::Error),
    #[error("Error while finding start and end point in data")]
    Seeking(#[source] seek::Error),
    #[error("Could not read data")]
    Reading(#[source] data::ReadError),
    #[error("Would need to collect more then usize::MAX samples to resample.")]
    TooMuchToResample,
    #[error("There was an issue checking the passed in header")]
    Header(#[source] builder::HeaderError),
    #[error("The line should be exactly: {required} bytes long, it was: {got}")]
    WrongLineLength { required: usize, got: usize },
}

impl ByteSeries {
    pub fn builder(
    ) -> builder::ByteSeriesBuilder<false, false, true, true, EmptyResampler> {
        builder::ByteSeriesBuilder::<false, false, true, true, EmptyResampler>::new()
    }

    #[instrument(skip(corruption_callback))]
    pub(crate) fn new_with_resamplers<R>(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: usize,
        user_header: &[u8],
        resampler: R,
        resample_configs: Vec<downsample::Config>,
        mut corruption_callback: Option<CorruptionCallback>,
    ) -> Result<ByteSeries, Error>
    where
        R: Resampler + Clone + Send + 'static,
        R::State: Send + 'static,
    {
        let header = file_header::SeriesParams {
            payload_size,
            version: 1,
        };
        let mut header = header.to_text();
        header.extend_from_slice(user_header);

        let payload_size = PayloadSize::from_raw(payload_size);
        let mut data =
            Data::new(name.as_ref(), payload_size, &header).map_err(Error::Create)?;
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
                        &mut corruption_callback,
                    )
                    .map_err(downsample::Error::Creating)
                })
                .map_ok(Box::new)
                .map_ok(|boxed| boxed as Box<dyn DownSampled>)
                .collect::<Result<Vec<_>, downsample::Error>>()
                .map_err(Error::Downsampled)?,
            data,
            corruption_callback,
        })
    }

    /// Caches one or more downsampled versions of the data
    /// line size in bytes, path is *without* any extension
    ///
    /// # Note
    /// If the data file got truncated (due to corruption/failed writes/another
    /// process) and the cache did not the library can panic. This should be
    /// exceedingly rare. Please let me know if this hits you and I'll see into
    /// fixing this behavior.
    #[instrument(skip(corruption_callback))]
    pub(crate) fn open_existing_with_resampler<R>(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: PayloadSizeOption,
        resampler: R,
        resample_configs: Vec<downsample::Config>,
        mut corruption_callback: Option<CorruptionCallback>,
    ) -> Result<(ByteSeries, Vec<u8>), Error>
    where
        R: Resampler + Clone + Send + 'static,
        R::State: Send + 'static,
    {
        let path = name.as_ref().with_extension("byteseries");
        let file = crate::file::FileWithHeader::open_existing(path.clone())
            .map_err(|source| data::OpenError::File { source, path })
            .map_err(Error::Open)?;
        let (file, header) = file.split_off_header();
        let (payload_size, user_header) =
            file_header::check_and_split_off_user_header(header.clone(), payload_size)?;

        let mut data =
            Data::open_existing(&name, file, payload_size, &mut corruption_callback)
                .map_err(Error::Open)?;
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
                            &mut corruption_callback,
                        )
                        .map_err(downsample::Error::OpenOrCreate)
                    })
                    .map_ok(Box::new)
                    .map_ok(|boxed| boxed as Box<dyn DownSampled>)
                    .collect::<Result<Vec<_>, downsample::Error>>()
                    .map_err(Error::Downsampled)?,
                data,
                corruption_callback,
            },
            user_header,
        ))
    }

    #[instrument(skip(self, line), level = "trace")]
    pub fn push_line(
        &mut self,
        ts: Timestamp,
        line: impl AsRef<[u8]>,
    ) -> Result<(), Error> {
        if line.as_ref().len() != self.data.payload_size().raw() {
            return Err(Error::WrongLineLength {
                required: self.data.payload_size().raw(),
                got: line.as_ref().len(),
            });
        }

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

    /// Will return zero samples if there is nothing to read. If `skip_corrupt_meta` is true this
    /// will skip data between a corrupt meta section and the next meta section.
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
        let Some(seek) = seek::RoughPos::new(
            &self.data,
            range.start_bound().cloned(),
            range.end_bound().cloned(),
        )
        .map_err(Error::InvalidRange)?
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
            .read_all(
                seek,
                &mut self.corruption_callback,
                decoder,
                timestamps,
                data,
            )
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

        let pos = match seek::RoughPos::new(&self.data, start, end) {
            Ok(pos) => pos,
            Err(seek::Error::EmptyFile) => return Ok(0),
            Err(other) => return Err(Error::InvalidRange(other)),
        };

        Ok(pos
            .refine(&mut self.data)
            .map_err(Error::Seeking)?
            .map(|pos| pos.lines(&self.data))
            .unwrap_or(0))
    }
    /// Will return between zero and two times `n` samples
    ///
    /// This might read more but will resample down using averages.
    /// No interpolation is performed.
    ///
    /// If `skip_corrupt_meta` is true a corrupt meta section is not an error but skipped
    /// beyond.
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
        skip_corrupt_meta: bool,
    ) -> Result<(), Error> {
        assert!(
            self.downsampled
                .windows(2)
                .all(|w| w[0].data().data_len >= w[1].data().data_len),
            "downsampled must be sorted in descending resolution/numb lines"
        );

        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();

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
            .map_err(Error::InvalidRange)?
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
            .read_resampling(
                seek,
                &mut self.corruption_callback,
                resampler,
                bucket_size,
                timestamps,
                data,
            )
            .map_err(Error::Reading)
    }

    /// Will return between zero and `n` samples
    ///
    /// This might read only part of the requested range.
    /// No interpolation or resampling is performed.
    ///
    /// # Errors
    ///
    /// See the [`Error`] docs for an exhaustive list of everything that can go wrong.
    /// Its mostly IO-issues.
    #[allow(clippy::missing_panics_doc)] // is bug if panic
    #[instrument(skip(self, decoder, timestamps, data),
        fields(range = format!("{:?}..{:?}", range.start_bound(), range.end_bound())))]
    pub fn read_first_n<D: Decoder>(
        &mut self,
        n: usize,
        decoder: &mut D,
        range: impl RangeBounds<Timestamp>,
        timestamps: &mut Vec<Timestamp>,
        data: &mut Vec<D::Item>,
    ) -> Result<(), Error> {
        let Some(seek) = seek::RoughPos::new(
            &self.data,
            range.start_bound().cloned(),
            range.end_bound().cloned(),
        )
        .map_err(Error::InvalidRange)?
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
            .read_first_n(
                n,
                seek,
                &mut self.corruption_callback,
                decoder,
                timestamps,
                data,
            )
            .map_err(Error::Reading)
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
        self.data.last_line(decoder, &mut self.corruption_callback)
    }

    /// # Errors
    /// When the OS fails to flush files to disk the underlying
    /// io error is returned
    pub fn flush_to_disk(&mut self) -> std::io::Result<()> {
        self.data.flush_to_disk()
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)] // is bug if panic
    pub fn range(&self) -> Option<core::ops::RangeInclusive<Timestamp>> {
        self.range.clone().into()
    }

    /// Returns the number of lines in the file.
    pub fn len(&self) -> u64 {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    pub fn payload_size(&self) -> usize {
        self.data.payload_size().raw()
    }
}
