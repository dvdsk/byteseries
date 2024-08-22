use core::fmt;
use std::path::Path;
use std::str::Utf8Error;

use ron::ser::PrettyConfig;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::downsample::resample::EmptyResampler;
use crate::{downsample, series, ByteSeries, Resampler};

pub struct ByteSeriesBuilder<const PAYLOAD_SET: bool, const HEADER_SET: bool, R, H> {
    payload_size: Option<usize>,
    create_new: bool,
    header: H,
    resampler: R,
    resample_configs: Vec<downsample::Config>,
}

impl<const PAYLOAD_SET: bool, const HEADER_SET: bool, R, H>
    ByteSeriesBuilder<PAYLOAD_SET, HEADER_SET, R, H>
where
    H: Serialize + DeserializeOwned + Eq + fmt::Debug,
    R: Resampler + Clone + Send + 'static,
    R::State: Send + 'static,
{
    pub(crate) fn new() -> ByteSeriesBuilder<false, false, EmptyResampler, ()> {
        ByteSeriesBuilder {
            payload_size: None,
            header: (),
            resampler: EmptyResampler,
            resample_configs: Vec::new(),
            create_new: false,
        }
    }
    /// Create a new file, fail if it already exists.
    ///
    /// Default is false. In which case if no file exists we return
    /// an error if one does we open it.
    pub fn create_new(
        mut self,
        create_new: bool,
    ) -> ByteSeriesBuilder<PAYLOAD_SET, HEADER_SET, R, H> {
        self.create_new = create_new;
        self
    }
    pub fn payload_size(self, bytes: usize) -> ByteSeriesBuilder<true, HEADER_SET, R, H> {
        ByteSeriesBuilder {
            payload_size: Some(bytes),
            header: self.header,
            resampler: self.resampler,
            resample_configs: self.resample_configs,
            create_new: self.create_new,
        }
    }
    /// If you pass in a header the file will be created with one and
    /// opening will fail if the passed in header mismatches with the
    /// one in the file.
    ///
    /// # Warning
    /// You must pass in a header when opening a file that was created
    /// with one. If you do not you will get a deserialization error.
    pub fn with_header<NewH: Serialize + DeserializeOwned + Eq + fmt::Debug>(
        self,
        header: NewH,
    ) -> Result<ByteSeriesBuilder<PAYLOAD_SET, true, R, NewH>, ron::Error>
where {
        Ok(ByteSeriesBuilder {
            payload_size: self.payload_size,
            header,
            resampler: self.resampler,
            resample_configs: self.resample_configs,
            create_new: self.create_new,
        })
    }
    pub fn with_downsampled_cache<NewR>(
        self,
        resampler: NewR,
        configs: Vec<downsample::Config>,
    ) -> ByteSeriesBuilder<PAYLOAD_SET, HEADER_SET, NewR, H> {
        ByteSeriesBuilder {
            payload_size: self.payload_size,
            header: self.header,
            resampler,
            resample_configs: configs,
            create_new: self.create_new,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HeaderError {
    #[error("Failed to serialize the header")]
    Serialize(ron::Error),
    #[error("Failed to deserialize header, maybe you passed the wrong type header?")]
    Deserialize(ron::de::SpannedError),
    #[error("Header is not valid utf8, maybe you are working with a binary header?")]
    NotUtf8(Utf8Error),
    #[error(
        "The header in the file is not the same as the one passed in. Note that \
        you have to provide the same header as you did when you created the file \
        The provided header was: {passed_in}. The header in the file tried to open \
        : {in_opened}"
    )]
    Mismatch {
        passed_in: String,
        in_opened: String,
    },
    #[error(
        "No header was given but the file did contain one, please provide \
        the correct header"
    )]
    Unexpected,
}

impl<R, H> ByteSeriesBuilder<true, true, R, H>
where
    H: Serialize + DeserializeOwned + Eq + fmt::Debug,
    R: Resampler + Clone + Send + 'static,
    R::State: Send + 'static,
{
    pub fn open(
        self,
        name: impl AsRef<Path> + fmt::Debug,
    ) -> Result<(ByteSeries, H), series::Error> {
        let (bs, header) = if self.create_new {
            let config = PrettyConfig::new();
            let serialized = ron::ser::to_string_pretty(&self.header, config)
                .map_err(HeaderError::Serialize)
                .map_err(series::Error::Header)?;
            let bytes = serialized.as_bytes();

            (
                ByteSeries::new_with_resamplers(
                    name,
                    self.payload_size.unwrap(),
                    bytes,
                    self.resampler,
                    self.resample_configs,
                )?,
                self.header,
            )
        } else {
            let (bs, header_bytes) = ByteSeries::open_existing_with_resampler(
                name,
                self.payload_size.unwrap(),
                self.resampler,
                self.resample_configs,
            )?;

            let header_text = std::str::from_utf8(&header_bytes)
                .map_err(HeaderError::NotUtf8)
                .map_err(series::Error::Header)?;
            let header = ron::from_str(header_text)
                .map_err(HeaderError::Deserialize)
                .map_err(series::Error::Header)?;

            if header != self.header {
                return Err(series::Error::Header(HeaderError::Mismatch {
                    passed_in: format!("{:?}", self.header),
                    in_opened: format!("{:?}", self.header),
                }));
            }

            (bs, header)
        };

        Ok((bs, header))
    }
}

impl<R> ByteSeriesBuilder<true, false, R, ()>
where
    R: Resampler + Clone + Send + 'static,
    R::State: Send + 'static,
{
    pub fn open(
        self,
        name: impl AsRef<Path> + fmt::Debug,
    ) -> Result<ByteSeries, series::Error> {
        let bs = if self.create_new {
            ByteSeries::new_with_resamplers(
                name,
                self.payload_size.unwrap(),
                &[],
                self.resampler,
                self.resample_configs,
            )?
        } else {
            let (bs, header_bytes) = ByteSeries::open_existing_with_resampler(
                name,
                self.payload_size.unwrap(),
                self.resampler,
                self.resample_configs,
            )?;

            if !header_bytes.is_empty() {
                return Err(series::Error::Header(HeaderError::Unexpected));
            }

            bs
        };

        Ok(bs)
    }
}
