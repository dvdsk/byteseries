use std::path::Path;
use std::str::Utf8Error;

use crate::downsample::resample::EmptyResampler;
use crate::{downsample, series, ByteSeries, Resampler};

#[derive(Debug)]
enum HeaderOption {
    MustMatch(Vec<u8>),
    Ignore,
}

impl HeaderOption {
    fn as_bytes(&self) -> &[u8] {
        match self {
            HeaderOption::MustMatch(vec) => vec,
            HeaderOption::Ignore => &[],
        }
    }
    fn into_bytes(self) -> Vec<u8> {
        match self {
            HeaderOption::MustMatch(vec) => vec,
            HeaderOption::Ignore => Vec::new(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum PayloadSizeOption {
    MustMatch(usize),
    Ignore,
}
impl PayloadSizeOption {
    fn expect(&self, panic_msg: &str) -> usize {
        match self {
            PayloadSizeOption::MustMatch(bytes) => *bytes,
            PayloadSizeOption::Ignore => panic!("{}", panic_msg),
        }
    }
}

pub struct ByteSeriesBuilder<
    const PAYLOAD_SET: bool,
    const HEADER_SET: bool,
    const CAN_CREATE_NEW: bool,
    const CAN_IGNORE_PAYLOADSIZE: bool,
    R,
> {
    payload_size: PayloadSizeOption,
    create_new: bool,
    header: HeaderOption,
    ignore_header: bool,
    resampler: R,
    resample_configs: Vec<downsample::Config>,
}

impl<
        const PAYLOAD_SET: bool,
        const HEADER_SET: bool,
        const CAN_IGNORE_PAYLOADSIZE: bool,
        R,
    > ByteSeriesBuilder<PAYLOAD_SET, HEADER_SET, true, CAN_IGNORE_PAYLOADSIZE, R>
where
    R: Resampler + Clone + Send + 'static,
    R::State: Send + 'static,
{
    /// Create a new file, fail if it already exists.
    ///
    /// Default is false. In which case if no file exists we return
    /// an error if one does we open it.
    pub fn create_new(
        self,
        create_new: bool,
    ) -> ByteSeriesBuilder<PAYLOAD_SET, HEADER_SET, true, false, R> {
        ByteSeriesBuilder {
            payload_size: self.payload_size,
            header: self.header,
            ignore_header: self.ignore_header,
            resampler: self.resampler,
            resample_configs: self.resample_configs,
            create_new,
        }
    }
}

impl<const PAYLOAD_SET: bool, const HEADER_SET: bool, const CAN_CREATE_NEW: bool, R>
    ByteSeriesBuilder<PAYLOAD_SET, HEADER_SET, CAN_CREATE_NEW, true, R>
where
    R: Resampler + Clone + Send + 'static,
    R::State: Send + 'static,
{
    pub fn retrieve_payload_size(
        self,
    ) -> ByteSeriesBuilder<true, HEADER_SET, false, true, R> {
        ByteSeriesBuilder {
            payload_size: PayloadSizeOption::Ignore,
            header: self.header,
            ignore_header: self.ignore_header,
            resampler: self.resampler,
            resample_configs: self.resample_configs,
            create_new: self.create_new,
        }
    }
}

impl<
        const PAYLOAD_SET: bool,
        const HEADER_SET: bool,
        const CAN_CREATE_NEW: bool,
        const CAN_IGNORE_PAYLOADSIZE: bool,
        R,
    >
    ByteSeriesBuilder<PAYLOAD_SET, HEADER_SET, CAN_CREATE_NEW, CAN_IGNORE_PAYLOADSIZE, R>
where
    R: Resampler + Clone + Send + 'static,
    R::State: Send + 'static,
{
    pub(crate) fn new() -> ByteSeriesBuilder<false, false, true, true, EmptyResampler> {
        ByteSeriesBuilder {
            payload_size: PayloadSizeOption::Ignore,
            header: HeaderOption::Ignore,
            ignore_header: false,
            resampler: EmptyResampler,
            resample_configs: Vec::new(),
            create_new: false,
        }
    }
    pub fn payload_size(
        self,
        bytes: usize,
    ) -> ByteSeriesBuilder<true, HEADER_SET, true, true, R> {
        ByteSeriesBuilder {
            payload_size: PayloadSizeOption::MustMatch(bytes),
            header: self.header,
            ignore_header: self.ignore_header,
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
    /// If you use this option you must pass in a header when opening a file
    /// that was created with one.
    pub fn with_header(
        self,
        header: Vec<u8>,
    ) -> ByteSeriesBuilder<PAYLOAD_SET, true, CAN_CREATE_NEW, CAN_IGNORE_PAYLOADSIZE, R>
    {
        ByteSeriesBuilder {
            payload_size: self.payload_size,
            header: HeaderOption::MustMatch(header),
            ignore_header: false,
            resampler: self.resampler,
            resample_configs: self.resample_configs,
            create_new: self.create_new,
        }
    }
    /// # Warning
    /// Ignore any existing header.
    pub fn with_any_header(
        self,
    ) -> ByteSeriesBuilder<PAYLOAD_SET, true, CAN_CREATE_NEW, CAN_IGNORE_PAYLOADSIZE, R>
    {
        ByteSeriesBuilder {
            payload_size: self.payload_size,
            header: HeaderOption::Ignore,
            ignore_header: true,
            resampler: self.resampler,
            resample_configs: self.resample_configs,
            create_new: self.create_new,
        }
    }
    pub fn with_downsampled_cache<NewR>(
        self,
        resampler: NewR,
        configs: Vec<downsample::Config>,
    ) -> ByteSeriesBuilder<
        PAYLOAD_SET,
        HEADER_SET,
        CAN_CREATE_NEW,
        CAN_IGNORE_PAYLOADSIZE,
        NewR,
    > {
        ByteSeriesBuilder {
            payload_size: self.payload_size,
            header: self.header,
            ignore_header: self.ignore_header,
            resampler,
            resample_configs: configs,
            create_new: self.create_new,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HeaderError {
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

impl HeaderError {
    fn mismatch(expected: Vec<u8>, in_file: Vec<u8>) -> Self {
        if let Ok(expected) = String::from_utf8(expected.clone()) {
            Self::Mismatch {
                passed_in: expected,
                in_opened: String::from_utf8_lossy(&in_file).to_string(),
            }
        } else {
            Self::Mismatch {
                passed_in: format!("{expected:?}"),
                in_opened: format!("{in_file:?}"),
            }
        }
    }
}

/// payload is set we can thus both create and open new series
impl<const CAN_IGNORE_PAYLOADSIZE: bool, R>
    ByteSeriesBuilder<true, true, true, CAN_IGNORE_PAYLOADSIZE, R>
where
    R: Resampler + Clone + Send + 'static,
    R::State: Send + 'static,
{
    pub fn open(
        self,
        path: impl AsRef<Path>,
    ) -> Result<(ByteSeries, Vec<u8>), series::Error> {
        let path = if path
            .as_ref()
            .extension()
            .is_some_and(|ext| ext == "byteseries")
        {
            path.as_ref().with_extension("")
        } else {
            path.as_ref().to_owned()
        };

        let bs = if self.create_new {
            ByteSeries::new_with_resamplers(
                path,
                self.payload_size.expect("CAN_CREATE_NEW is true"),
                self.header.as_bytes(),
                self.resampler,
                self.resample_configs,
            )?
        } else {
            let (bs, in_file) = ByteSeries::open_existing_with_resampler(
                path,
                self.payload_size,
                self.resampler,
                self.resample_configs,
            )?;

            match self.header {
                HeaderOption::MustMatch(expected) if in_file != expected => {
                    return Err(series::Error::Header(HeaderError::mismatch(
                        expected, in_file,
                    )))
                }
                HeaderOption::MustMatch(_) | HeaderOption::Ignore => (),
            };
            bs
        };

        Ok((bs, self.header.into_bytes()))
    }
}

/// payload is not set and thus we an only try and open a file
impl<const CAN_IGNORE_PAYLOADSIZE: bool, R>
    ByteSeriesBuilder<true, true, false, CAN_IGNORE_PAYLOADSIZE, R>
where
    R: Resampler + Clone + Send + 'static,
    R::State: Send + 'static,
{
    pub fn open(
        self,
        path: impl AsRef<Path>,
    ) -> Result<(ByteSeries, Vec<u8>), series::Error> {
        let path = if path
            .as_ref()
            .extension()
            .is_some_and(|ext| ext == "byteseries")
        {
            path.as_ref().with_extension("")
        } else {
            path.as_ref().to_owned()
        };

        let (bs, in_file) = ByteSeries::open_existing_with_resampler(
            path,
            self.payload_size,
            self.resampler,
            self.resample_configs,
        )?;

        match self.header {
            HeaderOption::MustMatch(expected) if in_file != expected => {
                return Err(series::Error::Header(HeaderError::mismatch(
                    expected, in_file,
                )))
            }
            HeaderOption::MustMatch(_) | HeaderOption::Ignore => (),
        };

        Ok((bs, self.header.into_bytes()))
    }
}
