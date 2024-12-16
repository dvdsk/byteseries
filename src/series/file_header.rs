use std::num::ParseIntError;
use std::str::Utf8Error;

use crate::builder::PayloadSizeOption;

use super::data::index::PayloadSize;

const VERSION: u16 = 1;

pub(crate) struct SeriesParams {
    pub(crate) payload_size: usize,
    pub(crate) version: u16,
}

impl SeriesParams {
    pub(crate) fn to_text(self) -> Vec<u8> {
        let Self {
            payload_size,
            version,
        } = self;
        let text = format!(
            "\nNote: NUMB_LINES line ASCII preamble followed by binary data.

    This is a byteseries {version} file, an embedded timeseries file. Time may here may
    be whatever value as long as it is monotonically increasing. The entries
    have a fixed length that never changes. For this file that is: {payload_size} bytes.

    The 'time' is stored as a 16 bit value for most entries. A line is a 16 bit
    little endian time followed by the entry. The 16 bit time is the number of
    time units since the last full time was stored.

    Every once in a while the full 64 bit time is stored. That is recognised by
    two consecutive lines starting not with a timestamp but the 16 bit pattern
    [255, 255]. The metadata is written in the remaining space. If more space is
    needed extra lines of nothing but time data are added. 

    # Example: Full time taking up 3 'lines' given an entry size is 2:

    [255, 255, a, b      first line 
     255, 255, c, d,     second line 
     e,     f, g, h]     third line, no preamble only time data

    'a' up till and including 'h' form the bytes of the 64 bit timestamp in
    little endian order.

    In the case the creator of this file wanted to store metadata in it that
    follows now:\n
     "
        );

        let n_lines = text.lines().count();
        let text = text.replace("NUMB_LINES", &n_lines.to_string());

        let length = text.len() as u32;
        let mut header = length.to_le_bytes().to_vec();
        header.extend_from_slice(text.as_bytes());
        header
    }

    pub(crate) fn from_text(text: &str) -> Result<Self, ParseError> {
        let version = parse_version(text)?;
        let payload_size = parse_payload_size(text)?;

        Ok(Self {
            payload_size,
            version,
        })
    }
}

fn parse_version(text: &str) -> Result<u16, ParseError> {
    const START_PAT: &'static str = "This is a byteseries ";
    let start = text
        .find(START_PAT)
        .ok_or(ParseError::MissingVersionStart)?
        + START_PAT.len();
    const END_PAT: &'static str = " file,";
    let end = text.find(END_PAT).ok_or(ParseError::MissingVersionEnd)?;
    let version = &text[start..end];
    version.parse().map_err(ParseError::ParseVersion)
}

fn parse_payload_size(text: &str) -> Result<usize, ParseError> {
    const START_PAT: &'static str = "For this file that is: ";
    let start = text
        .find(START_PAT)
        .ok_or(ParseError::MissingPayloadStart)?
        + START_PAT.len();
    const END_PAT: &'static str = " bytes.";
    let end = text.find(END_PAT).ok_or(ParseError::MissingPayloadEnd)?;
    let payload_size = &text[start..end];
    payload_size.parse().map_err(ParseError::ParsePayload)
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("Missing start of version anchor")]
    MissingVersionStart,
    #[error("Missing end of version anchor")]
    MissingVersionEnd,
    #[error("Could not parse version: {0}")]
    ParseVersion(ParseIntError),
    #[error("Missing start of payload size anchor")]
    MissingPayloadStart,
    #[error("Missing end of payload size anchor")]
    MissingPayloadEnd,
    #[error("Could not parse payload size: {0}")]
    ParsePayload(ParseIntError),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Could not parse info: {0}")]
    Parsing(
        #[from]
        #[source]
        ParseError,
    ),
    #[error(
        "The library version ({needed}) is incompatible with the version \
        of the file ({file})."
    )]
    VersionMismatch { needed: u16, file: u16 },
    #[error(
        "The payload size passed in ({given}) is different then that of the file. \
        ({file}). The payload size has to stay the same after creation however."
    )]
    PayloadSizeChanged { given: usize, file: usize },
    #[error("Should start with a 4 byte length")]
    TooShort,
    #[error("Should be valid utf8 however: {0}")]
    NotText(Utf8Error),
}

pub(crate) fn check_and_split_off_user_header(
    mut header: Vec<u8>,
    payload_size_option: PayloadSizeOption,
) -> Result<(PayloadSize, Vec<u8>), Error> {
    let text_len = header[0..4].try_into().map_err(|_| Error::TooShort)?;
    let text_len = u32::from_le_bytes(text_len) as usize;

    let text = &header[4..text_len];
    let text = core::str::from_utf8(text).map_err(Error::NotText)?;
    let params = SeriesParams::from_text(text)?;

    if params.version != VERSION {
        return Err(Error::VersionMismatch {
            needed: VERSION,
            file: params.version,
        });
    }

    match payload_size_option {
        PayloadSizeOption::MustMatch(configured) if params.payload_size != configured => {
            return Err(Error::PayloadSizeChanged {
                given: configured,
                file: params.payload_size,
            });
        }
        PayloadSizeOption::MustMatch(_) | PayloadSizeOption::Ignore => (),
    }

    header.drain(0..text_len + core::mem::size_of::<u32>());
    let payload_size = PayloadSize::from_raw(params.payload_size);
    Ok((payload_size, header))
}
