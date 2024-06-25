use crate::byteseries::data::index::restore;
pub use crate::search::SeekError;
use crate::Timestamp;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("could not open byteseries file: {0}")]
    Open(#[from] crate::util::OpenError),

    #[error("error accessing filesystem")]
    Io(#[from] std::io::Error),
    #[error("no data in series")]
    NoData,
    #[error("file corrupt, got only partial line")]
    PartialLine,
    #[error("could not find times")]
    Seek(#[from] SeekError),
    #[error("The header in the index and byteseries are different")]
    IndexAndDataHeaderDifferent,
    #[error("Could not restore the index")]
    RestoringIndex(restore::Error),
    #[error("The time for the new line ({new}) must be larger then the previous ({prev:?})")]
    NewLineBeforePrevious {
        new: Timestamp,
        prev: Option<Timestamp>,
    },
}
