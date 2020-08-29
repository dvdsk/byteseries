pub use crate::search::SeekError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error acessing filesystem")]
    Io(#[from] std::io::Error),
    #[error("no data in series")]
    NoData,
    #[error("file corrupt, got only partial line")]
    PartialLine,
    #[error("could not find times")]
    Seek(#[from] SeekError),
}
