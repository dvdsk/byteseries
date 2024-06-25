pub mod byteseries;
pub mod error;
mod search;
mod util;

pub use byteseries::ByteSeries;
pub use error::Error;
pub use search::TimeSeek;

pub type Timestamp = u64;

pub trait Decoder: core::fmt::Debug {
    type Item: core::fmt::Debug + Clone;
    fn decode_line(&mut self, line: &[u8]) -> Self::Item;
}
