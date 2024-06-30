pub mod byteseries;
pub mod search;
mod util;

pub use byteseries::ByteSeries;
pub use search::SeekPos;

pub type Timestamp = u64;

pub trait Decoder: core::fmt::Debug {
    type Item: core::fmt::Debug;
    fn decode_line(&mut self, line: &[u8]) -> Self::Item;
}

pub trait Encoder: core::fmt::Debug {
    type Item: core::fmt::Debug;
    fn encode_item(&mut self, item: &Self::Item) -> Vec<u8>;
}

pub trait Resampler: Decoder + Encoder<Item = <Self as Decoder>::Item> + core::fmt::Debug {
    type State: ResampleState<Item = <Self as Decoder>::Item>;
    fn state(&self) -> Self::State;
}

pub trait ResampleState: core::fmt::Debug {
    type Item: core::fmt::Debug;
    fn add(&mut self, item: Self::Item);
    fn finish(&mut self, collected: usize) -> Self::Item;
}
