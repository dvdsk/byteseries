pub mod file;
pub mod search;
pub mod series;

pub use search::SeekPos;
pub use series::{downsample, ByteSeries};

pub type Timestamp = u64;

pub trait Decoder: core::fmt::Debug {
    type Item: core::fmt::Debug;
    fn decode_payload(&mut self, payload: &[u8]) -> Self::Item;
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
    /// This must also reset self as if it was just created
    fn finish(&mut self, collected: usize) -> Self::Item;
}
