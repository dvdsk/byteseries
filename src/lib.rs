mod data;
mod header;
mod util;
pub use data::Timeseries;
mod search;
pub use search::BoundResult;
mod read;
pub use read::Selector;

mod test;

#[derive(Debug)]
pub struct DecodeParams {
    current_timestamp: i64,
    next_timestamp: i64,
    next_timestamp_pos: u64,
}
