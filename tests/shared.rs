#![allow(dead_code, unused_imports)]
use std::io::Write;

use byteseries::ByteSeries;
use num_traits::ToBytes;

pub type Timestamp = u64;

pub fn setup_tracing() {
    use tracing_subscriber::filter;
    use tracing_subscriber::fmt;
    use tracing_subscriber::prelude::*;

    let filter = filter::EnvFilter::builder().from_env().unwrap();

    let fmt = fmt::layer().pretty().with_line_number(true);

    let _ignore_err = tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .try_init();
}

#[derive(Debug)]
pub struct EmptyDecoder;
impl byteseries::Decoder for EmptyDecoder {
    type Item = ();
    fn decode_payload(&mut self, _: &[u8]) -> Self::Item {
        
    }
}

pub fn insert_uniform_arrays(
    data: &mut ByteSeries,
    n_to_insert: u32,
    step: u64,
    payload_size: usize,
    mut timestamp: Timestamp,
) {
    for i in 0..n_to_insert {
        let buffer = vec![i as u8; payload_size];
        data.push_line(timestamp, buffer).unwrap();
        timestamp += step;
    }
}

pub fn insert_timestamps(data: &mut ByteSeries, n_to_insert: u32, step: u64, mut ts: Timestamp) {
    for _ in 0..n_to_insert {
        data.push_line(ts, ts.to_ne_bytes()).unwrap();
        ts += step;
    }
}
