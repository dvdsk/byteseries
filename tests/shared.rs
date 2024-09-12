#![allow(dead_code, unused_imports, unused_macros)]
use std::io::Write;

use byteseries::ByteSeries;
use num_traits::ToBytes;
use rstest_reuse::template;

pub type Timestamp = u64;

pub fn setup_tracing() {
    use tracing_error::ErrorLayer;
    use tracing_subscriber::filter;
    use tracing_subscriber::fmt;
    use tracing_subscriber::prelude::*;

    let filter = filter::EnvFilter::builder().from_env().unwrap();
    let fmt = fmt::layer()
        .pretty()
        .with_line_number(true)
        .with_test_writer();

    let _ignore_err = tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .with(ErrorLayer::default())
        .try_init();
}

#[template]
#[rstest]
#[case(0)]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
#[case(5)]
#[case(6)]
fn payload_sizes(#[case] payload_size: PayloadSize) {}

#[derive(Debug, Clone)]
pub struct EmptyDecoder;
impl byteseries::Decoder for EmptyDecoder {
    type Item = ();
    fn decode_payload(&mut self, _: &[u8]) -> Self::Item {}
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

pub fn insert_timestamps(
    data: &mut ByteSeries,
    n_to_insert: u32,
    step: u64,
    mut ts: Timestamp,
) {
    for _ in 0..n_to_insert {
        data.push_line(ts, ts.to_ne_bytes()).unwrap();
        ts += step;
    }
}

#[derive(Debug, Clone)]
pub struct FloatResampler;
impl byteseries::Decoder for FloatResampler {
    type Item = f32;

    fn decode_payload(&mut self, line: &[u8]) -> Self::Item {
        let bytes: [u8; 4] = line[0..4].try_into().expect("line should be long enough");
        f32::from_le_bytes(bytes)
    }
}

impl byteseries::Encoder for FloatResampler {
    type Item = f32;

    fn encode_item(&mut self, item: &Self::Item) -> Vec<u8> {
        item.to_le_bytes().to_vec()
    }
}

impl byteseries::Resampler for FloatResampler {
    type State = f32;

    fn state(&self) -> Self::State {
        0f32
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FakeFloatResampler {
    pub(crate) payload_size: usize,
}

impl byteseries::Decoder for FakeFloatResampler {
    type Item = f32;

    fn decode_payload(&mut self, _: &[u8]) -> Self::Item {
        0f32
    }
}

impl byteseries::Encoder for FakeFloatResampler {
    type Item = f32;

    fn encode_item(&mut self, _: &Self::Item) -> Vec<u8> {
        vec![0; self.payload_size]
    }
}

impl byteseries::Resampler for FakeFloatResampler {
    type State = f32;

    fn state(&self) -> Self::State {
        0f32
    }
}

pub fn insert_lines(
    bs: &mut ByteSeries,
    n_points: u64,
    t_start: Timestamp,
    t_end: Timestamp,
) {
    let slope = 0.1;

    let dt = (t_end - t_start) / n_points as u64;
    assert_ne!(dt, 0);
    let mut time = t_start;

    for _ in 0..n_points {
        let val = time as f32 * slope;
        bs.push_line(time, val.to_le_bytes()).unwrap();
        time += dt;
    }
}
