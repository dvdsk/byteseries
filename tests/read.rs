use byteseries::{ByteSeries, ResampleState};
use pretty_assertions::assert_eq;
use temp_dir::TempDir;

mod shared;
use shared::setup_tracing;
use shared::{insert_timestamps, Timestamp};

#[derive(Debug, Clone)]
struct TsDecoder;

impl byteseries::Decoder for TsDecoder {
    type Item = Timestamp;

    fn decode_payload(&mut self, line: &[u8]) -> Self::Item {
        u64::from_ne_bytes(line.try_into().expect("is 8 long")) as Timestamp
    }
}

#[test]
fn last_line_is_correct() {
    setup_tracing();

    const NUMBER_TO_INSERT: u64 = 1_000;
    const PERIOD: Timestamp = 24 * 3600 / NUMBER_TO_INSERT;

    let timestamp = 1719330938;
    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_append_hashes_then_verify");
    let mut series = ByteSeries::builder()
        .create_new(true)
        .payload_size(8)
        .open(&test_path)
        .unwrap();
    insert_timestamps(&mut series, NUMBER_TO_INSERT as u32, PERIOD, timestamp);

    let (last_ts, last_item) = series.last_line(&mut TsDecoder).unwrap();
    assert_eq!(last_ts, last_item);
    assert_eq!(last_ts, timestamp + (NUMBER_TO_INSERT - 1) * PERIOD);
}

#[derive(Debug, Clone)]
struct RawLineDecoder;

impl byteseries::Decoder for RawLineDecoder {
    type Item = [u8; 5];

    fn decode_payload(&mut self, line: &[u8]) -> Self::Item {
        line.try_into().unwrap()
    }
}

impl byteseries::Encoder for RawLineDecoder {
    type Item = [u8; 5];

    fn encode_item(&mut self, _: &Self::Item) -> Vec<u8> {
        todo!()
    }
}

#[derive(Debug)]
struct RawLineState([u8; 5]);

impl ResampleState for RawLineState {
    type Item = [u8; 5];

    fn add(&mut self, line: Self::Item) {
        self.0 = line
    }

    fn finish(&mut self, _: usize) -> Self::Item {
        self.0
    }
}

impl byteseries::Resampler for RawLineDecoder {
    type State = RawLineState;

    fn state(&self) -> Self::State {
        RawLineState([0u8; 5])
    }
}

#[test]
fn read_a_single_item() {
    setup_tracing();

    let timestamp = 1700000000;
    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_append_hashes_then_verify");
    let mut series = ByteSeries::builder()
        .create_new(true)
        .payload_size(5)
        .open(test_path)
        .unwrap();
    series.push_line(timestamp, &[1u8; 5]).unwrap();

    let mut timestamps = Vec::new();
    let mut data = Vec::new();
    series
        .read_n(
            5,
            timestamp - 10..timestamp + 10,
            &mut RawLineDecoder,
            &mut timestamps,
            &mut data,
        )
        .unwrap();
    assert_eq!(timestamps.pop(), Some(timestamp));
}
