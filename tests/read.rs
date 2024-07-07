use byteseries::ByteSeries;
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
    let mut series = ByteSeries::new(test_path, 8, ()).unwrap();
    insert_timestamps(&mut series, NUMBER_TO_INSERT as u32, PERIOD, timestamp);

    let (last_ts, last_item) = series.last_line(&mut TsDecoder).unwrap();
    assert_eq!(last_ts, last_item);
    assert_eq!(last_ts, timestamp + (NUMBER_TO_INSERT - 1) * PERIOD);
}
