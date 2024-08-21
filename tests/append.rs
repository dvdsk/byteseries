use byteseries::ByteSeries;
use pretty_assertions::assert_eq;
use temp_dir::TempDir;

mod shared;
use shared::setup_tracing;
use shared::{insert_timestamps, Timestamp};

use crate::shared::EmptyDecoder;

#[derive(Debug)]
struct TsDecoder;

impl byteseries::Decoder for TsDecoder {
    type Item = Timestamp;

    fn decode_payload(&mut self, line: &[u8]) -> Self::Item {
        u64::from_ne_bytes(line.try_into().expect("is 8 long")) as Timestamp
    }
}

#[test]
fn compare_written_to_read() {
    setup_tracing();

    const NUMBER_TO_INSERT: u64 = 1_000;
    const PERIOD: Timestamp = 24 * 3600 / NUMBER_TO_INSERT;

    let timestamp = 1719330938;
    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_append_hashes_then_verify");
    let mut series = ByteSeries::new(test_path, 8, &[]).unwrap();
    insert_timestamps(&mut series, NUMBER_TO_INSERT as u32, PERIOD, timestamp);

    let t1 = timestamp;
    let t2 = timestamp + NUMBER_TO_INSERT * PERIOD;

    let mut timestamps = Vec::new();
    let mut data = Vec::new();
    series
        .read_all(t1..t2, &mut TsDecoder, &mut timestamps, &mut data)
        .unwrap();

    for (timestamp, data) in timestamps.into_iter().zip(data) {
        assert_eq!(
            data, timestamp,
            "the data (left) should be equal to the timestamp (right)"
        );
    }
}

#[test]
fn append_refused_if_time_old() {
    setup_tracing();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("append_refused_if_time_old");
    let mut bs = ByteSeries::new(&test_path, 0, &[]).unwrap();
    bs.push_line(1, &[]).unwrap();
    bs.push_line(2, &[]).unwrap();
    bs.push_line(3, &[]).unwrap();
    // duplicate
    let error = bs.push_line(2, &[]).unwrap_err();
    assert!(matches!(
        error,
        byteseries::series::Error::NewLineBeforePrevious { new: 2, prev: 3 }
    ));

    drop(bs);

    let mut bs = ByteSeries::open_existing(test_path, 0).unwrap().0;
    assert_eq!(bs.range(), Some(1..=3));
    let error = bs.push_line(2, &[]).unwrap_err();
    assert!(matches!(
        error,
        byteseries::series::Error::NewLineBeforePrevious { new: 2, prev: 3 }
    ));
}

#[test]
fn append_after_reopen_empty_works() {
    setup_tracing();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("append_after_reopen_empty");
    {
        let mut bs = ByteSeries::new(&test_path, 0, &[]).unwrap();
        assert!(matches!(
            bs.last_line(&mut EmptyDecoder),
            Err(byteseries::series::data::ReadError::NoData)
        ));
    }

    let (mut bs, _) = ByteSeries::open_existing(&test_path, 0).unwrap();
    bs.push_line(1700000000, &[]).unwrap();
}
