use std::u32;

use byteseries::ByteSeries;
use pretty_assertions::assert_eq;
use temp_dir::TempDir;

mod shared;
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
    shared::setup_tracing();

    const NUMBER_TO_INSERT: u64 = 1_000;
    const PERIOD: Timestamp = 24 * 3600 / NUMBER_TO_INSERT;

    let timestamp = 1719330938;
    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_append_hashes_then_verify");
    let (mut series, _) = ByteSeries::builder()
        .payload_size(8)
        .create_new(true)
        .with_any_header()
        .open(test_path)
        .unwrap();
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
fn append_after_reopen_empty_works() {
    shared::setup_tracing();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("append_after_reopen_empty");
    {
        let (mut bs, _) = ByteSeries::builder()
            .payload_size(0)
            .create_new(true)
            .with_any_header()
            .open(&test_path)
            .unwrap();
        assert!(matches!(
            bs.last_line(&mut EmptyDecoder),
            Err(byteseries::series::data::ReadError::NoData)
        ));
    }

    let (mut bs, _) = ByteSeries::builder()
        .payload_size(0)
        .with_any_header()
        .open(&test_path)
        .unwrap();
    bs.push_line(1700000000, &[]).unwrap();
}

mod refuses_old_time {
    use std::u16;

    use super::{shared, ByteSeries, TempDir};

    #[test]
    fn simple() {
        shared::setup_tracing();

        let test_dir = TempDir::new().unwrap();
        let test_path = test_dir.child("append_refused_if_time_old_simple");
        let (mut bs, _) = ByteSeries::builder()
            .payload_size(0)
            .create_new(true)
            .with_any_header()
            .open(&test_path)
            .unwrap();
        bs.push_line(1, &[]).unwrap();
        bs.push_line(2, &[]).unwrap();
        bs.push_line(3, &[]).unwrap();
        // duplicate
        let error = bs.push_line(2, &[]).unwrap_err();
        assert!(matches!(
            error,
            byteseries::series::Error::TimeNotAfterLast { new: 2, prev: 3 }
        ));

        drop(bs);

        let (mut bs, _) = ByteSeries::builder()
            .payload_size(0)
            .with_any_header()
            .open(test_path)
            .unwrap();
        assert_eq!(bs.range(), Some(1..=3));
        let error = bs.push_line(2, &[]).unwrap_err();
        assert!(matches!(
            error,
            byteseries::series::Error::TimeNotAfterLast { new: 2, prev: 3 }
        ));
    }

    #[test]
    fn large_range() {
        const TOTAL_LINES: u64 = u16::MAX as u64 * 2;

        shared::setup_tracing();

        let test_dir = TempDir::new().unwrap();
        let test_path = test_dir.child("append_refused_if_time_old_range");
        let (mut bs, _) = ByteSeries::builder()
            .payload_size(0)
            .create_new(true)
            .with_any_header()
            .open(&test_path)
            .unwrap();

        for i in 1..=TOTAL_LINES {
            bs.push_line(i, &[]).unwrap();
            let error = bs.push_line(i - 1, &[]).unwrap_err();
            assert!(matches!(
                error,
                byteseries::series::Error::TimeNotAfterLast { .. }
            ))
        }

        drop(bs);

        let (mut bs, _) = ByteSeries::builder()
            .payload_size(0)
            .with_any_header()
            .open(test_path)
            .unwrap();
        assert_eq!(bs.range(), Some(1..=TOTAL_LINES));

        let error = bs.push_line(2, &[]).unwrap_err();
        assert!(matches!(
            error,
            byteseries::series::Error::TimeNotAfterLast { .. }
        ));
    }

    #[test]
    fn around_meta_section() {
        const JUST_BEFORE_FIRST_META_SECTION: u64 = 1 * u16::MAX as u64 - 20;
        const JUST_BEFORE_SECOND_META_SECTION: u64 = 2 * u16::MAX as u64 - 20;

        shared::setup_tracing();

        let test_dir = TempDir::new().unwrap();

        for i in 0..40 {
            let path = format!("append_refused_if_time_old_around_meta_{i}");
            let test_path = test_dir.child(path);

            let (mut bs, _) = ByteSeries::builder()
                .payload_size(0)
                .create_new(true)
                .with_any_header()
                .open(&test_path)
                .unwrap();

            bs.push_line(0, &[]).unwrap();
            bs.push_line(JUST_BEFORE_SECOND_META_SECTION, &[]).unwrap();
            let error = bs
                .push_line(JUST_BEFORE_FIRST_META_SECTION + i, &[])
                .unwrap_err();
            assert!(matches!(
                error,
                byteseries::series::Error::TimeNotAfterLast { .. }
            ))
        }
    }
}
