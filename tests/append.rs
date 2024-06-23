use byteorder::{ByteOrder, NativeEndian};
use byteseries::{new_sampler, ByteSeries, Decoder};
use fxhash::hash64;
use temp_dir::TempDir;
use time::OffsetDateTime;

mod shared;
use shared::{insert_timestamp_arrays, insert_timestamp_hashes};

use crate::shared::setup_tracing;

#[derive(Debug)]
struct HashDecoder {}

impl Decoder<u64> for HashDecoder {
    fn decode(&mut self, bytes: &[u8], out: &mut Vec<u64>) {
        let hash = NativeEndian::read_u64(bytes);
        out.push(hash);
    }
}

#[test]
fn hashes_then_verify() {
    const NUMBER_TO_INSERT: i64 = 1_000;
    const PERIOD: i64 = 24 * 3600 / NUMBER_TO_INSERT;

    let time = OffsetDateTime::now_utc();
    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_append_hashes_then_verify");
    let mut data = ByteSeries::new(test_path, 8, ()).unwrap();
    insert_timestamp_hashes(&mut data, NUMBER_TO_INSERT as u32, PERIOD, time);

    let timestamp = time.unix_timestamp();
    let t1 = OffsetDateTime::from_unix_timestamp(timestamp).expect("valid timestamp");
    let t2 = OffsetDateTime::from_unix_timestamp(timestamp + NUMBER_TO_INSERT * PERIOD)
        .expect("valid timestamp");

    let n = 8_000;
    let decoder = HashDecoder {};
    let mut sampler = new_sampler(data, decoder)
        .points(n)
        .start(t1)
        .stop(t2)
        .build()
        .unwrap();

    sampler.sample_all().unwrap();

    for (timestamp, data) in sampler.into_iter() {
        let timestamp_hash = hash64::<i64>(&(timestamp as i64));
        assert_eq!(
            data, timestamp_hash,
            "the data (left) should be a hash of the timestamp (right) the timestamp was: {timestamp}"
        );
    }
}

#[test]
fn hashes_read_skipping_then_verify() {
    const NUMBER_TO_INSERT: i64 = 1_007;
    const PERIOD: i64 = 24 * 3600 / NUMBER_TO_INSERT;

    let time = OffsetDateTime::now_utc();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_read_skipping_then_verify");
    let mut data = ByteSeries::new(test_path, 8, ()).unwrap();
    insert_timestamp_hashes(&mut data, NUMBER_TO_INSERT as u32, PERIOD, time);

    let timestamp = time.unix_timestamp();
    let t1 = OffsetDateTime::from_unix_timestamp(timestamp).unwrap();
    let t2 = OffsetDateTime::from_unix_timestamp(timestamp + NUMBER_TO_INSERT * PERIOD).unwrap();

    let n = 100;
    let decoder = HashDecoder {};
    let mut sampler = new_sampler(data, decoder)
        .points(n)
        .start(t1)
        .stop(t2)
        .build()
        .unwrap();
    sampler.sample_all().unwrap();

    assert_eq!(sampler.values().len(), n);
    for (timestamp, hash) in sampler.into_iter() {
        let correct = hash64::<i64>(&(timestamp as i64));
        assert_eq!(hash, correct);
    }
}

#[derive(Debug)]
struct TimestampDecoder {}

impl Decoder<i64> for TimestampDecoder {
    fn decode(&mut self, bytes: &[u8], out: &mut Vec<i64>) {
        let ts = NativeEndian::read_i64(bytes);
        out.push(ts);
    }
}

#[test]
fn timestamps_then_verify() {
    const NUMBER_TO_INSERT: i64 = 10_000;
    const PERIOD: i64 = 24 * 3600 / NUMBER_TO_INSERT;

    setup_tracing();

    let time = OffsetDateTime::now_utc();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_append_timestamps_then_verify");
    let mut data = ByteSeries::new(test_path, 8, ()).unwrap();
    insert_timestamp_arrays(&mut data, NUMBER_TO_INSERT as u32, PERIOD, time);

    let timestamp = time.unix_timestamp();
    let t1 = time;
    let t2 = OffsetDateTime::from_unix_timestamp(timestamp + NUMBER_TO_INSERT * PERIOD).unwrap();

    let n = 8_000;
    let decoder = TimestampDecoder {};
    let mut sampler = new_sampler(data, decoder)
        .points(n)
        .start(t1)
        .stop(t2)
        .build()
        .unwrap();
    sampler.sample_all().unwrap();

    assert_eq!(sampler.values().len(), n);
    let mut prev = None;
    for (i, (timestamp, decoded)) in sampler.into_iter().enumerate() {
        let correct = timestamp as i64;
        assert_eq!(
            decoded, correct,
            "failed on element: {}.\nIt should have ts: {}, but has been given {}\nprev element has ts: {:?}, the step is: {}",
            i, timestamp, decoded, prev, PERIOD
        );
        prev = Some(timestamp);
    }
}
