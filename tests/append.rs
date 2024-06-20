#![cfg(test)]

use byteorder::{ByteOrder, NativeEndian};
use byteseries::{new_sampler, ByteSeries, Decoder};
use fxhash::hash64;
use std::fs;
use temp_dir::TempDir;
use time::OffsetDateTime;

mod shared;
use shared::{insert_timestamp_arrays, insert_timestamp_hashes, insert_uniform_arrays};

#[test]
fn basic() {
    const LINE_SIZE: usize = 10;
    const STEP: i64 = 5;
    const N_TO_INSERT: u32 = 100;

    let time = OffsetDateTime::now_utc();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_append");
    let mut data = ByteSeries::new(&test_path, LINE_SIZE, ()).unwrap();
    insert_uniform_arrays(&mut data, N_TO_INSERT, STEP, LINE_SIZE, time);

    let data_path = test_path.with_extension("byteseries");
    const FULL_LINE_SIZE: u32 = (LINE_SIZE + 2) as u32;
    const HEADER: u64 = 6;
    assert_eq!(
        fs::metadata(data_path).unwrap().len(),
        (FULL_LINE_SIZE * N_TO_INSERT) as u64 + HEADER
    );
    let index_path = test_path.with_extension("byteseries_index");
    assert_eq!(fs::metadata(index_path).unwrap().len(), 16 + HEADER);
}

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

    for (timestamp, hash) in sampler.into_iter() {
        let correct = hash64::<i64>(&(timestamp as i64));
        assert_eq!(hash, correct);
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
            "failed on element: {}, which should have ts: {}, but has been given {},
            prev element has ts: {:?}, the step is: {}",
            i, timestamp, decoded, prev, PERIOD
        );
        prev = Some(timestamp);
    }
}
