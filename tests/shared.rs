#![allow(dead_code, unused_imports)]
use byteorder::{ByteOrder, NativeEndian, WriteBytesExt};
use byteseries::ByteSeries;
use fxhash::hash64;
use time::OffsetDateTime;

pub fn insert_uniform_arrays(
    data: &mut ByteSeries,
    n_to_insert: u32,
    _step: i64,
    line_size: usize,
    time: OffsetDateTime,
) {
    let mut timestamp = time.unix_timestamp();
    for i in 0..n_to_insert {
        let buffer = vec![i as u8; line_size];

        let dt = OffsetDateTime::from_unix_timestamp(timestamp)
            .expect("unix timestamps are always in range");
        data.append_fast(dt, buffer.as_slice()).unwrap();
        timestamp += 5;
    }
}

pub fn insert_timestamp_hashes(
    data: &mut ByteSeries,
    n_to_insert: u32,
    step: i64,
    time: OffsetDateTime,
) {
    let mut timestamp = time.unix_timestamp();

    for _ in 0..n_to_insert {
        let hash = hash64::<i64>(&timestamp);

        let mut buffer = Vec::with_capacity(8);
        buffer.write_u64::<NativeEndian>(hash).unwrap();

        let dt = OffsetDateTime::from_unix_timestamp(timestamp)
            .expect("unix timestamps are always in range");
        data.append_fast(dt, buffer.as_slice()).unwrap();
        timestamp += step;
    }
}

pub fn insert_timestamp_arrays(
    data: &mut ByteSeries,
    n_to_insert: u32,
    step: i64,
    time: OffsetDateTime,
) {
    let mut timestamp = time.unix_timestamp();

    for _ in 0..n_to_insert {
        let mut buffer = Vec::with_capacity(8);

        let dt = OffsetDateTime::from_unix_timestamp(timestamp)
            .expect("unix timestamps are always in range");
        buffer.write_i64::<NativeEndian>(timestamp).unwrap();

        data.append_fast(dt, buffer.as_slice()).unwrap();
        timestamp += step;
    }
}
