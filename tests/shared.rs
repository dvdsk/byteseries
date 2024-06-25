#![allow(dead_code, unused_imports)]
use byteorder::{ByteOrder, NativeEndian, WriteBytesExt};
use byteseries::ByteSeries;
use fxhash::hash64;
use time::OffsetDateTime;

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

pub fn insert_uniform_arrays(
    data: &mut ByteSeries,
    n_to_insert: u32,
    _step: u64,
    line_size: usize,
    time: OffsetDateTime,
) {
    let mut timestamp = time.unix_timestamp();
    for i in 0..n_to_insert {
        let buffer = vec![i as u8; line_size];

        let dt = OffsetDateTime::from_unix_timestamp(timestamp)
            .expect("unix timestamps are always in range");
        data.push_line(dt, buffer).unwrap();
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
        data.push_line(dt, buffer).unwrap();
        timestamp += step;
    }
}

pub fn insert_timestamps(data: &mut ByteSeries, n_to_insert: u32, step: u64, mut ts: Timestamp) {
    for _ in 0..n_to_insert {
        let dt = OffsetDateTime::from_unix_timestamp(ts as i64)
            .expect("unix timestamps are always in range");

        data.push_line(dt, ts.to_ne_bytes()).unwrap();
        ts += step;
    }
}
