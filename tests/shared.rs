use byteorder::{ByteOrder, NativeEndian, WriteBytesExt};
use byteseries::Series;
use chrono::{DateTime, NaiveDateTime, Utc};
use fxhash::hash64;

pub fn insert_uniform_arrays(
    data: &mut Series,
    n_to_insert: u32,
    _step: i64,
    line_size: usize,
    time: DateTime<Utc>,
) {
    let mut timestamp = time.timestamp();
    for i in 0..n_to_insert {
        let buffer = vec![i as u8; line_size];

        let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
        data.append(dt, buffer.as_slice()).unwrap();
        timestamp += 5;
    }
}

pub fn insert_timestamp_hashes(
    data: &mut Series,
    n_to_insert: u32,
    step: i64,
    time: DateTime<Utc>,
) {
    let mut timestamp = time.timestamp();

    for _ in 0..n_to_insert {
        let hash = hash64::<i64>(&timestamp);

        let mut buffer = Vec::with_capacity(8);
        &buffer.write_u64::<NativeEndian>(hash).unwrap();

        let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
        data.append(dt, buffer.as_slice()).unwrap();
        timestamp += step;
    }
}

pub fn insert_timestamp_arrays(
    data: &mut Series,
    n_to_insert: u32,
    step: i64,
    time: DateTime<Utc>,
) {
    let mut timestamp = time.timestamp();

    for _ in 0..n_to_insert {
        let mut buffer = Vec::with_capacity(8);

        let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
        &buffer.write_i64::<NativeEndian>(timestamp).unwrap();

        data.append(dt, buffer.as_slice()).unwrap();
        timestamp += step;
    }
}
