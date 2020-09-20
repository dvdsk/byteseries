#![cfg(test)]

use byteseries::error::{Error, SeekError};
use byteseries::{new_sampler, EmptyDecoder, Series};
use chrono::{DateTime, NaiveDateTime, Utc};
use std::fs;
use std::path::Path;

mod shared;
use shared::insert_uniform_arrays;

#[test]
fn beyond_range() {
    if Path::new("test_beyond_range.h").exists() {
        fs::remove_file("test_beyond_range.h").unwrap();
    }
    if Path::new("test_beyond_range.dat").exists() {
        fs::remove_file("test_beyond_range.dat").unwrap();
    }
    const LINE_SIZE: usize = 8;
    const STEP: i64 = 5;
    const N_TO_INSERT: u32 = 100;
    let start_read_inlines = N_TO_INSERT as i64 + 1;
    let read_length_inlines = 10;

    let time = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(1539180000, 0), Utc);
    let timestamp = time.timestamp();
    println!("start timestamp {}", timestamp);
    let mut data = Series::open("test_beyond_range", LINE_SIZE).unwrap();

    insert_uniform_arrays(&mut data, N_TO_INSERT, STEP, LINE_SIZE, time);

    let t1 = DateTime::<Utc>::from_utc(
        NaiveDateTime::from_timestamp(timestamp + start_read_inlines * STEP, 0),
        Utc,
    );
    let t2 = DateTime::<Utc>::from_utc(
        NaiveDateTime::from_timestamp(
            timestamp + (start_read_inlines + read_length_inlines) * STEP,
            0,
        ),
        Utc,
    );

    let decoder = EmptyDecoder {};
    let sampler = new_sampler(&data, decoder)
        .points(10)
        .start(t1)
        .stop(t2)
        .build();

    match sampler {
        Err(e) => match e {
            Error::Seek(e) => assert!(
                std::mem::discriminant(&e) == std::mem::discriminant(&SeekError::StartAfterData)
            ),
            _ => panic!("sampler should be error StartAfterData"),
        },
        Ok(_) => panic!("should return an error as we are trying to read beyond the data"),
    }
}
