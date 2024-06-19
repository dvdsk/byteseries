#![cfg(test)]

use byteseries::error::{Error, SeekError};
use byteseries::{new_sampler, ByteSeries, EmptyDecoder};
use temp_dir::TempDir;
use time::OffsetDateTime;

mod shared;
use shared::insert_uniform_arrays;

#[test]
fn beyond_range() {
    const LINE_SIZE: usize = 8;
    const STEP: i64 = 5;
    const N_TO_INSERT: u32 = 100;
    let start_read_inlines = N_TO_INSERT as i64 + 1;
    let read_length_inlines = 10;

    let time = OffsetDateTime::from_unix_timestamp(1539180000).expect("valid unix timestamp");

    let timestamp = time.unix_timestamp();
    println!("start timestamp {}", timestamp);

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_beyond_range");
    let mut data = ByteSeries::open(test_path, LINE_SIZE).unwrap();

    insert_uniform_arrays(&mut data, N_TO_INSERT, STEP, LINE_SIZE, time);

    let t1 = OffsetDateTime::from_unix_timestamp(timestamp + start_read_inlines * STEP)
        .expect("valid timestamp");
    let t2 = OffsetDateTime::from_unix_timestamp(
        timestamp + (start_read_inlines + read_length_inlines) * STEP,
    )
    .expect("valid timestamp");

    let decoder = EmptyDecoder {};
    let sampler = new_sampler(data, decoder)
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
