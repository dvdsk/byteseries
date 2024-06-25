use byteseries::error::{Error, SeekError};
use byteseries::{ByteSeries, TimeSeek};
use temp_dir::TempDir;
use time::OffsetDateTime;

mod shared;
use shared::insert_uniform_arrays;

type Timestamp = u64;

#[test]
fn beyond_range() {
    const PAYLOAD_SIZE: usize = 8;
    const STEP: u64 = 5000;
    const N_TO_INSERT: u32 = 1000;
    let start_read_inlines = N_TO_INSERT as u64 + 1;
    let read_length_inlines = 10;

    let time = OffsetDateTime::from_unix_timestamp(1539180000).expect("valid unix timestamp");

    let timestamp = time.unix_timestamp() as Timestamp;
    println!("start timestamp {}", timestamp);

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_beyond_range");
    let mut data = ByteSeries::new(test_path, PAYLOAD_SIZE, ()).unwrap();

    insert_uniform_arrays(&mut data, N_TO_INSERT, STEP, PAYLOAD_SIZE, time);

    let t1 = timestamp + start_read_inlines * STEP;
    let t2 = timestamp + (start_read_inlines + read_length_inlines) * STEP;
    let seek_res = TimeSeek::new(&mut data, t1, t2);

    match seek_res {
        Err(e) => match e {
            Error::Seek(e) => assert!(
                std::mem::discriminant(&e) == std::mem::discriminant(&SeekError::StartAfterData)
            ),
            _ => panic!("sampler should be error StartAfterData"),
        },
        Ok(_) => panic!("should return an error as we are trying to read beyond the data"),
    }
}
