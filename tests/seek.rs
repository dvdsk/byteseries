use byteseries::search::SeekError;
use byteseries::ByteSeries;
use temp_dir::TempDir;

mod shared;
use shared::insert_uniform_arrays;

use crate::shared::EmptyDecoder;

#[test]
fn beyond_range() {
    const PAYLOAD_SIZE: usize = 8;
    const STEP: u64 = 5000;
    const N_TO_INSERT: u32 = 1000;
    let start_read_inlines = N_TO_INSERT as u64 + 1;
    let read_length_inlines = 10;

    let timestamp = 1539180000;
    println!("start timestamp {}", timestamp);

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_beyond_range");
    let mut series = ByteSeries::new(test_path, PAYLOAD_SIZE, ()).unwrap();

    insert_uniform_arrays(&mut series, N_TO_INSERT, STEP, PAYLOAD_SIZE, timestamp);

    let t1 = timestamp + start_read_inlines * STEP;
    let t2 = timestamp + (start_read_inlines + read_length_inlines) * STEP;
    let read_res = series.read_all(t1..t2, &mut EmptyDecoder, &mut Vec::new(), &mut Vec::new());

    match read_res {
        Err(e) => match e {
            byteseries::series::Error::InvalidRange(e) => assert!(
                std::mem::discriminant(&e) == std::mem::discriminant(&SeekError::StartAfterData)
            ),
            _ => panic!("sampler should be error StartAfterData"),
        },
        Ok(_) => panic!("should return an error as we are trying to read beyond the data"),
    }
}

macro_rules! assert_in_range {
    ($range:expr, $value:expr) => {
        assert!(
            $range.contains($value),
            "item: {} is not within required range: {:?}",
            $value,
            $range
        )
    };
}

#[test]
fn within_range() {
    const PAYLOAD_SIZE: usize = 8;
    const STEP: u64 = 1000;
    const N_TO_INSERT: u32 = 150;

    let timestamp = 10_000;

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_beyond_range");
    let mut bs = ByteSeries::new(test_path, PAYLOAD_SIZE, ()).unwrap();

    insert_uniform_arrays(&mut bs, N_TO_INSERT, STEP, PAYLOAD_SIZE, timestamp);

    let start_read = 50 * STEP; // lines from start
    let stop_read = 122 * STEP;
    let t1 = timestamp + start_read;
    let t2 = timestamp + stop_read;

    let mut timestamps = Vec::new();
    let mut data = Vec::new();
    bs.read_all(t1..=t2, &mut EmptyDecoder, &mut timestamps, &mut data)
        .unwrap();

    let first = timestamps.first().unwrap();
    let last = timestamps.last().unwrap();
    assert_in_range!(t1..(t1 + STEP), first);
    assert_in_range!((t2 - STEP)..=t2, last);
}
