use byteseries::{ByteSeries, TimeSeek};
use temp_dir::TempDir;
use time::OffsetDateTime;

mod shared;
use shared::{insert_timestamps, Timestamp};
use shared::setup_tracing;

#[derive(Debug)]
struct TsDecoder;


impl byteseries::Decoder for TsDecoder {
    type Item = Timestamp;

    fn decode_line(&mut self, line: &[u8]) -> Self::Item {
        u64::from_ne_bytes(line.try_into().expect("is 8 long")) as Timestamp
    }
}

#[test]
fn compare_written_to_read() {
    setup_tracing();

    const NUMBER_TO_INSERT: u64 = 1_000;
    const PERIOD: Timestamp = 24 * 3600 / NUMBER_TO_INSERT;

    let timestamp = OffsetDateTime::now_utc().unix_timestamp() as Timestamp;
    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_append_hashes_then_verify");
    let mut series = ByteSeries::new(test_path, 8, ()).unwrap();
    insert_timestamps(&mut series, NUMBER_TO_INSERT as u32, PERIOD, timestamp);

    let t1 = timestamp;
    let t2 = timestamp + NUMBER_TO_INSERT * PERIOD;

    let seek = TimeSeek::new(&mut series, t1, t2).unwrap();

    let mut timestamps = Vec::new();
    let mut data = Vec::new();
    series
        .read_to_data(
            seek.start,
            seek.stop,
            seek.first_full_ts,
            &mut TsDecoder,
            &mut timestamps,
            &mut data,
        )
        .unwrap();

    for (timestamp, data) in timestamps.into_iter().zip(data) {
        assert_eq!(
            data, timestamp,
            "the data (left) should be equal to the timestamp (right)"
        );
    }
}
