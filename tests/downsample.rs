use std::fs::OpenOptions;

use byteseries::series::downsample;
use byteseries::{ByteSeries, Timestamp};
use itertools::Itertools;
use pretty_assertions::assert_eq;
use rstest::rstest;
use temp_dir::TempDir;

mod shared;

const T1: Timestamp = 0;
const T2: Timestamp = 100_000;

fn insert_lines(bs: &mut ByteSeries, n_points: u64) {
    let t_start = T1;
    let t_end = T2;
    let slope = 0.1;

    let dt = (t_end - t_start) / n_points as u64;
    assert_ne!(dt, 0);
    let mut time = t_start;

    for _ in 0..n_points {
        let val = time as f32 * slope;
        bs.push_line(time, val.to_le_bytes()).unwrap();
        time += dt;
    }
}

#[derive(Debug, Clone)]
struct FloatResampler;
impl byteseries::Decoder for FloatResampler {
    type Item = f32;

    fn decode_payload(&mut self, line: &[u8]) -> Self::Item {
        let bytes: [u8; 4] = line[0..4].try_into().expect("line should be long enough");
        f32::from_le_bytes(bytes)
    }
}

impl byteseries::Encoder for FloatResampler {
    type Item = f32;

    fn encode_item(&mut self, item: &Self::Item) -> Vec<u8> {
        item.to_le_bytes().to_vec()
    }
}

impl byteseries::Resampler for FloatResampler {
    type State = f32;

    fn state(&self) -> Self::State {
        0f32
    }
}

fn assert_slope_ok(timestamps: &[Timestamp], data: &[f32]) {
    let slope_ok = timestamps
        .into_iter()
        .zip(data)
        .tuple_windows::<(_, _)>()
        .map(|((t1, d1), (t2, d2))| ((d2 - d1) as f64) / ((t2 - t1) as f64))
        .all(|s| (s - 0.1).abs() < 0.001);
    assert!(slope_ok)
}

#[rstest]
fn no_downsampled_cache(
    #[values(10, 100, 1000)] n_to_read: usize,
    #[values(10, 100, 1000, 10_000, 100_000)] n_lines: u64,
) {
    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_no_downsample_cache");
    let mut bs = ByteSeries::new(test_path, 4, ()).unwrap();
    insert_lines(&mut bs, n_lines);

    let mut timestamps = Vec::new();
    let mut data = Vec::new();
    bs.read_n(
        n_to_read,
        T1..T2,
        &mut FloatResampler,
        &mut timestamps,
        &mut data,
    )
    .unwrap();
    assert_slope_ok(&timestamps, &data)
}

#[test]
fn ideal_downsampled_cache() {
    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("downsampled_cache_present");
    let mut bs = ByteSeries::new_with_resamplers(
        test_path,
        4,
        (),
        FloatResampler,
        vec![downsample::Config {
            max_gap: None,
            bucket_size: 10,
        }],
    )
    .unwrap();
    insert_lines(&mut bs, 1000);

    let mut timestamps = Vec::new();
    let mut data = Vec::new();
    bs.read_n(10, T1..T2, &mut FloatResampler, &mut timestamps, &mut data)
        .unwrap();
    assert_slope_ok(&timestamps, &data)
}

#[test]
fn with_cache_same_as_without() {
    shared::setup_tracing();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("with_cache_same_as_without");
    let mut timestamps_without_cache = Vec::new();
    let mut data_without_cache = Vec::new();
    {
        let mut bs =
            ByteSeries::new_with_resamplers(&test_path, 4, (), FloatResampler, Vec::new()).unwrap();
        insert_lines(&mut bs, 1000);

        bs.read_n(
            10,
            T1..T2,
            &mut FloatResampler,
            &mut timestamps_without_cache,
            &mut data_without_cache,
        )
        .unwrap();
    }

    let mut timestamps_with_cache = Vec::new();
    let mut data_with_cache = Vec::new();
    {
        let (mut bs, _): (_, ()) = ByteSeries::open_existing_with_resampler(
            test_path,
            4,
            FloatResampler,
            vec![downsample::Config {
                max_gap: None,
                bucket_size: 10,
            }],
        )
        .unwrap();
        bs.range().unwrap();

        bs.read_n(
            10,
            T1..T2,
            &mut FloatResampler,
            &mut timestamps_with_cache,
            &mut data_with_cache,
        )
        .unwrap();
    }

    assert_eq!(
        timestamps_with_cache, timestamps_without_cache,
        "timestamps from the \
        cache (left) are different then does created by the resampler on \
        the fly (right)"
    );
    assert_eq!(data_with_cache, data_without_cache);
}

#[test]
fn truncated_downsampled_is_detected() {
    shared::setup_tracing();

    const MAX_GAP: usize = 10;
    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("with_cache_same_as_without");
    {
        let mut bs = ByteSeries::new_with_resamplers(
            &test_path,
            0,
            (),
            FloatResampler,
            vec![downsample::Config {
                max_gap: Some(10),
                bucket_size: 10,
            }],
        )
        .unwrap();
        let mut timestamps = (0..).into_iter();
        for ts in timestamps.by_ref().take(50) {
            bs.push_line(ts, &[0, 0, 0, 0]).unwrap();
        }
        for ts in timestamps.by_ref().take(5) {
            bs.push_line(ts, &[0, 0, 0, 0]).unwrap();
        }

        let mut timestamps = timestamps.map(|ts| ts + MAX_GAP as Timestamp + 1);
        for ts in timestamps.by_ref().take(5) {
            bs.push_line(ts, &[0, 0, 0, 0]).unwrap();
        }
    }

    {
        // corrupt the downsample cache
        let mut name = test_path.file_name().unwrap_or_default().to_owned();
        name.push("_Some(10)_10.byteseries");
        let mut path = test_path.to_path_buf();
        path.set_file_name(name);
        let downsampled_cache = OpenOptions::new()
            .write(true)
            .create_new(false)
            .create(false)
            .open(path)
            .unwrap();
        let len = downsampled_cache.metadata().unwrap().len();
        downsampled_cache.set_len(len - 1).unwrap();
    }

    let error = ByteSeries::open_existing_with_resampler::<(), _>(
        test_path,
        0,
        FloatResampler,
        vec![downsample::Config {
            max_gap: Some(10),
            bucket_size: 10,
        }],
    )
    .unwrap_err();

    use byteseries::series;
    use series::downsample;
    assert!(matches!(
        error,
        series::Error::Downsampled(downsample::Error::OpenOrCreate(
            downsample::OpenOrCreateError::Open(downsample::OpenError::OutOfSync { .. })
        ))
    ))
}

#[rstest]
#[trace]
#[case(0)]
#[case(1)]
#[case(9)]
fn undamaged_downsampled_passes_checks(#[case] lines_more_then_bucket_size: u64) {
    shared::setup_tracing();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("downsampled_cache_present");

    let resample_configs = vec![downsample::Config {
        max_gap: None,
        bucket_size: 10,
    }];
    {
        let mut bs = ByteSeries::new_with_resamplers(
            &test_path,
            4,
            (),
            FloatResampler,
            resample_configs.clone(),
        )
        .unwrap();
        insert_lines(&mut bs, 10 + lines_more_then_bucket_size);
    }

    let _bs = ByteSeries::open_existing_with_resampler::<(), FloatResampler>(
        test_path,
        4,
        FloatResampler,
        resample_configs,
    )
    .unwrap();
}
