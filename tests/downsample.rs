use byteseries::series::downsample;
use byteseries::{ByteSeries, Timestamp};
use itertools::Itertools;
use pretty_assertions::assert_eq;
use rstest::rstest;
use temp_dir::TempDir;

mod shared;
use shared::insert_lines;
use shared::FloatResampler;

const T1: Timestamp = 0;
const T2: Timestamp = 100_000;

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
    let mut bs = ByteSeries::new(test_path, 4, &[]).unwrap();
    insert_lines(&mut bs, n_lines, T1, T2);

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
        &[],
        FloatResampler,
        vec![downsample::Config {
            max_gap: None,
            bucket_size: 10,
        }],
    )
    .unwrap();
    insert_lines(&mut bs, 1000, T1, T2);

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
        let mut bs = ByteSeries::new_with_resamplers(
            &test_path,
            4,
            &[],
            FloatResampler,
            Vec::new(),
        )
        .unwrap();
        insert_lines(&mut bs, 1000, T1, T2);

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
        let mut bs = ByteSeries::open_existing_with_resampler(
            test_path,
            4,
            FloatResampler,
            vec![downsample::Config {
                max_gap: None,
                bucket_size: 10,
            }],
        )
        .unwrap()
        .0;
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
            &[],
            FloatResampler,
            resample_configs.clone(),
        )
        .unwrap();
        insert_lines(&mut bs, 10 + lines_more_then_bucket_size, T1, T2);
    }

    let _bs = ByteSeries::open_existing_with_resampler(
        test_path,
        4,
        FloatResampler,
        resample_configs,
    )
    .unwrap();
}
