use std::path::Path;

use byteseries::downsample;
use byteseries::ByteSeries;
use pretty_assertions::assert_eq;
use shared::insert_lines;
use shared::FloatResampler;
use shared::Timestamp;
use temp_dir::TempDir;

mod shared;
use shared::setup_tracing;

const T1: Timestamp = 0;
const T2: Timestamp = 351_000;

#[test]
fn before_matches_after_repair() {
    setup_tracing();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("before_matches_after_repair");

    let config = downsample::Config {
        max_gap: None,
        bucket_size: 10,
    };

    let (timestamps_before, data_before) = {
        let mut bs = create_and_fill(&test_path, config.clone());
        read(&mut bs)
    };

    shorten_downsampled(&test_path, config.clone());

    let (mut bs, _) = ByteSeries::builder()
        .payload_size(4)
        .with_downsampled_cache(FloatResampler, vec![config])
        .with_any_header()
        .open(&test_path)
        .unwrap();

    let (timestamps_after, data_after) = read(&mut bs);
    assert_eq!(timestamps_before, timestamps_after);
    assert_eq!(data_before, data_after);
}

#[test]
fn downsampled_has_more_items() {
    setup_tracing();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("downsampled_has_more_items");

    let config = downsample::Config {
        max_gap: None,
        bucket_size: 10,
    };

    let ((timestamps_before, data_before), range_before) = {
        let mut bs = create_and_fill(&test_path, config.clone());
        (read(&mut bs), bs.range())
    };

    shorten_source_data(&test_path, 4);

    let (mut bs, _) = ByteSeries::builder()
        .payload_size(4)
        .with_downsampled_cache(FloatResampler, vec![config])
        .with_any_header()
        .open(&test_path)
        .unwrap();
    let range_after = bs.range();

    let (timestamps_after, data_after) = read(&mut bs);
    assert_ne!(range_before, range_after);
    assert_eq!(timestamps_before, timestamps_after);
    assert_eq!(data_before, data_after);
}

#[test]
fn repair_empty() {
    setup_tracing();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("repair_empty");

    let config = downsample::Config {
        max_gap: None,
        bucket_size: 10,
    };

    {
        let (_, _) = ByteSeries::builder()
            .payload_size(4)
            .create_new(true)
            .with_downsampled_cache(FloatResampler, vec![config.clone()])
            .with_any_header()
            .open(&test_path)
            .unwrap();
    }

    let (_, _) = ByteSeries::builder()
        .payload_size(4)
        .with_downsampled_cache(FloatResampler, vec![config])
        .with_any_header()
        .open(&test_path)
        .unwrap();
}

fn shorten_source_data(test_path: &Path, to_shrink: u64) {
    let path = test_path.with_extension("byteseries");
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    let len = file.metadata().unwrap().len();
    file.set_len(len - to_shrink).unwrap();
}

fn shorten_downsampled(path: &Path, config: downsample::Config) {
    let path = downsampled_path(path, config);

    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    let len = file.metadata().unwrap().len();
    file.set_len(len - 3).unwrap();
}

fn downsampled_path(path: &Path, config: downsample::Config) -> std::path::PathBuf {
    let source_name = path.file_name().unwrap_or_default();
    let mut resampled_name = source_name.to_owned();
    resampled_name.push("_");
    resampled_name.push(config.file_name_suffix());
    let mut path = path.to_owned();
    path.set_file_name(resampled_name);
    path.set_extension("byteseries");
    path
}

fn read(bs: &mut ByteSeries) -> (Vec<Timestamp>, Vec<f32>) {
    let mut timestamps = Vec::new();
    let mut data = Vec::new();
    bs.read_n(
        10,
        T1..T2,
        &mut FloatResampler,
        &mut timestamps,
        &mut data,
        false,
    )
    .unwrap();

    (timestamps, data)
}

fn create_and_fill(test_path: &Path, config: downsample::Config) -> ByteSeries {
    let (mut bs, _) = ByteSeries::builder()
        .payload_size(4)
        .create_new(true)
        .with_downsampled_cache(FloatResampler, vec![config])
        .with_any_header()
        .open(&test_path)
        .unwrap();
    insert_lines(&mut bs, 1000, T1, T2);
    bs
}
