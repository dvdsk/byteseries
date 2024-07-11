use byteseries::search::SeekError;
use byteseries::ByteSeries;
use rstest::rstest;
use rstest_reuse::{apply, template};
use temp_dir::TempDir;

mod shared;
use shared::setup_tracing;

use crate::shared::EmptyDecoder;

#[template]
#[rstest]
#[case(0)]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
#[case(5)]
fn payload_sizes(#[case] payload_size: usize) {}

fn lines_per_metainfo(payload_size: usize) -> usize {
    let base_lines = 2; // needed to recognise meta section
    let extra_lines_needed = match payload_size {
        0 | 1 => 2,
        2 | 3 => 1,
        4.. => 0,
    };
    base_lines + extra_lines_needed
}

fn bytes_per_metainfo(payload_size: usize) -> usize {
    lines_per_metainfo(payload_size) * (payload_size + 2)
}

#[apply(payload_sizes)]
#[trace]
fn only_meta_section_in_file(#[case] payload_size: usize) {
    setup_tracing();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("only_meta_section_in_file");
    {
        let mut series = ByteSeries::new(&test_path, payload_size, ()).unwrap();
        series.push_line(42, vec![12; payload_size]).unwrap();
    }

    let series_path = test_path.clone().with_extension("byteseries");
    let series_file = std::fs::OpenOptions::new()
        .write(true)
        .open(series_path)
        .unwrap();
    series_file
        .set_len(bytes_per_metainfo(payload_size) as u64)
        .unwrap();

    let (mut series, _) = ByteSeries::open_existing::<()>(test_path, payload_size).unwrap();
    let mut timestamps = Vec::new();
    let res = series
        .read_all(40..44, &mut EmptyDecoder, &mut timestamps, &mut Vec::new())
        .unwrap_err();
    assert!(
        matches!(
            res,
            byteseries::series::Error::InvalidRange(SeekError::EmptyFile)
        ),
        "expected InvalidRange got: {res:?}"
    );
}

#[apply(payload_sizes)]
#[trace]
fn partial_meta_at_end(#[case] payload_size: usize) {
    setup_tracing();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("partial_meta_at_end");
    {
        let mut series = ByteSeries::new(&test_path, payload_size, ()).unwrap();
        series.push_line(42, vec![15; payload_size]).unwrap();
        series.push_line(100_000, vec![16; payload_size]).unwrap();
    }

    let series_path = test_path.clone().with_extension("byteseries");
    let series_file = std::fs::OpenOptions::new()
        .write(true)
        .open(series_path)
        .unwrap();
    let len = series_file.metadata().unwrap().len();
    series_file.set_len(len - 4).unwrap();

    let (mut series, _) = ByteSeries::open_existing::<()>(test_path, payload_size).unwrap();
    let mut timestamps = Vec::new();
    series
        .read_all(40..44, &mut EmptyDecoder, &mut timestamps, &mut Vec::new())
        .unwrap();
    assert_eq!(&timestamps, &[42]);
}

#[apply(payload_sizes)]
#[trace]
fn meta_start_as_last_line(#[case] payload_size: usize) {
    setup_tracing();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("meta_start_as_last_line");
    {
        let mut series = ByteSeries::new(&test_path, payload_size, ()).unwrap();
        series.push_line(42, vec![15; payload_size]).unwrap();
        series.push_line(100_000, vec![16; payload_size]).unwrap();
    }

    let series_path = test_path.clone().with_extension("byteseries");
    let series_file = std::fs::OpenOptions::new()
        .write(true)
        .open(series_path)
        .unwrap();
    let len = series_file.metadata().unwrap().len();

    let line_size = payload_size + 2;
    let data_plus_all_but_first_meta_line = lines_per_metainfo(payload_size) + line_size;
    series_file.set_len(len - data_plus_all_but_first_meta_line as u64).unwrap();
    dbg!("done messing everything up :)");

    let (mut series, _) = ByteSeries::open_existing::<()>(test_path, payload_size).unwrap();
    let mut timestamps = Vec::new();
    series
        .read_all(40..44, &mut EmptyDecoder, &mut timestamps, &mut Vec::new())
        .unwrap();
    assert_eq!(&timestamps, &[42]);
}
