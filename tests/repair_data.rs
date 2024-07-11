use byteseries::search::SeekError;
use byteseries::ByteSeries;
use temp_dir::TempDir;

mod shared;
use shared::setup_tracing;

use crate::shared::EmptyDecoder;

#[test]
fn only_meta_section_in_file() {
    setup_tracing();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("only_meta_section_in_file");
    {
        let mut series = ByteSeries::new(&test_path, 1, ()).unwrap();
        series.push_line(42, [12]).unwrap();
    }

    // todo gotta fix index too
    let series_path = test_path.clone().with_extension("byteseries");
    let series_file = std::fs::OpenOptions::new()
        .write(true)
        .open(series_path)
        .unwrap();
    series_file.set_len(12).unwrap();

    let (mut series, _) = ByteSeries::open_existing::<()>(test_path, 1).unwrap();
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

#[test]
fn partial_meta_at_end() {
    setup_tracing();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("partial_meta_at_end");
    {
        let mut series = ByteSeries::new(&test_path, 1, ()).unwrap();
        series.push_line(42, [15]).unwrap();
        series.push_line(100_000, [16]).unwrap();
    }

    let series_path = test_path.clone().with_extension("byteseries");
    let series_file = std::fs::OpenOptions::new()
        .write(true)
        .open(series_path)
        .unwrap();
    let len = series_file.metadata().unwrap().len();
    series_file.set_len(len - 4).unwrap();

    let (mut series, _) = ByteSeries::open_existing::<()>(test_path, 1).unwrap();
    let mut timestamps = Vec::new();
    series
        .read_all(40..44, &mut EmptyDecoder, &mut timestamps, &mut Vec::new())
        .unwrap();
    assert_eq!(&timestamps, &[42]);
}

#[test]
fn meta_start_as_last_line() {
    setup_tracing();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("meta_start_as_last_line");
    {
        let mut series = ByteSeries::new(&test_path, 1, ()).unwrap();
        series.push_line(42, [15]).unwrap();
        series.push_line(100_000, [16]).unwrap();
    }

    let series_path = test_path.clone().with_extension("byteseries");
    let series_file = std::fs::OpenOptions::new()
        .write(true)
        .open(series_path)
        .unwrap();
    let len = series_file.metadata().unwrap().len();
    series_file.set_len(len - 3 - 3 * 3).unwrap();
    dbg!("done messing everything up :)");

    let (mut series, _) = ByteSeries::open_existing::<()>(test_path, 1).unwrap();
    let mut timestamps = Vec::new();
    series
        .read_all(40..44, &mut EmptyDecoder, &mut timestamps, &mut Vec::new())
        .unwrap();
    assert_eq!(&timestamps, &[42]);
}
