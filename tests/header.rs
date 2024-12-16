use byteseries::series::Error;
use byteseries::ByteSeries;
use serde::{Deserialize, Serialize};
use temp_dir::TempDir;

mod shared;
use shared::setup_tracing;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestHeader(usize);

#[test]
fn opening_with_wrong_header_is_err() {
    setup_tracing();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("opening_with_wrong_header");

    {
        let _ = ByteSeries::builder()
            .create_new(true)
            .with_header("TestHeader 1".as_bytes().to_owned())
            .payload_size(0)
            .open(&test_path)
            .unwrap();
    }

    let res = ByteSeries::builder()
        .payload_size(0)
        .with_header("TestHeader 2".as_bytes().to_owned())
        .open(&test_path)
        .unwrap_err();

    assert!(matches!(res, Error::Header(_)))
}

#[test]
fn opening_with_correct_header_is_ok() {
    setup_tracing();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("opening_with_wrong_header");
    let test_header1 = "TestHeader 1".as_bytes().to_owned();

    {
        let _ = ByteSeries::builder()
            .create_new(true)
            .with_header(test_header1.clone())
            .payload_size(0)
            .open(&test_path)
            .unwrap();
    }

    let (_, header) = ByteSeries::builder()
        .payload_size(0)
        .with_header(test_header1.clone())
        .open(&test_path)
        .unwrap();

    assert_eq!(header, test_header1)
}
