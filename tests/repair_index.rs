use byteseries::ByteSeries;
use rstest::rstest;
use temp_dir::TempDir;

mod shared;
use shared::setup_tracing;

use shared::EmptyDecoder;

#[rstest]
#[case(1)]
#[case(8)]
#[case(16)]
#[case(32)]
#[trace]
fn truncated_index(#[case] bytes_removed: u64) {
    setup_tracing();
    const PAYLOAD_SIZE: usize = 0;

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("truncated_index");
    {
        let (mut series, _) = ByteSeries::builder()
            .payload_size(PAYLOAD_SIZE)
            .create_new(true)
            .with_any_header()
            .open(&test_path)
            .unwrap();
        series.push_line(42, vec![12; PAYLOAD_SIZE]).unwrap();
        series.push_line(100_000, vec![13; PAYLOAD_SIZE]).unwrap();
        series.push_line(500_000, vec![14; PAYLOAD_SIZE]).unwrap();
    }

    let index_path = test_path.clone().with_extension("byteseries_index");
    let index_file = std::fs::OpenOptions::new()
        .write(true)
        .open(index_path)
        .unwrap();
    let len = index_file.metadata().unwrap().len();
    index_file.set_len(len - bytes_removed).unwrap();

    let (mut series, _) = ByteSeries::builder()
        .payload_size(PAYLOAD_SIZE)
        .with_any_header()
        .open(&test_path)
        .unwrap();
    let mut timestamps = Vec::new();
    series
        .read_all(.., &mut EmptyDecoder, &mut timestamps, &mut Vec::new(), false)
        .unwrap();
    assert_eq!(timestamps, [42, 100_000, 500_000]);
}

#[test]
#[ignore = "not yet written"]
fn use_repaired_index() {
    todo!()
}
