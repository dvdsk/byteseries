use byteseries::ByteSeries;
use std::fs;
use temp_dir::TempDir;

mod shared;
use shared::insert_uniform_arrays;

use pretty_assertions::assert_eq;
use crate::shared::setup_tracing;

#[test]
fn reconstructed_index_works() {
    setup_tracing();

    const PAYLOAD_SIZE: usize = 4;
    const STEP: u64 = 5;
    const N_TO_INSERT: u32 = 2;

    let time = 1719330938;

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_append");
    let mut series: ByteSeries = ByteSeries::new(&test_path, PAYLOAD_SIZE, ()).unwrap();
    insert_uniform_arrays(&mut series, N_TO_INSERT, STEP, PAYLOAD_SIZE, time);

    let index_path = test_path.with_extension("byteseries_index");
    let created_index = fs::read(&index_path).unwrap();
    fs::remove_file(&index_path).unwrap();

    let _: (ByteSeries, ()) = ByteSeries::open_existing(&test_path, PAYLOAD_SIZE).unwrap();
    let reconstructed_index = fs::read(&index_path).unwrap();

    assert_eq!(created_index, reconstructed_index);
}
