use byteseries::ByteSeries;
use std::fs;
use temp_dir::TempDir;
use time::OffsetDateTime;

mod shared;
use shared::insert_uniform_arrays;

use pretty_assertions::assert_eq;
use crate::shared::setup_tracing;

#[test]
fn reconstructed_index_works() {
    setup_tracing();

    const LINE_SIZE: usize = 4;
    const STEP: u64 = 5;
    const N_TO_INSERT: u32 = 2;

    let time = OffsetDateTime::now_utc();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_append");
    let mut series: ByteSeries = ByteSeries::new(&test_path, LINE_SIZE, ()).unwrap();
    insert_uniform_arrays(&mut series, N_TO_INSERT, STEP, LINE_SIZE, time);

    let index_path = test_path.with_extension("byteseries_index");
    let created_index = fs::read(&index_path).unwrap();
    fs::remove_file(&index_path).unwrap();

    let _: (ByteSeries, ()) = ByteSeries::open_existing(&test_path, LINE_SIZE).unwrap();
    let reconstructed_index = fs::read(&index_path).unwrap();

    assert_eq!(created_index, reconstructed_index);
}

// #[derive(Debug)]
// struct FloatDecoder;
// impl byteseries::Decoder<f32> for FloatDecoder {
//     fn decode(&mut self, bytes: &[u8], out: &mut Vec<f32>) {
//         let mut arr = [0u8; 4];
//         arr.copy_from_slice(&bytes[0..4]);
//         let v = f32::from_be_bytes(arr);
//         out.push(v)
//     }
// }
