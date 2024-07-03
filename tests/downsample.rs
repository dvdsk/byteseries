use byteseries::byteseries::downsample;
use byteseries::{ByteSeries, Timestamp};
use itertools::Itertools;
use pretty_assertions::assert_eq;
use temp_dir::TempDir;

mod shared;

const T1: Timestamp = 0;
const T2: Timestamp = 10_000;

fn insert_line(ts: &mut ByteSeries) {
    let t_start = T1;
    let t_end = T2;
    let slope = 0.1;
    let n_points = 1000;

    let dt = (t_end - t_start) / n_points as u64;
    let mut time = t_start;

    for _ in 0..n_points {
        let val = time as f32 * slope;
        ts.push_line(time, val.to_le_bytes()).unwrap();
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
        // .inspect(|s| println!("slope: {s}"))
        .all(|s| (s - 0.1).abs() < 0.001);
    assert!(slope_ok)
}

#[test]
fn no_downsampled_cache() {
    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_no_downsample_cache");
    let mut bs = ByteSeries::new(test_path, 4, ()).unwrap();
    insert_line(&mut bs);

    let mut timestamps = Vec::new();
    let mut data = Vec::new();
    bs.read_n(10, T1..T2, &mut FloatResampler, &mut timestamps, &mut data)
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
    insert_line(&mut bs);

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
        insert_line(&mut bs);

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
        insert_line(&mut bs);

        bs.read_n(
            10,
            T1..T2,
            &mut FloatResampler,
            &mut timestamps_with_cache,
            &mut data_with_cache,
        )
        .unwrap();
    }

    assert_eq!(timestamps_with_cache, timestamps_without_cache);
    assert_eq!(data_with_cache, data_without_cache);
}

#[test]
#[ignore]
fn truncated_downsampled_is_detected() {
    todo!()
}
