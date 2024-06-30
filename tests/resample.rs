use byteseries::byteseries::downsample;
use byteseries::{ByteSeries, Timestamp};
use temp_dir::TempDir;

fn insert_data(ts: &mut ByteSeries, t_start: Timestamp, t_end: Timestamp, data: &[f32]) {
    let dt = (t_end - t_start) / (data.len() as u64);
    let mut time = t_start;

    for x in data {
        ts.push_line(time, x.to_le_bytes()).unwrap();
        time += dt;
    }
}

//period in numb of data points
fn line_with_slope(n: usize, slope: f32) -> Vec<f32> {
    (0..n).map(|i| i as f32 * slope).collect()
}

#[derive(Debug, Clone)]
struct FloatResampler;
impl byteseries::Decoder for FloatResampler {
    type Item = f32;

    fn decode_line(&mut self, line: &[u8]) -> Self::Item {
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

#[test]
fn no_downsampled_cache() {
    let t1 = 0;
    let t2 = 10_000;
    let slope = 0.1;
    let n_data_points = 1000;

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_no_downsample_cache");
    let mut bs = ByteSeries::new(test_path, 4, ()).unwrap();
    let data_to_insert = line_with_slope(n_data_points, slope);
    insert_data(&mut bs, t1, t2, &data_to_insert);

    let mut timestamps = Vec::new();
    let mut data = Vec::new();
    bs.read_n(10, t1..t2, &mut FloatResampler, &mut timestamps, &mut data)
        .unwrap();

    dbg!(&data);
    let dt = (t2 - t1) / data_to_insert.len() as u64;
    let slope = slope / (dt as f32);
    for (ts, val) in timestamps.into_iter().zip(data) {
        let expected_val = ts as f32 * slope as f32;
        assert!((expected_val - val) < 0.0001);
    }
}

#[test]
fn ideal_downsampled_cache() {
    let t1 = 0;
    let t2 = 100_000;
    let slope = 0.1;
    let n_data_points = 1000;

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
    let data_to_insert = line_with_slope(n_data_points, slope);
    insert_data(&mut bs, t1, t2, &data_to_insert);

    let mut timestamps = Vec::new();
    let mut data = Vec::new();
    bs.read_n(10, t1..t2, &mut FloatResampler, &mut timestamps, &mut data)
        .unwrap();

    dbg!(&data);
    let dt = (t2 - t1) / data_to_insert.len() as u64;
    let slope = slope / (dt as f32);
    for (ts, val) in timestamps.into_iter().zip(data) {
        let expected_val = ts as f32 * slope as f32;
        assert!((expected_val - val) < 0.0001);
    }
}

#[test]
fn with_cache_same_as_without() {
    let t1 = 0;
    let t2 = 100_000;
    let slope = 0.1;
    let n_data_points = 1000;

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("with_cache_same_as_without");
    let mut timestamps_without_cache = Vec::new();
    let mut data_without_cache = Vec::new();
    {
        let mut bs =
            ByteSeries::new_with_resamplers(&test_path, 4, (), FloatResampler, Vec::new()).unwrap();
        let data_to_insert = line_with_slope(n_data_points, slope);
        insert_data(&mut bs, t1, t2, &data_to_insert);

        bs.read_n(
            10,
            t1..t2,
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
            (),
            FloatResampler,
            vec![downsample::Config {
                max_gap: None,
                bucket_size: 10,
            }],
        )
        .unwrap();
        let data_to_insert = line_with_slope(n_data_points, slope);
        insert_data(&mut bs, t1, t2, &data_to_insert);

        bs.read_n(
            10,
            t1..t2,
            &mut FloatResampler,
            &mut timestamps_with_cache,
            &mut data_with_cache,
        )
        .unwrap();
    }
}
