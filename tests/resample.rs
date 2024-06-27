use byteseries::{ByteSeries, TimeSeek, Timestamp};
use temp_dir::TempDir;

fn insert_vector(ts: &mut ByteSeries, t_start: Timestamp, t_end: Timestamp, data: &[f32]) {
    let dt = (t_end - t_start) / (data.len() as u64);
    let mut time = t_start;

    for x in data {
        ts.push_line(time, &x.to_le_bytes()).unwrap();
        time = time + dt;
    }
}

//period in numb of data points
fn linear_array(n: usize, slope: f32) -> Vec<f32> {
    (0..n).map(|i| i as f32 * slope).collect()
}

#[derive(Debug)]
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
#[ignore]
fn mean_of_line_is_same_line() {
    let t1 = 0;
    let t2 = 10_000;
    let n = 10;
    let slope = 0.2;

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_combiner_diff_linear");
    let mut bs = ByteSeries::new(test_path, 4, ()).unwrap();
    let inserted_data = linear_array(n, slope);
    insert_vector(&mut bs, t1, t2, &inserted_data);

    let mut timestamps = Vec::new();
    let mut data = Vec::new();
    let seek = TimeSeek::new(&mut bs, t1, t2).unwrap();
    bs.read_n(n, seek, &mut FloatResampler, &mut timestamps, &mut data)
        .unwrap();

    let dt = (t2 - t1) / inserted_data.len() as u64;
    let slope = slope / (dt as f32);
    for (ts, val) in timestamps.into_iter().zip(data) {
        let expected_val = ts as f32 * slope as f32;
        assert!((expected_val - val) < 0.0001);
    }
}
