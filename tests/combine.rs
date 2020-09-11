#![cfg(test)]

use byteseries::{combiners, new_sampler, Decoder, Series};
use chrono::{Duration, DateTime, Utc};
use std::fs;
use std::path::Path;
use std::f32::consts::PI;
use float_eq::assert_float_eq;

fn insert_vector(ts: &mut Series, t_start: DateTime<Utc>, t_end: DateTime<Utc>, data: &[f32]){
    let dt = (t_end-t_start)/(data.len() as i32);
    let mut time = t_start;

    for x in data {
        ts.append(time, &x.to_be_bytes()).unwrap();
        time = time + dt;
    }
}

//period in numb of data points
fn sine_array(n: usize, mid: f32, period: f32) -> Vec<f32> {
    (0..n)
        .map(|i| i as f32 *2.*PI/period)
        .map(|x| mid*x.sin())
        .collect()
}

//period in numb of data points
fn linear_array(n: usize, slope: f32) -> Vec<f32> {
    (0..n)
        .map(|i| i as f32 * slope)
        .collect()
}


#[derive(Debug)]
struct FloatDecoder {}
impl Decoder<f32> for FloatDecoder {
    fn decode(&mut self, bytes: &[u8], out: &mut Vec<f32>){
        let mut arr = [0u8;4];
        arr.copy_from_slice(&bytes[0..4]);
        let v = f32::from_be_bytes(arr);
        out.push(v)
    }
}

#[test]
fn mean() {
    if Path::new("test_combiner_mean.h").exists() {
        fs::remove_file("test_combiner_mean.h").unwrap();
    }
    if Path::new("test_combiner_mean.dat").exists() {
        fs::remove_file("test_combiner_mean.dat").unwrap();
    }

    let now = Utc::now();
    let t1 = now - Duration::hours(2);
    let t2 = now;
    let n = 200;
    let s = 10;

    let mut ts = Series::open("test_combiner_mean", 4).unwrap();
    let data = sine_array(n, 5.0, 100.0);
    insert_vector(&mut ts, t1, t2, &data);

    let combiner = combiners::Mean::new(combiners::SampleBin::new(s));
    let mut decoder = FloatDecoder{};
    let mut sampler = new_sampler(&ts, &mut decoder)
        .points(n)
        .start(t1)
        .stop(t2)
        .build_with_combiner(combiner)
        .unwrap();
    sampler.sample_all().unwrap();

    assert_eq!(sampler.values().len(), n/s);
    for (sample, mean) in sampler.values().iter().zip(
        data.chunks(s).map(|c| c.iter().sum::<f32>()/(s as f32))){
        assert_eq!(*sample, mean);
    }
}

// #[test]
//fn diff_linear() {
//    if Path::new("test_combiner_diff_linear.h").exists() {
//        fs::remove_file("test_combiner_diff_linear.h").unwrap();
//    }
//    if Path::new("test_combiner_diff_linear.dat").exists() {
//        fs::remove_file("test_combiner_diff_linear.dat").unwrap();
//    }

//    let now = Utc::now();
//    let t1 = now - Duration::hours(2);
//    let t2 = now;
//    let n = 10;
//    let s = 2;
//    let slope = 0.2;

//    let mut ts = Series::open("test_combiner_diff_linear", 4).unwrap();
//    let data = linear_array(n, slope);
//    insert_vector(&mut ts, t1, t2, &data);

//    let mut decoder = FloatDecoder{};
//    let mut sampler = new_sampler(&ts, &mut decoder)
//        .points(n)
//        .start(t1)
//        .stop(t2)
//        .per_sample(s)
//        .build_with_combiner(combiners::Differentiate::default())
//        .unwrap();
//    sampler.sample_all().unwrap();

//    let dt = (t2-t1).num_seconds()/(data.len() as i64);
//    let slope = slope / (dt as f32);
//    for v in sampler.values(){
//        assert_float_eq!(*v, slope, abs <= 0.000_05);
//    }
//}

////no good tests, for now just plot the results and check they are somewhat cosiney
////use cargo t -- --nocapture diff_sine
//#[test]
//fn diff_sine() {
//    if Path::new("test_combiner_diff_sine.h").exists() {
//        fs::remove_file("test_combiner_diff_sine.h").unwrap();
//    }
//    if Path::new("test_combiner_diff_sine.dat").exists() {
//        fs::remove_file("test_combiner_diff_sine.dat").unwrap();
//    }

//    let now = Utc::now();
//    let t1 = now - Duration::hours(2);
//    let t2 = now;
//    let n = 200;
//    let s = 10;

//    let mut ts = Series::open("test_combiner_diff_sine", 4).unwrap();
//    let data = sine_array(n, 5.0, 100.0);
//    insert_vector(&mut ts, t1, t2, &data);

//    let mut decoder = FloatDecoder{};
//    let mut sampler = new_sampler(&ts, &mut decoder)
//        .points(n)
//        .start(t1)
//        .stop(t2)
//        .per_sample(s)
//        .build_with_combiner(combiners::Differentiate::default())
//        .unwrap();
//    sampler.sample_all().unwrap();

//    let values = sampler.values();
//    assert_float_eq!(*values.first().unwrap(), *values.last().unwrap(), abs <= 0.001);
//    assert_float_eq!(*values.first().unwrap(), 0.0, abs <= 0.01);
//}
