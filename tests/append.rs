#![cfg(test)]

use byteorder::{ByteOrder, NativeEndian};
use byteseries::{Decoder, SamplerBuilder, Series, EmptyCombiner, MeanCombiner};
use chrono::{DateTime, NaiveDateTime, Utc};
use fern::colors::{Color, ColoredLevelConfig};
use fxhash::hash64;
use std::fs;
use std::path::Path;

mod shared;
use shared::{insert_timestamp_arrays, insert_timestamp_hashes, insert_uniform_arrays};

#[allow(dead_code)]
fn setup_debug_logging(verbosity: u8) -> Result<(), fern::InitError> {
    let mut base_config = fern::Dispatch::new();
    let colors = ColoredLevelConfig::new()
        .info(Color::Green)
        .debug(Color::Yellow)
        .warn(Color::Magenta);

    base_config = match verbosity {
        0 =>
        // Let's say we depend on something which whose "info" level messages are too
        // verbose to include in end-user output. If we don't need them,
        // let's not include them.
        {
            base_config.level(log::LevelFilter::Info)
        }
        1 => base_config.level(log::LevelFilter::Debug),
        2 => base_config.level(log::LevelFilter::Trace),
        _3_or_more => base_config.level(log::LevelFilter::Trace),
    };

    let stdout_config = fern::Dispatch::new()
        .format(move |out, message, record| {
            out.finish(format_args!(
                "[{}][{}][{}] {}",
                chrono::Local::now().format("%H:%M"),
                record.target(),
                colors.color(record.level()),
                message
            ))
        })
        .chain(std::io::stdout());

    base_config.chain(stdout_config).apply()?;
    Ok(())
}

// #[test]
// fn basic() {
//     if Path::new("test_append.h").exists() {
//         fs::remove_file("test_append.h").unwrap();
//     }
//     if Path::new("test_append.dat").exists() {
//         fs::remove_file("test_append.dat").unwrap();
//     }
//     const LINE_SIZE: usize = 10;
//     const STEP: i64 = 5;
//     const N_TO_INSERT: u32 = 100;

//     let time = Utc::now();

//     let mut data = Series::open("test_append", LINE_SIZE).unwrap();
//     insert_uniform_arrays(&mut data, N_TO_INSERT, STEP, LINE_SIZE, time);

//     assert_eq!(
//         fs::metadata("test_append.dat").unwrap().len(),
//         ((LINE_SIZE + 2) as u32 * N_TO_INSERT) as u64
//     );
//     assert_eq!(fs::metadata("test_append.h").unwrap().len(), 16);
// }

/*#[test]
fn test_set_read() {
    if Path::new("test_set_read.h").exists() {
        fs::remove_file("test_set_read.h").unwrap();
    }
    if Path::new("test_set_read.dat").exists() {
        fs::remove_file("test_set_read.dat").unwrap();
    }
    const LINE_SIZE: usize = 10;
    const STEP: i64 = 5;
    const N_TO_INSERT: u32 = 100;

    let time = Utc::now();
    let timestamp = time.timestamp();

    let mut data = Series::open("test_set_read", LINE_SIZE).unwrap();
    insert_uniform_arrays(&mut data, N_TO_INSERT, STEP, LINE_SIZE, time);

    let t1 =
        DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp + 2 * STEP, 0), Utc);

    let mut decoder = EmptyDecoder{};
    let mut sampler = SamplerBuilder::new(&data, &mut decoder)
        .points(10)
        .start(t1)
        .stop(Utc::now())
        .finish().unwrap();

    sampler.sample(10);

    let bound_result = data.get_bounds(t1, Utc::now());
    match bound_result {
        BoundResult::IoError(_err) => panic!(),
        BoundResult::NoData => panic!(),
        BoundResult::Ok((start_byte, _stop_byte, _decode_params)) => {
            assert_eq!(start_byte, ((data.line_size + 2) * 2) as u64);
        }
    }
}*/

#[derive(Debug)]
struct HashDecoder {}

impl Decoder<u64> for HashDecoder {
    fn decode(&mut self, bytes: &[u8], out: &mut Vec<u64>) {
        let hash = NativeEndian::read_u64(bytes);
        out.push(hash);
    }
}

#[test]
fn hashes_then_verify() {
    const NUMBER_TO_INSERT: i64 = 1_000;
    const PERIOD: i64 = 24 * 3600 / NUMBER_TO_INSERT;

    if Path::new("test_append_hashes_then_verify.h").exists() {
        fs::remove_file("test_append_hashes_then_verify.h").unwrap();
    }
    if Path::new("test_append_hashes_then_verify.dat").exists() {
        fs::remove_file("test_append_hashes_then_verify.dat").unwrap();
    }

    let time = Utc::now();
    let mut data = Series::open("test_append_hashes_then_verify", 8).unwrap();
    insert_timestamp_hashes(&mut data, NUMBER_TO_INSERT as u32, PERIOD, time);

    let timestamp = time.timestamp();
    let t1 = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
    let t2 = DateTime::<Utc>::from_utc(
        NaiveDateTime::from_timestamp(timestamp + NUMBER_TO_INSERT * PERIOD, 0),
        Utc,
    );

    let n = 8_000;
    let mut decoder = HashDecoder {};
    let mut sampler = SamplerBuilder::new(&data, &mut decoder)
        .points(n)
        .start(t1)
        .stop(t2)
        .finish::<EmptyCombiner<_>>()
        .unwrap();

    sampler.sample(n);

    for (timestamp, hash) in sampler.into_iter() {
        let correct = hash64::<i64>(&(timestamp as i64));
        assert_eq!(hash, correct);
    }
}

#[test]
fn hashes_read_skipping_then_verify() {
    const NUMBER_TO_INSERT: i64 = 1_007;
    const PERIOD: i64 = 24 * 3600 / NUMBER_TO_INSERT;

    if Path::new("test_read_skipping_then_verify.h").exists() {
        fs::remove_file("test_read_skipping_then_verify.h").unwrap();
    }
    if Path::new("test_read_skipping_then_verify.dat").exists() {
        fs::remove_file("test_read_skipping_then_verify.dat").unwrap();
    }

    let time = Utc::now();

    let mut data = Series::open("test_read_skipping_then_verify", 8).unwrap();
    insert_timestamp_hashes(&mut data, NUMBER_TO_INSERT as u32, PERIOD, time);

    let timestamp = time.timestamp();
    let t1 = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
    let t2 = DateTime::<Utc>::from_utc(
        NaiveDateTime::from_timestamp(timestamp + NUMBER_TO_INSERT * PERIOD, 0),
        Utc,
    );

    let n = 100;
    let mut decoder = HashDecoder {};
    let mut sampler = SamplerBuilder::new(&data, &mut decoder)
        .points(n)
        .start(t1)
        .stop(t2)
        .finish::<EmptyCombiner<_>>()
        .unwrap();
    sampler.sample(n).unwrap();

    assert_eq!(sampler.values().len(), n);
    for (timestamp, hash) in sampler.into_iter() {
        let correct = hash64::<i64>(&(timestamp as i64));
        assert_eq!(hash, correct);
    }
}

#[derive(Debug)]
struct TimestampDecoder {}

impl Decoder<i64> for TimestampDecoder {
    fn decode(&mut self, bytes: &[u8], out: &mut Vec<i64>) {
        let ts = NativeEndian::read_i64(bytes);
        out.push(ts);
    }
}

//#[test]
//fn timestamps_then_verify() {
//    const NUMBER_TO_INSERT: i64 = 10_000;
//    const PERIOD: i64 = 24 * 3600 / NUMBER_TO_INSERT;

//    //setup_debug_logging(2).unwrap();

//    if Path::new("test_append_timestamps_then_verify.h").exists() {
//        fs::remove_file("test_append_timestamps_then_verify.h").unwrap();
//    }
//    if Path::new("test_append_timestamps_then_verify.dat").exists() {
//        fs::remove_file("test_append_timestamps_then_verify.dat").unwrap();
//    }

//    let time = Utc::now();

//    let mut data = Series::open("test_append_timestamps_then_verify", 8).unwrap();
//    insert_timestamp_arrays(&mut data, NUMBER_TO_INSERT as u32, PERIOD, time);
//    //println!("inserted test data");

//    let timestamp = time.timestamp();
//    let t1 = time;
//    let t2 = DateTime::<Utc>::from_utc(
//        NaiveDateTime::from_timestamp(timestamp + NUMBER_TO_INSERT * PERIOD, 0),
//        Utc,
//    );

//    let n = 8_000;
//    let mut decoder = TimestampDecoder {};
//    let mut sampler = SamplerBuilder::new(&data, &mut decoder)
//        .points(n)
//        .start(t1)
//        .stop(t2)
//        .finish()
//        .unwrap();

//    sampler.sample(n);

//    assert_eq!(sampler.values().len(), n);
//    let mut prev = None;
//    for (i, (timestamp, decoded)) in sampler.into_iter().enumerate() {
//        let correct = timestamp as i64;
//        assert_eq!(
//            decoded, correct,
//            "failed on element: {}, which should have ts: {}, but has been given {},
//            prev element has ts: {:?}, the step is: {}",
//            i, timestamp, decoded, prev, PERIOD
//        );
//        prev = Some(timestamp);
//    }
//}
