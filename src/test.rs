#![cfg(test)]

use crate::{BoundResult, Selector, ByteSeries};
use byteorder::ByteOrder;
use byteorder::NativeEndian;
use byteorder::WriteBytesExt;
use chrono::{DateTime, NaiveDateTime, Utc};
use fern::colors::{Color, ColoredLevelConfig};
use std::io::ErrorKind;

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

fn insert_uniform_arrays(
    data: &mut ByteSeries,
    n_to_insert: u32,
    _step: i64,
    line_size: usize,
    time: DateTime<Utc>,
) {
    let mut timestamp = time.timestamp();
    for i in 0..n_to_insert {
        let buffer = vec![i as u8; line_size];

        let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
        data.append(dt, buffer.as_slice()).unwrap();
        timestamp += 5;
    }
}

mod appending {
    use super::*;
    use fxhash::hash64;
    use std::fs;
    use std::path::Path;

    fn insert_timestamp_hashes(
        data: &mut ByteSeries,
        n_to_insert: u32,
        step: i64,
        time: DateTime<Utc>,
    ) {
        let mut timestamp = time.timestamp();

        for _ in 0..n_to_insert {
            let hash = hash64::<i64>(&timestamp);

            let mut buffer = Vec::with_capacity(8);
            &buffer.write_u64::<NativeEndian>(hash).unwrap();

            let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
            data.append(dt, buffer.as_slice()).unwrap();
            timestamp += step;
        }
    }

    fn insert_timestamp_arrays(
        data: &mut ByteSeries,
        n_to_insert: u32,
        step: i64,
        time: DateTime<Utc>,
    ) {
        let mut timestamp = time.timestamp();

        for _ in 0..n_to_insert {
            let mut buffer = Vec::with_capacity(8);

            let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
            &buffer.write_i64::<NativeEndian>(timestamp).unwrap();

            data.append(dt, buffer.as_slice()).unwrap();
            timestamp += step;
        }
    }

    #[test]
    fn basic() {
        if Path::new("test_append.h").exists() {
            fs::remove_file("test_append.h").unwrap();
        }
        if Path::new("test_append.dat").exists() {
            fs::remove_file("test_append.dat").unwrap();
        }
        const LINE_SIZE: usize = 10;
        const STEP: i64 = 5;
        const N_TO_INSERT: u32 = 100;

        let time = Utc::now();

        let mut data = ByteSeries::open("test_append", LINE_SIZE).unwrap();
        insert_uniform_arrays(&mut data, N_TO_INSERT, STEP, LINE_SIZE, time);

        assert_eq!(
            data.data.metadata().unwrap().len(),
            ((LINE_SIZE + 2) as u32 * N_TO_INSERT) as u64
        );
        assert_eq!(data.header.file.metadata().unwrap().len(), 16);
    }

    #[test]
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
        //println!("now: {}, u8_ts: {}", time, timestamp as u8 );

        let mut data = ByteSeries::open("test_set_read", LINE_SIZE).unwrap();
        insert_uniform_arrays(&mut data, N_TO_INSERT, STEP, LINE_SIZE, time);
        //println!("data length: {}",data.data.metadata().unwrap().len());

        let t1 =
            DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp + 2 * STEP, 0), Utc);

        let bound_result = data.get_bounds(t1, Utc::now());
        match bound_result {
            BoundResult::IoError(_err) => panic!(),
            BoundResult::NoData => panic!(),
            BoundResult::Ok((start_byte, _stop_byte, _decode_params)) => {
                assert_eq!(start_byte, ((data.line_size + 2) * 2) as u64);
            }
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

        let mut data = ByteSeries::open("test_append_hashes_then_verify", 8).unwrap();
        insert_timestamp_hashes(&mut data, NUMBER_TO_INSERT as u32, PERIOD, time);
        //println!("inserted test data");

        let timestamp = time.timestamp();
        let t1 = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
        let t2 = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(timestamp + NUMBER_TO_INSERT * PERIOD, 0),
            Utc,
        );

        if let BoundResult::Ok((mut start_byte, stop_byte, mut decode_params)) =
            data.get_bounds(t1, t2)
        {
            println!("stop: {}, start: {}", stop_byte, start_byte);
            let lines_in_range = (stop_byte - start_byte) / ((data.line_size + 2) as u64) + 1;
            assert_eq!(lines_in_range, (NUMBER_TO_INSERT) as u64);

            let n = 8_000;
            let loops_to_check_everything =
                NUMBER_TO_INSERT / n + if NUMBER_TO_INSERT % n > 0 { 1 } else { 0 };

            for _ in 0..loops_to_check_everything {
                //println!("loop, {} at the time",n);
                let (timestamps, decoded) = data
                    .decode_time(n as usize, &mut start_byte, stop_byte, &mut decode_params)
                    .unwrap();
                //println!("timestamps: {:?}", timestamps);
                for (timestamp, decoded) in timestamps.iter().zip(decoded.chunks(data.line_size)) {
                    let hash1 = hash64::<i64>(&(*timestamp as i64));
                    let hash2 = NativeEndian::read_u64(decoded);
                    assert_eq!(hash1, hash2);
                }
            }
        } else {
            panic!();
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

        let mut data = ByteSeries::open("test_read_skipping_then_verify", 8).unwrap();
        insert_timestamp_hashes(&mut data, NUMBER_TO_INSERT as u32, PERIOD, time);
        //println!("inserted test data");

        let timestamp = time.timestamp();
        let t1 = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
        let t2 = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(timestamp + NUMBER_TO_INSERT * PERIOD, 0),
            Utc,
        );

        if let BoundResult::Ok((mut start_byte, stop_byte, mut decode_params)) =
            data.get_bounds(t1, t2)
        {
            println!("stop: {}, start: {}", stop_byte, start_byte);
            let lines_in_range = (stop_byte - start_byte) / ((data.line_size + 2) as u64);
            assert_eq!(lines_in_range, (NUMBER_TO_INSERT - 1) as u64);

            let n = 100;
            let loops_to_check_everything =
                (lines_in_range - 6) / n + if (lines_in_range - 6) % n > 0 { 1 } else { 0 };

            let numb_lines: u64 = (stop_byte - start_byte) / data.full_line_size as u64;
            if let Some(mut idx_checker) = Selector::new(100, numb_lines, &data) {
                dbg!(&idx_checker);
                dbg!(loops_to_check_everything);
                let mut timestamps = Vec::new();
                let mut line_data = Vec::new();
                for _ in 0..loops_to_check_everything {
                    data.decode_time_into_given_skipping(
                        &mut timestamps,
                        &mut line_data,
                        n as usize,
                        &mut start_byte,
                        stop_byte,
                        &mut decode_params,
                        &mut idx_checker,
                    )
                    .unwrap();

                    assert_eq!(timestamps.len(), 100);
                    for (timestamp, decoded) in
                        timestamps.iter().zip(line_data.chunks(data.line_size))
                    {
                        let hash1 = hash64::<i64>(&(*timestamp as i64));
                        let hash2 = NativeEndian::read_u64(decoded);
                        assert_eq!(hash1, hash2);
                    }
                }
            } else {
                panic!(); //With data len 1007 we should never arrive here
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn timestamps_then_verify() {
        const NUMBER_TO_INSERT: i64 = 10_000;
        const PERIOD: i64 = 24 * 3600 / NUMBER_TO_INSERT;

        //setup_debug_logging(2).unwrap();

        if Path::new("test_append_timestamps_then_verify.h").exists() {
            fs::remove_file("test_append_timestamps_then_verify.h").unwrap();
        }
        if Path::new("test_append_timestamps_then_verify.dat").exists() {
            fs::remove_file("test_append_timestamps_then_verify.dat").unwrap();
        }

        let time = Utc::now();

        let mut data = ByteSeries::open("test_append_timestamps_then_verify", 8).unwrap();
        insert_timestamp_arrays(&mut data, NUMBER_TO_INSERT as u32, PERIOD, time);
        //println!("inserted test data");

        let timestamp = time.timestamp();
        let t1 = time;
        let t2 = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(timestamp + NUMBER_TO_INSERT * PERIOD, 0),
            Utc,
        );

        if let BoundResult::Ok((mut start_byte, stop_byte, mut decode_params)) =
            data.get_bounds(t1, t2)
        {
            let lines_in_range = (stop_byte - start_byte) / ((data.line_size + 2) as u64) + 1;
            assert_eq!(lines_in_range, (NUMBER_TO_INSERT) as u64);

            let n = 8_000;
            let loops_to_check_everything =
                NUMBER_TO_INSERT / n + if NUMBER_TO_INSERT % n > 0 { 1 } else { 0 };
            for _ in 0..loops_to_check_everything {
                let (timestamps, decoded) = data
                    .decode_time(n as usize, &mut start_byte, stop_byte, &mut decode_params)
                    .unwrap();
                for (i, (timestamp, decoded)) in timestamps
                    .iter()
                    .zip(decoded.chunks(data.line_size))
                    .enumerate()
                {
                    let ts_from_decode = *timestamp as i64;
                    let ts_from_data = NativeEndian::read_i64(decoded);

                    //println!("ts_from_decode: {}, ts_from_data: {}",ts_from_decode,ts_from_data);
                    assert_eq!(
                        ts_from_decode,
                        ts_from_data,
                        "failed on element: {}, which should have ts: {}, but has been given {},
                prev element has ts: {}, the step is: {}",
                        i,
                        ts_from_data,
                        ts_from_decode,
                        timestamps[i - 1],
                        PERIOD
                    );
                }
            }
        }
        assert_eq!(2 + 2, 4);
    }

    /*#[test]
    fn multiple_hashes_then_verify() {
        const NUMBER_TO_INSERT: i64 = 8_00;
        const PERIOD: i64 = 5;

        if Path::new("test_multiple_append_hashes_then_verify.h").exists()    {fs::remove_file("test_multiple_append_hashes_then_verify.h").unwrap();   }
        if Path::new("test_multiple_append_hashes_then_verify.data").exists() {fs::remove_file("test_multiple_append_hashes_then_verify.data").unwrap();}
        let time = Utc::now();

        let mut data = Timeseries::open("test_multiple_append_hashes_then_verify", 8).unwrap();
        insert_timestamp_arrays(&mut data, NUMBER_TO_INSERT as u32 /3 , PERIOD, time);
        let last_time_in_data = data.last_time_in_data;
        insert_timestamp_arrays(&mut data, NUMBER_TO_INSERT as u32 /3 , PERIOD, last_time_in_data);
        //println!("inserted test data");

        let timestamp = time.timestamp();
        let t1 = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
        let t2 = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp + (2*NUMBER_TO_INSERT/3-20)*PERIOD, 0), Utc);

        data.set_read_start(t1).unwrap();
        data.set_read_stop( t2);
        assert_eq!((data.stop_byte-data.start_byte)/((data.line_size+2) as u64), (2*NUMBER_TO_INSERT/3) as u64);

        let n = 8_000;
        let loops_to_check_everything = 2*NUMBER_TO_INSERT/3/n + if 2*NUMBER_TO_INSERT/3 % n > 0 {1} else {0};
        for _ in 0..loops_to_check_everything {
            //println!("loop, {} at the time",n);
            let (timestamps, decoded) = data.decode_time(n as usize).unwrap();
            //println!("timestamps: {:?}", timestamps);
            for (timestamp, decoded) in timestamps.iter().zip(decoded.chunks(data.line_size)){
                let ts_from_decode = *timestamp as i64;
                let ts_from_data = NativeEndian::read_i64(decoded);

                //println!("ts_from_decode: {}, ts_from_data: {}",ts_from_decode,ts_from_data);
                assert_eq!(ts_from_decode, ts_from_data);
            }
        }

        let last_time_in_data = data.last_time_in_data;
        insert_timestamp_arrays(&mut data, NUMBER_TO_INSERT as u32 /3, PERIOD, last_time_in_data);

        data.set_read_start(t2).unwrap();
        let t3 = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp + (3*NUMBER_TO_INSERT)*PERIOD+1000, 0), Utc);
        data.set_read_stop( t3);
        assert_eq!((data.stop_byte-data.start_byte)/((data.line_size+2) as u64), (NUMBER_TO_INSERT) as u64);

        let n = 8_000;
        let loops_to_check_everything = NUMBER_TO_INSERT/3/n + if NUMBER_TO_INSERT/3 % n > 0 {1} else {0};
        for _ in 0..loops_to_check_everything {
            //println!("loop, {} at the time",n);
            let (timestamps, decoded) = data.decode_time(n as usize).unwrap();
            //println!("timestamps: {:?}", timestamps);
            for (timestamp, decoded) in timestamps.iter().zip(decoded.chunks(data.line_size)){
                let ts_from_decode = *timestamp as i64;
                let ts_from_data = NativeEndian::read_i64(decoded);

                //println!("ts_from_decode: {}, ts_from_data: {}",ts_from_decode,ts_from_data);
                assert_eq!(ts_from_decode, ts_from_data);
            }
        }
    }*/
}

mod seek {
    use super::*;
    use std::fs;
    use std::path::Path;

    #[test]
    fn varing_length() {
        if Path::new("test_varing_length.h").exists() {
            fs::remove_file("test_varing_length.h").unwrap();
        }
        if Path::new("test_varing_length.dat").exists() {
            fs::remove_file("test_varing_length.dat").unwrap();
        }
        const LINE_SIZE: usize = 8;
        const STEP: i64 = 5;
        const N_TO_INSERT: u32 = 100;
        let start_read_inlines = 10;
        let read_length_inlines = 10;

        //let time = Utc::now();
        let time = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(1539180000, 0), Utc);
        let timestamp = time.timestamp();
        println!("start timestamp {}", timestamp);
        let mut data = ByteSeries::open("test_varing_length", LINE_SIZE).unwrap();

        insert_uniform_arrays(&mut data, N_TO_INSERT, STEP, LINE_SIZE, time);

        //TODO for loop this over random sizes
        let t1 = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(timestamp + start_read_inlines * STEP, 0),
            Utc,
        );
        let t2 = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(
                timestamp + (start_read_inlines + read_length_inlines) * STEP,
                0,
            ),
            Utc,
        );

        if let BoundResult::Ok((mut start_byte, stop_byte, mut decode_params)) =
            data.get_bounds(t1, t2)
        {
            println!(
                "t1: {}, t2: {}, start_byte: {}, stop_byte: {}",
                t1.timestamp(),
                t2.timestamp(),
                start_byte,
                stop_byte
            );

            assert_eq!(
                start_byte,
                (start_read_inlines * (LINE_SIZE as i64 + 2)) as u64
            );
            assert_eq!(
                (stop_byte - start_byte) / ((LINE_SIZE as u64 + 2) as u64),
                read_length_inlines as u64
            );

            //let (timestamps, decoded) = data
            //.decode_time(read_length_inlines as usize)
            //.unwrap();
            //println!("timestamps: {:?}", timestamps);

            let (timestamps, _decoded) = data
                .decode_time(10 as usize, &mut start_byte, stop_byte, &mut decode_params)
                .unwrap();
            assert!(
                timestamps[0] as i64 >= timestamp + 10 * STEP
                    && timestamps[0] as i64 <= timestamp + 20 * STEP
            );
        }
    }

    #[test]
    fn beyond_range() {
        if Path::new("test_beyond_range.h").exists() {
            fs::remove_file("test_beyond_range.h").unwrap();
        }
        if Path::new("test_beyond_range.dat").exists() {
            fs::remove_file("test_beyond_range.dat").unwrap();
        }
        const LINE_SIZE: usize = 8;
        const STEP: i64 = 5;
        const N_TO_INSERT: u32 = 100;
        let start_read_inlines = 101;
        let read_length_inlines = 10;

        //let time = Utc::now();
        let time = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(1539180000, 0), Utc);
        let timestamp = time.timestamp();
        println!("start timestamp {}", timestamp);
        let mut data = ByteSeries::open("test_beyond_range", LINE_SIZE).unwrap();

        insert_uniform_arrays(&mut data, N_TO_INSERT, STEP, LINE_SIZE, time);

        let t1 = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(timestamp + start_read_inlines * STEP, 0),
            Utc,
        );
        let t2 = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(
                timestamp + (start_read_inlines + read_length_inlines) * STEP,
                0,
            ),
            Utc,
        );

        if let BoundResult::IoError(error) = data.get_bounds(t1, t2) {
            assert_eq!(error.kind(), ErrorKind::NotFound);
        } else {
            panic!();
        }
    }
}
