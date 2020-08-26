#![cfg(test)]

use byteseries::{Series, TimeSeek, Decoder, EmptyDecoder, SamplerBuilder};

use byteorder::ByteOrder;
use byteorder::NativeEndian;
use byteorder::WriteBytesExt;
use chrono::{DateTime, NaiveDateTime, Utc};
use fern::colors::{Color, ColoredLevelConfig};
use std::io::ErrorKind;
use std::path::Path;
use std::fs;


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
    let mut data = Series::open("test_varing_length", LINE_SIZE).unwrap();

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

