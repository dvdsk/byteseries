use std::env::args;
use std::path::PathBuf;

use byteseries::{ByteSeries, Decoder};
use color_eyre::eyre::{Context, Result};
use pretty_assertions::assert_eq;

#[derive(Debug)]
struct EmptyDecoder;
impl Decoder for EmptyDecoder {
    type Item = ();

    fn decode_payload(&mut self, _: &[u8]) -> Self::Item {}
}

fn main() -> Result<()> {
    color_eyre::install().unwrap();
    let path = parse_args();

    // let (input_series, _) = ByteSeries::builder()
    //     .retrieve_payload_size()
    //     .with_any_header()
    //     .open(&path)
    //     .wrap_err("Could not open backup input")?;
    // let ts1 = read_in_chunks(input_series)?;
    // validate_ts(&ts1);
    // eprintln!("read in chuncks timestamps validated");
    // assert!(ts1.contains(&1730173323));

    let (input_series, _) = ByteSeries::builder()
        .retrieve_payload_size()
        .with_any_header()
        .open(&path)
        .wrap_err("Could not open backup input")?;
    let ts2 = read_all(input_series)?;
    assert_eq!(ts2.len(), ts2.len());
    validate_ts(&ts2);
    eprintln!("read all timestamps validated");

    // assert_eq!(ts1, ts2);

    Ok(())
}

fn read_in_chunks(mut input_series: ByteSeries) -> Result<Vec<u64>> {
    let mut timestamps = Vec::new();
    let mut data = Vec::new();
    let mut read_start = *input_series.range().unwrap().start();

    loop {
        if let Err(byteseries::series::Error::InvalidRange(
            byteseries::seek::Error::StartAfterData { .. },
        )) = input_series.read_first_n(
            100_000,
            &mut EmptyDecoder,
            read_start..,
            &mut timestamps,
            &mut data,
        ) {
            return Ok(timestamps);
        }

        let Some(last_ts) = timestamps.last() else {
            return Ok(timestamps);
        };
        read_start = *last_ts + 1;
    }
}

fn read_all(mut input_series: ByteSeries) -> Result<Vec<u64>> {
    let mut timestamps = Vec::new();
    let mut data = Vec::new();
    input_series
        .read_all(.., &mut EmptyDecoder, &mut timestamps, &mut data)
        .wrap_err("Could not read all data")?;
    Ok(timestamps)
}

fn validate_ts(timestamps: &[u64]) {
    let mut prev = 0;
    assert!(
        timestamps.first().is_some_and(|ts| *ts > prev),
        "only works with data starting at timestamp > 0"
    );
    for ts in timestamps.iter() {
        assert!(*ts > prev, "ts: {ts}, prev: {prev}");
        prev = *ts;
    }
}

fn parse_args() -> PathBuf {
    let mut args = args().skip(1);
    let path: PathBuf = args
        .next()
        .expect("needs one argument: the path to the byteseries")
        .into();

    assert!(
        path.with_extension("byteseries").exists(),
        "Path must exist"
    );
    path
}
