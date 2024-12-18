/// Converts between an earlier version of byteseries
/// that erroneously allowed timestamps before the last
/// timestamp to be appended.
use std::env::args;
use std::io::{self, ErrorKind};
use std::path::PathBuf;

use byteseries::series::Error;
use byteseries::{ByteSeries, Decoder};
use color_eyre::eyre::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};

#[derive(Debug)]
struct CopyDecoder;

impl Decoder for CopyDecoder {
    type Item = Vec<u8>;

    fn decode_payload(&mut self, payload: &[u8]) -> Self::Item {
        payload.to_vec()
    }
}

fn main() -> Result<()> {
    color_eyre::install().unwrap();
    let path = parse_args();

    let backup_path = make_backup(&path)?;
    let (input_series, header) = ByteSeries::builder()
        .retrieve_payload_size()
        .with_any_header()
        .open(&backup_path)
        .wrap_err("Could not open backup input")?;

    std::fs::remove_file(path.with_extension("byteseries_index"))
        .wrap_err("Could not remove index")?;
    let res = std::fs::remove_file(&path);
    if res.as_ref().map_err(io::Error::kind) != Err(ErrorKind::NotFound) {
        res.wrap_err("Could not remove file taking up the place of the output")?;
    }

    let (output_series, _) = ByteSeries::builder()
        .payload_size(input_series.payload_size())
        .create_new(true)
        .with_header(header)
        .open(path)
        .wrap_err("Could not create new output series")?;

    let Some(read_start) = input_series.range().map(|range| *range.start()) else {
        println!("Input series is empty, replaced with fresh empty series");
        return Ok(());
    };

    let report = copy_over_content(input_series, read_start, output_series);
    println!("copy report: {report:?}");
    Ok(())
}

#[derive(Debug, Default)]
struct Report {
    same_time: usize,
    earlier_time: usize,
}

fn copy_over_content(
    mut input_series: ByteSeries,
    mut read_start: u64,
    mut output_series: ByteSeries,
) -> Result<Report> {
    let mut report = Report::default();

    let bar = ProgressBar::new(input_series.len());
    bar.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg} [{eta}]",
        )
        .unwrap()
        .progress_chars("##-"),
    );

    let mut largest_ts = 0;
    loop {
        let mut timestamps = Vec::new();
        let mut data = Vec::new();

        if let Err(Error::InvalidRange(byteseries::seek::Error::StartAfterData {
            ..
        })) = input_series.read_first_n(
            100_000,
            &mut CopyDecoder,
            read_start..,
            &mut timestamps,
            &mut data,
        ) {
            bar.finish();
            break Ok(report);
        }

        let Some(last_ts) = timestamps.last() else {
            break Ok(report); // all data consumed
        };

        read_start = *last_ts + 1;
        for (ts, line) in timestamps.into_iter().zip(data.into_iter()) {
            bar.inc(1);
            let res = output_series.push_line(ts, line);
            match res {
                Ok(_) => {
                    assert!(
                        ts > largest_ts,
                        "timeseries must be monotonically increasing"
                    );
                    largest_ts = ts;
                }
                Err(Error::TimeNotAfterLast { prev, new }) if new == prev => {
                    report.same_time += 1;
                }
                Err(Error::TimeNotAfterLast { .. }) => {
                    report.earlier_time += 1;
                }
                Err(_) => res.wrap_err("Could not push line to output")?,
            }
        }
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

fn make_backup(path: &PathBuf) -> Result<PathBuf> {
    let backup = path.with_file_name(
        path.file_name()
            .expect("file should have name")
            .to_string_lossy()
            .to_string()
            + "_backup",
    );
    if backup.exists()
        && backup
            .metadata()
            .wrap_err("could not check metadata for existing backup")?
            .len()
            >= path
                .metadata()
                .wrap_err("could not check metadata for input")?
                .len()
    {
        eprintln!("Backup seems to already exist, not overwriting");
        return Ok(backup);
    }

    std::fs::rename(
        path.with_extension("byteseries"),
        backup.with_extension("byteseries"),
    )
    .wrap_err("could not turn origional input into backup")?;
    Ok(backup)
}
