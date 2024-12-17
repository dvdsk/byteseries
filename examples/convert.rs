use std::env::args;
use std::path::PathBuf;

use byteseries::series::Error;
use byteseries::{ByteSeries, Decoder};
use indicatif::{ProgressBar, ProgressStyle};

#[derive(Debug)]
struct CopyDecoder;

impl Decoder for CopyDecoder {
    type Item = Vec<u8>;

    fn decode_payload(&mut self, payload: &[u8]) -> Self::Item {
        payload.to_vec()
    }
}

fn main() {
    color_eyre::install().unwrap();
    let path = parse_args();

    let backup_path = make_backup(&path);
    let (input_series, header) = ByteSeries::builder()
        .retrieve_payload_size()
        .with_any_header()
        .open(backup_path)
        .expect("Open should work");

    std::fs::remove_file(path.with_extension("byteseries_index"))
        .expect("should be able to remove index");
    let (output_series, _) = ByteSeries::builder()
        .payload_size(input_series.payload_size())
        .create_new(true)
        .with_header(header)
        .open(path)
        .expect("Open should work");

    let read_start = *input_series
        .range()
        .expect("series must not be empty")
        .start();

    let report = copy_over_content(input_series, read_start, output_series);
    println!("copy report: {report:?}")
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
) -> Report {
    let mut report = Report::default();

    let bar = ProgressBar::new(input_series.len());
    bar.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg} [{eta}]",
        )
        .unwrap()
        .progress_chars("##-"),
    );

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
            break report;
        }

        let Some(last_ts) = timestamps.last() else {
            bar.finish();
            break report; // all data consumed
        };

        if read_start == *last_ts {
            report.same_time += 1;
            bar.finish();
            break report;
        }

        read_start = *last_ts + 1;
        for (ts, line) in timestamps.into_iter().zip(data.into_iter()) {
            bar.inc(1);
            match output_series.push_line(ts, line) {
                Ok(_) => (),
                Err(Error::TimeNotAfterLast { prev, new }) if new == prev => {
                    report.same_time += 1;
                }
                Err(Error::TimeNotAfterLast { .. }) => {
                    report.earlier_time += 1;
                }
                Err(other) => panic!("No error should happen during copy, got: {other}"),
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
        path.extension().is_none(),
        "Give the path without the .byteseries extension"
    );
    assert!(
        path.with_extension("byteseries").exists(),
        "Path must exist"
    );
    path
}

fn make_backup(path: &PathBuf) -> PathBuf {
    let backup = path.with_file_name(
        path.file_name()
            .expect("file should have name")
            .to_string_lossy()
            .to_string()
            + "_backup",
    );
    if backup.exists()
        && backup.metadata().expect("file exists").len()
            >= path.metadata().expect("verified exist in parse args").len()
    {
        eprintln!("Backup seems to already exist, not overwriting");
        return backup;
    }

    std::fs::rename(
        path.with_extension("byteseries"),
        backup.with_extension("byteseries"),
    )
    .expect("Copy should succeed");
    backup
}
