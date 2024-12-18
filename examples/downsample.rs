/// Converts between an earlier version of byteseries
/// that erroneously allowed timestamps before the last
/// timestamp to be appended.
use std::env::args;
use std::fs;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};

use byteseries::{downsample, ByteSeries, Decoder, Encoder, ResampleState, Resampler};
use color_eyre::eyre::{Context, OptionExt, Result};
use color_eyre::Section;

#[derive(Debug)]
struct EmptyState;

impl ResampleState for EmptyState {
    type Item = ();
    fn add(&mut self, _: Self::Item) {}
    fn finish(&mut self, _: usize) -> Self::Item {}
}

#[derive(Debug, Clone, Default)]
struct CopyResampler {
    payload_size: usize,
}

impl Resampler for CopyResampler {
    type State = EmptyState;

    fn state(&self) -> Self::State {
        EmptyState
    }
}

impl Encoder for CopyResampler {
    type Item = ();

    fn encode_item(&mut self, _: &Self::Item) -> Vec<u8> {
        vec![0; self.payload_size]
    }
}

impl Decoder for CopyResampler {
    type Item = ();

    fn decode_payload(&mut self, payload: &[u8]) -> Self::Item {
        self.payload_size = payload.len();
        ()
    }
}

fn main() -> Result<()> {
    color_eyre::install().unwrap();
    let path = parse_args();

    remove_existing_caches(&path).wrap_err("Could not remove existing caches")?;
    let (_, _) = ByteSeries::builder()
        .retrieve_payload_size()
        .with_any_header()
        .with_downsampled_cache(
            CopyResampler::default(),
            vec![downsample::Config {
                max_gap: None,
                bucket_size: 10,
            }],
        )
        .open(&path)
        .wrap_err("Could not open backup input")?;

    std::fs::remove_file(path.with_extension("byteseries_index"))
        .wrap_err("Could not remove index")?;
    let res = std::fs::remove_file(&path);
    if res.as_ref().map_err(io::Error::kind) != Err(ErrorKind::NotFound) {
        res.wrap_err("Could not remove file taking up the place of the output")?;
    }

    Ok(())
}

fn remove_existing_caches(path: &Path) -> Result<()> {
    let byteseries_name = path
        .file_stem()
        .ok_or_eyre("file stem is missing")?
        .to_str()
        .ok_or_eyre("non utf8 file names not supported")?;
    let same_series = |e: &fs::DirEntry| {
        e.path().file_stem().is_some_and(|stem| {
            stem.to_str()
                .is_some_and(|stem| stem.starts_with(byteseries_name))
        })
    };
    let is_byteseries_file = |e: &fs::DirEntry| {
        e.path()
            .extension()
            .is_some_and(|ext| ext == "byteseries_index" || ext == "byteseries")
    };

    let dir = path.parent().unwrap();
    for file in fs::read_dir(dir)?
        .filter_map(Result::ok)
        .filter(is_byteseries_file)
        .filter(same_series)
        .filter(has_two_downsample_params)
    {
        fs::remove_file(file.path())
            .wrap_err("Could not remove file")
            .with_note(|| format!("path: {}", file.file_name().to_string_lossy()))?;
    }

    Ok(())
}

fn has_two_downsample_params(file: &fs::DirEntry) -> bool {
    let file_name = file.file_name();
    let file_name = file_name.to_string_lossy();

    // file name should contain two downsample parameters
    let Some(param_start) = file_name.find('_') else {
        return false;
    };
    let Some(len1) = file_name[1 + param_start..].find('_') else {
        return false;
    };
    let Some(len2) = file_name[2 + param_start + len1..].find('.') else {
        return false;
    };

    let first = &file_name[param_start + 1..param_start + len1 + 1];
    let second = &file_name[param_start + len1 + 2..param_start + len1 + len2 + 2];

    let param_is_digits = |param: &str| param.chars().all(|c| c.is_ascii_digit());
    let param_is_none = |param: &str| param == "None";
    let is_valid_param = |param| param_is_digits(param) || param_is_none(param);
    is_valid_param(first) && is_valid_param(second)
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
