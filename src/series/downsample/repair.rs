use std::ops::Bound;

use tracing::{instrument, warn};

use super::data::Data;
use super::Config;
use crate::seek::{RoughSeekPos, SeekError};
use crate::series::data;
use crate::Resampler;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Could not seek in source")]
    SeekingSource(#[from] SeekError),
    #[error("Could not empty (clear) downsampled data")]
    ClearingDownsampled(std::io::Error),
    #[error("Could not read from source")]
    ReadingSource(data::ReadError),
    #[error("Could not add new items to downsampled data")]
    AppendingToDownsampled(data::PushError),
}

#[instrument]
pub(super) fn repair_missing_data(
    source: &mut Data,
    downsampled: &mut Data,
    config: &Config,
    resampler: &mut impl Resampler,
) -> Result<(), Error> {
    let start_bound = match downsampled.last_time().unwrap() {
        Some(ts) => Bound::Excluded(ts),
        None => Bound::Unbounded,
    };
    let Some(seek) = RoughSeekPos::new(source, start_bound, Bound::Unbounded)
        .map(|p| p.refine(source))
        .transpose()?
    else {
        if !downsampled.is_empty() {
            warn!("Repairing downsampled data cache, it has items while the source is empty");
            downsampled.clear().map_err(Error::ClearingDownsampled)?;
        }
        return Ok(());
    };

    let mut timestamps = Vec::new();
    let mut data = Vec::new();
    source
        .read_resampling(
            seek,
            resampler,
            config.bucket_size,
            &mut timestamps,
            &mut data,
        )
        .map_err(Error::ReadingSource)?;

    if !timestamps.is_empty() {
        warn!(
            "Repairing downsampled data cache, it is missing {} item(s)",
            timestamps.len()
        );
    }

    for (ts, item) in timestamps.into_iter().zip(data.into_iter()) {
        let bytes = resampler.encode_item(&item);
        downsampled
            .push_data(ts, &bytes)
            .map_err(Error::AppendingToDownsampled)?;
    }

    Ok(())
}