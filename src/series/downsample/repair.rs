use std::ops::Bound;

use tracing::{instrument, warn};

use super::data::Data;
use super::Config;
use crate::seek::RoughSeekPos;
use crate::Resampler;

#[instrument]
pub(super) fn repair_missing_data(
    source: &mut Data,
    downsampled: &mut Data,
    config: &Config,
    resampler: &mut impl Resampler,
) {
    let start_bound = match downsampled.last_time().unwrap() {
        Some(ts) => Bound::Excluded(ts),
        None => Bound::Unbounded,
    };
    let seek = RoughSeekPos::new(source, start_bound, Bound::Unbounded)
        .unwrap()
        .refine(source)
        .unwrap();

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
        .unwrap();

    if !timestamps.is_empty() {
        warn!(
            "Repairing downsampled data cache, it is missing {} item(s)",
            timestamps.len()
        );
    }

    for (ts, item) in timestamps.into_iter().zip(data.into_iter()) {
        let bytes = resampler.encode_item(&item);
        downsampled.push_data(ts, &bytes).unwrap();
    }
}
