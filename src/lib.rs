use chrono::{DateTime, NaiveDateTime, Utc};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

mod data;
pub mod error;
mod header;
mod sampler;
mod search;
mod util;

use data::ByteSeries;
pub use error::Error;
pub use sampler::{Decoder, EmptyDecoder, SampleCombiner, combiners, Sampler, SamplerBuilder, new_sampler};
pub use search::TimeSeek;

#[derive(Debug, Clone)]
pub struct Series {
    shared: Arc<Mutex<data::ByteSeries>>,
}

impl Series {
    fn lock(&mut self) -> MutexGuard<data::ByteSeries> {
        self.shared.lock().unwrap()
    }

    pub fn open<P: AsRef<Path>>(name: P, line_size: usize) -> Result<Self, Error> {
        let series = ByteSeries::open(name, line_size)?;
        Ok(Self {
            shared: Arc::new(Mutex::new(series)),
        })
    }

    pub fn last_line<'a, T: std::fmt::Debug + std::clone::Clone>(
        &mut self,
        decoder: &'a mut (dyn Decoder<T> + 'a),
    ) -> Result<(DateTime<Utc>, Vec<T>), Error> {
        let mut series = self.lock();
        let (time, bytes) = series.decode_last_line()?;
        let time = DateTime::from_utc(NaiveDateTime::from_timestamp(time, 0), Utc);
        let data = decoder.decoded(&bytes);
        Ok((time, data))
    }

    pub fn last_line_raw(&mut self) -> Result<(DateTime<Utc>, Vec<u8>), Error> {
        let mut series = self.lock();
        let (time, bytes) = series.decode_last_line()?;
        let time = DateTime::from_utc(NaiveDateTime::from_timestamp(time, 0), Utc);
        Ok((time, bytes))
    }

    pub fn append(&mut self, time: DateTime<Utc>, line: &[u8]) -> Result<(), Error> {
        let mut series = self.lock();
        series.append(time, line)?;
        Ok(())
    }
}
