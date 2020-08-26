use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

mod data;
mod header;
mod util;
mod search;
mod sampler;
mod error;
//mod test;

use data::ByteSeries;
pub use error::Error;
pub use search::TimeSeek;
pub use sampler::{Decoder, EmptyDecoder, SamplerBuilder};

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

    pub fn last_line<'a, T: std::fmt::Debug>(&mut self, decoder: &'a mut (dyn Decoder<T> + 'a))
        -> Result<(i64, Vec<T>), Error> {
        let mut series = self.lock();
        let (time, bytes) = series.decode_last_line()?;
        let data = decoder.decoded(&bytes);
        Ok((time, data))
    }
}
