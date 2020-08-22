use std::path::Path;
use std::sync::{Arc, Mutex};
use std::io::Error;

mod data;
mod header;
mod util;
mod search;
mod sampler;
//mod test;

use data::ByteSeries;
pub use search::TimeSeek;
pub use sampler::{Decoder, EmptyDecoder};

#[derive(Debug)]
pub struct Series {
    shared: Arc<Mutex<data::ByteSeries>>,
}

impl Series {
    pub fn open<P: AsRef<Path>>(name: P, line_size: usize) -> Result<Self, Error> {
        let series = ByteSeries::open(name, line_size)?;
        Ok(Self {
            shared: Arc::new(Mutex::new(series)), 
        })
    }

    pub fn last_line<'a, T>(&self, decoder: &'a mut (dyn Decoder<T> + 'a))
        -> Result<(i64, Vec<f64>), Error> {
        let series = self.series.lock().unwrap();
        let (time, bytes) = series.decode_last_line()?;
        let data = decoder.decode(bytes);
        Ok((time, data))
    }
}
