use crate::{Error, Series, TimeSeek};
use std::fmt::Debug;
use std::clone::Clone;

mod decoders;
pub mod combiners;
mod builder;
pub use combiners::SampleCombiner;
pub use builder::{new_sampler, SamplerBuilder};
pub use decoders::{Decoder, EmptyDecoder};

pub struct Sampler<'a, T, C> {
    series: Series,
    selector: Option<Selector>,
    decoder: &'a mut (dyn Decoder<T> + 'a), //check if generic better fit
    combiners: Vec<C>, 
    binsize: usize,
    seek: TimeSeek,

    time: Vec<i64>,
    values: Vec<T>,
    buff: Vec<u8>, 
    decoded_per_line: usize,
}

impl<'a, T, C> Debug for Sampler<'a, T, C>
where
    T: Clone + Debug,  
    C: Debug
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        //only print first n values
        let time = self.time[..5.min(self.time.len())].to_vec();
        let values = self.values[..5.min(self.time.len())].to_vec();
        let buff = self.buff[..5.min(self.time.len())].to_vec();
        f.debug_struct("Sampler")
            .field("series", &self.series)
            .field("selector", &self.selector)
            .field("decoder", &self.decoder)
            .field("combiner", &self.combiners)
            .field("binsize", &self.binsize)
            .field("seek", &self.seek)
            .field("time", &time)
            .field("values", &values)
            .field("buff", &buff)
            .field("decoded_per_line", &self.decoded_per_line)
            .finish()
    }
}

impl<'a, T, C> Sampler<'a, T, C>
where
    C: SampleCombiner<T>,
    T: Debug + Clone,
{
    pub fn sample_all(&mut self) -> Result<(), Error> {
        loop {
            self.sample()?;
            if self.done() {
                break;
            }
        }
        Ok(())
    }
    ///decodes and averages enough lines to get n samples unless the end of the file
    ///given range is reached
    pub fn sample(&mut self) -> Result<(), Error> {
        self.time.reserve_exact(self.values.len());
        self.values
            .reserve_exact(self.values.len() + self.decoded_per_line);

        let mut series = self.series.clone();
        let mut byteseries = series.lock();
        
        let seek = &mut self.seek;
        let selector = &mut self.selector;
        let full_line_size = byteseries.full_line_size;
        
        let n_read = byteseries.read(&mut self.buff, &mut seek.start, seek.stop)?;

        let mut n = 0; //TODO FIXME 
        let mut time_sum = 0; //TODO FIXME these both should be persistent across reads 
        for (line, pos) in self.buff[..n_read]
            .chunks(full_line_size)
            .zip((seek.curr..).step_by(full_line_size))
            .filter(|_| selector.as_mut().map(|s| s.use_index()).unwrap_or(true))
        {
            let time = byteseries.get_timestamp::<i64>(line, pos, &mut seek.full_time); 
            time_sum += time;
            let mut values = self.decoder.decoded(&line[2..]);
            for (v, comb) in values.drain(..).zip(self.combiners.iter_mut()) {
                comb.add(v, time);
            }
            
            n += 1;
            if n >= self.binsize {
                n=0;
                self.time.push(time_sum/self.binsize as i64);
                time_sum = 0;
                for comb in self.combiners.iter_mut() {
                    self.values.push(comb.combine());
                }
            }
        }
        if n >= 2 { //combine any leftovers
            self.time.push(time_sum/self.binsize as i64);
            for comb in self.combiners.iter_mut() {
                self.values.push(comb.combine());
            } 
        }

        seek.curr += n_read as u64;
        drop(byteseries);
        Ok(())
    }
    ///returns true if this sampler has read its entire range
    pub fn done(&self) -> bool {
        self.seek.curr == self.seek.stop
    }
    ///swap the time and values vectors with the given ones, returning the
    ///original
    pub fn swap_data(&mut self, times: &mut Vec<i64>, value: &mut Vec<T>) {
        std::mem::swap(&mut self.time, times);
        std::mem::swap(&mut self.values, value);
    }
    ///de-constructs the sampler into the time and values data
    pub fn into_data(self) -> (Vec<i64>, Vec<T>) {
        let Sampler { time, values, .. } = self;
        (time, values)
    }
    ///return the read values as slice
    pub fn values(&self) -> &[T] {
        &self.values
    }
}

impl<'a, T, C> std::iter::IntoIterator for Sampler<'a, T, C>
where
    T: Debug + Clone,
    C: SampleCombiner<T>,
{
    type Item = (i64, T);
    type IntoIter = std::iter::Zip<std::vec::IntoIter<i64>, std::vec::IntoIter<T>>;

    fn into_iter(self) -> Self::IntoIter {
        let (time, values) = self.into_data();
        time.into_iter().zip(values.into_iter())
    }
}

#[derive(Debug)]
pub struct Selector {
    spacing: f32, //in lines
    next_to_use: u64, 
    line: u64, //starts at 0
    used: u64
}

impl Selector {
    pub fn new(max_plot_points: usize, n_lines: u64, binsize: usize) -> Option<Self> {
        if n_lines as usize <= max_plot_points*binsize {
            return None;
        }
       
        let spacing = n_lines as f32 / max_plot_points as f32;
        Some(Self {
            spacing,
            next_to_use: (spacing/2.0) as u64,
            line: 0,
            used: 0,
        })
    }

    //calculate if element with index idx should be used
    fn use_index(&mut self) -> bool {
        if self.line == self.next_to_use {
            self.line += 1;
            self.used += 1;
            self.next_to_use = (self.used as f32*self.spacing) as u64;
            true
        } else {
            self.line += 1;
            false
        }
    }
}
