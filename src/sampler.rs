use crate::data::ByteSeries;
use crate::{Error, Series, TimeSeek};
use std::fmt::Debug;
use std::clone::Clone;
use std::default::Default;
use std::ops::{AddAssign, Div};
use num_traits::identities::Zero;

pub trait Decoder<T>: Debug
where
    T: Debug + Clone,
{
    fn decode(&mut self, bytes: &[u8], out: &mut Vec<T>);
    fn decoded(&mut self, bytes: &[u8]) -> Vec<T> {
        let mut values = Vec::new();
        self.decode(bytes, &mut values);
        values
    }
}

#[derive(Debug, Clone)]
pub struct EmptyDecoder {}
impl Decoder<u8> for EmptyDecoder {
    fn decode(&mut self, bytes: &[u8], out: &mut Vec<u8>) {
        out.extend_from_slice(&bytes[2..]);
    }
}

/// the combiner gets both the value and the time, though unused 
/// by simple combinators such as the MeanCombiner this allows 
/// to combine values and time for example to calculate the derivative
pub trait SampleCombiner<T>: Debug {
    fn add(&mut self, value: T, time: i64);
    fn combine(&mut self) -> T;
}

#[derive(Debug, Clone, Default)]
pub struct MeanCombiner<T> {
    v_sum: T,
    t_sum: i64,
    n: usize,
}
impl<T> SampleCombiner<T> for MeanCombiner<T>
where
    T: Debug + Clone + AddAssign + Div<usize> + Zero,
    <T as Div<usize>>::Output: Into<T>,
{
    fn add(&mut self, value: T, time: i64) {
        self.v_sum += value;
        self.t_sum += time;
        self.n += 1;
    }
    fn combine(&mut self) -> T {
        let v = (self.v_sum.clone() / self.n).into();
        self.v_sum = T::zero(); 
        self.n = 0;
        v
    }
}

#[derive(Debug, Clone, Default)]
pub struct EmptyCombiner<T> {v: T, t: i64}
impl<T> SampleCombiner<T> for EmptyCombiner<T>
where
    T: Debug + Clone
{
    fn add(&mut self, value: T, time: i64){
        self.v = value;
        self.t = time;
    }
    fn combine(&mut self) -> T {
        self.v.clone() 
    }
}

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

pub struct SamplerBuilder<'a, T> {
    series: Series,
    decoder: &'a mut (dyn Decoder<T> + 'a),
    start: Option<chrono::DateTime<chrono::Utc>>,
    stop: Option<chrono::DateTime<chrono::Utc>>,
    binsize: usize,
    points: Option<usize>,
}

impl<'a, T> SamplerBuilder<'a, T>
where
    T: Debug + Clone,
{
    pub fn new(series: &Series, decoder: &'a mut (dyn Decoder<T> + 'a)) -> Self {
        Self {
            series: series.clone(),
            decoder,
            binsize: 1,
            start: None,
            stop: None,
            points: None,
        }
    }
    pub fn start(mut self, start: chrono::DateTime<chrono::Utc>) -> Self {
        self.start = Some(start);
        self
    }
    pub fn stop(mut self, stop: chrono::DateTime<chrono::Utc>) -> Self {
        self.stop = Some(stop);
        self
    }
    pub fn points(mut self, n: usize) -> Self {
        self.points = Some(n);
        self
    }
    pub fn combine(mut self, binsize: usize) -> Self {
        self.binsize = binsize;
        self//TODO make this return a different type that has the combiner set
    }
    pub fn finish<C: SampleCombiner<T>+Default + Clone>(self) -> Result<Sampler<'a, T, C>, Error> {
        let Self {
            series,
            decoder,
            binsize,
            start,
            stop,
            points,
        } = self;
        let mut byteseries = series.shared.lock().unwrap();
        let start = start.unwrap();
        let stop = stop.unwrap();
        let seek = TimeSeek::new(&mut byteseries, start, stop)?;
        let selector = points
            .map(|p| Selector::new(p, seek.lines(&byteseries), binsize))
            .flatten();

        let dummy = vec![0u8; byteseries.full_line_size];
        let decoded_per_line = decoder.decoded(&dummy).len();
        drop(byteseries);

        Ok(Sampler {
            series,
            selector,
            decoder,
            combiners: vec![C::default(); decoded_per_line],
            binsize,
            seek,
            time: Vec::new(),
            values: Vec::new(),
            buff: vec![0u8; 409600],//TODO MAKE BUFFER SMALLER
            decoded_per_line,
        })
    }
}

impl<'a, T, C> Sampler<'a, T, C>
where
    C: SampleCombiner<T>,
    T: Debug + Clone,
{
    ///decodes and averages enough lines to get n samples unless the end of the file
    ///given range is reached
    pub fn sample(&mut self, n: usize) -> Result<(), Error> {
        self.time.reserve_exact(self.values.len());
        self.values
            .reserve_exact(self.values.len() + self.decoded_per_line);

        let mut series = self.series.clone();
        let mut byteseries = series.lock();
        let n_read = byteseries.read(&mut self.buff, &mut self.seek.start, self.seek.stop)?;

        let seek = &mut self.seek;
        let selector = &mut self.selector;
        let full_line_size = byteseries.full_line_size;

        let mut n = 0;
        let mut time_sum = 0;
        for (line, pos) in self.buff[..n_read]
            .chunks(full_line_size)
            .zip((seek.curr..).step_by(full_line_size))
            .filter(|_| selector.as_mut().map(|s| s.use_index()).unwrap_or(true))
        {
            let time = byteseries.get_timestamp::<i64>(line, pos, &mut self.seek.full_time); 
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
        drop(byteseries);
        self.seek.curr += n_read as u64;
        Ok(())
    }
    ///returns true if this sampler has read its entire range
    pub fn done(&self) -> bool {
        self.seek.curr == self.seek.stop
    }
    ///swap the time and values vectors with the given ones, returning the
    ///origional
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
    next_to_use: f32, 
    current: u64, //starts at 0
}

impl Selector {
    pub fn new(max_plot_points: usize, n_lines: u64, binsize: usize) -> Option<Self> {
        if n_lines as usize <= max_plot_points*binsize {
            return None;
        }
       
        let remainder = n_lines % max_plot_points as u64;
        let spacing = n_lines as f32 / max_plot_points as f32;

        Some(Self {
            spacing,
            next_to_use: remainder as f32 /2.0,
            current: 0,
        })
    }

    //calculate if element with index idx should be used
    fn use_index(&mut self) -> bool {
        let to_use = if self.current == self.next_to_use as u64 {
            self.next_to_use += self.spacing;
            true
        } else {
            false
        };
        self.current += 1;
        to_use
    }
}

/*impl Timeseries {
    pub fn decode_time_into_given(
        &mut self,
        timestamps: &mut Vec<u64>,
        line_data: &mut Vec<u8>,
        lines_to_read: usize,
        start_byte: &mut u64,
        stop_byte: u64,
        decode_params: &mut DecodeParams,
    ) -> Result<(), Error> {
        //let mut buf = Vec::with_capacity(lines_to_read*self.full_line_size);
        let mut buf = vec![0; lines_to_read * self.full_line_size];
        timestamps.clear();
        line_data.clear();

        //save file pos indicator before read call moves it around
        let file_pos = *start_byte;
        let n_read = self.read(&mut buf, start_byte, stop_byte)? as usize;
        log::trace!("read: {} bytes", n_read);
        for (line, file_pos) in buf[..n_read]
            .chunks(self.full_line_size)
            .zip((file_pos..).step_by(self.full_line_size))
        {
            timestamps.push(self.get_timestamp::<u64>(line, file_pos, decode_params));
            line_data.extend_from_slice(&line[2..]);
        }
        Ok(())
    }

    pub fn decode_time_into_given_skipping(
        &mut self,
        timestamps: &mut Vec<u64>,
        line_data: &mut Vec<u8>,
        lines_to_read: usize,
        start_byte: &mut u64,
        stop_byte: u64,
        decode_params: &mut DecodeParams,
        selector: &mut Selector,
    ) -> Result<(), Error> {
        //let mut buf = Vec::with_capacity(lines_to_read*self.full_line_size);
        let lines_to_skip = selector.n_to_skip(lines_to_read);
        let mut buf = vec![0; (lines_to_read + lines_to_skip) * self.full_line_size]; //TODO FIXME
        timestamps.clear();
        line_data.clear();

        //save file pos indicator before read call moves it around
        let file_pos = *start_byte;
        let n_read = self.read(&mut buf, start_byte, stop_byte)? as usize;
        log::trace!("read: {} bytes", n_read);
        dbg!(n_read);
        for (line, file_pos) in buf[..n_read]
            .chunks(self.full_line_size)
            .zip((file_pos..).step_by(self.full_line_size))
            .filter(|_| selector.use_index())
        {
            timestamps.push(self.get_timestamp::<u64>(line, file_pos, decode_params));
            line_data.extend_from_slice(&line[2..]);
        }
        Ok(())
    }
}*/
