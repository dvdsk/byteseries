use crate::{Series, TimeSeek, Error};
use crate::data::ByteSeries;

pub trait Decoder<T> : std::fmt::Debug
where T: std::fmt::Debug
{
    fn decode(&mut self, bytes: &[u8], out: &mut Vec<T>);
    fn decoded(&mut self, bytes: &[u8]) -> Vec<T>{
        let mut values = Vec::new();
        self.decode(bytes, &mut values);
        values
    }
}

#[derive(Debug)]
pub struct EmptyDecoder {}
impl Decoder<u8> for EmptyDecoder {
    fn decode(&mut self, bytes: &[u8], out: &mut Vec<u8>){
        out.extend_from_slice(&bytes[2..]);
    }
}

#[derive(Debug)]
pub struct Sampler<'a, T> 
// where T: std::fmt::Debug
{
    series: Series,
    selector: Option<Selector>,
    decoder: &'a mut (dyn Decoder<T> + 'a), //check if generic better fit
    seek: TimeSeek,

    time: Vec<i64>,
    values: Vec<T>,
    buff: Vec<u8>, //TODO replace with array for perf
    decoded_per_line: usize,
}

//impl<'a,T: std::fmt::Debug> std::fmt::Debug for Sampler<'a, T> {
//    fn fmt(&self,  f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//        f.debug_struct("Sampler")
//            .field("series", &self.series)
//            .field("selector", &self.selector)
//            .field("seek", &self.seek)
//            .field("time", &self.time)
//            .field("values", &self.values)
//            .field("buff", &self.buff)
//            .field("decoded_per_line", &self.decoded_per_line)
//            //.finish_non_exhaustive)
//            .finish()
//    }
//}

pub struct SamplerBuilder<'a,T> {
    series: Series,
    decoder: &'a mut (dyn Decoder<T> + 'a),
    start: Option<chrono::DateTime<chrono::Utc>>,
    stop: Option<chrono::DateTime<chrono::Utc>>,
    points: Option<usize>,
}


impl<'a, T> SamplerBuilder<'a, T> 
where T: std::fmt::Debug
{
    pub fn new(series: &Series, decoder: &'a mut (dyn Decoder<T> + 'a), ) -> Self {
        Self {
            series: series.clone(),
            decoder,
            start: None,
            stop: None,
            points: None,
        }
    }
    pub fn start(mut self, start: chrono::DateTime<chrono::Utc>) -> Self {
        self.start = Some(start); self
    }
    pub fn stop(mut self, stop: chrono::DateTime<chrono::Utc>) -> Self {
        self.stop = Some(stop); self
    }
    pub fn points(mut self, n: usize) -> Self {
        self.points = Some(n); self
    }
    pub fn finish(self) -> Result<Sampler<'a,T>, Error> {
        let Self{series, decoder, start, stop, points} = self;
        let mut byteseries = series.shared.lock().unwrap();
        let start = start.unwrap();
        let stop = stop.unwrap();
        let seek = TimeSeek::new(&mut byteseries, start, stop)?;
        let selector = points.map(|p| Selector::new(p, seek.lines(&byteseries), &byteseries)).flatten();
        
        let dummy = vec![0u8; byteseries.full_line_size];
        let decoded_per_line = decoder.decoded(&dummy).len();
        drop(byteseries);

        Ok(Sampler {
            series,
            selector,
            decoder,
            seek,
            time: Vec::new(),
            values: Vec::new(),
            buff: Vec::with_capacity(4096),
            decoded_per_line,
        })
    }
}


impl<'a,T> Sampler<'a,T> 
where T: std::fmt::Debug
{
    ///decodes and averages enough lines to get n samples unless the end of the file 
    ///given range is reached
    pub fn sample(&mut self, n: usize) {
        self.time.reserve_exact(self.values.len());
        self.values.reserve_exact(self.values.len()+self.decoded_per_line);
        
        let mut series = self.series.clone();
        let mut byteseries = series.lock();
        let n_read = byteseries.read(&mut self.buff, &mut self.seek.start, self.seek.stop).unwrap();

        let seek = &mut self.seek;
        let selector = &mut self.selector;
        let full_line_size = byteseries.full_line_size;

        for(line, pos) in self.buff[..n_read]
            .chunks(full_line_size)
            .zip((seek.curr..).step_by(full_line_size))
            .filter(|_| selector.as_mut().map(|s| s.use_index()).unwrap_or(true))
        {
            self.time.push(byteseries.get_timestamp::<i64>(line, pos, &mut self.seek.full_time));
            self.decoder.decode(&line[2..], &mut self.values);
        }
        drop(byteseries);
        self.seek.curr += n_read as u64;
    }
    ///returns true if this sampler has read its entire range
    pub fn done(&self) -> bool {
        self.seek.curr == self.seek.stop
    }
    ///swap the time and values vectors with the given ones, returning the 
    ///origional
    pub fn swap_data(&mut self, times: &mut Vec<i64>, value: &mut Vec<T>){
        std::mem::swap(&mut self.time, times);
        std::mem::swap(&mut self.values, value);
    }
    ///de-constructs the sampler into the time and values data
    pub fn into_data(self) -> (Vec<i64>,Vec<T>) {
        let Sampler {time, values, ..} = self;
        (time, values) 
    }
}

#[derive(Debug)]
pub struct Selector {
    spacing: u64, //in lines
    counter: u64, //starts at 1
    current: u64, //starts at 0

    full_line_size: usize,
    pub lines_per_sample: std::num::NonZeroUsize,
    //binsize; usize//in lines
}

impl Selector {
    pub fn new(max_plot_points: usize, numb_lines: u64, timeseries: &ByteSeries) -> Option<Self> {
        if numb_lines <= max_plot_points as u64 {
            return None;
        }

        let full_line_size = timeseries.full_line_size;
        let lines_to_skip: u64 = numb_lines % max_plot_points as u64;

        let lines_per_sample =
            std::num::NonZeroUsize::new((numb_lines / max_plot_points as u64) as usize).unwrap();

        Some(Self {
            spacing: ((numb_lines - lines_to_skip) as u64) / lines_to_skip,
            counter: 1,
            current: 0,
            full_line_size,
            lines_per_sample,
        })
    }

    //calculate if element with index idx should be used
    fn use_index(&mut self) -> bool {
        if self.current == self.counter * self.spacing {
            self.counter += 1;
            //dont increment the current counter as we will skip this point
            false
        } else {
            self.current += 1;
            true
        }
    }

    //one and a halve spacing
    fn n_to_skip(&self, lines_to_read: usize) -> usize {
        let stop_pos: u64 = self.current + lines_to_read as u64; //can we use current though? what happens after skip?
        let first_skip_pos: u64 = self.counter * self.spacing;

        //check if skip in this read chunk
        if first_skip_pos > stop_pos {
            0
        } else {
            //there is at least one skip, check if there are more
            let numb_of_additional_skips =
                (stop_pos.saturating_sub(first_skip_pos) / self.spacing) as usize;
            1 + numb_of_additional_skips
        }
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
