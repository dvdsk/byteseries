use super::{Sampler, Decoder, SampleCombiner, EmptyCombiner, Selector};

use chrono::{Utc, DateTime, NaiveDateTime};
use crate::{Series, TimeSeek, Error};
use std::default::Default;
use std::fmt::Debug;
use std::marker::PhantomData;

//some stuff to create the builder struct
//see: https://dev.to/mindflavor/rust-builder-pattern-with-types-3chf
#[derive(Debug, Default)]
pub struct Yes;
#[derive(Debug, Default)]
pub struct No;

pub trait ToAssign: Debug {}
pub trait Assigned: ToAssign {}
pub trait NotAssigned: ToAssign {}

impl ToAssign for Yes {}
impl ToAssign for No {}

impl Assigned for Yes {}
impl NotAssigned for No {}


pub struct SamplerBuilder<'a, T, C, START_SET, BINSIZE_SET, COMBINER_SET> 
where 
    START_SET: ToAssign,
    BINSIZE_SET: ToAssign,
    COMBINER_SET: ToAssign,
{
    start_set: PhantomData<START_SET>,
    binsize_set: PhantomData<BINSIZE_SET>,
    combiner_set: PhantomData<COMBINER_SET>,

    series: Series,
    decoder: &'a mut (dyn Decoder<T> + 'a),
    start: Option<chrono::DateTime<chrono::Utc>>,
    stop: Option<chrono::DateTime<chrono::Utc>>,
    binsize: usize,
    points: Option<usize>,
    combiners: Option<Vec<C>>,
    combiner: Option<C>,
}

impl<'a, T, C, START_SET, BINSIZE_SET, COMBINER_SET> SamplerBuilder<'a, T, C, START_SET, BINSIZE_SET, COMBINER_SET>
where
    T: Debug + Clone + Default,
    C: SampleCombiner<T>,
    START_SET: ToAssign,
    BINSIZE_SET: ToAssign,
    COMBINER_SET: ToAssign,
{
    pub fn new(series: &Series, decoder: &'a mut (dyn Decoder<T> + 'a)) -> Self {
        Self {
            start_set: PhantomData {},
            binsize_set: PhantomData {},
            combiner_set: PhantomData {},

            series: series.clone(),
            decoder,
            start: None,
            stop: None,
            binsize: 0,
            points: None,
            combiners: None,
            combiner: None,
        }
    }
    /// set the start time
    pub fn start(mut self, start: chrono::DateTime<chrono::Utc>)
     -> SamplerBuilder<'a, T, C, Yes, BINSIZE_SET, COMBINER_SET> {
        
        SamplerBuilder {
            start_set: PhantomData {},
            binsize_set: PhantomData {},
            combiner_set: PhantomData {},

            series: self.series,
            decoder: self.decoder,
            start: Some(start),
            stop: self.stop,
            binsize: self.binsize,
            points: self.points,
            combiners: self.combiners,
            combiner: self.combiner,
        }
    }
    /// set the stop time
    pub fn stop(mut self, stop: chrono::DateTime<chrono::Utc>) -> Self {
        self.stop = Some(stop);
        self
    }
    /// set the number of points to read
    pub fn points(mut self, n: usize) -> Self {
        self.points = Some(n);
        self
    }
    pub fn per_sample(mut self, binsize: usize) 
     -> SamplerBuilder<'a, T, C, START_SET, Yes, BINSIZE_SET> {
        
        SamplerBuilder {
            start_set: PhantomData {},
            binsize_set: PhantomData {},
            combiner_set: PhantomData {},

            series: self.series,
            decoder: self.decoder,
            start: self.start,
            binsize,
            stop: self.stop,
            points: self.points,
            combiners: self.combiners,
            combiner: self.combiner,
        }
    }
    pub fn with_combiners(mut self, combiners: Vec<C>) 
     -> SamplerBuilder<'a, T, C, START_SET, BINSIZE_SET, Yes> {
        
        SamplerBuilder {
            start_set: PhantomData {},
            binsize_set: PhantomData {},
            combiner_set: PhantomData {},

            series: self.series,
            decoder: self.decoder,
            start: self.start,
            binsize: self.binsize,
            stop: self.stop,
            points: self.points,
            combiners: Some(combiners),
            combiner: None,
        }
    }
    pub fn with_combiner(mut self, combiner: C) 
     -> SamplerBuilder<'a, T, C, START_SET, BINSIZE_SET, Yes> {
        
        SamplerBuilder {
            start_set: PhantomData {},
            binsize_set: PhantomData {},
            combiner_set: PhantomData {},

            series: self.series,
            decoder: self.decoder,
            start: self.start,
            binsize: self.binsize,
            stop: self.stop,
            points: self.points,
            combiners: None,
            combiner: Some(combiner),
        }
    }
}


impl<'a, T, C> SamplerBuilder<'a, T, C, Yes, No, No>
where
    T: Debug + Clone + Default,
    C: SampleCombiner<T>,
{
    pub fn finish(self) -> Result<Sampler<'a, T, EmptyCombiner<T>>, Error> {
        let Self {
            series,
            decoder,
            start,
            stop,
            points, ..
        } = self;
        let mut byteseries = series.shared.lock().unwrap();
        
        let stop = stop.or_else(|| byteseries.last_time_in_data
            .map(|ts| DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(ts, 0), Utc)))
            .ok_or(Error::NoData)?;
        let seek = TimeSeek::new(&mut byteseries, start.unwrap(), stop)?;
        
        let binsize = 1;
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
            combiners: vec![EmptyCombiner::<T>::default(); decoded_per_line],
            binsize,
            seek,
            time: Vec::new(),
            values: Vec::new(),
            buff: vec![0u8; 409600],//TODO MAKE BUFFER SMALLER
            decoded_per_line,
        })
    }
}

impl<'a, T, C> SamplerBuilder<'a, T, C, Yes, Yes, Yes>
where
    T: Debug + Clone + Default,
    C: SampleCombiner<T> + Clone,
{
    pub fn finish(self) -> Result<Sampler<'a, T, C>, Error> {
        let Self {
            series,
            decoder,
            start,
            stop,
            points,
            binsize, ..
        } = self;
        let mut byteseries = series.shared.lock().unwrap();
        
        let stop = stop.or_else(|| byteseries.last_time_in_data
            .map(|ts| DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(ts, 0), Utc)))
            .ok_or(Error::NoData)?;
        let seek = TimeSeek::new(&mut byteseries, start.unwrap(), stop)?;
        
        let selector = points
            .map(|p| Selector::new(p, seek.lines(&byteseries), binsize))
            .flatten();

        let dummy = vec![0u8; byteseries.full_line_size];
        let decoded_per_line = decoder.decoded(&dummy).len();
        drop(byteseries);

        let combiners = self.combiners.unwrap_or(vec![self.combiner.unwrap(); decoded_per_line]);

        Ok(Sampler {
            series,
            selector,
            decoder,
            combiners,
            binsize,
            seek,
            time: Vec::new(),
            values: Vec::new(),
            buff: vec![0u8; 409600],//TODO MAKE BUFFER SMALLER
            decoded_per_line,
        })
    }
}
