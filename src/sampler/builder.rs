use super::{Sampler, Decoder, SampleCombiner, combiners, Selector};

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


pub struct SamplerBuilder<'a, T, StartSet, BinSizeSet> 
where 
    StartSet: ToAssign,
    BinSizeSet: ToAssign,
{
    start_set: PhantomData<StartSet>,
    binsize_set: PhantomData<BinSizeSet>,

    series: Series,
    decoder: &'a mut (dyn Decoder<T> + 'a),
    start: Option<chrono::DateTime<chrono::Utc>>,
    stop: Option<chrono::DateTime<chrono::Utc>>,
    binsize: usize,
    points: Option<usize>,
}

pub fn new_sampler<'a,T>(series: &Series, decoder: &'a mut (dyn Decoder<T> + 'a)) -> SamplerBuilder<'a, T, No, No> {
    SamplerBuilder {
        start_set: PhantomData {},
        binsize_set: PhantomData {},

        series: series.clone(),
        decoder,
        start: None,
        stop: None,
        binsize: 0,
        points: None,
    }
}

impl<'a, T, StartSet, BinSizeSet> SamplerBuilder<'a, T, StartSet, BinSizeSet>
where
    T: Debug + Clone + Default,
    StartSet: ToAssign,
    BinSizeSet: ToAssign,
{
    /// set the start time
    pub fn start(self, start: chrono::DateTime<chrono::Utc>)
     -> SamplerBuilder<'a, T, Yes, BinSizeSet> {
        
        SamplerBuilder {
            start_set: PhantomData {},
            binsize_set: PhantomData {},

            series: self.series,
            decoder: self.decoder,
            start: Some(start),
            stop: self.stop,
            binsize: self.binsize,
            points: self.points,
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
    pub fn per_sample(self, binsize: usize) 
     -> SamplerBuilder<'a, T, StartSet, Yes> {
        
        SamplerBuilder {
            start_set: PhantomData {},
            binsize_set: PhantomData {},

            series: self.series,
            decoder: self.decoder,
            start: self.start,
            binsize,
            stop: self.stop,
            points: self.points,
        }
    }
}


impl<'a, T> SamplerBuilder<'a, T, Yes, No>
where
    T: Debug + Clone + Default,
{
    pub fn build(self) -> Result<Sampler<'a, T, combiners::Empty<T>>, Error> {
        self.per_sample(1)
            .build_with_combiner(combiners::Empty::<T>::default())
    }
}

impl<'a, T> SamplerBuilder<'a, T, Yes, Yes>
where
    T: Debug + Clone + Default,
{
    pub fn build_with_combiner<C>(self, combiner: C) -> Result<Sampler<'a, T, C>, Error> 
    where
        C: SampleCombiner<T> + Clone + Default
    {
        let SamplerBuilder {
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

        let combiners = vec![combiner; decoded_per_line];

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
