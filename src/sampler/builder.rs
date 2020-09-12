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


pub struct SamplerBuilder<'a, T, StartSet> 
where 
    StartSet: ToAssign,
{
    start_set: PhantomData<StartSet>,

    series: Series,
    decoder: &'a mut (dyn Decoder<T> + 'a),
    start: Option<chrono::DateTime<chrono::Utc>>,
    stop: Option<chrono::DateTime<chrono::Utc>>,
    points: Option<usize>,
}

pub fn new_sampler<'a,T>(series: &Series, decoder: &'a mut (dyn Decoder<T> + 'a)) -> SamplerBuilder<'a, T, No> {
    SamplerBuilder {
        start_set: PhantomData {},

        series: series.clone(),
        decoder,
        start: None,
        stop: None,
        points: None,
    }
}

impl<'a, T, StartSet> SamplerBuilder<'a, T, StartSet>
where
    T: Debug + Clone + Default,
    StartSet: ToAssign,
{
    /// set the start time
    pub fn start(self, start: chrono::DateTime<chrono::Utc>)
     -> SamplerBuilder<'a, T, Yes> {
        
        SamplerBuilder {
            start_set: PhantomData {},
            series: self.series,
            decoder: self.decoder,
            start: Some(start),
            stop: self.stop,
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
}


impl<'a, T> SamplerBuilder<'a, T, Yes>
where
    T: Debug + Clone + Default,
{
    pub fn build(self) -> Result<Sampler<'a, T, combiners::Empty>, Error> {
            self.build_with_combiner(combiners::Empty::default())
    }

    pub fn build_with_combiner<C>(self, mut combiner: C) -> Result<Sampler<'a, T, C>, Error> 
    where
        C: SampleCombiner<T>
    {
        let SamplerBuilder {
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
        
        let lines = seek.lines(&byteseries);
        let selector = points
            .map(|p| Selector::new(p, lines, combiner.binsize(), combiner.binoffset()))
            .flatten();

        let dummy = vec![0u8; byteseries.full_line_size];
        let decoded_per_line = decoder.decoded(&dummy).len();
        combiner.set_decoded_size(decoded_per_line);
        drop(byteseries);

        Ok(Sampler {
            series,
            selector,
            decoder,
            combiner,
            seek,
            time: Vec::new(),
            values: Vec::new(),
            buff: vec![0u8; 409600],//TODO MAKE BUFFER SMALLER
            decoded_per_line,
        })
    }
}
