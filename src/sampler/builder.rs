use super::{combiners, Decoder, SampleCombiner, Sampler, Selector};

use crate::{ByteSeries, Error, TimeSeek};
use std::default::Default;
use std::fmt::Debug;
use std::marker::PhantomData;
use time::OffsetDateTime;

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

pub struct SamplerBuilder<D, T, StartSet>
where
    StartSet: ToAssign,
    D: Decoder<T>,
    T: Debug + Clone,
{
    start_set: PhantomData<StartSet>,
    decoded: PhantomData<T>,

    series: ByteSeries,
    decoder: D,
    start: Option<OffsetDateTime>,
    stop: Option<OffsetDateTime>,
    points: Option<usize>,
}

pub fn new_sampler<D, T>(series: ByteSeries, decoder: D) -> SamplerBuilder<D, T, No>
where
    T: Debug + Clone,
    D: Decoder<T>,
{
    SamplerBuilder {
        start_set: PhantomData {},
        decoded: PhantomData {},

        series,
        decoder,
        start: None,
        stop: None,
        points: None,
    }
}

impl<D, T, StartSet> SamplerBuilder<D, T, StartSet>
where
    T: Debug + Clone + Default,
    D: Decoder<T>,
    StartSet: ToAssign,
{
    /// set the start time
    pub fn start(self, start: OffsetDateTime) -> SamplerBuilder<D, T, Yes> {
        SamplerBuilder {
            start_set: PhantomData {},
            decoded: PhantomData {},
            series: self.series,
            decoder: self.decoder,
            start: Some(start),
            stop: self.stop,
            points: self.points,
        }
    }
    /// set the stop time
    pub fn stop(mut self, stop: OffsetDateTime) -> Self {
        self.stop = Some(stop);
        self
    }
    /// set the number of points to read
    pub fn points(mut self, n: usize) -> Self {
        self.points = Some(n);
        self
    }
}

impl<D, T> SamplerBuilder<D, T, Yes>
where
    T: Debug + Clone + Default,
    D: Decoder<T>,
{
    pub fn build(self) -> Result<Sampler<D, T, combiners::Empty>, Error> {
        self.build_with_combiner(combiners::Empty::default())
    }

    pub fn build_with_combiner<C>(self, mut combiner: C) -> Result<Sampler<D, T, C>, Error>
    where
        C: SampleCombiner<T>,
    {
        let SamplerBuilder {
            mut series,
            mut decoder,
            start,
            stop,
            points,
            ..
        } = self;

        let stop = stop
            .or_else(|| series.last_time_in_data())
            .ok_or(Error::NoData)?;
        let seek = TimeSeek::new(&mut series, start.unwrap(), stop)?;

        let lines = seek.lines(&series);
        let selector = points
            .map(|p| Selector::new(p, lines, combiner.binsize(), combiner.binoffset()))
            .flatten();

        let dummy = vec![0u8; series.full_line_size];
        let decoded_per_line = decoder.decoded(&dummy).len();
        combiner.set_decoded_size(decoded_per_line);

        Ok(Sampler {
            series,
            selector,
            decoder,
            combiner,
            seek,
            time: Vec::new(),
            values: Vec::new(),
            buff: vec![0u8; 64_000],
            decoded_per_line,
        })
    }
}
