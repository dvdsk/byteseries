use std::clone::Clone;
use std::default::Default;
use std::fmt::Debug;

pub trait Bin: Debug {
    fn update_bin(&mut self, t: i64) -> Option<i64>;
    fn binsize(&self) -> usize;
}

#[derive(Debug)]
pub struct SampleBin {binsize: usize, n: usize, t_sum: i64}
impl SampleBin {
    pub fn new(binsize: usize) -> Self {
        SampleBin {
            binsize,
            n: 0,
            t_sum: 0,
        }
    }
}

impl Bin for SampleBin {
    fn update_bin(&mut self, t: i64) -> Option<i64> {
        self.n += 1;
        self.t_sum += t;
        if self.n >= self.binsize {
            let t = self.t_sum/(self.binsize as i64);
            self.t_sum = 0;
            self.n = 0;
            Some(t)
        } else {
            None
        }
    }
    fn binsize(&self) -> usize {
        self.binsize
    }
}

#[derive(Debug)]
pub struct TimeBin {period: i64, first: Option<i64>}
impl TimeBin {
    pub fn new(period: chrono::Duration) -> Self {
        Self {
            period: period.num_seconds(),
            first: None,
        }
    }
}

impl Bin for TimeBin {
    fn update_bin(&mut self, t: i64) -> Option<i64> {
        if let Some(s) = self.first {
            if t-s > self.period {
                Some(self.first.take().unwrap()+self.period/2)
            } else {
                None
            }
        } else {
            self.first = Some(t);
            None
        }
    }
    //since we don not have a concept of binsize
    //we return one, this will cause sampler to return
    //a unknown amount of points
    fn binsize(&self) -> usize {
        1
    }
}


/// the combiner gets both the value and the time, though unused 
/// by simple combinators such as the MeanCombiner this allows 
/// to combine values and time for example to calculate the derivative
pub trait SampleCombiner<T: Sized>: Debug {
    fn process(&mut self, time: i64, values: Vec<T>) -> Option<(i64, Vec<T>)>;
    fn binsize(&self) -> usize;
    fn binoffset(&self) -> usize {0}
    fn set_decoded_size(&mut self, _n_values: usize) {}
}

#[derive(Debug, Clone, Default)]
pub struct Empty {}
impl<T: Debug + Clone + Sized> SampleCombiner<T> for Empty {
    fn process(&mut self, t: i64, v: Vec<T>) -> Option<(i64, Vec<T>)> {
        Some((t,v)) 
    }
    fn binsize(&self) -> usize {
        1
    }
}

#[derive(Debug, Clone)]
pub struct Mean<B> {
    v_sum: Vec<f32>,
    t_sum: i64,
    n: usize,
    bin: B,
}

impl<B> Mean<B> {
    pub fn new(bin: B) -> Self {
        Mean {
            v_sum: Vec::new(),
            t_sum: 0,
            n: 0,
            bin,
        }
    }
}

impl<B> SampleCombiner<f32> for Mean<B>
where
    B: Bin
{
    fn process(&mut self, time: i64, mut values: Vec<f32>) -> Option<(i64,Vec<f32>)> {
        self.v_sum.iter_mut().zip(values.drain(..)).for_each(|(s,v)| *s+=v);
        self.n += 1;

        if let Some(binned_time) = self.bin.update_bin(time){
            let v = self.v_sum.iter().map(|s| s/(self.n as f32)).collect();
            self.v_sum.iter_mut().for_each(|s| *s=0.0);
            self.n = 0;
            Some((binned_time,v))
        } else {
            None
        }
    }
    fn binsize(&self) -> usize {
        self.bin.binsize()
    }
    fn set_decoded_size(&mut self, n_values: usize) {
        self.v_sum = vec![0.0; n_values];
    }
}

#[derive(Debug, Clone, Default)]
pub struct Combiner<A,B> 
where 
    A: SampleCombiner<f32>,
    B: SampleCombiner<f32>,
{
    a: A,
    b: B,
}

impl<A,B> Combiner<A,B>
where 
    A: SampleCombiner<f32>,
    B: SampleCombiner<f32>,
{
    #[allow(dead_code)]
    pub fn new(a: A, b: B) -> Self {
        Self {
            a,
            b,
        }
    }
}

impl<A,B> SampleCombiner<f32> for Combiner<A,B> 
where 
    A: SampleCombiner<f32>,
    B: SampleCombiner<f32>,
{
    fn process(&mut self, time: i64, values: Vec<f32>) -> Option<(i64,Vec<f32>)> {
        if let Some((time, values)) = self.a.process(time, values){
            if let Some((time, values)) = self.b.process(time, values){
                return Some((time,values));
            }
        }
        None
    }
    fn set_decoded_size(&mut self, n_values: usize) {
        self.a.set_decoded_size(n_values);
        self.b.set_decoded_size(n_values);
    }
    fn binsize(&self) -> usize {
        self.a.binsize()*self.b.binsize()
    }
    fn binoffset(&self) -> usize {
        self.a.binoffset()*self.b.binsize()+self.b.binoffset()
    }
}

//minimum sample size is 2
#[derive(Debug, Clone, Default)]
pub struct Differentiate {
    pair_1: Option<(i64, Vec<f32>)>,
}
impl SampleCombiner<f32> for Differentiate {
    fn process(&mut self, t2: i64, v2: Vec<f32>) -> Option<(i64,Vec<f32>)> {
        if self.pair_1.is_none() {
            self.pair_1 = Some((t2, v2));
            None
        } else {
            let (t1, v1) = self.pair_1.as_ref().unwrap();

            let dt = (t2 - t1) as f32;
            let dv = v1.iter().zip(v2.iter()).map(|(v1, v2)| v2-v1);
            let dvdt = dv.map(|dv| dv/dt).collect();
            let mean_time = (t1+t2)/2;
            self.pair_1 = Some((t2, v2));
            Some((mean_time, dvdt))
        }
    }
    fn binsize(&self) -> usize {
        1 //we return after receiving a sample
    }
    fn binoffset(&self) -> usize {
        1 //we need the first sample to get started
    }
}
