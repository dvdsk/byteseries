use std::iter::Sum;
use std::clone::Clone;
use std::default::Default;
use std::fmt::Debug;
use std::ops::{AddAssign, Div, Sub};
use num_traits::identities::Zero;
use std::marker::PhantomData;

/// the combiner gets both the value and the time, though unused 
/// by simple combinators such as the MeanCombiner this allows 
/// to combine values and time for example to calculate the derivative
pub trait SampleCombiner<T>: Debug {
    fn add(&mut self, value: T, time: i64);
    fn combine(&mut self) -> T;
    // binsize multiplier, used when combining.... combiners
    // unless you are doing anything crazy keep the default impl
    fn binsize(&self) -> usize {
        1
    }
}

#[derive(Debug, Clone, Default)]
pub struct Combiner<T,A,B> 
where 
    A: SampleCombiner<T>,
    B: SampleCombiner<T>,
    T: Debug + Clone + Default
{
    a: A,
    b: B,
    binsize_a: usize,
    n: usize,
    time_sum: i64,
    t: PhantomData<T>,
}

impl<T,A,B> Combiner<T,A,B>
where 
    T: Debug + Clone + Default,
    A: SampleCombiner<T>,
    B: SampleCombiner<T>,
{
    fn new(a: A, b: B, binsize_a: usize) -> Self {
        Self {
            a,
            b,
            binsize_a,
            n: 0,
            time_sum: 0,
            t: PhantomData,
        }
    }
}

impl<A,B,T> SampleCombiner<T> for Combiner<T,A,B> 
where 
    T: Debug + Clone + Default,
    A: SampleCombiner<T>,
    B: SampleCombiner<T>,
{

    fn add(&mut self, value: T, time: i64){
        if self.n < self.binsize_a {
            self.n += 1;
            self.time_sum += time;
            self.a.add(value,time);
        } else {
            let time = self.time_sum/(self.n as i64);
            self.b.add(self.a.combine(), time);
            self.n = 0;
        }
    }
    fn combine(&mut self) -> T {
        self.b.combine()
    }
    // the wanted binsize should be multiplied by the binsize of B
    // in the read loop of the sampler
    fn binsize(&self) -> usize {
        self.binsize_a
    }
}


#[derive(Debug, Clone, Default)]
pub struct Mean<T> {
    v_sum: T,
    t_sum: i64,
    n: usize,
    binsize: usize,
}

impl<T> SampleCombiner<T> for Mean<T>
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
    fn binsize(&self) -> usize {
        self.binsize
    }
}

#[derive(Debug, Clone, Default)]
pub struct Empty<T> {v: T, t: i64}
impl<T> SampleCombiner<T> for Empty<T>
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
    fn binsize(&self) -> usize {
        1
    }
}

//TODO generic over array length when it stabilizes
//minimum sample size is 2
#[derive(Debug, Clone, Default)]
pub struct Differentiate<T> {
    values: Vec<T>, 
    times: Vec<i64>,
}
//ENHANCEMENT rewrite using Sum<&T> (stuck on lifetimes)
impl<T> SampleCombiner<T> for Differentiate<T>
where
    T: Debug + Clone + Sum<T> + Sub<T> + Div<i64>,
    <T as Sub<T>>::Output: Into<T>,
    <T as Div<i64>>::Output: Into<T>,
{
    fn add(&mut self, v: T, t: i64){
        self.values.push(v);
        self.times.push(t);
    }
    fn combine(&mut self) -> T {
        let len = self.values.len();
        let v1: T = self.values[..len/2].iter().cloned().sum();
        let v2: T = self.values[len/2..].iter().cloned().sum();
        let t1: i64 = self.times[..len/2].iter().sum();
        let t2: i64 = self.times[len/2..].iter().sum();

        self.values.clear();
        self.times.clear();
        ((v2-v1).into()/(t2-t1)).into()
    }
}
