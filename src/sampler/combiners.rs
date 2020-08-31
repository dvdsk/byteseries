use super::SampleCombiner;

use std::clone::Clone;
use std::default::Default;
use std::fmt::Debug;
use std::ops::{AddAssign, Div};
use num_traits::identities::Zero;

#[derive(Debug, Clone, Default)]
pub struct Mean<T> {
    v_sum: T,
    t_sum: i64,
    n: usize,
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
}
