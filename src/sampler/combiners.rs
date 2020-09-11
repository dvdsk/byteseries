use std::clone::Clone;
use std::default::Default;
use std::fmt::Debug;

pub trait Bin: Debug {
    fn update_bin(&mut self, t: i64) -> Option<i64>;
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
            dbg!();
            let t = self.t_sum/(self.binsize as i64);
            self.t_sum = 0;
            self.n = 0;
            Some(t)
        } else {
            None
        }
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
}


/// the combiner gets both the value and the time, though unused 
/// by simple combinators such as the MeanCombiner this allows 
/// to combine values and time for example to calculate the derivative
pub trait SampleCombiner<T: Sized>: Debug {
    fn process(&mut self, time: i64, values: Vec<T>) -> Option<(i64, Vec<T>)>;
    // // binsize multiplier, used when combining.... combiners
    // // unless you are doing anything crazy keep the default impl
    // fn binsize(&self) -> usize {
    //     1
    // }
    fn set_decoded_size(&mut self, _n_values: usize) {}
}

#[derive(Debug, Clone, Default)]
pub struct Empty {}
impl<T: Debug + Clone + Sized> SampleCombiner<T> for Empty {
    fn process(&mut self, t: i64, v: Vec<T>) -> Option<(i64, Vec<T>)> {
        Some((t,v)) 
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
            dbg!();
            let v = self.v_sum.iter().map(|s| s/(self.n as f32)).collect();
            self.v_sum.iter_mut().for_each(|s| *s=0.0);
            self.n = 0;
            Some((binned_time,v))
        } else {
            None
        }
    }
    // fn binsize(&self) -> usize {
    //     self.binsize
    // }
    fn set_decoded_size(&mut self, n_values: usize) {
        self.v_sum = vec![0.0; n_values];
    }
}

// #[derive(Debug, Clone, Default)]
// pub struct Combiner<T,A,B> 
// where 
//     A: SampleCombiner<T>,
//     B: SampleCombiner<T>,
//     T: Debug + Clone + Default
// {
//     a: A,
//     b: B,
//     binsize_a: usize,
//     n: usize,
//     time_sum: i64,
//     t: PhantomData<T>,
// }

// impl<T,A,B> Combiner<T,A,B>
// where 
//     T: Debug + Clone + Default,
//     A: SampleCombiner<T>,
//     B: SampleCombiner<T>,
// {
//     #[allow(dead_code)]
//     fn new(a: A, b: B, binsize_a: usize) -> Self {
//         Self {
//             a,
//             b,
//             binsize_a,
//             n: 0,
//             time_sum: 0,
//             t: PhantomData,
//         }
//     }
// }

// impl<A,B,T> SampleCombiner<T> for Combiner<T,A,B> 
// where 
//     T: Debug + Clone + Default,
//     A: SampleCombiner<T>,
//     B: SampleCombiner<T>,
// {

//     fn add(&mut self, value: T, time: i64){
//         if self.n < self.binsize_a {
//             self.n += 1;
//             self.time_sum += time;
//             self.a.add(value,time);
//         } else {
//             let time = self.time_sum/(self.n as i64);
//             self.b.add(self.a.combine(), time);
//             self.n = 0;
//         }
//     }
//     fn combine(&mut self) -> T {
//         self.b.combine()
//     }
//     // the wanted binsize should be multiplied by the binsize of B
//     // in the read loop of the sampler
//     fn binsize(&self) -> usize {
//         self.binsize_a
//     }
// }


// #[derive(Debug, Clone, Default)]
// pub struct Mean {
//     v_sum: f32,
//     t_sum: i64,
//     n: usize,
//     binsize: usize,
// }

//impl SampleCombiner<f32> for Mean
//where
//{
//    fn add(&mut self, value: f32, time: i64) {
//        self.v_sum += value;
//        self.t_sum += time;
//        self.n += 1;
//    }
//    fn combine(&mut self) -> f32 {
//        let v = (self.v_sum.clone() / self.n as f32).into();
//        self.v_sum = f32::zero(); 
//        self.n = 0;
//        v
//    }
//    fn binsize(&self) -> usize {
//        self.binsize
//    }
//}

//#[derive(Debug, Clone, Default)]
//pub struct Empty<T> {v: T, t: i64}
//impl<T: Debug + Clone> SampleCombiner<T> for Empty<T> {
//    fn add(&mut self, value: T, time: i64){
//        self.v = value;
//        self.t = time;
//    }
//    fn combine(&mut self) -> T {
//        self.v.clone() 
//    }
//    fn binsize(&self) -> usize {
//        1
//    }
//}

//////TODO generic over array length when it stabilizes
//////minimum sample size is 2
////#[derive(Debug, Clone, Default)]
////pub struct Differentiate {
////    values: Vec<f32>, 
////    times: Vec<i64>,
////}
//////ENHANCEMENT rewrite using Sum<&T> (stuck on lifetimes)
////impl SampleCombiner<f32> for Differentiate {
////    fn add(&mut self, v: f32, t: i64){
////        self.values.push(v);
////        self.times.push(t);
////    }
////    fn combine(&mut self) -> f32 {
////        let len = self.values.len();
////        let v1: f32 = self.values[..len/2].iter().cloned().sum();
////        let v2: f32 = self.values[len/2..].iter().cloned().sum();
////        let t1: i64 = self.times[..len/2].iter().sum();
////        let t2: i64 = self.times[len/2..].iter().sum();

////        self.values.clear();
////        self.times.clear();
////        (v2-v1)/((t2-t1) as f32)
////    }
////}

////minimum sample size is 2
//#[derive(Debug, Clone, Default)]
//pub struct Differentiate {
//    pair_1: Option<(f32, i64)>,
//    pair_2: Option<(f32, i64)>,
//}
//impl SampleCombiner<f32> for Differentiate {
//    fn add(&mut self, v: f32, t: i64){
//        if self.pair_1.is_none() {
//            self.pair_1 = Some((v,t));
//        } else {
//            self.pair_2 = Some((v,t));
//        }
//    }
//    fn combine(&mut self) -> f32 {
//        let p1 = self.pair_1.take().expect("binsize must be at least 2 to determine numerical derivative");
//        let p2 = self.pair_2.take().expect("binsize must be at least 2 to determine numerical derivative");
//        (p2.0-p1.0)/((p2.1-p1.1) as f32)
//    }
//}

/////should be used with a binsize of 1
//#[derive(Debug, Clone, Default)]
//pub struct MovingAverage {
//    pair_1: Option<(f32, i64)>,
//    pair_2: Option<(f32, i64)>,
//}
//impl SampleCombiner<f32> for MovingAverage {
//    fn add(&mut self, v: f32, t: i64){
//        if self.pair_1.is_none() {
//            self.pair_1 = Some((v,t));
//        } else {
//            self.pair_2 = Some((v,t));
//        }
//    }
//    fn combine(&mut self) -> f32 {
//        let p1 = self.pair_1.take().expect("binsize must be at least 2 to determine numerical derivative");
//        let p2 = self.pair_2.take().expect("binsize must be at least 2 to determine numerical derivative");
//        (p2.0-p1.0)/((p2.1-p1.1) as f32)
//    }
//}
