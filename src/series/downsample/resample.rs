#[cfg(feature = "smallvec")]
use smallvec::SmallVec;
use std::ops::AddAssign;

use num_traits::Zero;

use crate::ResampleState;

mod empty;
pub(crate) use empty::EmptyResampler;

#[cfg(feature = "smallvec")]
impl<const LEN: usize, NUM> ResampleState for SmallVec<NUM, LEN>
where
    NUM: Zero + AddAssign + core::fmt::Debug,
    NUM: num_traits::FromPrimitive,
    for<'a> &'a NUM: std::ops::Div<NUM, Output = NUM>,
{
    type Item = SmallVec<NUM, LEN>;

    fn add(&mut self, item: Self::Item) {
        assert_eq!(
            self.len(),
            item.len(),
            "Self should be same length \
            as the item your resampling/adding"
        );
        for (state, new) in self.iter_mut().zip(item) {
            *state += new;
        }
    }

    fn finish(&mut self, collected: usize) -> Self::Item {
        #[allow(clippy::cast_precision_loss)]
        let res = self
            .iter()
            .map(|s| {
                s / (NUM::from_usize(collected))
                    .expect("bucket size to large, can not be represented by type")
            })
            .collect();
        for i in self {
            *i = NUM::zero();
        }
        res
    }
}

impl<NUM> ResampleState for Vec<NUM>
where
    NUM: Zero + AddAssign + core::fmt::Debug,
    NUM: num_traits::FromPrimitive,
    for<'a> &'a NUM: std::ops::Div<NUM, Output = NUM>,
{
    type Item = Vec<NUM>;

    fn add(&mut self, item: Self::Item) {
        assert_eq!(
            self.len(),
            item.len(),
            "Self should be same length \
            as the item your resampling/adding"
        );
        for (state, new) in self.iter_mut().zip(item) {
            *state += new;
        }
    }

    fn finish(&mut self, collected: usize) -> Self::Item {
        #[allow(clippy::cast_precision_loss)]
        let res = self
            .iter()
            .map(|s| {
                s / (NUM::from_usize(collected))
                    .expect("bucket size to large, can not be represented by type")
            })
            .collect();
        for i in self {
            *i = NUM::zero();
        }
        res
    }
}

impl<const LEN: usize, NUM> ResampleState for [NUM; LEN]
where
    NUM: Clone + Zero + AddAssign + core::fmt::Debug + std::ops::Div<Output = NUM>,
    NUM: num_traits::FromPrimitive,
    for<'a> &'a NUM: std::ops::Div<NUM, Output = NUM>,
{
    type Item = [NUM; LEN];

    fn add(&mut self, item: Self::Item) {
        for (state, new) in self.iter_mut().zip(item) {
            *state += new;
        }
    }

    fn finish(&mut self, collected: usize) -> Self::Item {
        #[allow(clippy::cast_precision_loss)]
        let res = self.clone().map(|s| {
            s / (NUM::from_usize(collected))
                .expect("bucket size to large, can not be represented by type")
        });
        for i in self {
            *i = NUM::zero();
        }
        res
    }
}

/// Try implement resample state for a type, needs the type to implement:
/// - [`num_traits::FromPrimitive`], specifically needs `from_usize`
/// - [`num_traits::Zero`]
/// - [`Div<Self, Output = Self>`](std::ops::Div)
/// - [`AddAssign<Self>`](std::ops::AddAssign)
/// - [`Debug`](core::fmt::Debug)
/// - [`Clone`](core::clone::Clone)
#[macro_export]
macro_rules! impl_resample_state {
    ($NUM:ty) => {
        impl ResampleState for $NUM {
            type Item = $NUM;

            fn add(&mut self, item: Self::Item) {
                *self += item;
            }
            fn finish(&mut self, collected: usize) -> Self::Item {
                use num_traits::FromPrimitive;
                use num_traits::Zero;

                #[allow(clippy::cast_precision_loss)]
                let res = *self
                    / <$NUM>::from_usize(collected)
                        .expect("bucket size to large, can not be represented by type");
                *self = <$NUM>::zero();
                res
            }
        }
    };
}

pub use impl_resample_state;

impl_resample_state!(f32);
impl_resample_state!(f64);
impl_resample_state!(usize);
impl_resample_state!(u128);
impl_resample_state!(u64);
impl_resample_state!(u32);
impl_resample_state!(u16);
impl_resample_state!(u8);
impl_resample_state!(isize);
impl_resample_state!(i128);
impl_resample_state!(i64);
impl_resample_state!(i32);
impl_resample_state!(i16);
impl_resample_state!(i8);
