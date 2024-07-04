use crate::ResampleState;

mod empty;
pub(crate) use empty::EmptyResampler;

impl<const N: usize> ResampleState for [f32; N] {
    type Item = [f32; N];

    fn add(&mut self, item: Self::Item) {
        for (state, new) in self.iter_mut().zip(item) {
            *state += new;
        }
    }

    fn finish(&mut self, collected: usize) -> Self::Item {
        #[allow(clippy::cast_precision_loss)]
        let res = self.map(|s| s / (collected as f32));
        for i in self {
            *i = 0.0;
        }
        res
    }
}

impl ResampleState for f32 {
    type Item = f32;

    fn add(&mut self, item: Self::Item) {
        *self += item;
    }
    fn finish(&mut self, collected: usize) -> Self::Item {
        #[allow(clippy::cast_precision_loss)]
        let res = *self / collected as f32;
        *self = 0.0;
        res
    }
}
