use crate::{Decoder, Encoder, ResampleState, Resampler};

#[derive(Debug, Clone)]
pub(crate) struct EmptyResampler;

impl Decoder for EmptyResampler {
    type Item = EmptySample;

    fn decode_payload(&mut self, _: &[u8]) -> Self::Item {
        EmptySample
    }
}

impl Encoder for EmptyResampler {
    type Item = EmptySample;

    fn encode_item(&mut self, _: &Self::Item) -> Vec<u8> {
        Vec::new()
    }
}

impl Resampler for EmptyResampler {
    type State = EmptySampleState;
    fn state(&self) -> Self::State {
        EmptySampleState
    }
}

#[derive(Debug)]
pub(crate) struct EmptySample;

#[derive(Debug)]
pub(crate) struct EmptySampleState;

impl ResampleState for EmptySampleState {
    type Item = EmptySample;

    fn add(&mut self, _: Self::Item) {}
    fn finish(&mut self, _: usize) -> Self::Item {
        EmptySample
    }
}
