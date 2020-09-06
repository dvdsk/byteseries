use std::clone::Clone;
use std::fmt::Debug;

pub trait Decoder<T>: Debug
where
    T: Debug + Clone,
{
    fn decode(&mut self, bytes: &[u8], out: &mut Vec<T>);
    fn decoded(&mut self, bytes: &[u8]) -> Vec<T> {
        let mut values = Vec::new();
        self.decode(bytes, &mut values);
        values
    }
}

#[derive(Debug, Clone)]
pub struct EmptyDecoder {}
impl Decoder<u8> for EmptyDecoder {
    fn decode(&mut self, bytes: &[u8], out: &mut Vec<u8>) {
        out.extend_from_slice(&bytes[2..]);
    }
}
