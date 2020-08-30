use super::Decoder;

#[derive(Debug, Clone)]
pub struct EmptyDecoder {}
impl Decoder<u8> for EmptyDecoder {
    fn decode(&mut self, bytes: &[u8], out: &mut Vec<u8>) {
        out.extend_from_slice(&bytes[2..]);
    }
}
