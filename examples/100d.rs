use byteseries::{combiners, new_sampler, Decoder, Series};
use chrono::Duration;
use simplelog::{LevelFilter, Config, SimpleLogger};

#[derive(Debug)]
struct TestDecoder {}

impl Decoder<f32> for TestDecoder {
    fn decode(&mut self, bytes: &[u8], out: &mut Vec<f32>) {
        out.extend(bytes.iter().map(|b| *b as f32));
    }
}

fn main() {
    SimpleLogger::init(LevelFilter::Trace, Config::default()).unwrap();

    let mut decoder = TestDecoder {};
    let mut ts = Series::open("data/2", 103).unwrap();
    let (endtime, _data) = ts.last_line(&mut decoder).unwrap();

    let bin = combiners::SampleBin::new(5);
    let combiner = combiners::Mean::new(bin);
    let mut sampler = new_sampler(&ts, decoder)
        .points(10)
        .start(endtime - Duration::days(100))
        .stop(endtime)
        .build_with_combiner(combiner)
        .unwrap();
    sampler.sample_all().unwrap();
}
