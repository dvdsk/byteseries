use byteseries::{new_sampler, ByteSeries, EmptyDecoder};
use time::Duration;

fn main() {
    let mut decoder = EmptyDecoder {};
    let mut ts = ByteSeries::open("examples/data/4", 24).unwrap();
    let (endtime, _data) = ts.last_line(&mut decoder).unwrap();

    let mut sampler = new_sampler(ts, decoder)
        .points(10)
        .start(endtime.saturating_sub(Duration::hours(90)))
        .stop(endtime)
        .build()
        .unwrap();
    sampler.sample_all().unwrap();
}
