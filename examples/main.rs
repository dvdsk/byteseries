use byteseries::{EmptyDecoder, SamplerBuilder, Series};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};

fn main() {
    let mut decoder = EmptyDecoder {};
    let mut ts = Series::open("examples/data/4", 24).unwrap();
    let (endtime, data) = ts.last_line(&mut decoder).unwrap();
    dbg!(endtime);
    let endtime = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(endtime, 0), Utc);
    dbg!(endtime);

    let mut sampler = SamplerBuilder::new(&ts, &mut decoder)
        .points(10)
        .start(endtime - Duration::hours(90))
        .stop(endtime)
        .finish()
        .unwrap();

    sampler.sample(10);
}
