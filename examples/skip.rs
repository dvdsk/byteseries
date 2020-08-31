use byteseries::{EmptyDecoder, new_sampler, Series};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};

fn main() {
    let mut decoder = EmptyDecoder {};
    let mut ts = Series::open("examples/data/4", 24).unwrap();
    let (endtime, _data) = ts.last_line(&mut decoder).unwrap();
    let endtime = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(endtime, 0), Utc);

    let mut sampler = new_sampler(&ts, &mut decoder)
        .points(10)
        .start(endtime - Duration::hours(90))
        .stop(endtime)
        .build()
        .unwrap();
    sampler.sample_all().unwrap();
}
