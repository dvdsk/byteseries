use byteseries::{EmptyDecoder, new_sampler, Series, combiners};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};

fn main() {
    let mut decoder = EmptyDecoder {};
    let mut ts = Series::open("examples/data/4", 24).unwrap();
    let (endtime, _data) = ts.last_line(&mut decoder).unwrap();
    let endtime = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(endtime, 0), Utc);
    
    let combiner = combiners::Mean::default();
    let mut sampler = new_sampler(&ts, &mut decoder)
        .points(10)
        .per_sample(10)
        .start(endtime - Duration::hours(90))
        .stop(endtime)
        .build_with_combiner(combiner)
        .unwrap();
    sampler.sample_all().unwrap();
}
    
