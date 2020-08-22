use byteseries::{Series, EmptyDecoder};
use chrono::Duration;

fn main() {
    let decoder = EmptyDecoder();
    let mut ts = Series::open("examples/data/4", 24).unwrap();
    let (endtime, data) = ts.last_line(decoder).unwrap();
    dbg!(endtime);

}
