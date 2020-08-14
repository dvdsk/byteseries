use byteseries::Timeseries;
use chrono::Duration;

fn main() {
    let mut ts = Timeseries::open("examples/data/4", 24).unwrap();
    let (endtime, data) = ts.decode_last_line().unwrap();
    dbg!(endtime);

    let bounds = ts.get_bounds(endtime - Duration::seconds(40), endtime);
    dbg!(bounds);

    let start_byte = 0;
    //let decoded = ts.decode_time(1,&mut start_byte, 0+ts.full_line_size as u64, decode_params);
}
