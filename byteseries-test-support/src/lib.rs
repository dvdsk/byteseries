use std::collections::VecDeque;
use std::mem;
use std::path::Path;

use byteseries::{ByteSeries, Decoder};
use rand::SeedableRng;
use rand_distr::Distribution;
use rand_xoshiro::Xoshiro128StarStar;

#[derive(Debug)]
pub struct CopyDecoder;

impl Decoder for CopyDecoder {
    type Item = Vec<u8>;

    fn decode_payload(&mut self, payload: &[u8]) -> Self::Item {
        payload.to_vec()
    }
}

#[derive(Debug, Clone)]
pub enum Action {
    // 50% chance
    WriteShortInterval {
        seed: u64,
        num_lines: usize,
        minimum: u64,
    },
    // 1% chance
    WriteLongInterval {
        interval: u32,
        minimum: u64,
    },
    // 40% chance
    ReOpen,
    // 9% chance
    ReOpenTruncated,
}

impl Action {
    pub fn is_truncate(&self) -> bool {
        matches!(self, Action::ReOpenTruncated)
    }

    pub fn perform(
        &self,
        mut series: ByteSeries,
        recent_actions: &VecDeque<Action>,
        test_path: &Path,
        curr_minimum: &mut u64,
    ) -> ByteSeries {
        match self {
            Action::WriteShortInterval {
                seed,
                num_lines,
                minimum,
            } => {
                let ts_gen = TsGen::from_seed_and_minimum(*seed, *minimum);
                *curr_minimum =
                    add_lines(ts_gen, &mut series, *num_lines, &recent_actions);
            }
            Action::WriteLongInterval { interval, minimum } => {
                *curr_minimum = add_line(&mut series, *interval, *minimum);
            }
            Action::ReOpen => series = re_open(series, &test_path),
            Action::ReOpenTruncated => series = re_open_trunctated(series, &test_path),
        };
        series
    }
}

pub struct Checker {
    ts_gen: TsGen,
    pub since_last_check: Vec<Action>,
    last_timestamp_read: Option<u64>,
    timestamps: Vec<u64>,
    data: Vec<Vec<u8>>,
    counter: usize,
}

pub struct CheckError {
    pub expected: u64,
    pub read: Option<u64>,
    pub ts_before: Vec<u64>,
}

impl Checker {
    pub fn init_from(seed: u64, minimum: u64) -> Self {
        Self {
            ts_gen: TsGen::from_seed_and_minimum(seed, minimum),
            timestamps: Vec::new(),
            data: Vec::new(),
            counter: 0,
            since_last_check: Vec::new(),
            last_timestamp_read: None,
        }
    }

    pub fn check_once_in_a_while(
        &mut self,
        series: &mut ByteSeries,
    ) -> Result<(), CheckError> {
        self.counter += 1;
        if self.counter % 10 == 0 {
            self.check(series)
        } else {
            Ok(())
        }
    }

    pub fn check(&mut self, series: &mut ByteSeries) -> Result<(), CheckError> {
        use Action as A;
        let actions = mem::take(&mut self.since_last_check);
        for window in actions.windows(2) {
            let [action, next_action] = [&window[0], &window[1]];
            match action {
                A::WriteShortInterval {
                    seed, num_lines, ..
                } => {
                    self.check_short_interval(*seed, *num_lines, series, next_action)?;
                }
                A::WriteLongInterval { interval, minimum } => {
                    self.check_long_interval(series, *interval, *minimum, next_action)?;
                }
                A::ReOpenTruncated | A::ReOpen => (),
            }

            self.timestamps.clear();
            self.data.clear();
        }

        match actions.last().unwrap() {
            A::WriteShortInterval {
                seed, num_lines, ..
            } => self.check_short_interval(*seed, *num_lines, series, &A::ReOpen)?,
            A::WriteLongInterval { interval, minimum } => {
                self.check_long_interval(series, *interval, *minimum, &A::ReOpen)?
            }
            A::ReOpen | A::ReOpenTruncated => (),
        }
        self.timestamps.clear();
        self.data.clear();
        Ok(())
    }

    fn check_long_interval(
        &mut self,
        series: &mut ByteSeries,
        interval: u32,
        minimum: u64,
        next_action: &Action,
    ) -> Result<(), CheckError> {
        let start = self.last_timestamp_read.unwrap_or(minimum);
        match series.read_first_n(
            1,
            &mut CopyDecoder,
            start..start + interval as u64 + 1,
            &mut self.timestamps,
            &mut self.data,
        ) {
            Ok(()) => (),
            Err(e) => panic!("{e}"),
        };

        if self.timestamps[0] != start + interval as u64 && !next_action.is_truncate() {
            Err(CheckError {
                expected: self.ts_gen.next(),
                read: self.timestamps.first().copied(),
                ts_before: Vec::new(),
            })
        } else {
            Ok(())
        }
    }

    fn check_short_interval(
        &mut self,
        seed: u64,
        num_lines: usize,
        series: &mut ByteSeries,
        next_action: &Action,
    ) -> Result<(), CheckError> {
        self.ts_gen.reset_rng(seed);
        let start = self.ts_gen.peek();

        self.timestamps.clear();
        self.data.clear();
        series
            .read_first_n(
                num_lines as usize,
                &mut CopyDecoder,
                start..,
                &mut self.timestamps,
                &mut self.data,
            )
            .unwrap();

        self.last_timestamp_read = Some(*self.timestamps.last().unwrap());
        let mut timestamps = self.timestamps.iter();

        for i in 0..(num_lines - 1) {
            let read = timestamps.next().copied();
            if read != Some(self.ts_gen.next()) {
                return Err(CheckError {
                    expected: self.ts_gen.next(),
                    read,
                    ts_before: self.timestamps[i.saturating_sub(10)..i].to_vec(),
                });
            }
        }

        // check last element that should have been inserted
        match timestamps.next().copied() {
            Some(ts) if ts == self.ts_gen.peek() => {
                self.ts_gen.next();
                Ok(())
            }
            Some(read) => Err(CheckError {
                expected: self.ts_gen.peek(),
                read: Some(read),
                ts_before: self.timestamps[num_lines.saturating_sub(10)..].to_vec(),
            }),
            None if next_action.is_truncate() => Ok(()),
            None => Err(CheckError {
                expected: self.ts_gen.peek(),
                read: None,
                ts_before: self.timestamps[num_lines.saturating_sub(10)..].to_vec(),
            }),
        }
    }
}

fn re_open_trunctated(series: ByteSeries, test_path: &Path) -> ByteSeries {
    drop(series);

    let series_path = test_path.with_extension("byteseries");
    let len = std::fs::metadata(&series_path).unwrap().len();
    let series_file = std::fs::OpenOptions::new()
        .write(true)
        .open(series_path)
        .unwrap();
    series_file.set_len(len - 2).unwrap();

    let (series, _) = ByteSeries::builder()
        .payload_size(0)
        .create_new(false)
        .with_any_header()
        .open(test_path)
        .unwrap();
    series
}

fn re_open(series: ByteSeries, test_path: &Path) -> ByteSeries {
    drop(series);

    let (series, _) = ByteSeries::builder()
        .payload_size(0)
        .create_new(false)
        .with_any_header()
        .open(test_path)
        .unwrap();
    series
}

fn add_lines(
    mut ts_gen: TsGen,
    series: &mut ByteSeries,
    num_lines: usize,
    recent_actions: &VecDeque<Action>,
) -> u64 {
    for _ in 0..num_lines {
        let ts_before = ts_gen.minimum;
        let ts = ts_gen.next();
        if let Err(e) = series.push_line(ts, &[]) {
            eprintln!("");
            eprintln!("{e}");
            eprintln!("Generated ts: {ts}, ts generated before that: {ts_before}");
            eprintln!("Recent actions:");
            print_recent_actions(&recent_actions, true);
            panic!();
        }
    }
    ts_gen.minimum
}

fn add_line(series: &mut ByteSeries, interval: u32, minimum: u64) -> u64 {
    let ts = minimum + interval as u64;
    series.push_line(ts, &[]).unwrap();
    ts
}

#[derive(Debug, Clone)]
struct TsGen {
    rng: Xoshiro128StarStar,
    minimum: u64,
}

impl Iterator for TsGen {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.next())
    }
}

impl TsGen {
    fn from_seed_and_minimum(seed: u64, minimum: u64) -> Self {
        Self {
            rng: Xoshiro128StarStar::seed_from_u64(seed),
            minimum,
        }
    }

    fn next(&mut self) -> u64 {
        let start = self.minimum + 1;

        let distr = rand_distr::Normal::new(0.0, 0.5).unwrap();
        let random: f32 = distr.sample(&mut self.rng);
        let interval = (random.abs() * 10.0) as u64;
        let next = start + interval;

        self.minimum = next;
        next
    }

    fn peek(&self) -> u64 {
        self.clone().next()
    }

    fn reset_rng(&mut self, seed: u64) {
        self.rng = Xoshiro128StarStar::seed_from_u64(seed);
    }
}

pub fn print_recent_actions(recent_actions: &VecDeque<Action>, failed_mid_action: bool) {
    for (i, action) in recent_actions.iter().enumerate() {
        if i == 0 && failed_mid_action {
            eprint!("\t{} (current) ", i + 1);
        } else if i == 0 {
            eprint!("\t{} (last) ", i + 1);
        } else {
            eprint!("\t{} ", i + 1);
        }

        match action {
            Action::WriteShortInterval {
                seed,
                num_lines,
                minimum,
            } => {
                let mut gen = TsGen::from_seed_and_minimum(*seed, *minimum);
                eprint!(
                    "{num_lines} lines, seed: {seed}, minimum: {minimum}, ts range: {}..{}",
                    gen.next(),
                    gen.nth(num_lines - 2).unwrap(),
                )
            }
            Action::WriteLongInterval { interval, .. } => {
                eprint!("wrote 1 line at {interval} from last")
            }
            Action::ReOpen => eprint!("closed then opened "),
            Action::ReOpenTruncated => eprint!("closed damaged then opened"),
        }
        eprint!("\n");
    }
}
