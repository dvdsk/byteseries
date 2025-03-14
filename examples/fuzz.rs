use std::collections::VecDeque;
use std::mem;
use std::path::Path;

use byteseries::{ByteSeries, Decoder};
use rand::{Rng, SeedableRng};
use rand_distr::Distribution;
use rand_xoshiro::Xoshiro128StarStar;
use temp_dir::TempDir;

#[derive(Debug)]
struct CopyDecoder;

impl Decoder for CopyDecoder {
    type Item = Vec<u8>;

    fn decode_payload(&mut self, payload: &[u8]) -> Self::Item {
        payload.to_vec()
    }
}

#[derive(Debug, Clone)]
enum Action {
    // 50% chance
    WriteShortInterval { seed: u64, num_lines: usize },
    // 1% chance
    WriteLongInterval { interval: u32 },
    // 40% chance
    ReOpen,
    // 9% chance
    ReOpenTruncated,
}
impl Action {
    fn is_truncate(&self) -> bool {
        matches!(self, Action::ReOpenTruncated)
    }
}

struct ActionGen {
    rng: Xoshiro128StarStar,
}

impl ActionGen {
    fn new() -> Self {
        Self {
            rng: Xoshiro128StarStar::seed_from_u64(0),
        }
    }
    fn next(&mut self) -> Action {
        match self.rng.random_range(0..100) {
            0..50 => Action::WriteShortInterval {
                seed: self.rng.random(),
                num_lines: self.rng.random_range(1..10_000),
            },
            50..51 => Action::WriteLongInterval {
                interval: self.rng.random_range(1_000..100_000),
            },
            51..90 => Action::ReOpen,
            90..100 => Action::ReOpenTruncated,
            100u8.. => unreachable!(),
        }
    }
}

fn main() {
    color_eyre::install().unwrap();

    let mut action_gen = ActionGen::new();
    let mut recent_actions = VecDeque::new();
    let mut ts_gen = TsGen::new();
    let mut checker = Checker::new();
    let mut progress = Progress::default();

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("test_append_hashes_then_verify");
    let (mut series, _) = ByteSeries::builder()
        .payload_size(0)
        .create_new(true)
        .with_any_header()
        .open(&test_path)
        .unwrap();

    loop {
        progress.print_report_once_in_a_while();

        let action = action_gen.next();
        recent_actions.push_front(action.clone());
        recent_actions.truncate(10);

        match action {
            Action::WriteShortInterval { seed, num_lines } => {
                ts_gen.reset_rng(seed);
                add_lines(&mut ts_gen, &mut series, num_lines, &recent_actions);
            }
            Action::WriteLongInterval { interval } => {
                add_line(&mut ts_gen, &mut series, interval)
            }
            Action::ReOpen => series = re_open(series, &test_path),
            Action::ReOpenTruncated => series = re_open_trunctated(series, &test_path),
        };
        progress.update(&action);
        checker.since_last_check.push(action);

        if let Err(CheckError { expected, read }) =
            checker.check_once_in_a_while(&mut series)
        {
            eprintln!("");
            eprintln!("Expected {expected} instead read {read:?}");
            eprintln!("Recent actions:");
            print_recent_actions(&mut recent_actions, false);
            panic!();
        }
    }
}

fn print_recent_actions(recent_actions: &VecDeque<Action>, failed_mid_action: bool) {
    for (i, action) in recent_actions.iter().rev().enumerate() {
        if i == 0 && failed_mid_action {
            eprint!("\t{} (current)", i + 1);
        } else if i == 0 {
            eprint!("\t{} (last)", i + 1);
        } else {
            eprint!("\t{} ", i + 1);
        }

        match action {
            Action::WriteShortInterval { seed, num_lines } => {
                eprint!("wrote {num_lines} lines, ts gen seed: {seed}")
            }
            Action::WriteLongInterval { interval } => {
                eprint!("wrote 1 line at {interval} from last")
            }
            Action::ReOpen => eprint!("closed then opened "),
            Action::ReOpenTruncated => eprint!("closed damaged then opened"),
        }
        eprint!("\n");
    }
}

#[derive(Debug, Default)]
struct Progress {
    short_interval: u64,
    long_interval: u64,
    re_opened: u64,
    re_openend_trunctated: u64,
    counter: usize,
}

impl Progress {
    fn update(&mut self, action: &Action) {
        match action {
            Action::WriteShortInterval { num_lines, .. } => {
                self.short_interval += *num_lines as u64
            }
            Action::ReOpen => self.re_opened += 1,
            Action::ReOpenTruncated => self.re_openend_trunctated += 1,
            Action::WriteLongInterval { .. } => self.long_interval += 1,
        }
    }
    fn print_report_once_in_a_while(&mut self) {
        self.counter += 1;
        if self.counter % 10 != 0 {
            return;
        }

        eprint!(
            "\rFuzzing, wrote {} lines, re-opened ({}/{}) (normal/truncated)",
            self.short_interval, self.re_opened, self.re_openend_trunctated
        );
    }
}

struct Checker {
    ts_gen: TsGen,
    since_last_check: Vec<Action>,
    last_timestamp_read: Option<u64>,
    timestamps: Vec<u64>,
    data: Vec<Vec<u8>>,
    counter: usize,
}

struct CheckError {
    expected: u64,
    read: Option<u64>,
}

impl Checker {
    fn new() -> Self {
        Self {
            ts_gen: TsGen::new(),
            timestamps: Vec::new(),
            data: Vec::new(),
            counter: 0,
            since_last_check: Vec::new(),
            last_timestamp_read: None,
        }
    }

    fn check_once_in_a_while(
        &mut self,
        series: &mut ByteSeries,
    ) -> Result<(), CheckError> {
        if self.counter % 10 == 0 {
            self.check(series)
        } else {
            Ok(())
        }
    }

    fn check(&mut self, series: &mut ByteSeries) -> Result<(), CheckError> {
        use Action as A;
        let actions = mem::take(&mut self.since_last_check);
        for window in actions.windows(2) {
            let [action, next_action] = [&window[0], &window[1]];
            match action {
                A::WriteShortInterval { seed, num_lines } => {
                    self.check_short_interval(*seed, *num_lines, series, next_action)?;
                }
                A::WriteLongInterval { interval } => {
                    self.check_long_interval(series, *interval, next_action)?;
                }
                A::ReOpenTruncated | A::ReOpen => (),
            }

            self.timestamps.clear();
            self.data.clear();
        }
        Ok(())
    }

    fn check_long_interval(
        &mut self,
        series: &mut ByteSeries,
        interval: u32,
        next_action: &Action,
    ) -> Result<(), CheckError> {
        let start = self.last_timestamp_read.unwrap();
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
        let end = self.ts_gen.clone().nth(num_lines).unwrap();
        series
            .read_first_n(
                num_lines as usize,
                &mut CopyDecoder,
                start..end,
                &mut self.timestamps,
                &mut self.data,
            )
            .unwrap();

        self.last_timestamp_read = Some(*self.timestamps.last().unwrap());
        let mut timestamps = self.timestamps.iter();

        for _ in 0..(num_lines - 1) {
            let read = timestamps.next().copied();
            if read != Some(self.ts_gen.next()) {
                return Err(CheckError {
                    expected: self.ts_gen.next(),
                    read,
                });
            }
        }

        // check last element that should have been inserted
        match timestamps.next().copied() {
            Some(ts) if ts == self.ts_gen.peek() => Ok(()),
            Some(read) => Err(CheckError {
                expected: self.ts_gen.next(),
                read: Some(read),
            }),
            None if next_action.is_truncate() => Ok(()),
            None => Err(CheckError {
                expected: self.ts_gen.peek(),
                read: None,
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

fn add_lines(ts_gen: &mut TsGen, series: &mut ByteSeries, num_lines: usize, recent_actions: &VecDeque<Action>) {
    for _ in 0..num_lines {
        let ts_before = ts_gen.last_generated;
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
}

fn add_line(ts_gen: &mut TsGen, series: &mut ByteSeries, interval: u32) {
    let ts = ts_gen.last_generated + interval as u64;
    ts_gen.last_generated = ts;
    series.push_line(ts, &[]).unwrap()
}

#[derive(Debug, Clone)]
struct TsGen {
    rng: Xoshiro128StarStar,
    last_generated: u64,
}

impl Iterator for TsGen {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.next())
    }
}

impl TsGen {
    fn new() -> Self {
        Self {
            rng: Xoshiro128StarStar::seed_from_u64(0),
            last_generated: 0,
        }
    }

    fn next(&mut self) -> u64 {
        let start = self.last_generated + 1;

        let distr = rand_distr::Normal::new(0.0, 0.5).unwrap();
        let random: f32 = distr.sample(&mut self.rng);
        let interval = (random.abs() * 10.0) as u64;
        let next = start + interval;

        self.last_generated = next;
        next
    }

    fn peek(&self) -> u64 {
        self.clone().next()
    }

    fn reset_rng(&mut self, seed: u64) {
        self.rng = Xoshiro128StarStar::seed_from_u64(seed);
    }
}
