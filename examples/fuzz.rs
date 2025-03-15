use std::collections::VecDeque;

use byteseries::ByteSeries;
use rand::{Rng, SeedableRng};
use rand_xoshiro::Xoshiro128StarStar;
use temp_dir::TempDir;

use byteseries_test_support::{print_recent_actions, Action, CheckError, Checker};

struct ActionGen {
    rng: Xoshiro128StarStar,
}

impl ActionGen {
    fn new() -> Self {
        Self {
            rng: Xoshiro128StarStar::seed_from_u64(0),
        }
    }
    fn next(&mut self, minimum: u64) -> Action {
        match self.rng.random_range(0..100) {
            0..50 => Action::WriteShortInterval {
                seed: self.rng.random(),
                num_lines: self.rng.random_range(1..10_000),
                minimum,
            },
            50..51 => Action::WriteLongInterval {
                interval: self.rng.random_range(1_000..100_000),
                minimum,
            },
            51..90 => Action::ReOpen,
            90..100 => Action::ReOpenTruncated,
            100u8.. => unreachable!(),
        }
    }
}

fn main() {
    color_eyre::install().unwrap();

    let mut curr_minimum = 0;
    let mut action_gen = ActionGen::new();
    let mut recent_actions = VecDeque::new();
    let mut checker = Checker::init_from(0, curr_minimum);
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

        let action = action_gen.next(curr_minimum);
        recent_actions.push_front(action.clone());
        recent_actions.truncate(10);

        series = action.perform(series, &recent_actions, &test_path, &mut curr_minimum);
        progress.update(&action);
        checker.since_last_check.push(action);

        if let Err(CheckError {
            expected,
            read,
            ts_before,
        }) = checker.check_once_in_a_while(&mut series)
        {
            eprintln!("");
            eprintln!("Expected {expected} instead read {read:?}");
            eprintln!("Timestamps just before: {ts_before:?}");
            eprintln!("Recent actions:");
            print_recent_actions(&mut recent_actions, false);
            panic!();
        }
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
