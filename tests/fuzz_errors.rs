use std::collections::VecDeque;

use byteseries::ByteSeries;
use byteseries_test_support::{print_recent_actions, Action, CheckError, Checker};
use temp_dir::TempDir;

#[test]
fn one() {
    let mut recent_actions = VecDeque::new();

    let actions = [
        // last to apply first
        // Action::ReOpenTruncated,
        Action::WriteShortInterval {
            seed: 14520734064206991880,
            num_lines: 9042,
            minimum: 28754,
        },
        Action::ReOpen,
        Action::WriteShortInterval {
            seed: 14004595328206938198,
            num_lines: 6412,
            minimum: 0,
        },
    ];

    let test_dir = TempDir::new().unwrap();
    let test_path = test_dir.child("fuzz_one");

    let (mut series, _) = ByteSeries::builder()
        .payload_size(0)
        .create_new(true)
        .with_any_header()
        .open(&test_path)
        .unwrap();

    let (first_seed, first_minimum) = actions
        .iter()
        .rev()
        .filter_map(|action| match action {
            Action::WriteShortInterval { seed, minimum, .. } => Some((seed, minimum)),
            Action::WriteLongInterval { .. }
            | Action::ReOpen
            | Action::ReOpenTruncated => None,
        })
        .next()
        .unwrap();
    let mut checker = Checker::init_from(*first_seed, *first_minimum);

    for action in actions.iter().rev() {
        recent_actions.push_front(action.clone());

        series = action.perform(series, &recent_actions, &test_path, &mut 0);
        checker.since_last_check.push(action.clone());

        if let Err(CheckError {
            expected,
            read,
            ts_before,
        }) = checker.check(&mut series)
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
