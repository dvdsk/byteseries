use byteseries::ByteSeries;
use byteseries_test_support::{print_recent_actions, Action, CheckError, Checker, RecentActions};
use temp_dir::TempDir;

fn test_fuzz(actions: &[Action]) {
    let mut recent_actions = RecentActions::with_max_length(10);
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
        recent_actions.push(action.clone());

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

#[test]
fn one() {
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
    test_fuzz(&actions);
}

#[test]
fn two() {
    let actions = [
        // last to apply first
        Action::ReOpen,
        Action::WriteShortInterval {
            seed: 7275941297628290141,
            num_lines: 5017,
            minimum: 106013,
        },
        Action::ReOpenTruncated,
        Action::ReOpen,
        Action::ReOpen,
        Action::ReOpen,
        Action::ReOpen,
        Action::WriteShortInterval {
            seed: 6377996370679194661,
            num_lines: 8190,
            minimum: 69419,
        },
        Action::ReOpenTruncated,
        Action::ReOpen,
        Action::ReOpenTruncated,
        Action::WriteShortInterval {
            seed: 14520734064206991880,
            num_lines: 9042,
            minimum: 28754,
        },
        Action::ReOpen,
        Action::ReOpen,
        Action::ReOpen,
        Action::WriteShortInterval {
            seed: 14004595328206938198,
            num_lines: 6412,
            minimum: 0,
        },
        Action::ReOpen,
        Action::ReOpen,
        Action::ReOpen,
        Action::ReOpen,
    ];
    test_fuzz(&actions);
}
