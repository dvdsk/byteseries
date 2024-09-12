use byteseries::downsample;
use byteseries::ByteSeries;
use copy_dir::copy_dir;
use shared::FakeFloatResampler;
use temp_dir::TempDir;

mod shared;
use shared::setup_tracing;

#[test]
fn reported_crash1() {
    setup_tracing();

    let test_dir = TempDir::new().unwrap();
    copy_dir(
        "assets/reported_crash1",
        test_dir.path().join("reported_crash1"),
    )
    .unwrap();

    let resample_configs = vec![
        downsample::Config {
            max_gap: None,
            bucket_size: 10,
        },
        downsample::Config {
            max_gap: None,
            bucket_size: 100,
        },
        downsample::Config {
            max_gap: None,
            bucket_size: 1000,
        },
    ];

    let test_path = test_dir.child("reported_crash1").join("mhz14");
    let mut bs = ByteSeries::builder()
        .payload_size(2)
        .with_downsampled_cache(FakeFloatResampler { payload_size: 2 }, resample_configs)
        .with_any_header()
        .open(&test_path)
        .unwrap();

    let mut resampler = FakeFloatResampler { payload_size: 2 };

    bs.read_n(
        300,
        1726050193..=1726136593,
        &mut resampler,
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .unwrap();
}

/*
 * Sanitized output from crash
 *
data-store: The application panicked (crashed).
data-store: Message:  assertion failed: end_time <= MAX_SMALL_TS
data-store: Location: /home/david/.cargo/git/checkouts/byteseries-91c191b460a7dcf6/03fd661/src/seek.rs:148
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ SPANTRACE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
0: byteseries::seek::refine with
    self=RoughPos {
    start_ts: 1726050193,
    start_search_area: Window(LinePos(462228), MetaPos(475200)),
    end_ts: 1726136593,
    end_search_area: TillEnd(LinePos(475212)),
    start_section_full_ts: 1725992708,
    end_section_full_ts: 1726071056
    }
    data=Data {
        file_handle: FileWithInlineMeta {
            file_handle: OffsetFile {
                handle: File {
                    fd: 32,
                    path:"/home/ha/data/largebedroom/bed/mhz14.byteseries",
                    read: true,
                    write: true
                },
            offset: 1439
            },
            payload_size: PayloadSize(2)
        },
        index: Index {
            file: OffsetFile {
                handle: File {
                    fd: 33,
                    path: "/home/ha/data/largebedroom/bed/mhz14.byteseries_index",
                    read: true,
                    write: true
                },
                offset: 1439
            },
            # entries: 16,
            last_timestamp: Some(1726071056)
        },
        payload_size: PayloadSize(2),
        data_len: 506024,
        last_time: Some(1726136589)
    }
at byteseries-91c191b460a7dcf6/03fd661/src/seek.rs:106

1: byteseries::series::read_n with
    n=300
    range="Included(1726050193)..Included(1726136593)"
at byteseries-91c191b460a7dcf6/03fd661/src/series.rs:334

2: data_store::server::db::series::read with readings=[LargeBedroom(Bed(Co2(0)))] start=2024-09-11T10:23:13Z end=2024-09-12T10:23:13.038583722Z n=300
at crates/data-store/src/server/db/series.rs:160

3: rpc::server::handle_client with client_name="sensor-tui@pop-os"
at crates/rpc/src/server.rs:121

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ BACKTRACE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
data-store:                                 ⋮ 8 frames hidden ⋮
data-store:    9: core::panicking::panic::hcfd0e463f23c4be3
data-store:       at /rustc/3f5fd8dd41153bc5fdca9427e9e05be2c767ba23/library/core/src/panicking.rs:146
data-store:   10: byteseries::seek::RoughPos::end_small_ts::h761d8e05c1118217
data-store:       at /home/david/.cargo/git/checkouts/byteseries-91c191b460a7dcf6/03fd661/src/seek.rs:148
data-store:   11: byteseries::seek::RoughPos::refine::h8c4d06f679ff0fce
data-store:       at /home/david/.cargo/git/checkouts/byteseries-91c191b460a7dcf6/03fd661/src/seek.rs:<unknown line>
data-store:   12: byteseries::series::ByteSeries::read_n::hf523fea67c4cc044
*/
