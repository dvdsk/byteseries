use byteseries::ByteSeries;
use copy_dir::copy_dir;
use shared::FakeFloatResampler;
use temp_dir::TempDir;

mod shared;
use shared::setup_tracing;

#[test]
fn reported_crash2() {
    setup_tracing();

    let test_dir = TempDir::new().unwrap();
    copy_dir(
        "assets/reported_crash2",
        test_dir.path().join("reported_crash2"),
    )
    .unwrap();

    let test_path = test_dir.child("reported_crash2").join("sps30");
    let (mut bs, _) = ByteSeries::builder()
        .payload_size(204)
        .with_any_header()
        .open(&test_path)
        .unwrap();

    bs.read_all(
        1726145167..=1726577167,
        &mut FakeFloatResampler { payload_size: 204 },
        &mut Vec::new(),
        &mut Vec::new(),
        false,
    )
    .unwrap();
}

/*
 * Sanitized output from crash
 *
The application panicked (crashed).
Message:  assertion failed: end_time <= MAX_SMALL_TS
Location: byteseries-91c191b460a7dcf6/03fd661/src/seek.rs:148
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ SPANTRACE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   0: byteseries::seek::refine with
    self=RoughPos {
      start_ts: 1726145167,
      start_search_area: Window(LinePos(39964),
      MetaPos(40788)),
      end_ts: 1726577167,
      end_search_area: TillEnd(LinePos(42024)),
      start_section_full_ts: 1726097463,
      end_section_full_ts: 1726444746
    }
    data=Data {
        file_handle: FileWithInlineMeta {
            file_handle: OffsetFile {
                handle: File {
                    fd: 11,
                    path: "/home/ha/sensor-logs/largebedroom/bed/sps30.byteseries",
                    read: true,
                    write: true
                },
                offset: 1315
            },
            payload_size: PayloadSize(204)
        },
        index: Index {
            file: OffsetFile {
                handle: File {
                    fd: 12,
                    path: "/home/ha/sensor-logs/largebedroom/bed/sps30.byteseries_index",
                    read: true,
                    write: true
                },
                offset: 1315
                },
                # entries: 6,
                last_timestamp: Some(1726444746)
            },
        payload_size: PayloadSize(204),
        data_len: 42230,
        last_time: Some(1726444746)
    }

      at byteseries-91c191b460a7dcf6/03fd661/src/seek.rs:106
   1: rpc::server::handle_client with client_name="sensor-tui@pop-os"
      at crates/rpc/src/server.rs:121
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ BACKTRACE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                ⋮ 8 frames hidden ⋮
   9: core::panicking::panic::hcfd0e463f23c4be3
      at /rustc/3f5fd8dd41153bc5fdca9427e9e05be2c767ba23/library/core/src/panicking.rs:146
  10: byteseries::seek::RoughPos::end_small_ts::h761d8e05c1118217
      at /home/david/.cargo/git/checkouts/byteseries-91c191b460a7dcf6/03fd661/src/seek.rs:148
  11: byteseries::seek::RoughPos::refine::h8c4d06f679ff0fce
      at /home/david/.cargo/git/checkouts/byteseries-91c191b460a7dcf6/03fd661/src/seek.rs:<unknown line>
  12: byteseries::series::ByteSeries::n_lines_between::h06ec040a73cc906f
      at /home/david/.cargo/git/checkouts/byteseries-91c191b460a7dcf6/03fd661/src/series.rs:314
  13: log_store::server::db::log::Log::get::h8dddebe351fe69a3
      at /home/david/Documents/HomeAutomation/crates/log-store/src/server/db/log.rs:201
  14: log_store::server::db::log::Logs::get::{{closure}}::h992cc3a4103ffca2
      at /home/david/Documents/HomeAutomation/crates/log-store/src/server/db/log.rs:299
  15: log_store::server::clients::perform_request_inner::{{closure}}::h79935fe4ef4d8e4c
      at /home/david/Documents/HomeAutomation/crates/log-store/src/server/clients.rs:30
  16: log_store::server::clients::perform_request::{{closure}}::he9c17bcb9e6d3709
      at /home/david/Documents/HomeAutomation/crates/log-store/src/server/clients.rs:18
  17: rpc::server::handle_client::{{closure}}::{{closure}}::h2538c99642036fcf
      at /home/david/Documents/HomeAutomation/crates/rpc/src/server.rs:148
  18: <tracing::instrument::Instrumented<T> as core::future::future::Future>::poll::h70780971ea75d816
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tracing-0.1.40/src/instrument.rs:321
  19: rpc::server::handle_client::{{closure}}::h3ec95fde1cb9b462
      at /home/david/Documents/HomeAutomation/crates/rpc/src/server.rs:121
  20: tokio::runtime::task::core::Core<T,S>::poll::{{closure}}::hef887d4b2fb1d619
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/task/core.rs:331
  21: tokio::loom::std::unsafe_cell::UnsafeCell<T>::with_mut::hdc8354fbf3e05ad3
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/loom/std/unsafe_cell.rs:16
  22: tokio::runtime::task::core::Core<T,S>::poll::hb097d2ba2a285626
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/task/core.rs:320
  23: tokio::runtime::task::harness::poll_future::{{closure}}::h9ee1f428aaa1b93d
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/task/harness.rs:500
  24: <core::panic::unwind_safe::AssertUnwindSafe<F> as core::ops::function::FnOnce<()>>::call_once::h968819e9c8a9d0d1
      at /rustc/3f5fd8dd41153bc5fdca9427e9e05be2c767ba23/library/core/src/panic/unwind_safe.rs:272
  25: std::panicking::try::do_call::hbfd23598dc46bbce
      at /rustc/3f5fd8dd41153bc5fdca9427e9e05be2c767ba23/library/std/src/panicking.rs:559
  26: std::panicking::try::h00166555d3710fab
      at /rustc/3f5fd8dd41153bc5fdca9427e9e05be2c767ba23/library/std/src/panicking.rs:523
  27: std::panic::catch_unwind::h09758e694bfc3808
      at /rustc/3f5fd8dd41153bc5fdca9427e9e05be2c767ba23/library/std/src/panic.rs:149
  28: tokio::runtime::task::harness::poll_future::hdcbe86b37644aee7
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/task/harness.rs:488
  29: tokio::runtime::task::harness::Harness<T,S>::poll_inner::h59232c1139a4d2cd
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/task/harness.rs:209
  30: tokio::runtime::task::harness::Harness<T,S>::poll::hba4dbfba44e5a093
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/task/harness.rs:154
  31: tokio::runtime::task::raw::RawTask::poll::he857f1b01f8fe0a8
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/task/raw.rs:201
  32: tokio::runtime::task::LocalNotified<S>::run::ha7e6f46b79c67fe4
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/task/mod.rs:436
  33: tokio::runtime::scheduler::multi_thread::worker::Context::run_task::{{closure}}::h2a983da950ead5d0
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/scheduler/multi_thread/worker.rs:598
  34: tokio::runtime::coop::with_budget::h36bec49b64654c69
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/coop.rs:107
  35: tokio::runtime::coop::budget::h9771ca9ae6b5db7b
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/coop.rs:73
  36: tokio::runtime::scheduler::multi_thread::worker::Context::run_task::h5cb4b0ddeb6cb205
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/scheduler/multi_thread/worker.rs:597
  37: tokio::runtime::scheduler::multi_thread::worker::Context::run::ha7b77c8f5ec96672
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/scheduler/multi_thread/worker.rs:<unknown line>
  38: tokio::runtime::scheduler::multi_thread::worker::run::{{closure}}::{{closure}}::hcec7b36a97f101b7
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/scheduler/multi_thread/worker.rs:513
  39: tokio::runtime::context::scoped::Scoped<T>::set::h42432fa349e2a77b
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/context/scoped.rs:40
  40: tokio::runtime::scheduler::multi_thread::worker::run::{{closure}}::h1cee0bf1524715ed
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/scheduler/multi_thread/worker.rs:508
  41: tokio::runtime::context::runtime::enter_runtime::hb495fdb4a484e109
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/context/runtime.rs:65
  42: tokio::runtime::scheduler::multi_thread::worker::run::hc5f989725633061b
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/scheduler/multi_thread/worker.rs:500
  43: tokio::runtime::scheduler::multi_thread::worker::Launch::launch::{{closure}}::hd51346e6e30892b8
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/scheduler/multi_thread/worker.rs:466
  44: <tokio::runtime::blocking::task::BlockingTask<T> as core::future::future::Future>::poll::hc7e6ebce99eab824
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/blocking/task.rs:42
  45: tokio::runtime::task::core::Core<T,S>::poll::{{closure}}::habf8f8508483948f
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/task/core.rs:331
  46: tokio::loom::std::unsafe_cell::UnsafeCell<T>::with_mut::ha0267825dab0e47d
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/loom/std/unsafe_cell.rs:16
  47: tokio::runtime::task::core::Core<T,S>::poll::h95ecce148c89d728
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/task/core.rs:320
  48: tokio::runtime::task::harness::poll_future::{{closure}}::hbc4b6731301d3464
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/task/harness.rs:500
  49: <core::panic::unwind_safe::AssertUnwindSafe<F> as core::ops::function::FnOnce<()>>::call_once::hf07b352e39f501a6
      at /rustc/3f5fd8dd41153bc5fdca9427e9e05be2c767ba23/library/core/src/panic/unwind_safe.rs:272
  50: std::panicking::try::do_call::hbb811d8b5ea4203f
      at /rustc/3f5fd8dd41153bc5fdca9427e9e05be2c767ba23/library/std/src/panicking.rs:559
  51: std::panicking::try::h1cab090016738601
      at /rustc/3f5fd8dd41153bc5fdca9427e9e05be2c767ba23/library/std/src/panicking.rs:523
  52: std::panic::catch_unwind::hbbdb75eab087aa55
      at /rustc/3f5fd8dd41153bc5fdca9427e9e05be2c767ba23/library/std/src/panic.rs:149
  53: tokio::runtime::task::harness::poll_future::h6a3ef7af1a553ce2
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/task/harness.rs:488
  54: tokio::runtime::task::harness::Harness<T,S>::poll_inner::ha8c34f164851188f
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/task/harness.rs:209
  55: tokio::runtime::task::harness::Harness<T,S>::poll::ha776c1520cb89329
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/task/harness.rs:154
  56: tokio::runtime::task::raw::RawTask::poll::he857f1b01f8fe0a8
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/task/raw.rs:201
  57: tokio::runtime::task::UnownedTask<S>::run::he960b0f51ffed5eb
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/task/mod.rs:473
  58: tokio::runtime::blocking::pool::Task::run::h7d07bb6beac0e093
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/blocking/pool.rs:160
  59: tokio::runtime::blocking::pool::Inner::run::he9ebf293ef5d19fe
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/blocking/pool.rs:518
  60: tokio::runtime::blocking::pool::Spawner::spawn_thread::{{closure}}::hd103cf262ab6b0d3
      at /home/david/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.40.0/src/runtime/blocking/pool.rs:476
                                ⋮ 11 frames hidden ⋮
Run with COLORBT_SHOW_HIDDEN=1 environment variable to disable frame filtering.
Run with RUST_BACKTRACE=full to include source snippets.
*/
