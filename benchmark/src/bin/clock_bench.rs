use omnipaxos_kv::clock::simulator::{Clock, ClockConfig};
use std::hint::black_box;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let iters: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);

    let clock = Clock::new(ClockConfig {
        node_id: 1,
        drift_rate_us_per_sec: 100,
        sync_uncertainty_us: 100,
        sync_period_us: 10_000,
        seed: Some(42),
    });

    // Warm-up
    for _ in 0..10_000 {
        let (_sim, unc) = clock.get_time_with_uncertainty();
        black_box(unc);
    }

    let start = Instant::now();
    for _ in 0..iters {
        let (_sim, unc) = clock.get_time_with_uncertainty();
        black_box(unc);
    }
    let elapsed = start.elapsed();
    let total_ns = elapsed.as_secs_f64() * 1e9;
    let per_call_ns = total_ns / iters as f64;

    println!("iters: {}", iters);
    println!("total: {:.3} ms", elapsed.as_secs_f64() * 1e3);
    println!("per_call: {:.1} ns", per_call_ns);
}
