// This is a simple clock simulator that simulates multiple clocks with different drift rates and synchronization behavior. It periodically samples the simulated time and writes it to a CSV file for analysis.
// Usage:
// cargo run --bin clock_sim -- <out_csv> <duration_ms> <sample_ms> <sync_period_us> <uncertainty_us> <drift_list>
// Visualization:
// You can visualize the output CSV using python code in /scripts/plot_clock_sim.py
// python scripts/plot_clock_sim.py <out_csv>

use omnipaxos_kv::clock::simulator::{Clock, ClockConfig};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let out_csv = args
        .get(1)
        .map(|s| s.as_str())
        .unwrap_or("scripts/clock_sim.csv");
    let duration_ms: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(2000);
    let sample_ms: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(20);
    let sync_period_us: i64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let uncertainty_us: i64 = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(50);
    let drift_list = args.get(6).map(|s| s.as_str()).unwrap_or("-100,0,100,250");

    let drift_rates: Vec<i64> = drift_list
        .split(',')
        .filter_map(|s| s.trim().parse::<i64>().ok())
        .collect();

    if drift_rates.is_empty() {
        eprintln!("No valid drift rates provided.");
        std::process::exit(1);
    }

    let mut clocks: Vec<Arc<Clock>> = Vec::new();

    for (i, drift) in drift_rates.iter().enumerate() {
        let clock = Arc::new(Clock::new(ClockConfig {
            node_id: (i + 1) as u64,
            drift_rate_us_per_sec: *drift,
            sync_uncertainty_us: uncertainty_us,
            sync_period_us,
            seed: Some(10_000 + i as u64),
        }));
        clocks.push(clock);
    }

    let file = File::create(out_csv).expect("failed to create csv");
    let mut w = BufWriter::new(file);
    writeln!(w, "node_id,real_ms,real_us,true_us,sim_us,error_us,uncertainty_us").unwrap();

    let start = Instant::now();
    let end = start + Duration::from_millis(duration_ms);

    while Instant::now() < end {
        let now = Instant::now();
        let real_us = now.duration_since(start).as_micros() as i64;
        let real_ms = real_us as f64 / 1000.0;
        for clock in &clocks {
            let (sim_us, unc) = clock.get_time_with_uncertainty();
            let true_us = clock.get_last_true_time();
            let error_us = sim_us - true_us;
            writeln!(
                w,
                "{},{:.3},{},{},{},{},{}",
                clock.node_id(),
                real_ms,
                real_us,
                true_us,
                sim_us,
                error_us,
                unc
            )
            .unwrap();
        }
        thread::sleep(Duration::from_millis(sample_ms));
    }

    w.flush().unwrap();
    eprintln!("Wrote {}", out_csv);
}
