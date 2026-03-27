pub mod simulator {
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use serde::{Deserialize, Serialize};
    use std::sync::{Mutex};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    pub type NodeId = u64;

    #[derive(Debug, Clone, Default, Deserialize, Serialize)]
    pub struct ClockConfig {
        pub node_id: NodeId,
        /// Drift rate in microseconds per simulated second.
        pub drift_rate_us_per_sec: i64,
        /// Synchronization uncertainty bound (±ε) in microseconds.
        pub sync_uncertainty_us: i64,
        /// Synchronization period in simulated microseconds.
        pub sync_period_us: i64,
        /// Optional RNG seed for deterministic simulation.
        pub seed: Option<u64>,
    }

    #[derive(Debug)]
    struct ClockState {
        base_sim_offset_us: i64,
        last_sim_us: i64,
        // Next resync deadline in simulated time (true clock).
        next_resync_sim_us: i64,
        rng: StdRng,
        last_sync_true_time: i64,
        last_true_time: i64,
        last_sim_time: i64,
    }

    #[derive(Debug)]
    pub struct Clock {
        cfg: ClockConfig,
        base_instant: Instant,
        base_epoch_us: i64,
        state: Mutex<ClockState>,
    }

    impl Clock {
        pub fn new(cfg: ClockConfig) -> Self {
            assert!(cfg.sync_uncertainty_us >= 0, "sync_uncertainty_us must be >= 0");
            assert!(cfg.sync_period_us > 0, "sync_period_us must be > 0");
            let base_instant = Instant::now();
            let base_epoch_us = system_time_us();
            let seed = cfg.seed.unwrap_or_else(|| {
                // Non-deterministic seed when not provided.
                rand::thread_rng().gen::<u64>()
            });
            let rng = StdRng::seed_from_u64(seed);

            let next_resync_sim_us = base_epoch_us.saturating_add(cfg.sync_period_us);

            Clock {
                cfg,
                base_instant,
                base_epoch_us,
                state: Mutex::new(ClockState {
                    base_sim_offset_us: 0,
                    last_sim_us: base_epoch_us,
                    next_resync_sim_us,
                    rng,
                    last_sync_true_time: base_epoch_us,
                    last_true_time: base_epoch_us,
                    last_sim_time: base_epoch_us,
                }),
            }
        }

        pub fn node_id(&self) -> NodeId {
            self.cfg.node_id
        }
        
        pub fn get_time(&self) -> i64 {
            let now = Instant::now();
            let true_time = self.compute_true_time(now);
            let mut state = self.state.lock().unwrap();

            // Resync at scheduled times (catch up if we missed intervals).
            while true_time >= state.next_resync_sim_us {
                let scheduled_time = state.next_resync_sim_us;
                self.resync_at(scheduled_time, &mut state);
            }

            let sim_time = self.compute_sim_time(
                true_time,
                state.base_sim_offset_us,
                state.last_sync_true_time,
            );
            let (min_allowed, max_allowed) = self.allowed_range(true_time);
            let clamped =
                self.clamp_sim_time(sim_time, min_allowed, max_allowed, &mut state);
            state.last_sim_us = clamped;
            state.last_true_time = true_time;
            state.last_sim_time = clamped;
            clamped
        }

        pub fn get_time_with_uncertainty(&self) -> (i64, i64) {
            let now = Instant::now();
            let true_time = self.compute_true_time(now);
            let mut state = self.state.lock().unwrap();

            // Resync at scheduled times (catch up if we missed intervals).
            while true_time >= state.next_resync_sim_us {
                let scheduled_time = state.next_resync_sim_us;
                self.resync_at(scheduled_time, &mut state);
            }

            let sim_time = self.compute_sim_time(
                true_time,
                state.base_sim_offset_us,
                state.last_sync_true_time,
            );
            let (min_allowed, max_allowed) = self.allowed_range(true_time);
            let clamped =
                self.clamp_sim_time(sim_time, min_allowed, max_allowed, &mut state);
            state.last_sim_us = clamped;
            state.last_true_time = true_time;
            state.last_sim_time = clamped;

            let error_abs = abs_i64(state.last_sim_time - state.last_true_time);
            let upper = self.cfg.sync_uncertainty_us.max(error_abs);
            let unc = if upper == 0 || upper == error_abs {
                upper
            } else {
                state.rng.gen_range(error_abs..=upper)
            };
            (clamped, unc)
        }

        pub fn get_true_time(&self) -> i64 {
            self.compute_true_time(Instant::now())
        }

        pub fn get_last_true_time(&self) -> i64 {
            let state = self.state.lock().unwrap();
            state.last_true_time
        }

        pub fn get_uncertainty(&self) -> i64 {
            let mut state = self.state.lock().unwrap();
            let error_abs = abs_i64(state.last_sim_time - state.last_true_time);
            let upper = self.cfg.sync_uncertainty_us.max(error_abs);
            if upper == 0 || upper == error_abs {
                return upper;
            }
            state.rng.gen_range(error_abs..=upper)
        }
        
        fn compute_true_time(&self, now: Instant) -> i64 {
            let real_elapsed_us = duration_to_us(now.duration_since(self.base_instant));
            let sim_elapsed_us = real_elapsed_us;
            self.base_epoch_us.saturating_add(sim_elapsed_us)
        }

        fn compute_sim_time(
            &self,
            true_time: i64,
            base_sim_offset_us: i64,
            last_sync_true_time: i64,
        ) -> i64 {
            let sim_elapsed_us = true_time.saturating_sub(last_sync_true_time);
            let drift_us = sim_elapsed_us
                .saturating_mul(self.cfg.drift_rate_us_per_sec)
                / 1_000_000;

            true_time
                .saturating_add(base_sim_offset_us)
                .saturating_add(drift_us)
        }

        fn allowed_range(&self, true_time: i64) -> (i64, i64) {
            let bound = self.cfg.sync_uncertainty_us.max(0);
            let min_allowed = true_time.saturating_sub(bound);
            let max_allowed = true_time.saturating_add(bound);
            (min_allowed, max_allowed)
        }

        fn clamp_sim_time(
            &self,
            sim_time: i64,
            min_allowed: i64,
            max_allowed: i64,
            state: &mut ClockState,
        ) -> i64 {
            let lower = std::cmp::max(state.last_sim_us, min_allowed);
            let clamped = if sim_time < lower {
                lower
            } else if sim_time > max_allowed {
                max_allowed
            } else {
                sim_time
            };
            if clamped != sim_time {
                // Adjust offset to keep future reads consistent.
                state.base_sim_offset_us =
                    state.base_sim_offset_us.saturating_add(clamped - sim_time);
            }
            clamped
        }

        fn resync_at(&self, true_time: i64, state: &mut ClockState) {
            let (min_allowed, max_allowed) = self.allowed_range(true_time);
            // let bound = self.cfg.sync_uncertainty_us.max(0);
            // let new_error = if bound == 0 {
            //     0
            // } else {
            //     state.rng.gen_range(-bound..=bound)
            // };
            let new_error = 0; // For simplicity
            let target_time = true_time.saturating_add(new_error);
            let min_target = min_allowed;
            let max_target = max_allowed;
            let clamped_target = if target_time < min_target {
                min_target
            } else if target_time > max_target {
                max_target
            } else {
                target_time
            };
            // After resync, drift is computed from last_sync_true_time = true_time,
            // so set offset relative to true_time (no drift yet).
            state.base_sim_offset_us = clamped_target.saturating_sub(true_time);
            state.last_sync_true_time = true_time;
            let period = self.cfg.sync_period_us;
            let mut next = state.next_resync_sim_us;
            if period > 0 {
                while true_time >= next {
                    next = next.saturating_add(period);
                }
            }
            state.next_resync_sim_us = next;
        }
    }

    fn duration_to_us(d: Duration) -> i64 {
        let secs = d.as_secs() as i64;
        let micros = d.subsec_micros() as i64;
        secs.saturating_mul(1_000_000).saturating_add(micros)
    }

    fn system_time_us() -> i64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => {
                let us = d.as_micros();
                if us > i64::MAX as u128 {
                    i64::MAX
                } else {
                    us as i64
                }
            }
            Err(_) => 0,
        }
    }

    fn abs_i64(v: i64) -> i64 {
        let v = v as i128;
        if v < 0 {
            (-v).min(i64::MAX as i128) as i64
        } else {
            v as i64
        }
    }

}

#[cfg(feature = "protocol-caop")]
impl crate::omnipaxos_api::util::PhysicalClock for simulator::Clock {
    fn get_time(&self) -> i64 {
        simulator::Clock::get_time(self)
    }

    fn get_uncertainty(&self) -> i64 {
        simulator::Clock::get_uncertainty(self)
    }

    fn get_time_with_uncertainty(&self) -> (i64, i64) {
        simulator::Clock::get_time_with_uncertainty(self)
    }
}
