# Clock Simulator (Summary)

## How the clock is simulated
- **True time** is derived from a **SystemTime epoch at startup** plus **Instant-based elapsed time**.
- This keeps true time monotonic and aligned across servers as long as `SystemTime` is synchronized.
- The true time is expressed in simulated microseconds (no scaling).
- **Drift** is applied as `drift_rate_us_per_sec` over simulated elapsed time.
- The **simulated clock time** is:
  - `true_time + drift + offset`
- The clock is **clamped** on every `get_time()` so that:
  - `sim_time` stays within `true_time ± sync_uncertainty_us`
  - **Monotonicity** is preserved by not returning values below the last returned time　within the same sync period.
- **Resync** happens every `sync_period_us` (simulated time).
  - At resync, a new `offset` is chosen so that `sim_time` is within `true_time ± sync_uncertainty_us`.
  - Since, we assume `true_time` is within `sim_time ± sync_uncertainty_us` of the actual time, resync ensures that new `sim_time` is within `true_time ± sync_uncertainty_us`.

## How the clock is used by OmniPaxos
- The clock implements `PhysicalClock` directly.
- `get_time()` and `get_uncertainty()` are **`&self`** methods.
- `get_uncertainty()` is aligned with the **most recent `get_time()`** result.
- If you need an atomic pair, use `get_time_with_uncertainty()`.

## Per-server clock
- Each server process initializes **one clock instance**.
- The server uses a `OnceLock<Clock>` and passes `&Clock` into `OmniPaxosConfig::build(...)`.
- This means **one clock per server process** (which matches the default setup: one server per process).
- Therefore, we did not implement any clock sharing or synchronization between servers, but assume that each server's `Instant::now()` is a reasonable approximation of the same "true time" for all servers.

## Config
Clock parameters live under the `[clock]` section in the server config TOML:
```toml
[clock]
drift_rate_us_per_sec = 0
sync_uncertainty_us = 100
sync_period_us = 10000
seed = 42
```
