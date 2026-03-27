CA-OP Benchmarks
=================

This directory contains the benchmarking harness used to compare Clock Assisted OmniPaxos (CA-OP) with the upstream OmniPaxos baseline.

## What Is Benchmarked

The workload is a simple key-value store built on top of OmniPaxos. The benchmark records:

- client-side request latency from `client-*.csv`
- achieved throughput from completed responses per run
- fast-path ratio from `server-*.json`
- DOM one-way delay estimates from `server-*-owd.csv` when CA-OP is active

By default the runner starts two clients:

- `client-1` connected to `server-1`
- `client-2` connected to `server-5`

Use `CLIENT_SERVER_IDS_CSV` to override this mapping.

## Protocols

- `caop`: builds against [`../lib/omnipaxos`](../lib/omnipaxos)
- `baseline`: builds against the vendored upstream repository in [`../vendor/omnipaxos-baseline`](../vendor/omnipaxos-baseline)

The runner switches protocols through Cargo features:

- `protocol-caop`
- `protocol-baseline`

## Directory Layout

- `src/`: benchmark server/client implementation
- `scripts/`: benchmark entrypoints (`build-images.sh`, `run-local.sh`, `run-clab.sh`, `run-clock-sweep.sh`)
- `settings/`: static benchmark config and topology templates
- `settings/clab/*.clab.yml`: containerlab topology templates
- `settings/templates/`: generated run config templates
- `results/`: generated benchmark outputs
- `visualize/graph_clock_benchmark.py`: protocol comparison plots for a scenario

Generated logs use this layout:

```text
results/<experiment>/<protocol>/<scenario>/run-*/
```

Examples:

- `results/local/caop/local-high-rps10000/run-0/`
- `results/containerlab/baseline/small_jitter-rps1000/run-1/`

## Prerequisites

Local runs:

- Rust toolchain
- Cargo

Containerlab runs:

- Docker
- Containerlab

The container images are built from:

- [`server.dockerfile`](./server.dockerfile)
- [`client.dockerfile`](./client.dockerfile)

These images install `tc` so the topology templates can apply `netem` settings.

## Running Benchmarks

### Local smoke test

```bash
cd CA-OP/benchmark
./scripts/run-local.sh caop medium 1000 1
./scripts/run-local.sh baseline medium 1000 1
```

### Clock-quality sweep

```bash
cd CA-OP/benchmark
./scripts/run-clock-sweep.sh 3 caop
./scripts/run-clock-sweep.sh 3 baseline
```

This runs local clock-quality comparisons at `500 req/s`.

- `caop`: runs `high`, `medium`, and `low`
- `baseline`: runs one local baseline scenario only

### Containerlab experiments

```bash
cd CA-OP/benchmark
./scripts/build-images.sh caop
./scripts/run-clab.sh caop small_jitter medium sweep 3

./scripts/build-images.sh baseline
./scripts/run-clab.sh baseline imbalance medium 1000 1
```

Containerlab uses protocol-specific local Docker tags:

- `omnipaxos-server-caop`
- `omnipaxos-client-caop`
- `omnipaxos-server-baseline`
- `omnipaxos-client-baseline`

You must build the images for the protocol you are about to run before calling `run-clab.sh`.

Supported topologies:

- `small_jitter`: single-router star topology with 10 ms latency and 5 ms jitter on each leaf-facing egress
- `large_jitter`: single-router star topology with 100 ms latency and 20 ms jitter on each leaf-facing egress
- `imbalance`: two-router topology with 3 fast nodes and 2 slow nodes

Supported workloads:

- `100`
- `500`
- `1000`
- `5000`
- `10000`
- `sweep`

Clock quality now controls only synchronization quality:

- `high`: `sync_uncertainty_us=10`, `sync_period_us=1000`
- `medium`: `sync_uncertainty_us=100`, `sync_period_us=10000`
- `low`: `sync_uncertainty_us=1000`, `sync_period_us=100000`

Per-server drift is fixed across all clock qualities. The runner uses:

- `server-1`: `400`
- `server-2`: `800`
- `server-3`: `-500`
- `server-4`: `-1000`
- `server-5`: `100`

## Collecting Results

Each run directory contains:

- `client-1.csv`: per-request timestamps
- `client-1.json`: client config snapshot
- `server-*.json`: server config and decision statistics
- `server-*-owd.csv`: OWD time series for CA-OP and a zero-valued placeholder for true baseline runs
- `*-stdout.log`, `*-stderr.log`: process logs

## Plotting

`graph_clock_benchmark.py` reads one scenario directory at a time and saves plots into `results/.../plots/`.

Basic usage:

```bash
cd CA-OP/benchmark/visualize
python graph_clock_benchmark.py ../results/local local-medium-rps1000
python graph_clock_benchmark.py ../results/containerlab small_jitter-medium-rps1000
```

`MPLBACKEND=Agg` is useful when you only want files and not an interactive window:

```bash
cd CA-OP/benchmark/visualize
MPLBACKEND=Agg python graph_clock_benchmark.py ../results/local local-medium-rps1000
```

Saved figures:

- `plots/<scenario>-latency-by-client.png`
- `plots/<scenario>-deadline-wait-approx.png` for CA-OP scenarios with OWD data
- `plots/clock-quality-rps<rps>-latency-comparison.png` for local clock-quality scenarios
- `plots/clock-quality-rps<rps>-fast-path-comparison.png` for local clock-quality scenarios

What each figure means:

- `latency-by-client`
  - compares `CA-OP` and `baseline`
  - separates `client-1 -> server-1` and `client-2 -> server-5`
  - shows `mean`, `median`, `p95`, and `p99`
- `deadline-wait-approx`
  - CA-OP only
  - upper plot overlays client mean latency with leader `max_outgoing_estimate`
  - lower plot shows peer-wise slack `max_outgoing_estimate - outgoing_estimate(peer)`
  - this is a proxy for deadline-induced waiting, not an exact buffer wait measurement
- `clock-quality-rps<rps>-latency-comparison`
  - local runs only
  - compares `high`, `medium`, and `low` for CA-OP
  - uses the single local baseline run as a reference line
- `clock-quality-rps<rps>-fast-path-comparison`
  - local runs only
  - compares CA-OP `fast_path_ratio` across `high`, `medium`, and `low`
  - draws baseline as a `0` reference line

Input assumptions:

- local clock-quality comparison expects:
  - `results/local/caop/local-high-rps<rps>/`
  - `results/local/caop/local-medium-rps<rps>/`
  - `results/local/caop/local-low-rps<rps>/`
  - `results/local/baseline/local-rps<rps>/`
- containerlab scenario plots expect one concrete scenario such as:
  - `small_jitter-medium-rps1000`
  - `large_jitter-medium-rps1000`
  - `imbalance-medium-rps1000`

## Known Constraints

- The upstream baseline API differs from CA-OP. The benchmark uses a small compatibility layer so both protocols can run under the same harness.
- `fast_path_ratio` is meaningful only for CA-OP. Baseline runs report `0`.
- The containerlab templates apply delay and jitter explicitly with `tc qdisc ... netem` inside node `exec` hooks on the router interfaces.
- `clock_quality` is still a required CLI argument for `run-local.sh` and `run-clab.sh`, but baseline scenarios do not split outputs by quality.
- Compatibility wrappers remain in `settings/run-benchmark.sh`, `settings/run-clock-benchmark.sh`, and `settings/run-local-cluster.sh`, but the canonical entrypoints are the scripts under `benchmark/scripts/`.
