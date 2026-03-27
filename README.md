Clock Assisted OmniPaxos (CA-OP)
==============================

## Background

This project extends [OmniPaxos](https://github.com/haraldng/OmniPaxos), a Rust library for replicated log consensus.
The initial implementation was developed as part of the KTH course `ID2203: Distributed Systems, Advanced Course`, where we explored clock-assisted consensus.

CA-OP combines OmniPaxos with deadline-ordered message delivery inspired by the paper ["Nezha: Deployable and High-Performance Consensus Using Synchronized Clocks"](https://www.vldb.org/pvldb/vol16/p629-geng.pdf).

## My Contributions

- **CA-OP algorithm design**: designed the integration of OmniPaxos and deadline-ordered delivery
- **Implementation**: added the fast-path optimization that can decide in one RTT
- **Benchmarking**: built a reproducible benchmark harness to compare CA-OP against upstream OmniPaxos

## Algorithm Overview

The protocol introduces a fast path that reduces latency when proposals arrive before their deadline.

| | Fast Path | Original Path |
|---|---|---|
| **RTT** | **1** | 2 |
| **Communication Steps** | **3** | 4 |
| **Quorum** (`#Node = 2f+1`) | `f + floor(f/2) + 1` | `f + 1` |

### Fast Path

![Fast Path](./docs/fast_path.svg)

When a client request arrives and proposals reach followers before the deadline, followers respond with `FastAccepted`, allowing the leader to decide in one RTT.

### Slow Path

![Slow Path](./docs/slow_path.svg)

If the deadline is missed or the fast quorum is not reached, CA-OP falls back to the original OmniPaxos `Accept` / `Accepted` / `Decide` path.

## Benchmarking

The repository includes a benchmark harness for comparing `caop` and upstream `baseline` OmniPaxos.
Outputs are written under:

```text
benchmark/results/<experiment>/<protocol>/<scenario>/run-*/
```

Canonical entrypoints:

- local: `benchmark/scripts/run-local.sh`
- clock-quality sweep: `benchmark/scripts/run-clock-sweep.sh`
- containerlab: `benchmark/scripts/build-images.sh` and `benchmark/scripts/run-clab.sh`
- plotting: `benchmark/visualize/graph_clock_benchmark.py`

Examples:

```bash
cd benchmark
./scripts/run-local.sh caop medium 1000 1
./scripts/run-clock-sweep.sh 3 caop
./scripts/build-images.sh caop
./scripts/run-clab.sh caop small_jitter medium 1000 1
```

Full benchmark and visualization instructions are in [`benchmark/README.md`](./benchmark/README.md).

## Known Constraints

- The upstream baseline API differs from CA-OP. The benchmark uses a compatibility layer to run both protocols through the same client/server harness.
- `fast_path_ratio` is only meaningful for CA-OP. Baseline runs always report `0`.
- The containerlab templates shape traffic explicitly with `tc netem` on router interfaces, which is sufficient for scenario-level comparisons but not a full link-matrix emulator.
- `clock_quality` remains a CLI argument for the runners, but baseline scenarios do not split outputs by clock quality.
