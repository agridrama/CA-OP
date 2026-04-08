Clock Assisted OmniPaxos (CA-OP)
==============================

This project shows that clock-assisted consensus can reduce latency both theoretically and experimentally. CA-OP adds a one-RTT fast path to OmniPaxos, and the benchmark results show measurable latency reductions in favorable network settings.

## Background

This project extends [OmniPaxos](https://github.com/haraldng/OmniPaxos), a Rust library for replicated log consensus.
The initial implementation was developed as part of the KTH course `ID2203: Distributed Systems, Advanced Course`, where we explored clock-assisted consensus.

CA-OP combines OmniPaxos with deadline-ordered message delivery inspired by the paper ["Nezha: Deployable and High-Performance Consensus Using Synchronized Clocks"](https://www.vldb.org/pvldb/vol16/p629-geng.pdf).

## My Contributions

- **CA-OP algorithm design**: designed the integration of OmniPaxos and deadline-ordered delivery
- **Implementation**: added the fast-path optimization that can decide in one RTT
- **Benchmarking**: built a reproducible benchmark harness to compare CA-OP against upstream OmniPaxos

## Main Results
- CA-OP can reduce end-to-end latency by up to about **75 ms / 12%** in high-jitter (100 ms latency ± 20 ms jitter) scenarios.
- CA-OP's performance and gains are dependent on 1) **network conditions** (e.g., jitter, topology) and 2) **clock quality**, which affects the fast-path ratio.
- CA-OP adds about **2-5 ms** of protocol/runtime overhead relative to baseline, so it requires sufficient network-delay reduction to be beneficial overall.

The benchmark results are detailed in the [benchmark README ##Result](./benchmark/README.md?plain=1#L154).

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

## Benchmark Findings

Detailed results are in the [benchmark README](./benchmark/README.md), but the high-level observations are summarized below.

The strongest gains appear for follower-connected clients under symmetric delay. In `small_jitter`, CA-OP improved mean latency by up to about `5 ms`. In `large_jitter`, the gain reached about `75 ms`, which is close to removing most of one `100 ms` hop.

Local runs show the cost floor: CA-OP adds about `2-5 ms` of protocol/runtime overhead relative to baseline. In practice, this means CA-OP needs more network-delay reduction than that before it is likely to win overall.

The benefit is not uniform. For leader-connected clients, CA-OP was worse by about `7-10 ms` in `small_jitter` and `30-70 ms` in `large_jitter`. In `imbalance`, CA-OP also regressed for both clients because the fast-path ratio dropped under the asymmetric topology.

## Known Constraints

- The upstream baseline API differs from CA-OP. The benchmark uses a compatibility layer to run both protocols through the same client/server harness.
- `fast_path_ratio` is only meaningful for CA-OP. Baseline runs always report `0`.
- The containerlab templates shape traffic explicitly with `tc netem` on router interfaces, which is sufficient for scenario-level comparisons but not a full link-matrix emulator.
- `clock_quality` remains a CLI argument for the runners, but baseline scenarios do not split outputs by clock quality.
- Performance also varies noticeably by `clock_quality`, so CA-OP still requires tuning and should be read with that limitation in mind.
