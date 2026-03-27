"""
Graph benchmark results for logs laid out as:

  benchmark/results/<experiment>/<protocol>/<scenario>/run-*/

Usage:
  python graph_clock_benchmark.py [log_root] [scenario]

Examples:
  python graph_clock_benchmark.py
  python graph_clock_benchmark.py ../results/local local-high-rps10000
  python graph_clock_benchmark.py ../results/containerlab small_jitter-rps1000
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

LOG_ROOT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("../results/local")
REQUESTED_SCENARIO = sys.argv[2] if len(sys.argv) > 2 else None
PROTOCOL_LABELS = {"caop": "CA-OP", "baseline": "OmniPaxos"}
PROTOCOL_COLORS = {"caop": "#1f7a8c", "baseline": "#d95d39"}
CLIENT_LABELS = {"client-1": "Client 1 -> Server 1", "client-2": "Client 2 -> Server 5"}
LATENCY_METRICS = ("mean", "median", "p95", "p99")
CLOCK_QUALITIES = ("high", "medium", "low")
TIME_BIN_MS = 500
LEADER_SERVER_ID = 1


def available_protocols() -> list[str]:
    if not LOG_ROOT.exists():
        return []
    return sorted(path.name for path in LOG_ROOT.iterdir() if path.is_dir())


def choose_scenario(protocols: list[str]) -> str:
    if REQUESTED_SCENARIO:
        return REQUESTED_SCENARIO
    candidates = None
    for protocol in protocols:
        protocol_dir = LOG_ROOT / protocol
        protocol_scenarios = {path.name for path in protocol_dir.iterdir() if path.is_dir()}
        candidates = protocol_scenarios if candidates is None else candidates & protocol_scenarios
    if not candidates:
        raise SystemExit(f"No shared scenario directories found under {LOG_ROOT}")
    return sorted(candidates)[0]


def parse_local_clock_scenario(scenario: str) -> tuple[str, str] | None:
    match = re.fullmatch(r"local-(high|medium|low)-rps(.+)", scenario)
    if not match:
        return None
    return match.group(1), match.group(2)


def parse_local_baseline_scenario(scenario: str) -> str | None:
    match = re.fullmatch(r"local-rps(.+)", scenario)
    if not match:
        return None
    return match.group(1)


def parse_quality_scenario(scenario: str) -> tuple[str, str, str] | None:
    match = re.fullmatch(r"(.+)-(high|medium|low)-rps(.+)", scenario)
    if not match:
        return None
    return match.group(1), match.group(2), match.group(3)


def parse_baseline_scenario(scenario: str) -> tuple[str, str] | None:
    match = re.fullmatch(r"(.+)-rps(.+)", scenario)
    if not match:
        return None
    topology = match.group(1)
    if topology in CLOCK_QUALITIES:
        return None
    return topology, match.group(2)


def shared_clock_quality_scenarios(protocols: list[str], base_scenario: str) -> tuple[dict[str, str], str | None]:
    parsed_clock = parse_quality_scenario(base_scenario)
    parsed_baseline = parse_baseline_scenario(base_scenario)
    if parsed_clock is not None:
        topology, _, rps = parsed_clock
    elif parsed_baseline is not None:
        topology, rps = parsed_baseline
    else:
        return {}, None

    scenarios = {}
    caop_root = LOG_ROOT / "caop"
    baseline_root = LOG_ROOT / "baseline"
    baseline_scenario = f"{topology}-rps{rps}"
    if not baseline_root.exists() or not (baseline_root / baseline_scenario).exists():
        return {}, None

    for quality in CLOCK_QUALITIES:
        candidate = f"{topology}-{quality}-rps{rps}"
        if caop_root.exists() and (caop_root / candidate).exists():
            scenarios[quality] = candidate
    return scenarios, baseline_scenario


def load_client_frames(protocol: str, scenario: str) -> list[pd.DataFrame]:
    frames = []
    scenario_dir = LOG_ROOT / protocol / scenario
    if not scenario_dir.exists():
        return frames
    for run_dir in sorted(scenario_dir.glob("run-*")):
        for csv_file in sorted(run_dir.glob("client-*.csv")):
            df = pd.read_csv(csv_file)
            if df.empty:
                continue
            df["response_latency"] = df["response_time"] - df["request_time"]
            df = df.dropna(subset=["response_latency"])
            df = df[df["response_latency"] > 0]
            if df.empty:
                continue
            df["run_id"] = run_dir.name
            df["client_id"] = csv_file.stem
            frames.append(df)
    return frames


def load_server_stats(protocol: str, scenario: str) -> list[dict]:
    stats = []
    scenario_dir = LOG_ROOT / protocol / scenario
    if not scenario_dir.exists():
        return stats
    for run_dir in sorted(scenario_dir.glob("run-*")):
        leader_stat = None
        for server_file in sorted(run_dir.glob("server-*.json")):
            data = json.loads(server_file.read_text())
            candidate = {
                "run_id": run_dir.name,
                "fast_path_ratio": float(data["fast_path_ratio"]),
                "fast_path_decisions": int(data["fast_path_decisions"]),
                "slow_path_decisions": int(data["slow_path_decisions"]),
            }
            total = candidate["fast_path_decisions"] + candidate["slow_path_decisions"]
            if leader_stat is None:
                leader_stat = candidate
                continue
            current_total = leader_stat["fast_path_decisions"] + leader_stat["slow_path_decisions"]
            if total > current_total:
                leader_stat = candidate
        if leader_stat is not None:
            stats.append(leader_stat)
    return stats


def compute_latency_stats(frames: list[pd.DataFrame]) -> dict[str, dict]:
    if not frames:
        return {}

    combined = pd.concat(frames, ignore_index=True)
    stats_by_client = {}
    for client_id, client_df in combined.groupby("client_id"):
        run_stats = []
        for _, run_df in client_df.groupby("run_id"):
            latencies = run_df["response_latency"]
            run_stats.append(
                {
                    "mean": latencies.mean(),
                    "median": latencies.median(),
                    "p95": latencies.quantile(0.95),
                    "p99": latencies.quantile(0.99),
                }
            )
        stats_df = pd.DataFrame(run_stats)
        stats_by_client[client_id] = {
            "mean": float(stats_df["mean"].mean()),
            "mean_std": float(stats_df["mean"].std(ddof=0)),
            "median": float(stats_df["median"].mean()),
            "median_std": float(stats_df["median"].std(ddof=0)),
            "p95": float(stats_df["p95"].mean()),
            "p95_std": float(stats_df["p95"].std(ddof=0)),
            "p99": float(stats_df["p99"].mean()),
            "p99_std": float(stats_df["p99"].std(ddof=0)),
        }
    return stats_by_client


def compute_throughput_stats(frames: list[pd.DataFrame]) -> dict[str, dict]:
    if not frames:
        return {}

    combined = pd.concat(frames, ignore_index=True)
    stats_by_client = {}
    for client_id, client_df in combined.groupby("client_id"):
        throughputs = []
        for _, run_df in client_df.groupby("run_id"):
            duration_sec = (run_df["response_time"].max() - run_df["request_time"].min()) / 1000
            if duration_sec > 0:
                throughputs.append(len(run_df) / duration_sec)
        if throughputs:
            stats_by_client[client_id] = {
                "mean": float(np.mean(throughputs)),
                "std": float(np.std(throughputs)),
            }
    return stats_by_client


def compute_fast_path_stats(stats: list[dict]) -> dict:
    if not stats:
        return {}
    ratios = [entry["fast_path_ratio"] for entry in stats]
    return {"mean": float(np.mean(ratios)), "std": float(np.std(ratios))}


def load_leader_owd_frames(protocol: str, scenario: str, leader_server_id: int = LEADER_SERVER_ID) -> list[pd.DataFrame]:
    frames = []
    scenario_dir = LOG_ROOT / protocol / scenario
    if not scenario_dir.exists():
        return frames
    pattern = f"server-{leader_server_id}-owd.csv"
    for run_dir in sorted(scenario_dir.glob("run-*")):
        csv_file = run_dir / pattern
        if not csv_file.exists():
            continue
        df = pd.read_csv(csv_file)
        if df.empty:
            continue
        df["run_id"] = run_dir.name
        frames.append(df)
    return frames


def aggregate_latency_timeseries(frames: list[pd.DataFrame], bin_ms: int = TIME_BIN_MS) -> dict[str, pd.DataFrame]:
    if not frames:
        return {}

    combined = pd.concat(frames, ignore_index=True)
    combined["elapsed_ms"] = combined.groupby("run_id")["response_time"].transform(lambda s: s - s.min())
    combined["elapsed_bin_ms"] = (combined["elapsed_ms"] // bin_ms) * bin_ms

    series_by_client = {}
    for client_id, client_df in combined.groupby("client_id"):
        per_run = (
            client_df.groupby(["run_id", "elapsed_bin_ms"], as_index=False)["response_latency"]
            .mean()
            .rename(columns={"response_latency": "mean_latency_ms"})
        )
        aggregated = (
            per_run.groupby("elapsed_bin_ms", as_index=False)["mean_latency_ms"]
            .mean()
            .sort_values("elapsed_bin_ms")
        )
        aggregated["elapsed_s"] = aggregated["elapsed_bin_ms"] / 1000.0
        series_by_client[client_id] = aggregated
    return series_by_client


def aggregate_deadline_wait_timeseries(frames: list[pd.DataFrame], bin_ms: int = TIME_BIN_MS) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not frames:
        return pd.DataFrame(), pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined["peer_id"] = pd.to_numeric(combined["peer_id"], errors="coerce")
    combined["elapsed_ms"] = combined.groupby("run_id")["timestamp_ms"].transform(lambda s: s - s.min())
    combined["elapsed_bin_ms"] = (combined["elapsed_ms"] // bin_ms) * bin_ms

    max_outgoing = (
        combined[combined["metric"] == "max_outgoing"]
        .groupby(["run_id", "elapsed_bin_ms"], as_index=False)["owd_us"]
        .mean()
        .rename(columns={"owd_us": "max_outgoing_us"})
    )
    max_aggregated = (
        max_outgoing.groupby("elapsed_bin_ms", as_index=False)["max_outgoing_us"]
        .mean()
        .sort_values("elapsed_bin_ms")
    )
    if not max_aggregated.empty:
        max_aggregated["elapsed_s"] = max_aggregated["elapsed_bin_ms"] / 1000.0
        max_aggregated["max_outgoing_ms"] = max_aggregated["max_outgoing_us"] / 1000.0

    outgoing = (
        combined[(combined["metric"] == "outgoing") & combined["peer_id"].notna()]
        .groupby(["run_id", "elapsed_bin_ms", "peer_id"], as_index=False)["owd_us"]
        .mean()
        .rename(columns={"owd_us": "outgoing_us"})
    )

    if outgoing.empty or max_outgoing.empty:
        return max_aggregated, pd.DataFrame()

    slack = outgoing.merge(max_outgoing, on=["run_id", "elapsed_bin_ms"], how="inner")
    slack["slack_us"] = (slack["max_outgoing_us"] - slack["outgoing_us"]).clip(lower=0)
    slack_aggregated = (
        slack.groupby(["elapsed_bin_ms", "peer_id"], as_index=False)["slack_us"]
        .mean()
        .sort_values(["peer_id", "elapsed_bin_ms"])
    )
    slack_aggregated["elapsed_s"] = slack_aggregated["elapsed_bin_ms"] / 1000.0
    slack_aggregated["slack_ms"] = slack_aggregated["slack_us"] / 1000.0
    return max_aggregated, slack_aggregated


def plot_latency_by_client(axes, latency_stats: dict[str, dict[str, dict]], protocols: list[str], clients: list[str]):
    x = np.arange(len(clients))
    width = 0.35 if len(protocols) > 1 else 0.5

    for metric_index, metric in enumerate(LATENCY_METRICS):
        ax = axes[metric_index]
        for protocol_index, protocol in enumerate(protocols):
            values = []
            errors = []
            for client_id in clients:
                client_stats = latency_stats.get(protocol, {}).get(client_id, {})
                values.append(client_stats.get(metric, 0.0))
                errors.append(client_stats.get(f"{metric}_std", 0.0))
            offset = (protocol_index - (len(protocols) - 1) / 2) * width
            bars = ax.bar(
                x + offset,
                values,
                width,
                yerr=errors,
                capsize=6,
                color=PROTOCOL_COLORS.get(protocol, "#4c4c4c"),
                alpha=0.85,
                label=PROTOCOL_LABELS.get(protocol, protocol),
            )
            for bar, value in zip(bars, values):
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height(),
                    f"{value:.2f}",
                    ha="center",
                    va="bottom",
                    fontsize=9,
                )

        ax.set_title(metric.upper(), fontsize=14)
        ax.set_ylabel("Latency (ms)", fontsize=12)
        ax.set_xticks(x)
        ax.set_xticklabels([CLIENT_LABELS.get(client, client) for client in clients], fontsize=10)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.set_ylim(bottom=0)

    axes[0].legend(fontsize=10)


def load_baseline_latency(baseline_scenario: str, clients: list[str]) -> dict[str, dict]:
    frames = load_client_frames("baseline", baseline_scenario)
    stats = compute_latency_stats(frames)
    aggregated = {}
    for client_id in clients:
        client_stats = stats.get(client_id)
        if client_stats:
            aggregated[client_id] = client_stats
    return aggregated


def plot_clock_quality_comparison(axes, quality_latency_stats: dict[str, dict[str, dict[str, dict]]], clients: list[str], baseline_latency: dict[str, dict]):
    x = np.arange(len(CLOCK_QUALITIES))
    width = 0.35 if len(clients) > 1 else 0.5

    for metric_index, metric in enumerate(LATENCY_METRICS):
        ax = axes[metric_index]
        for client_index, client_id in enumerate(clients):
            values = []
            errors = []
            for quality in CLOCK_QUALITIES:
                client_stats = (
                    quality_latency_stats.get(quality, {})
                    .get("caop", {})
                    .get(client_id, {})
                )
                values.append(client_stats.get(metric, 0.0))
                errors.append(client_stats.get(f"{metric}_std", 0.0))

            offset = (client_index - (len(clients) - 1) / 2) * width
            alpha = 0.85 if client_index == 0 else 0.45
            hatch = None if client_index == 0 else "//"
            label = f"CA-OP / {CLIENT_LABELS.get(client_id, client_id)}"
            bars = ax.bar(
                x + offset,
                values,
                width,
                yerr=errors,
                capsize=5,
                color=PROTOCOL_COLORS["caop"],
                alpha=alpha,
                hatch=hatch,
                label=label if metric_index == 0 else None,
            )
            for bar, value in zip(bars, values):
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height(),
                    f"{value:.2f}",
                    ha="center",
                    va="bottom",
                    fontsize=8,
                )

            baseline_value = baseline_latency.get(client_id, {}).get(metric)
            if baseline_value is not None:
                line_style = "--" if client_index == 0 else ":"
                ax.axhline(
                    baseline_value,
                    color=PROTOCOL_COLORS["baseline"],
                    linestyle=line_style,
                    linewidth=2,
                    label=(
                        f"Baseline / {CLIENT_LABELS.get(client_id, client_id)}"
                        if metric_index == 0
                        else None
                    ),
                )
                ax.text(
                    x[-1] + 0.45,
                    baseline_value,
                    f"{baseline_value:.2f}",
                    color=PROTOCOL_COLORS["baseline"],
                    fontsize=8,
                    va="center",
                )

        ax.set_title(f"{metric.upper()} by Clock Quality", fontsize=14)
        ax.set_ylabel("Latency (ms)", fontsize=12)
        ax.set_xticks(x)
        ax.set_xticklabels([quality.capitalize() for quality in CLOCK_QUALITIES], fontsize=10)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.set_ylim(bottom=0)

    axes[0].legend(fontsize=8)


def plot_fast_path_clock_quality(ax, quality_fast_path_stats: dict[str, dict], baseline_fast_path: dict | None):
    x = np.arange(len(CLOCK_QUALITIES))
    values = [quality_fast_path_stats.get(quality, {}).get("mean", 0.0) for quality in CLOCK_QUALITIES]
    errors = [quality_fast_path_stats.get(quality, {}).get("std", 0.0) for quality in CLOCK_QUALITIES]

    bars = ax.bar(
        x,
        values,
        width=0.55,
        yerr=errors,
        capsize=6,
        color=PROTOCOL_COLORS["caop"],
        alpha=0.85,
        label="CA-OP",
    )
    for bar, value in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{value:.3f}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    baseline_value = 0.0 if not baseline_fast_path else baseline_fast_path.get("mean", 0.0)
    ax.axhline(
        baseline_value,
        color=PROTOCOL_COLORS["baseline"],
        linestyle="--",
        linewidth=2,
        label="Baseline",
    )

    ax.set_title("Fast Path Ratio by Clock Quality", fontsize=14)
    ax.set_ylabel("Fast Path Ratio", fontsize=12)
    ax.set_xticks(x)
    ax.set_xticklabels([quality.capitalize() for quality in CLOCK_QUALITIES], fontsize=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_ylim(0, 1.05)
    ax.legend(fontsize=9)


def plot_deadline_wait_approx(
    axes,
    latency_series: dict[str, pd.DataFrame],
    max_outgoing_series: pd.DataFrame,
    slack_series: pd.DataFrame,
    clients: list[str],
):
    latency_ax = axes[0]
    for client_id in clients:
        client_series = latency_series.get(client_id)
        if client_series is None or client_series.empty:
            continue
        latency_ax.plot(
            client_series["elapsed_s"],
            client_series["mean_latency_ms"],
            linewidth=2,
            label=CLIENT_LABELS.get(client_id, client_id),
        )
    latency_ax.set_title("Latency vs Approx Deadline Wait", fontsize=14)
    latency_ax.set_xlabel("Elapsed Time (s)", fontsize=11)
    latency_ax.set_ylabel("Client Mean Latency (ms)", fontsize=12)
    latency_ax.spines["top"].set_visible(False)

    wait_ax = latency_ax.twinx()
    if not max_outgoing_series.empty:
        wait_ax.plot(
            max_outgoing_series["elapsed_s"],
            max_outgoing_series["max_outgoing_ms"],
            color="#222222",
            linestyle="--",
            linewidth=2,
            label="Approx deadline wait",
        )
    wait_ax.set_ylabel("Approx Deadline Wait (ms)", fontsize=12, color="#222222")
    wait_ax.tick_params(axis="y", colors="#222222")
    wait_ax.spines["top"].set_visible(False)

    lines, labels = latency_ax.get_legend_handles_labels()
    wait_lines, wait_labels = wait_ax.get_legend_handles_labels()
    latency_ax.legend(lines + wait_lines, labels + wait_labels, fontsize=9, loc="upper right")

    slack_ax = axes[1]
    if not slack_series.empty:
        for peer_id, peer_series in slack_series.groupby("peer_id"):
            slack_ax.plot(
                peer_series["elapsed_s"],
                peer_series["slack_ms"],
                linewidth=2,
                label=f"Peer {int(peer_id)} slack",
            )
    slack_ax.set_title("Approx Peer Slack to Deadline", fontsize=14)
    slack_ax.set_xlabel("Elapsed Time (s)", fontsize=11)
    slack_ax.set_ylabel("Slack (ms)", fontsize=12)
    slack_ax.spines["top"].set_visible(False)
    slack_ax.spines["right"].set_visible(False)
    slack_ax.set_ylim(bottom=0)
    if not slack_series.empty:
        slack_ax.legend(fontsize=9, loc="upper right")


def print_summary(scenario: str, protocols: list[str], clients: list[str], latency_stats: dict, throughput_stats: dict, fast_path_stats: dict):
    print(f"Scenario: {scenario}")
    for protocol in protocols:
        print(PROTOCOL_LABELS.get(protocol, protocol))
        for client_id in clients:
            latency = latency_stats.get(protocol, {}).get(client_id)
            throughput = throughput_stats.get(protocol, {}).get(client_id)
            label = CLIENT_LABELS.get(client_id, client_id)
            print(f"  {label}")
            print(f"    latency={latency}")
            print(f"    throughput={throughput}")
        print(f"  fast_path={fast_path_stats.get(protocol)}")


def main():
    protocols = available_protocols()
    if not protocols:
        raise SystemExit(f"No protocol directories found under {LOG_ROOT}")
    scenario = choose_scenario(protocols)

    latency_stats = {}
    throughput_stats = {}
    fast_path_stats = {}
    clients = set()
    for protocol in protocols:
        frames = load_client_frames(protocol, scenario)
        latency_stats[protocol] = compute_latency_stats(frames)
        throughput_stats[protocol] = compute_throughput_stats(frames)
        fast_path_stats[protocol] = compute_fast_path_stats(load_server_stats(protocol, scenario))
        clients.update(latency_stats[protocol].keys())

    ordered_clients = sorted(clients)
    print_summary(scenario, protocols, ordered_clients, latency_stats, throughput_stats, fast_path_stats)

    fig, axes = plt.subplots(1, len(LATENCY_METRICS), figsize=(22, 5), layout="constrained")
    plot_latency_by_client(axes, latency_stats, protocols, ordered_clients)

    output_dir = LOG_ROOT / "plots"
    output_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_dir / f"{scenario}-latency-by-client.png", dpi=200, bbox_inches="tight")

    # caop_latency_frames = load_client_frames("caop", scenario)
    # caop_owd_frames = load_leader_owd_frames("caop", scenario)
    # if caop_latency_frames and caop_owd_frames:
    #     latency_series = aggregate_latency_timeseries(caop_latency_frames)
    #     max_outgoing_series, slack_series = aggregate_deadline_wait_timeseries(caop_owd_frames)
    #     if latency_series and not max_outgoing_series.empty:
    #         wait_fig, wait_axes = plt.subplots(2, 1, figsize=(14, 8), layout="constrained")
    #         plot_deadline_wait_approx(
    #             wait_axes,
    #             latency_series,
    #             max_outgoing_series,
    #             slack_series,
    #             ordered_clients,
    #         )
    #         wait_fig.savefig(
    #             output_dir / f"{scenario}-deadline-wait-approx.png",
    #             dpi=200,
    #             bbox_inches="tight",
    #         )

    quality_scenarios, baseline_quality_scenario = shared_clock_quality_scenarios(protocols, scenario)
    if quality_scenarios:
        if parse_quality_scenario(scenario) is not None:
            topology, _, rps = parse_quality_scenario(scenario)
        else:
            topology, rps = parse_baseline_scenario(scenario)
        quality_latency_stats = {}
        quality_fast_path_stats = {}
        for quality, quality_scenario in quality_scenarios.items():
            quality_latency_stats[quality] = {}
            frames = load_client_frames("caop", quality_scenario)
            quality_latency_stats[quality]["caop"] = compute_latency_stats(frames)
            quality_fast_path_stats[quality] = compute_fast_path_stats(load_server_stats("caop", quality_scenario))

        baseline_latency = load_baseline_latency(baseline_quality_scenario, ordered_clients)
        baseline_fast_path = compute_fast_path_stats(load_server_stats("baseline", baseline_quality_scenario))
        quality_fig, quality_axes = plt.subplots(1, len(LATENCY_METRICS), figsize=(24, 5), layout="constrained")
        plot_clock_quality_comparison(quality_axes, quality_latency_stats, ordered_clients, baseline_latency)
        quality_fig.savefig(
            output_dir / f"{topology}-clock-quality-rps{rps}-latency-comparison.png",
            dpi=200,
            bbox_inches="tight",
        )

        fast_path_fig, fast_path_ax = plt.subplots(1, 1, figsize=(8, 5), layout="constrained")
        plot_fast_path_clock_quality(fast_path_ax, quality_fast_path_stats, baseline_fast_path)
        fast_path_fig.savefig(
            output_dir / f"{topology}-clock-quality-rps{rps}-fast-path-comparison.png",
            dpi=200,
            bbox_inches="tight",
        )

    plt.show()


if __name__ == "__main__":
    main()
