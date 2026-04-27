"""CLI command for analyzing inter-metric arrival latency across pipelines."""

import argparse
import json
import sys

from pipewatch.collector import MetricCollector
from pipewatch.config import load_config
from pipewatch.latency import analyze_all_latencies


def parse_args(argv=None):
    """Parse CLI arguments for the latency command."""
    parser = argparse.ArgumentParser(
        prog="pipewatch latency",
        description="Analyze inter-metric arrival latency for pipeline metrics.",
    )
    parser.add_argument(
        "--config",
        default="pipewatch.yaml",
        help="Path to pipewatch config file (default: pipewatch.yaml).",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        help="Restrict analysis to a specific pipeline name.",
    )
    parser.add_argument(
        "--metric",
        default=None,
        help="Restrict analysis to a specific metric name.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--min-gap",
        type=float,
        default=None,
        help="Only show results where average gap exceeds this value (seconds).",
    )
    return parser.parse_args(argv)


def _format_text(results):
    """Render latency results as human-readable text."""
    if not results:
        return "No latency data available."
    lines = []
    for r in results:
        lines.append(
            f"[{r.pipeline}] {r.metric_name}: "
            f"avg_gap={r.average_gap:.2f}s  "
            f"min_gap={r.min_gap:.2f}s  "
            f"max_gap={r.max_gap:.2f}s  "
            f"samples={r.sample_count}"
        )
    return "\n".join(lines)


def main(argv=None):
    """Entry point for the latency CLI command."""
    args = parse_args(argv)

    config = load_config(args.config)
    thresholds = config.get("thresholds", {})

    collector = MetricCollector()
    for pipeline, metrics in thresholds.items():
        for metric_name, cfg in metrics.items():
            collector.add_threshold(pipeline, metric_name, cfg.get("warning"), cfg.get("critical"))

    history = collector.get_history()

    # Filter history by pipeline / metric if requested
    filtered = {}
    for key, series in history.items():
        p, m = key
        if args.pipeline and p != args.pipeline:
            continue
        if args.metric and m != args.metric:
            continue
        filtered[key] = series

    results = analyze_all_latencies(filtered)

    # Apply minimum-gap filter
    if args.min_gap is not None:
        results = [r for r in results if r.average_gap >= args.min_gap]

    if args.format == "json":
        output = json.dumps([r.to_dict() for r in results], indent=2)
        print(output)
    else:
        print(_format_text(results))

    if not results:
        sys.exit(0)


if __name__ == "__main__":
    main()
