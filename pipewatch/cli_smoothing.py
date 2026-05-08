"""CLI for inspecting exponential smoothing of pipeline metric series."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.collector import MetricCollector
from pipewatch.config import load_config
from pipewatch.smoothing import smooth_all


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipewatch-smoothing",
        description="Apply exponential smoothing to recorded metric series.",
    )
    parser.add_argument(
        "--config",
        default="pipewatch.yaml",
        help="Path to the pipewatch config file (default: pipewatch.yaml).",
    )
    parser.add_argument(
        "--alpha",
        type=float,
        default=0.3,
        help="Smoothing factor in (0, 1]. Higher values weight recent data more (default: 0.3).",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        help="Restrict output to a specific pipeline name.",
    )
    parser.add_argument(
        "--metric",
        default=None,
        help="Restrict output to a specific metric name.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def _format_text(results: list) -> str:
    """Render smoothed series as a human-readable table."""
    if not results:
        return "No smoothed series available."

    lines: list[str] = []
    for s in results:
        header = f"[{s.pipeline} / {s.metric_name}]  alpha={s.alpha:.3f}  points={len(s.smoothed)}"
        lines.append(header)
        # Show last five smoothed values for brevity
        tail = s.smoothed[-5:]
        formatted = "  ".join(f"{v:.4f}" for v in tail)
        lines.append(f"  last {len(tail)} smoothed values: {formatted}")
        lines.append("")
    return "\n".join(lines).rstrip()


def main(argv: list[str] | None = None) -> None:  # pragma: no cover – integration entry point
    args = parse_args(argv)

    config = load_config(args.config)
    thresholds = config.get("thresholds", {})

    collector = MetricCollector()
    for metric_name, cfg in thresholds.items():
        collector.add_threshold(metric_name, cfg.get("warning", 0), cfg.get("critical", 0))

    results = smooth_all(collector, alpha=args.alpha)

    # Apply optional filters
    if args.pipeline:
        results = [r for r in results if r.pipeline == args.pipeline]
    if args.metric:
        results = [r for r in results if r.metric_name == args.metric]

    if not results:
        print("No smoothed series matched the given filters.", file=sys.stderr)
        sys.exit(0)

    if args.format == "json":
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(_format_text(results))


if __name__ == "__main__":  # pragma: no cover
    main()
