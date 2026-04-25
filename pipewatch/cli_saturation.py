"""CLI entry-point for saturation analysis."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.config import load_config
from pipewatch.cli_report import build_collector_from_config
from pipewatch.saturation import analyze_saturation


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report metric saturation relative to critical thresholds."
    )
    parser.add_argument(
        "--config",
        default="pipewatch.yaml",
        help="Path to pipewatch config file (default: pipewatch.yaml)",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--min-saturation",
        type=float,
        default=0.0,
        help="Only show entries at or above this saturation %% (default: 0)",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    config = load_config(args.config)

    thresholds_cfg = config.get("thresholds", {})
    if not thresholds_cfg:
        print("No thresholds defined in config.", file=sys.stderr)
        sys.exit(1)

    # Build critical-threshold map: key -> critical value
    critical_map = {
        key: float(spec["critical"])
        for key, spec in thresholds_cfg.items()
        if "critical" in spec
    }

    if not critical_map:
        print("No critical thresholds found in config.", file=sys.stderr)
        sys.exit(1)

    collector = build_collector_from_config(config)
    results = analyze_saturation(collector, critical_map)
    results = [r for r in results if r.saturation_pct >= args.min_saturation]

    if not results:
        print("No saturation data available.")
        return

    if args.format == "json":
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        header = f"{'Pipeline':<20} {'Metric':<20} {'Value':>10} {'Critical':>10} {'Saturation%':>12} {'Status':<10}"
        print(header)
        print("-" * len(header))
        for r in results:
            print(
                f"{r.pipeline:<20} {r.metric_name:<20} "
                f"{r.current_value:>10.2f} {r.critical_threshold:>10.2f} "
                f"{r.saturation_pct:>11.1f}% {r.status.value:<10}"
            )


if __name__ == "__main__":  # pragma: no cover
    main()
