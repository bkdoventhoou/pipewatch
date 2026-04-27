"""CLI entry point for volatility analysis."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config
from pipewatch.volatility import analyze_all_volatility


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze metric volatility using coefficient of variation."
    )
    parser.add_argument("--config", default="pipewatch.yaml", help="Path to config file")
    parser.add_argument(
        "--threshold-cv",
        type=float,
        default=0.5,
        help="CV threshold above which a metric is considered volatile (default: 0.5)",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=4,
        help="Minimum number of samples required (default: 4)",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format",
    )
    parser.add_argument(
        "--volatile-only",
        action="store_true",
        help="Only show volatile metrics",
    )
    return parser.parse_args(argv)


def _format_text(results: dict) -> str:
    if not results:
        return "No volatility data available (insufficient samples)."
    lines = []
    for key, r in sorted(results.items()):
        flag = " [VOLATILE]" if r.is_volatile else ""
        lines.append(
            f"{r.pipeline}/{r.metric_name}{flag}  "
            f"cv={r.coefficient_of_variation:.4f}  "
            f"mean={r.mean:.4f}  "
            f"std={r.std_dev:.4f}  "
            f"n={r.sample_count}"
        )
    return "\n".join(lines)


def main(argv=None) -> None:
    args = parse_args(argv)
    config = load_config(args.config)
    collector = build_collector_from_config(config)

    results = analyze_all_volatility(
        collector,
        threshold_cv=args.threshold_cv,
        min_samples=args.min_samples,
    )

    if args.volatile_only:
        results = {k: v for k, v in results.items() if v.is_volatile}

    if args.format == "json":
        print(json.dumps({k: v.to_dict() for k, v in results.items()}, indent=2))
    else:
        print(_format_text(results))


if __name__ == "__main__":
    main()
