"""CLI for outlier detection across collected pipeline metrics."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.outlier import detect_all_outliers


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Detect IQR-based outliers in pipeline metric history."
    )
    parser.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file"
    )
    parser.add_argument(
        "--multiplier",
        type=float,
        default=1.5,
        help="IQR fence multiplier (default: 1.5)",
    )
    parser.add_argument(
        "--only-outliers",
        action="store_true",
        help="Only show series where an outlier was detected",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    collector = build_collector_from_config(args.config)
    results = detect_all_outliers(
        collector.get_history(), multiplier=args.multiplier
    )

    if args.only_outliers:
        results = [r for r in results if r.is_outlier]

    if not results:
        print("No outlier data available.")
        return

    if args.format == "json":
        print(json.dumps([r.to_dict() for r in results], indent=2))
        return

    for r in results:
        flag = " [OUTLIER]" if r.is_outlier else ""
        print(
            f"{r.pipeline}/{r.metric_name}: value={r.value:.4f} "
            f"IQR=[{r.lower_fence:.4f}, {r.upper_fence:.4f}]{flag}"
        )


if __name__ == "__main__":
    main()
