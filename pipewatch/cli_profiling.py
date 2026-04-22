"""CLI entry point for pipewatch profiling command."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config
from pipewatch.profiling import profile_all


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipewatch profile",
        description="Show statistical profiles for all tracked pipeline metrics.",
    )
    parser.add_argument(
        "--config",
        default="pipewatch.yaml",
        help="Path to config file (default: pipewatch.yaml)",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        help="Filter output to a specific pipeline name",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        dest="fmt",
        help="Output format (default: text)",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    cfg = load_config(args.config)
    collector = build_collector_from_config(cfg)

    history = collector.get_history()
    profiles = profile_all(history)

    if args.pipeline:
        profiles = [p for p in profiles if p.pipeline == args.pipeline]

    if not profiles:
        print("No profiles available.", file=sys.stderr)
        return

    if args.fmt == "json":
        print(json.dumps([p.to_dict() for p in profiles], indent=2))
        return

    # text output
    header = f"{'PIPELINE':<20} {'METRIC':<20} {'COUNT':>6} {'MEAN':>10} {'STD':>10} {'MIN':>10} {'MAX':>10} {'P50':>10} {'P95':>10}"
    print(header)
    print("-" * len(header))
    for p in profiles:
        print(
            f"{p.pipeline:<20} {p.metric_name:<20} {p.count:>6} "
            f"{p.mean:>10.4f} {p.std:>10.4f} {p.min_val:>10.4f} "
            f"{p.max_val:>10.4f} {p.p50:>10.4f} {p.p95:>10.4f}"
        )


if __name__ == "__main__":  # pragma: no cover
    main()
