"""CLI entry-point for cycle detection."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.cycle import detect_all_cycles


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Detect repeating cycles in pipeline metric history."
    )
    parser.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file."
    )
    parser.add_argument(
        "--format", choices=["text", "json"], default="text", dest="fmt"
    )
    parser.add_argument(
        "--min-samples", type=int, default=8, help="Minimum samples required."
    )
    parser.add_argument(
        "--confidence", type=float, default=0.75, help="Confidence threshold (0-1)."
    )
    parser.add_argument(
        "--only-cyclic", action="store_true", help="Only show cyclic results."
    )
    return parser.parse_args(argv)


def _format_text(results: dict) -> str:
    if not results:
        return "No cycle results found."
    lines = []
    for key, r in results.items():
        cyclic_label = "CYCLIC" if r.is_cyclic else "stable"
        lines.append(
            f"{key}: {cyclic_label}  period={r.period}  confidence={r.confidence:.3f}"
        )
    return "\n".join(lines)


def main(argv=None) -> None:
    args = parse_args(argv)
    collector = build_collector_from_config(args.config)
    results = detect_all_cycles(
        collector,
        min_samples=args.min_samples,
        confidence_threshold=args.confidence,
    )
    if args.only_cyclic:
        results = {k: v for k, v in results.items() if v.is_cyclic}
    if args.fmt == "json":
        print(json.dumps({k: v.to_dict() for k, v in results.items()}, indent=2))
    else:
        print(_format_text(results))


if __name__ == "__main__":
    main()
