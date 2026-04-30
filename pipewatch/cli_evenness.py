"""CLI entry point for evenness analysis."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.evenness import analyze_all_evenness


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyse distribution evenness of pipeline metrics."
    )
    parser.add_argument("--config", default="pipewatch.yaml", help="Config file path.")
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.5,
        help="Normalised entropy below which a metric is considered uneven (default: 0.5).",
    )
    parser.add_argument(
        "--bins",
        type=int,
        default=10,
        help="Number of histogram bins (default: 10).",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=5,
        dest="min_samples",
        help="Minimum samples required for analysis (default: 5).",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        dest="fmt",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--uneven-only",
        action="store_true",
        dest="uneven_only",
        help="Only show metrics flagged as uneven.",
    )
    return parser.parse_args(argv)


def _format_text(results) -> str:
    if not results:
        return "No evenness results available."
    lines = []
    for key, r in sorted(results.items()):
        flag = " [UNEVEN]" if r.is_uneven else ""
        lines.append(
            f"{r.pipeline}/{r.metric_name}: entropy={r.entropy:.4f}"
            f" samples={r.sample_count}{flag}"
        )
    return "\n".join(lines)


def main(argv=None) -> None:
    args = parse_args(argv)
    collector = build_collector_from_config(args.config)
    results = analyze_all_evenness(
        collector,
        entropy_threshold=args.threshold,
        bins=args.bins,
        min_samples=args.min_samples,
    )
    if args.uneven_only:
        results = {k: v for k, v in results.items() if v.is_uneven}

    if args.fmt == "json":
        print(json.dumps({k: v.to_dict() for k, v in results.items()}, indent=2))
    else:
        print(_format_text(results))


if __name__ == "__main__":  # pragma: no cover
    main(sys.argv[1:])
