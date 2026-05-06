"""CLI command: pipewatch entropy — show value-distribution entropy per metric."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.entropy import analyze_all_entropies


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipewatch entropy",
        description="Analyse Shannon entropy of metric value distributions.",
    )
    parser.add_argument("--config", default="pipewatch.yaml", help="Config file path")
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.75,
        help="Normalised entropy threshold for high-entropy flag (default 0.75)",
    )
    parser.add_argument(
        "--bins",
        type=int,
        default=10,
        help="Number of histogram bins (default 10)",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        dest="fmt",
    )
    return parser.parse_args(argv)


def _format_text(results) -> str:
    if not results:
        return "No entropy results available."
    lines = []
    for r in results:
        flag = " [HIGH]" if r.high_entropy else ""
        lines.append(
            f"{r.pipeline}/{r.metric_name}: "
            f"entropy={r.entropy:.4f} normalised={r.normalised:.4f} "
            f"n={r.sample_count}{flag}"
        )
    return "\n".join(lines)


def main(argv=None) -> None:
    args = parse_args(argv)
    collector = build_collector_from_config(args.config)
    results = analyze_all_entropies(
        collector.get_history(),
        threshold=args.threshold,
        bins=args.bins,
    )
    if args.fmt == "json":
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(_format_text(results))


if __name__ == "__main__":  # pragma: no cover
    main()
