"""CLI: pipewatch dispersion — report value spread across pipeline metrics."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config
from pipewatch.dispersion import analyze_all_dispersions


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipewatch dispersion",
        description="Analyse value dispersion (spread) for each pipeline metric.",
    )
    parser.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file."
    )
    parser.add_argument(
        "--cv-threshold",
        type=float,
        default=0.5,
        help="Coefficient-of-variation threshold above which dispersion is 'high' (default: 0.5).",
    )
    parser.add_argument(
        "--high-only",
        action="store_true",
        help="Only show metrics with high dispersion.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def _format_text(results) -> str:
    if not results:
        return "No dispersion results available."
    lines = []
    for r in results:
        flag = " [HIGH]" if r.is_high else ""
        cv_str = f"{r.cv:.4f}" if r.cv is not None else "n/a"
        lines.append(
            f"{r.pipeline}/{r.metric_name}{flag}  "
            f"mean={r.mean:.4f}  std={r.std_dev:.4f}  "
            f"range={r.range_:.4f}  cv={cv_str}  n={r.count}"
        )
    return "\n".join(lines)


def main(argv=None) -> None:
    args = parse_args(argv)
    config = load_config(args.config)
    collector = build_collector_from_config(config)
    history = collector.get_history()

    results = analyze_all_dispersions(history, cv_threshold=args.cv_threshold)

    if args.high_only:
        results = [r for r in results if r.is_high]

    if args.format == "json":
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(_format_text(results))


if __name__ == "__main__":  # pragma: no cover
    main()
