"""CLI entry point for the heatmap command."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config
from pipewatch.heatmap import build_heatmap


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipewatch heatmap",
        description="Show a time-bucketed status heatmap per pipeline.",
    )
    parser.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file."
    )
    parser.add_argument(
        "--pipeline", default=None, help="Restrict output to a specific pipeline."
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    return parser.parse_args(argv)


def _format_text(cells) -> str:
    if not cells:
        return "No heatmap data available."
    lines = [f"{'BUCKET':<18} {'PIPELINE':<20} {'OK':>5} {'WARN':>5} {'CRIT':>5} STATUS"]
    lines.append("-" * 65)
    for cell in cells:
        lines.append(
            f"{cell.bucket:<18} {cell.pipeline:<20} "
            f"{cell.ok:>5} {cell.warning:>5} {cell.critical:>5} "
            f"{cell.dominant_status.upper()}"
        )
    return "\n".join(lines)


def main(argv=None) -> None:
    args = parse_args(argv)
    config = load_config(args.config)
    collector = build_collector_from_config(config)
    history = collector.get_history()
    cells = build_heatmap(history, pipeline=args.pipeline)

    if args.format == "json":
        print(json.dumps([c.to_dict() for c in cells], indent=2))
    else:
        print(_format_text(cells))


if __name__ == "__main__":
    main()
