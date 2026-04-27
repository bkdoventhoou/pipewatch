"""CLI entry-point: pipewatch velocity — show rate-of-change for pipeline metrics."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config
from pipewatch.velocity import compute_all_velocities


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipewatch velocity",
        description="Show rate-of-change (velocity) for monitored metrics.",
    )
    parser.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file."
    )
    parser.add_argument(
        "--pipeline", default=None, help="Filter to a specific pipeline."
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    return parser.parse_args(argv)


def _format_text(results) -> str:
    if not results:
        return "No velocity data available."
    lines = []
    for r in results:
        lines.append(
            f"{r.pipeline}/{r.metric_name}: "
            f"{r.velocity:+.4f}/s  [{r.direction}]  "
            f"(n={r.sample_count}, span={r.span_seconds:.1f}s)"
        )
    return "\n".join(lines)


def main(argv=None) -> None:
    args = parse_args(argv)
    cfg = load_config(args.config)
    collector = build_collector_from_config(cfg)

    results = compute_all_velocities(collector.get_history())

    if args.pipeline:
        results = [r for r in results if r.pipeline == args.pipeline]

    if args.format == "json":
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(_format_text(results))


if __name__ == "__main__":  # pragma: no cover
    main()
