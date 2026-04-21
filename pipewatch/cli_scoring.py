"""CLI entry-point for pipeline health scoring."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config
from pipewatch.scoring import score_all


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipewatch-score",
        description="Compute health scores for monitored pipelines.",
    )
    parser.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file."
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        help="Limit output to a specific pipeline.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:  # pragma: no cover – integration entry-point
    args = parse_args(argv)
    config = load_config(args.config)
    collector = build_collector_from_config(config)

    all_metrics = [
        m
        for history in collector.get_history().values()
        for m in history
    ]

    scores = score_all(all_metrics)
    if args.pipeline:
        scores = [s for s in scores if s.pipeline == args.pipeline]

    if not scores:
        print("No pipeline data available.", file=sys.stderr)
        sys.exit(1)

    if args.format == "json":
        print(json.dumps([s.to_dict() for s in scores], indent=2))
    else:
        for s in scores:
            print(
                f"[{s.grade}] {s.pipeline:30s}  score={s.score:6.1f}  "
                f"ok={s.ok_count}  warn={s.warning_count}  crit={s.critical_count}"
            )


if __name__ == "__main__":  # pragma: no cover
    main()
