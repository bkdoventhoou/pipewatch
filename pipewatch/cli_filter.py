"""CLI entry point for filtering and displaying pipeline metrics from history."""

import argparse
import sys
from typing import List

from pipewatch.config import load_config
from pipewatch.cli_report import build_collector_from_config
from pipewatch.metrics import MetricStatus
from pipewatch.filter import apply_filters
from pipewatch.formatters import get_formatter


STATUS_MAP = {
    "ok": MetricStatus.OK,
    "warning": MetricStatus.WARNING,
    "critical": MetricStatus.CRITICAL,
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Filter pipeline metrics from collected history."
    )
    parser.add_argument("--config", default="pipewatch.yaml", help="Config file path")
    parser.add_argument("--pipeline", default=None, help="Filter by pipeline name")
    parser.add_argument("--metric", default=None, help="Filter by metric name")
    parser.add_argument(
        "--status",
        nargs="+",
        choices=["ok", "warning", "critical"],
        default=None,
        help="Filter by status (multiple allowed)",
    )
    parser.add_argument(
        "--format",
        dest="fmt",
        choices=["text", "json"],
        default="text",
        help="Output format",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    collector = build_collector_from_config(config)

    all_metrics = collector.get_history()

    statuses = (
        [STATUS_MAP[s] for s in args.status] if args.status else None
    )

    filtered = apply_filters(
        all_metrics,
        pipeline=args.pipeline,
        name=args.metric,
        statuses=statuses,
    )

    formatter = get_formatter(args.fmt)
    for metric in filtered:
        print(formatter(metric))

    return 0


if __name__ == "__main__":
    sys.exit(main())
