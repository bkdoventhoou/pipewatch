"""CLI for viewing the pipeline metric audit log."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.audit import AuditLog
from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show audit log of metric status transitions"
    )
    parser.add_argument("--config", default="pipewatch.yaml", help="Config file path")
    parser.add_argument("--pipeline", default=None, help="Filter by pipeline name")
    parser.add_argument("--metric", default=None, help="Filter by metric name")
    parser.add_argument(
        "--format", choices=["text", "json"], default="text", dest="fmt"
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    config = load_config(args.config)
    collector = build_collector_from_config(config)

    log = AuditLog()
    for metrics in collector.get_history().values():
        for m in metrics:
            log.record(m)

    events = log.get_events(pipeline=args.pipeline, metric_name=args.metric)

    if not events:
        print("No audit events found.")
        return

    if args.fmt == "json":
        print(json.dumps([e.to_dict() for e in events], indent=2))
    else:
        for e in events:
            prev = e.previous_status.value if e.previous_status else "(new)"
            print(
                f"[{e.timestamp.isoformat()}] "
                f"{e.pipeline}/{e.metric_name}: "
                f"{prev} -> {e.current_status.value} "
                f"(value={e.value})"
            )


if __name__ == "__main__":
    main(sys.argv[1:])
