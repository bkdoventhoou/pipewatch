"""CLI for managing alert suppression windows."""

from __future__ import annotations

import argparse
import json
import sys
import time

from pipewatch.suppression import SuppressionRegistry, SuppressionWindow


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipewatch-suppress",
        description="Manage alert suppression windows.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    add_p = sub.add_parser("add", help="Add a suppression window")
    add_p.add_argument("pipeline", help="Pipeline name to suppress")
    add_p.add_argument("--metric", default=None, help="Specific metric name (omit for all)")
    add_p.add_argument("--duration", type=float, default=3600.0, help="Duration in seconds (default: 3600)")
    add_p.add_argument("--reason", default="", help="Reason for suppression")
    add_p.add_argument("--format", dest="fmt", choices=["text", "json"], default="text")

    list_p = sub.add_parser("list", help="List active suppression windows")
    list_p.add_argument("--format", dest="fmt", choices=["text", "json"], default="text")
    list_p.add_argument("--all", dest="show_all", action="store_true", help="Show all including expired")

    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    registry = SuppressionRegistry()
    now = time.time()

    if args.command == "add":
        window = SuppressionWindow(
            pipeline=args.pipeline,
            metric_name=args.metric,
            start_ts=now,
            end_ts=now + args.duration,
            reason=args.reason,
        )
        registry.add(window)
        if args.fmt == "json":
            print(json.dumps(window.to_dict(), indent=2))
        else:
            scope = f"{args.pipeline}/{args.metric}" if args.metric else args.pipeline
            print(f"[SUPPRESSED] {scope} for {args.duration:.0f}s"
                  + (f" — {args.reason}" if args.reason else ""))

    elif args.command == "list":
        windows = registry.all_windows() if args.show_all else registry.active_windows(now)
        if args.fmt == "json":
            print(json.dumps([w.to_dict() for w in windows], indent=2))
        else:
            if not windows:
                print("No active suppression windows.")
            else:
                for w in windows:
                    scope = f"{w.pipeline}/{w.metric_name}" if w.metric_name else w.pipeline
                    remaining = max(0.0, w.end_ts - now)
                    print(f"  {scope:40s}  {remaining:6.0f}s remaining  {w.reason}")


if __name__ == "__main__":
    main()
