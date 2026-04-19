"""CLI for capturing and diffing pipeline snapshots."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config
from pipewatch.snapshot import capture_snapshot, diff_snapshots, load_snapshot, save_snapshot


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture or diff pipeline snapshots")
    parser.add_argument("--config", default="pipewatch.yaml", help="Config file")
    sub = parser.add_subparsers(dest="command")

    cap = sub.add_parser("capture", help="Capture current metrics to a snapshot file")
    cap.add_argument("--output", required=True, help="Path to write snapshot JSON")

    diff = sub.add_parser("diff", help="Diff two snapshots")
    diff.add_argument("--old", required=True, help="Path to older snapshot")
    diff.add_argument("--new", required=True, help="Path to newer snapshot")

    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)

    if args.command == "capture":
        config = load_config(args.config)
        collector = build_collector_from_config(config)
        metrics = collector.latest()
        snap = capture_snapshot(metrics)
        save_snapshot(snap, args.output)
        print(f"Snapshot saved to {args.output} ({len(snap.entries)} entries)")

    elif args.command == "diff":
        old = load_snapshot(args.old)
        new = load_snapshot(args.new)
        diffs = diff_snapshots(old, new)
        if not diffs:
            print("No status changes detected.")
        else:
            print(f"{len(diffs)} status change(s) detected:")
            for d in diffs:
                print(
                    f"  [{d['pipeline']}] {d['metric_name']}: "
                    f"{d['old_status']} -> {d['new_status']} "
                    f"(value: {d['old_value']} -> {d['new_value']})"
                )
    else:
        print("Specify a command: capture | diff", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
