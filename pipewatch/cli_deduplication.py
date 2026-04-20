"""CLI for inspecting deduplication state across a live collector run."""

from __future__ import annotations

import argparse
import json
import sys
import time

from pipewatch.config import load_config
from pipewatch.cli_report import build_collector_from_config
from pipewatch.deduplication import AlertDeduplicator


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show deduplication stats for pipeline alerts."
    )
    parser.add_argument(
        "--config",
        default="pipewatch.yaml",
        help="Path to config file (default: pipewatch.yaml)",
    )
    parser.add_argument(
        "--window",
        type=float,
        default=300.0,
        help="Deduplication window in seconds (default: 300)",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        dest="fmt",
        help="Output format (default: text)",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    config = load_config(args.config)
    collector = build_collector_from_config(config)
    dedup = AlertDeduplicator(window_seconds=args.window)

    results = []
    for key, history in collector.get_history().items():
        for metric in history:
            if not dedup.is_duplicate(metric):
                entry = dedup.get_entry(metric)
                if entry:
                    results.append(entry.to_dict())

    evicted = dedup.evict_expired()
    stats = dedup.stats()
    stats["evicted_this_run"] = evicted

    if args.fmt == "json":
        output = {"stats": stats, "entries": results}
        print(json.dumps(output, indent=2))
    else:
        print(f"Deduplication window : {args.window}s")
        print(f"Tracked entries      : {stats['tracked']}")
        print(f"Evicted this run     : {evicted}")
        if results:
            print("\nNew alerts (not duplicates):")
            for r in results:
                print(
                    f"  [{r['status'].upper()}] {r['key'][:12]}... "
                    f"first={r['first_seen']:.0f} count={r['count']}"
                )
        else:
            print("\nNo new alerts recorded.")


if __name__ == "__main__":
    main()
