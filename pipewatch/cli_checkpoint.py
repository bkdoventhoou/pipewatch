"""CLI for managing and inspecting pipeline checkpoints."""

from __future__ import annotations

import argparse
import json
import sys
import time

from pipewatch.checkpoint import (
    CheckpointStore,
    build_checkpoint,
    load_checkpoint_store,
    save_checkpoint_store,
)
from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect pipeline checkpoints")
    parser.add_argument("--config", default="pipewatch.yaml", help="Config file path")
    parser.add_argument("--checkpoint-file", default="checkpoints.json", help="Checkpoint store path")
    parser.add_argument(
        "--action",
        choices=["record", "show", "history"],
        default="show",
        help="Action to perform",
    )
    parser.add_argument("--pipeline", default=None, help="Filter by pipeline name")
    parser.add_argument("--run-id", default=None, help="Run ID for record action")
    parser.add_argument("--format", choices=["text", "json"], default="text", dest="fmt")
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    config = load_config(args.config)
    collector = build_collector_from_config(config)

    try:
        store = load_checkpoint_store(args.checkpoint_file)
    except (FileNotFoundError, json.JSONDecodeError):
        store = CheckpointStore()

    if args.action == "record":
        pipelines = [args.pipeline] if args.pipeline else list(collector.history.keys())
        run_id = args.run_id or str(int(time.time()))
        for pipeline in pipelines:
            metrics = collector.get_history(pipeline)
            if not metrics:
                continue
            entry = build_checkpoint(pipeline, run_id, metrics)
            store.record(entry)
        save_checkpoint_store(store, args.checkpoint_file)
        print(f"Checkpoints recorded (run_id={run_id}).")
        return

    if args.action == "show":
        pipelines = [args.pipeline] if args.pipeline else list(store.entries.keys())
        results = []
        for p in pipelines:
            entry = store.latest(p)
            if entry:
                results.append(entry.to_dict())
        if args.fmt == "json":
            print(json.dumps(results, indent=2))
        else:
            for r in results:
                print(
                    f"{r['pipeline']} | run={r['run_id']} | "
                    f"ok={r['ok_count']} warn={r['warning_count']} crit={r['critical_count']}"
                )
        return

    if args.action == "history":
        pipeline = args.pipeline
        if not pipeline:
            print("--pipeline required for history action", file=sys.stderr)
            sys.exit(1)
        entries = store.history(pipeline)
        if args.fmt == "json":
            print(json.dumps([e.to_dict() for e in entries], indent=2))
        else:
            for e in entries:
                print(
                    f"run={e.run_id} ts={e.timestamp:.0f} "
                    f"ok={e.ok_count} warn={e.warning_count} crit={e.critical_count}"
                )


if __name__ == "__main__":
    main()
