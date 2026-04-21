"""CLI entry point for metric sampling."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config
from pipewatch.sampling import sample_all


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipewatch-sampling",
        description="Downsample pipeline metric histories.",
    )
    parser.add_argument("--config", default="pipewatch.yaml", help="Config file path")
    parser.add_argument(
        "--max-count",
        type=int,
        default=None,
        help="Maximum number of samples to keep per series",
    )
    parser.add_argument(
        "--every-nth",
        type=int,
        default=None,
        help="Keep every Nth metric in each series",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)

    if args.max_count is None and args.every_nth is None:
        print("Error: specify --max-count or --every-nth", file=sys.stderr)
        sys.exit(1)

    cfg = load_config(args.config)
    collector = build_collector_from_config(cfg)
    history = collector.get_history()

    series_list = sample_all(
        history,
        max_count=args.max_count,
        every_nth=args.every_nth,
    )

    if args.format == "json":
        print(json.dumps([s.to_dict() for s in series_list], indent=2))
    else:
        if not series_list:
            print("No series available.")
            return
        for s in series_list:
            print(
                f"[{s.pipeline}] {s.metric_name}: {s.sample_count} samples"
                if hasattr(s, 'sample_count')
                else f"[{s.pipeline}] {s.metric_name}: {len(s.samples)} samples"
            )
            for m in s.samples:
                print(f"  ts={m.timestamp:.3f}  value={m.value}  status={m.status.value}")


if __name__ == "__main__":
    main()
