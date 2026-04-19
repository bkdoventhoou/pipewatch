"""CLI command for displaying metric trend analysis."""
import argparse
import json
import sys
from pipewatch.config import load_config
from pipewatch.cli_report import build_collector_from_config
from pipewatch.trend import analyze_all_trends


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Analyze metric trends from pipeline history.")
    parser.add_argument("--config", default="pipewatch.yaml", help="Path to config file")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    parser.add_argument("--threshold", type=float, default=0.01, help="Slope threshold for stable classification")
    parser.add_argument("--pipeline", default=None, help="Filter by pipeline name")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    collector = build_collector_from_config(config)

    raw_history = collector.get_history()
    grouped = {}
    for metric in raw_history:
        key = (metric.pipeline, metric.name)
        grouped.setdefault(key, []).append(metric)

    if args.pipeline:
        grouped = {k: v for k, v in grouped.items() if k[0] == args.pipeline}

    trends = analyze_all_trends(grouped, threshold=args.threshold)

    if not trends:
        print("No trend data available.")
        sys.exit(0)

    if args.format == "json":
        print(json.dumps([t.to_dict() for t in trends], indent=2))
    else:
        for t in trends:
            print(
                f"[{t.pipeline}] {t.metric_name}: {t.direction.upper()} "
                f"(slope={t.slope:.4f}, avg={t.avg:.4f}, min={t.min}, max={t.max}, n={len(t.values)})"
            )


if __name__ == "__main__":
    main()
