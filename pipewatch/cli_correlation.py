"""CLI entry point for metric correlation analysis."""
import argparse
import json
from pipewatch.config import load_config
from pipewatch.cli_report import build_collector_from_config
from pipewatch.correlation import correlate_all, correlate_metrics


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Correlate pipeline metrics")
    p.add_argument("--config", default="pipewatch.yaml")
    p.add_argument("--pipeline", required=True, help="Pipeline name")
    p.add_argument("--metric-a", default=None)
    p.add_argument("--metric-b", default=None)
    p.add_argument("--format", choices=["text", "json"], default="text")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    cfg = load_config(args.config)
    collector = build_collector_from_config(cfg)
    all_metrics = []
    for name in collector.get_metric_names():
        all_metrics.extend(collector.get_history(name))

    if args.metric_a and args.metric_b:
        result = correlate_metrics(all_metrics, args.pipeline, args.metric_a, args.metric_b)
        results = [result] if result else []
    else:
        results = correlate_all(all_metrics, args.pipeline)

    if args.format == "json":
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        if not results:
            print("No correlation data available.")
            return
        for r in results:
            print(
                f"{r.pipeline} | {r.metric_a} <-> {r.metric_b} "
                f"| r={r.coefficient:.4f} (n={r.sample_size})"
            )


if __name__ == "__main__":
    main()
