"""CLI entry point for generating pipeline health reports."""
import argparse
import sys

from pipewatch.config import load_config
from pipewatch.collector import MetricCollector
from pipewatch.metrics import PipelineMetric
from pipewatch.reporter import build_report
from pipewatch.formatters import get_formatter


def build_collector_from_config(config: dict) -> MetricCollector:
    """Initialise a MetricCollector from parsed config dict."""
    from pipewatch.config import load_thresholds
    collector = MetricCollector()
    thresholds = load_thresholds(config)
    for name, tc in thresholds.items():
        collector.add_threshold(name, warning=tc.warning, critical=tc.critical)
    return collector


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        prog="pipewatch report",
        description="Generate a pipeline health report.",
    )
    parser.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file."
    )
    parser.add_argument(
        "--format",
        choices=["text", "plain", "json"],
        default="text",
        help="Output format.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        help="Exit with non-zero code if any metric is CRITICAL.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        config = load_config(args.config)
    except FileNotFoundError:
        print(f"Config file not found: {args.config}", file=sys.stderr)
        sys.exit(2)

    collector = build_collector_from_config(config)
    report = build_report(collector)

    formatter = get_formatter(args.format)
    print(formatter(report))

    if args.exit_code:
        summary = report.summary()
        if summary.get("critical", 0) > 0:
            sys.exit(1)


if __name__ == "__main__":
    main()
