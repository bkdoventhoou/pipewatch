"""CLI entry point for generating and displaying pipeline digest reports."""

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.collector import MetricCollector
from pipewatch.digest import build_digest, format_digest_text
from pipewatch.formatters import get_formatter


def parse_args(argv=None):
    """Parse CLI arguments for the digest command."""
    parser = argparse.ArgumentParser(
        prog="pipewatch-digest",
        description="Generate a summary digest report of pipeline health metrics.",
    )
    parser.add_argument(
        "--config",
        default="pipewatch.yaml",
        help="Path to the pipewatch YAML config file (default: pipewatch.yaml).",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format: text (default) or json.",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        help="Restrict digest to a specific pipeline name.",
    )
    parser.add_argument(
        "--min-status",
        choices=["ok", "warning", "critical"],
        default=None,
        dest="min_status",
        help="Only include pipelines at or above this severity level.",
    )
    return parser.parse_args(argv)


_STATUS_RANK = {"ok": 0, "warning": 1, "critical": 2}


def main(argv=None):
    """Run the digest CLI command."""
    args = parse_args(argv)

    config = load_config(args.config)
    collector = MetricCollector()

    # Load thresholds and seed the collector from config
    from pipewatch.cli_report import build_collector_from_config
    build_collector_from_config(config, collector)

    # Build the digest from whatever metrics are recorded
    digest = build_digest(collector)

    # Filter by pipeline name if requested
    if args.pipeline:
        digest.pipelines = [
            p for p in digest.pipelines if p.pipeline == args.pipeline
        ]

    # Filter by minimum status rank if requested
    if args.min_status:
        min_rank = _STATUS_RANK[args.min_status]
        digest.pipelines = [
            p for p in digest.pipelines
            if _STATUS_RANK.get(p.health, 0) >= min_rank
        ]

    if args.format == "json":
        formatter = get_formatter("json")
        print(formatter(digest.to_dict()))
    else:
        print(format_digest_text(digest))

    # Exit with non-zero code if any pipeline is critical
    for pipeline in digest.pipelines:
        if pipeline.health == "critical":
            sys.exit(2)
    for pipeline in digest.pipelines:
        if pipeline.health == "warning":
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
