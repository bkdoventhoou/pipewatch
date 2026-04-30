"""CLI: enrich and display pipeline metrics with contextual metadata."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config
from pipewatch.enrichment import build_enricher_from_config
from pipewatch.formatters import get_formatter


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Display pipeline metrics enriched with contextual metadata."
    )
    parser.add_argument(
        "--config",
        default="pipewatch.yaml",
        help="Path to configuration file (default: pipewatch.yaml)",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        help="Filter output to a specific pipeline name",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        dest="fmt",
        help="Output format (default: text)",
    )
    return parser.parse_args(argv)


def _format_enriched_text(enriched) -> None:
    """Print enriched metrics in human-readable text format."""
    if not enriched:
        print("No metrics found.")
        return
    for e in enriched:
        ctx_str = ", ".join(f"{k}={v}" for k, v in e.context.items())
        ctx_display = f" [{ctx_str}]" if ctx_str else ""
        print(
            f"{e.metric.pipeline}/{e.metric.name} "
            f"{e.metric.status.value} "
            f"value={e.metric.value}{ctx_display}"
        )


def main(argv=None) -> None:  # pragma: no cover — thin integration glue
    args = parse_args(argv)
    config = load_config(args.config)
    collector = build_collector_from_config(config)
    enricher = build_enricher_from_config(config)

    all_metrics = [
        m
        for key in collector.get_history()
        for m in collector.get_history()[key]
    ]

    if args.pipeline:
        all_metrics = [m for m in all_metrics if m.pipeline == args.pipeline]

    enriched = enricher.enrich_all(all_metrics)

    if args.fmt == "json":
        print(json.dumps([e.to_dict() for e in enriched], indent=2))
    else:
        _format_enriched_text(enriched)


if __name__ == "__main__":
    main()
