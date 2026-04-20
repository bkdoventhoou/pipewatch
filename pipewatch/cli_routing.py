"""CLI entry point for displaying active routing rules."""

import argparse
import json
import sys

from pipewatch.config import load_config
from pipewatch.routing import build_router_from_config


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show active alert routing rules from config."
    )
    parser.add_argument(
        "--config",
        default="pipewatch.yaml",
        help="Path to pipewatch config file (default: pipewatch.yaml)",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    config = load_config(args.config)
    router = build_router_from_config(config)
    rules = router.rules()

    if not rules:
        print("No routing rules configured.")
        return

    if args.format == "json":
        print(json.dumps([r.to_dict() for r in rules], indent=2))
    else:
        print(f"{'HANDLER':<20} {'PIPELINE':<20} {'METRIC':<20} {'MIN_STATUS':<12}")
        print("-" * 74)
        for rule in rules:
            print(
                f"{rule.handler_name:<20} "
                f"{rule.pipeline or '*':<20} "
                f"{rule.metric_name or '*':<20} "
                f"{rule.min_status.name:<12}"
            )


if __name__ == "__main__":
    main()
