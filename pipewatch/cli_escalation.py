"""CLI for inspecting current escalation state from live collector data."""

import argparse
import json
from pipewatch.config import load_config
from pipewatch.cli_report import build_collector_from_config
from pipewatch.escalation import AlertEscalator
from pipewatch.metrics import MetricStatus


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show escalation state for pipeline metrics."
    )
    parser.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file."
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=3,
        help="Consecutive non-OK alerts before escalation (default: 3).",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    config = load_config(args.config)
    collector = build_collector_from_config(config)
    escalator = AlertEscalator(threshold=args.threshold)

    results = []
    for key, history in collector.get_history().items():
        for metric in history:
            resolved = escalator.evaluate(metric)
            if resolved is not None:
                entry = escalator.get_entry(metric)
                if entry:
                    results.append(entry.to_dict())

    if args.format == "json":
        print(json.dumps(results, indent=2))
    else:
        if not results:
            print("No escalated or repeated alerts.")
            return
        for r in results:
            tag = " [ESCALATED]" if r["escalated"] else ""
            print(
                f"[{r['status'].upper()}{tag}] {r['pipeline']}/{r['metric_name']} "
                f"— repeated {r['count']}x"
            )


if __name__ == "__main__":
    main()
