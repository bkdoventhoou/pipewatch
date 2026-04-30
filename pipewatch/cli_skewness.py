"""CLI entry point for skewness analysis."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.collector import MetricCollector
from pipewatch.config import load_config
from pipewatch.skewness import SkewnessResult, analyze_all_skewness


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze skewness of pipeline metric distributions."
    )
    parser.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file."
    )
    parser.add_argument(
        "--pipeline", default=None, help="Filter to a specific pipeline."
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.5,
        help="Skewness magnitude threshold for left/right classification (default: 0.5).",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    return parser.parse_args(argv)


def _format_text(results: list[SkewnessResult]) -> str:
    if not results:
        return "No skewness results available (insufficient data)."
    lines = []
    for r in results:
        lines.append(
            f"[{r.pipeline}] {r.metric_name}: skewness={r.skewness:+.4f} "
            f"({r.interpretation}), mean={r.mean:.4f}, n={r.sample_count}"
        )
    return "\n".join(lines)


def main(argv=None) -> None:
    args = parse_args(argv)
    config = load_config(args.config)

    collector = MetricCollector()
    for name, cfg in config.get("thresholds", {}).items():
        from pipewatch.metrics import ThresholdConfig
        collector.add_threshold(
            name,
            ThresholdConfig(
                warning=cfg.get("warning"),
                critical=cfg.get("critical"),
            ),
        )

    history = collector.get_history()

    if args.pipeline:
        history = {
            k: v for k, v in history.items() if v and v[0].pipeline == args.pipeline
        }

    results = analyze_all_skewness(history, threshold=args.threshold)

    if args.format == "json":
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(_format_text(results))


if __name__ == "__main__":  # pragma: no cover
    main()
