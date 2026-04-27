"""CLI entry-point for degradation detection."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.collector import MetricCollector
from pipewatch.config import load_config
from pipewatch.degradation import detect_all_degradations


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Detect steadily-worsening pipeline metrics."
    )
    p.add_argument("--config", default="pipewatch.yaml", help="Config file path")
    p.add_argument(
        "--min-samples",
        type=int,
        default=4,
        dest="min_samples",
        help="Minimum history length to analyse (default: 4)",
    )
    p.add_argument(
        "--slope-threshold",
        type=float,
        default=0.1,
        dest="slope_threshold",
        help="Minimum slope to flag as degrading (default: 0.1)",
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        dest="fmt",
    )
    p.add_argument(
        "--only-degrading",
        action="store_true",
        dest="only_degrading",
        help="Only show degrading pipelines",
    )
    return p.parse_args(argv)


def _format_text(results) -> str:
    if not results:
        return "No degradation results available."
    lines = []
    for r in results:
        flag = "[DEGRADING]" if r.degrading else "[stable]  "
        lines.append(
            f"{flag} {r.pipeline}/{r.metric_name} "
            f"slope={r.score_slope:+.4f} "
            f"samples={r.sample_count} "
            f"latest={r.latest_status.value}"
        )
    return "\n".join(lines)


def main(argv=None) -> None:
    args = parse_args(argv)
    cfg = load_config(args.config)

    collector = MetricCollector()
    for name, tcfg in cfg.get("thresholds", {}).items():
        from pipewatch.metrics import ThresholdConfig
        collector.add_threshold(
            name,
            ThresholdConfig(
                warning=tcfg.get("warning"),
                critical=tcfg.get("critical"),
            ),
        )

    results = detect_all_degradations(
        collector.get_all_history(),
        min_samples=args.min_samples,
        slope_threshold=args.slope_threshold,
    )

    if args.only_degrading:
        results = [r for r in results if r.degrading]

    if args.fmt == "json":
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(_format_text(results))


if __name__ == "__main__":
    main()
