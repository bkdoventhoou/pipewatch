"""CLI entry-point for the compaction feature."""

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.compaction import compact_all


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Compact pipeline metric history into consecutive status runs."
    )
    parser.add_argument("--config", default="pipewatch.yaml", help="Path to config file")
    parser.add_argument(
        "--pipeline", default=None, help="Filter output to a specific pipeline"
    )
    parser.add_argument(
        "--format", choices=["text", "json"], default="text", dest="fmt"
    )
    return parser.parse_args(argv)


def _format_text(compacted: dict) -> str:
    lines = []
    for (pipeline, metric_name), runs in compacted.items():
        for r in runs:
            lines.append(
                f"[{r.status.value.upper():8}] {pipeline}/{metric_name} "
                f"x{r.count}  avg={r.avg_value:.4f}  "
                f"ts={r.start_ts:.0f}..{r.end_ts:.0f}"
            )
    return "\n".join(lines) if lines else "(no data)"


def main(argv=None):
    args = parse_args(argv)
    collector = build_collector_from_config(args.config)
    history = collector.get_history()

    compacted = compact_all(history)

    if args.pipeline:
        compacted = {
            k: v for k, v in compacted.items() if k[0] == args.pipeline
        }

    if args.fmt == "json":
        output = {
            f"{p}/{m}": [r.to_dict() for r in runs]
            for (p, m), runs in compacted.items()
        }
        print(json.dumps(output, indent=2))
    else:
        print(_format_text(compacted))


if __name__ == "__main__":  # pragma: no cover
    main()
