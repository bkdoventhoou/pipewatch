"""CLI entry point for pipeline topology inspection."""
from __future__ import annotations
import argparse
import json
import sys
from pipewatch.topology import build_topology_from_config
from pipewatch.config import load_config


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect pipeline topology and detect cycles."
    )
    parser.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file"
    )
    parser.add_argument(
        "--format", choices=["text", "json"], default="text", help="Output format"
    )
    parser.add_argument(
        "--pipeline", default=None, help="Show neighbours for a specific pipeline"
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    config = load_config(args.config)
    topology_cfg = config.get("topology", {})
    graph = build_topology_from_config(topology_cfg)

    cycle_detected = graph.has_cycle()

    if args.format == "json":
        output = {
            "cycle_detected": cycle_detected,
            "nodes": graph.to_dict(),
        }
        if args.pipeline:
            node = graph.get(args.pipeline)
            output["nodes"] = {args.pipeline: node.to_dict()} if node else {}
        print(json.dumps(output, indent=2))
        return

    # text format
    if cycle_detected:
        print("WARNING: cycle detected in topology graph!")
    else:
        print("No cycles detected.")

    pipelines = [args.pipeline] if args.pipeline else graph.all_pipelines()
    if not pipelines:
        print("No topology edges defined.")
        return

    for name in pipelines:
        node = graph.get(name)
        if node is None:
            print(f"  {name}: (not found)")
            continue
        up = ", ".join(node.upstream) or "(none)"
        down = ", ".join(node.downstream) or "(none)"
        print(f"  {name}")
        print(f"    upstream:   {up}")
        print(f"    downstream: {down}")


if __name__ == "__main__":  # pragma: no cover
    main()
