"""CLI command: pipewatch dependency — show pipeline dependency health."""
import argparse
import json
import sys
from pipewatch.config import load_config
from pipewatch.cli_report import build_collector_from_config
from pipewatch.dependency import DependencyGraph, propagate_status
from pipewatch.metrics import MetricStatus
from pipewatch.reporter import build_report


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show pipeline dependency health propagation."
    )
    parser.add_argument("--config", default="pipewatch.yaml", help="Config file path")
    parser.add_argument(
        "--format", choices=["text", "json"], default="text", help="Output format"
    )
    return parser.parse_args(argv)


def _build_graph_from_config(cfg: dict) -> DependencyGraph:
    graph = DependencyGraph()
    pipelines = cfg.get("pipelines", {})
    for name, opts in pipelines.items():
        deps = opts.get("depends_on", []) if isinstance(opts, dict) else []
        graph.add(name, deps)
    return graph


def main(argv=None) -> None:
    args = parse_args(argv)
    cfg = load_config(args.config)
    collector = build_collector_from_config(cfg)
    report = build_report(collector)

    pipeline_statuses = {}
    for entry in report.entries:
        cur = pipeline_statuses.get(entry.pipeline, MetricStatus.OK)
        rank = {MetricStatus.OK: 0, MetricStatus.WARNING: 1, MetricStatus.CRITICAL: 2}
        if rank[entry.status] > rank[cur]:
            pipeline_statuses[entry.pipeline] = entry.status

    graph = _build_graph_from_config(cfg)
    for p in pipeline_statuses:
        if graph.get(p) is None:
            graph.add(p, [])

    propagated = propagate_status(graph, pipeline_statuses)

    if args.format == "json":
        print(json.dumps({p: v.to_dict() for p, v in propagated.items()}, indent=2))
    else:
        for p, ps in propagated.items():
            own = ps.own_status.value.upper()
            eff = ps.propagated_status.value.upper()
            blockers = ", ".join(ps.blocking_pipelines) if ps.blocking_pipelines else "none"
            print(f"{p:30s}  own={own:8s}  effective={eff:8s}  blockers=[{blockers}]")


if __name__ == "__main__":
    main()
