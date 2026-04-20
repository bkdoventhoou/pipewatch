import argparse
import json
from pipewatch.config import load_config
from pipewatch.cli_report import build_collector_from_config
from pipewatch.tagging import TagRegistry


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Filter pipeline metrics by tag")
    parser.add_argument("--config", default="pipewatch.yaml")
    parser.add_argument("--tag-key", required=True, help="Tag key to filter by")
    parser.add_argument("--tag-value", default=None, help="Tag value to match (optional)")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    collector = build_collector_from_config(config)

    registry = TagRegistry()
    for entry in config.get("tags", []):
        registry.tag(entry["pipeline"], entry["metric"], entry["tags"])

    all_metrics = []
    for pipeline in collector.pipelines():
        all_metrics.extend(collector.get_history(pipeline))

    tagged = registry.apply(all_metrics)
    filtered = registry.filter_by_tag(tagged, args.tag_key, args.tag_value)

    if args.format == "json":
        print(json.dumps([t.to_dict() for t in filtered], indent=2))
    else:
        for t in filtered:
            m = t.metric
            tag_str = ", ".join(f"{k}={v}" for k, v in t.tags.items())
            print(f"[{m.pipeline}] {m.name} = {m.value} ({m.status.value}) tags=[{tag_str}]")


if __name__ == "__main__":
    main()
