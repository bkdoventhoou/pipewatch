"""CLI entry point for listing and testing notification channels."""
from __future__ import annotations

import argparse
import json
from typing import List

from pipewatch.config import load_config
from pipewatch.notification import NotificationChannel, NotificationManager
from pipewatch.alerts import Alert
from pipewatch.metrics import PipelineMetric, MetricStatus


def _build_manager_from_config(cfg: dict) -> NotificationManager:
    """Build a NotificationManager from config dict."""
    manager = NotificationManager()
    channels_cfg = cfg.get("notifications", {}).get("channels", [])
    for entry in channels_cfg:
        name = entry.get("name", "unnamed")
        min_status = entry.get("min_status", "warning")
        pipelines = entry.get("pipelines", None)
        # In real usage the handler would write to slack/email/etc.
        # For CLI listing we use a no-op handler.
        channel = NotificationChannel(
            name=name,
            handler=lambda a, n=name: print(f"[{n}] {a}"),
            min_status=min_status,
            pipelines=pipelines,
        )
        manager.register(channel)
    return manager


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="List or test pipewatch notification channels."
    )
    parser.add_argument("--config", default="pipewatch.yaml", help="Config file path")
    parser.add_argument(
        "--format", choices=["text", "json"], default="text", help="Output format"
    )
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("list", help="List registered channels")
    test_p = sub.add_parser("test", help="Send a test alert through all channels")
    test_p.add_argument("--pipeline", default="test-pipeline")
    test_p.add_argument("--metric", default="row_count")
    test_p.add_argument("--status", default="warning", choices=["warning", "critical"])
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    cfg = load_config(args.config)
    manager = _build_manager_from_config(cfg)

    if args.command == "test":
        status = MetricStatus(args.status.upper())
        metric = PipelineMetric(
            pipeline=args.pipeline,
            name=args.metric,
            value=0.0,
            status=status,
        )
        alert = Alert(metric=metric, message=f"Test alert: {args.metric} is {args.status}")
        notified = manager.notify(alert)
        if args.format == "json":
            print(json.dumps({"notified_channels": notified}))
        else:
            if notified:
                print(f"Alert sent to channels: {', '.join(notified)}")
            else:
                print("No channels accepted the test alert.")
        return

    # default: list
    channels = [c.to_dict() for c in (manager.get_channel(n) for n in manager.channel_names()) if c]
    if args.format == "json":
        print(json.dumps({"channels": channels}, indent=2))
    else:
        if not channels:
            print("No notification channels configured.")
        for ch in channels:
            pipelines = ", ".join(ch["pipelines"]) if ch["pipelines"] else "all"
            print(f"  {ch['name']}  min_status={ch['min_status']}  pipelines={pipelines}")


if __name__ == "__main__":
    main()
