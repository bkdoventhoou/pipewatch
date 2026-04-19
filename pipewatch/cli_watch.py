"""CLI entry point: continuously watch pipelines at a configured interval."""

import argparse
import logging
import signal
import sys
from typing import Optional

from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config
from pipewatch.alerts import AlertDispatcher
from pipewatch.handlers import build_handlers_from_config
from pipewatch.watcher import PipelineWatcher
from pipewatch.scheduler import PipelineScheduler

logger = logging.getLogger(__name__)


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipewatch-watch",
        description="Continuously monitor ETL pipeline health metrics.",
    )
    parser.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file"
    )
    parser.add_argument(
        "--interval", type=float, default=None,
        help="Override poll interval in seconds"
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable debug logging"
    )
    return parser.parse_args(argv)


def main(argv: Optional[list] = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = load_config(args.config)
    interval = args.interval or cfg.get("watch", {}).get("interval_seconds", 60.0)

    collector = build_collector_from_config(args.config)
    dispatcher = AlertDispatcher()
    for handler in build_handlers_from_config(cfg):
        dispatcher.register(handler)

    watcher = PipelineWatcher(collector=collector, dispatcher=dispatcher)
    scheduler = PipelineScheduler(interval_seconds=interval, task=watcher.run_once)

    def _shutdown(sig, frame):  # noqa: ANN001
        logger.info("Received signal %s, shutting down…", sig)
        scheduler.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("Starting pipewatch (interval=%.1fs). Press Ctrl+C to stop.", interval)
    scheduler.start()
    # Block main thread
    scheduler._thread.join()  # type: ignore[union-attr]


if __name__ == "__main__":
    main()
