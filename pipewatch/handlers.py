"""Built-in alert handlers: console and log file."""

import logging
import sys
from pathlib import Path
from pipewatch.alerts import Alert


def console_handler(alert: Alert) -> None:
    """Print alert to stderr with colour hints."""
    colours = {"warning": "\033[33m", "critical": "\033[31m"}
    reset = "\033[0m"
    colour = colours.get(alert.status.value, "")
    print(f"{colour}{alert}{reset}", file=sys.stderr)


def make_file_handler(log_path: str) -> callable:
    """Return a handler that appends alerts to *log_path*."""
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(f"pipewatch.file.{log_path}")
    if not logger.handlers:
        handler = logging.FileHandler(path)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.WARNING)

    def _handler(alert: Alert) -> None:
        level = (
            logging.CRITICAL
            if alert.status.value == "critical"
            else logging.WARNING
        )
        logger.log(level, str(alert))

    return _handler


def build_handlers_from_config(cfg: dict) -> list:
    """Instantiate handlers listed under cfg['alerts']."""
    handlers = []
    alert_cfg = cfg.get("alerts", {})
    if alert_cfg.get("console", True):
        handlers.append(console_handler)
    log_file = alert_cfg.get("log_file")
    if log_file:
        handlers.append(make_file_handler(log_file))
    return handlers
