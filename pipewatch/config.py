"""Load pipewatch configuration from YAML."""
import yaml
from pathlib import Path
from typing import List
from pipewatch.metrics import ThresholdConfig


DEFAULT_CONFIG_PATH = Path("pipewatch.yaml")


def load_thresholds(config_path: Path = DEFAULT_CONFIG_PATH) -> List[ThresholdConfig]:
    """Parse threshold configurations from a YAML config file."""
    if not config_path.exists():
        return []
    with open(config_path, "r") as f:
        data = yaml.safe_load(f) or {}

    thresholds = []
    for entry in data.get("thresholds", []):
        thresholds.append(
            ThresholdConfig(
                metric_name=entry["metric_name"],
                warning=float(entry["warning"]),
                critical=float(entry["critical"]),
                comparison=entry.get("comparison", "gt"),
            )
        )
    return thresholds


def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict:
    """Return the full parsed config dict."""
    if not config_path.exists():
        return {}
    with open(config_path, "r") as f:
        return yaml.safe_load(f) or {}
