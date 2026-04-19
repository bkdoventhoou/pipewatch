import argparse
import json
from pipewatch.config import load_config
from pipewatch.cli_report import build_collector_from_config
from pipewatch.anomaly import detect_all_anomalies


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Detect anomalies in pipeline metrics")
    parser.add_argument("--config", default="pipewatch.yaml", help="Config file path")
    parser.add_argument(
        "--threshold", type=float, default=2.5, help="Z-score threshold for anomaly"
    )
    parser.add_argument(
        "--format", choices=["text", "json"], default="text", dest="fmt"
    )
    parser.add_argument(
        "--only-anomalies", action="store_true", help="Only show detected anomalies"
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    collector = build_collector_from_config(config)
    history = collector.get_history()
    results = detect_all_anomalies(history, threshold=args.threshold)

    if args.only_anomalies:
        results = [r for r in results if r.is_anomaly]

    if args.fmt == "json":
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        if not results:
            print("No anomaly data available.")
            return
        for r in results:
            flag = " [ANOMALY]" if r.is_anomaly else ""
            print(
                f"{r.pipeline}/{r.metric_name}: value={r.value} "
                f"mean={r.mean:.4f} std={r.std:.4f} z={r.z_score:.4f}{flag}"
            )


if __name__ == "__main__":
    main()
