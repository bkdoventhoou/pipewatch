"""CLI for replaying saved snapshots."""
import argparse
import sys
from pipewatch.replay import load_replay_session
from pipewatch.formatters import get_formatter


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Replay pipeline snapshots")
    parser.add_argument("snapshots", nargs="+", help="Snapshot JSON files to replay")
    parser.add_argument("--frame", type=int, default=None, help="Show a specific frame index")
    parser.add_argument("--format", choices=["text", "json"], default="text", dest="fmt")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    session = load_replay_session(args.snapshots)

    if len(session) == 0:
        print("No frames loaded.", file=sys.stderr)
        sys.exit(1)

    formatter = get_formatter(args.fmt)
    frames = [session.get(args.frame)] if args.frame is not None else session.frames

    if args.frame is not None and frames[0] is None:
        print(f"Frame {args.frame} not found.", file=sys.stderr)
        sys.exit(1)

    rows = [f.to_dict() for f in frames]

    if args.fmt == "json":
        import json
        print(json.dumps(rows, indent=2))
    else:
        for f in frames:
            print(f"--- Frame {f.index} | {f.snapshot.timestamp} ---")
            for entry in f.snapshot.entries:
                m = entry.metric
                print(f"  [{m.status.value}] {m.pipeline}/{m.name} = {m.value}")


if __name__ == "__main__":
    main()
