# pipewatch

Lightweight CLI for monitoring and alerting on ETL pipeline health metrics.

## Features

- **Metrics & Thresholds** – define warning/critical bands per pipeline metric
- **Alerts** – pluggable dispatcher with console and file handlers
- **Reports** – text and JSON summaries of pipeline health
- **Watcher** – periodic polling loop with alert dispatch
- **Scheduler** – interval-based task runner
- **Exporter** – dump reports to JSON or CSV
- **Filter** – slice metrics by status, pipeline, or name
- **Aggregator** – min/max/avg rollups per pipeline+metric key
- **Summary** – per-pipeline health overview
- **Trend** – linear slope analysis over metric history
- **Baseline** – save and compare against a reference snapshot
- **Anomaly** – z-score spike detection
- **Correlation** – Pearson correlation between metric series
- **Snapshot** – point-in-time capture with diff support
- **Replay** – step through historical snapshots
- **Digest** – concise multi-pipeline health digest
- **Retention** – prune history by age or count
- **Tagging** – attach arbitrary key/value tags to metrics
- **Throttle** – per-metric alert cooldown
- **Routing** – rule-based alert channel routing
- **Deduplication** – suppress repeated identical alerts within a window
- **Escalation** – auto-escalate alerts after repeated breaches
- **Suppression** – time-windowed alert suppression
- **Enrichment** – attach contextual metadata to metrics
- **Silencing** – rule-based alert silencing with optional expiry
- **Rate Limiting** – cap alert volume per pipeline/metric
- **Grouping** – aggregate metrics into named groups
- **Checkpoint** – record and compare pipeline status snapshots
- **Dependency** – model upstream/downstream pipeline relationships
- **SLA Tracking** – track and report SLA breaches
- **Notifications** – multi-channel notification dispatch
- **Audit** – log status transition events
- **Forecast** – linear extrapolation of future metric values
- **Rollup** – time-bucketed metric aggregation
- **Sampling** – downsample long metric series
- **Labeling** – structured label registry with filter support
- **Scoring** – composite health score per pipeline
- **Profiling** – statistical profile (mean, std, percentiles)
- **Heatmap** – hourly status heatmap per pipeline
- **Windowing** – sliding time-window slicing
- **Budget** – error budget tracking per pipeline
- **Capacity** – threshold-based capacity breach reporting
- **Drift** – detect mean-shift between two halves of a series
- **Topology** – directed graph of pipeline relationships
- **Partitioning** – partition metrics by time bucket
- **Watermark** – track high/low watermarks per metric
- **Jitter** – detect oscillating/flapping metrics
- **Latency** – inter-metric arrival gap analysis
- **Saturation** – utilisation ratio relative to a capacity limit
- **Outlier** – IQR-based outlier detection
- **Velocity** – rate-of-change analysis
- **Normalization** – min-max and z-score normalization
- **Stickiness** – detect metrics stuck in a non-OK state
- **Recurrence** – identify metrics that repeatedly breach thresholds

## Quick Start

```bash
pip install pipewatch
pipewatch-report --config pipewatch.yaml
pipewatch-watch  --config pipewatch.yaml
pipewatch-recurrence --config pipewatch.yaml --recurring-only
```

## Configuration (`pipewatch.yaml`)

See `pipewatch.yaml` for a full example.
