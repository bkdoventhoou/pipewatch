# pipewatch

Lightweight CLI for monitoring and alerting on ETL pipeline health metrics.

## Features

- **Metric collection** – record pipeline metrics with configurable warning/critical thresholds
- **Alerting** – dispatch alerts via console or file handlers with deduplication, throttling, rate-limiting, suppression, silencing, and escalation
- **Reporting** – text and JSON reports with per-pipeline summaries
- **Watching** – continuous polling loop with configurable interval
- **Filtering** – filter metrics by status, pipeline, or name
- **Aggregation** – min/max/avg/count rollups per pipeline and metric
- **Trend analysis** – linear slope detection over metric history
- **Baseline comparison** – compare current metrics against a saved baseline
- **Anomaly detection** – z-score based spike detection
- **Correlation** – Pearson correlation between metric series
- **Snapshots & replay** – capture and replay point-in-time metric state
- **Digest** – periodic summary reports across all pipelines
- **Retention** – prune old metrics by age or count
- **Tagging** – attach key/value tags to metrics for richer context
- **Routing** – route alerts to different handlers based on pipeline/metric/status rules
- **Enrichment** – attach contextual metadata to metrics before alerting
- **Audit log** – record status transition events per pipeline
- **Forecasting** – linear extrapolation of metric values
- **Rollup** – bucket metrics into time windows
- **Grouping** – group metrics by pipeline with aggregate health
- **Checkpointing** – record periodic pipeline health snapshots
- **Dependency propagation** – propagate status through a pipeline dependency graph
- **SLA tracking** – evaluate SLA targets and record breach events
- **Notifications** – multi-channel notification manager with minimum severity filtering
- **Sampling** – downsample high-frequency metric histories by count or stride

## Installation

```bash
pip install -e .
```

## Configuration

See `pipewatch.yaml` for an example configuration.

## CLI Commands

| Command | Description |
|---|---|
| `pipewatch-report` | Run a one-shot report |
| `pipewatch-watch` | Continuously watch pipelines |
| `pipewatch-filter` | Filter and display metrics |
| `pipewatch-aggregate` | Aggregate metric statistics |
| `pipewatch-trend` | Analyse metric trends |
| `pipewatch-baseline` | Compare against a saved baseline |
| `pipewatch-anomaly` | Detect metric anomalies |
| `pipewatch-correlation` | Correlate metric pairs |
| `pipewatch-snapshot` | Capture and diff snapshots |
| `pipewatch-replay` | Replay a recorded session |
| `pipewatch-digest` | Generate a digest report |
| `pipewatch-routing` | Show alert routing rules |
| `pipewatch-tagging` | Manage metric tags |
| `pipewatch-throttle` | Show throttle state |
| `pipewatch-deduplication` | Show deduplication state |
| `pipewatch-escalation` | Show escalation state |
| `pipewatch-suppression` | Manage suppression windows |
| `pipewatch-enrichment` | Apply enrichment rules |
| `pipewatch-ratelimit` | Show rate limit state |
| `pipewatch-checkpoint` | Record and show checkpoints |
| `pipewatch-dependency` | Show dependency graph |
| `pipewatch-sla` | Track SLA breaches |
| `pipewatch-notification` | Send notifications |
| `pipewatch-audit` | Show audit log |
| `pipewatch-forecast` | Forecast metric values |
| `pipewatch-rollup` | Rollup metrics into buckets |
| `pipewatch-sampling` | Downsample metric histories |
