# pipewatch

Lightweight CLI for monitoring and alerting on ETL pipeline health metrics.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install -e .
```

---

## Usage

```bash
# Monitor a pipeline and check health metrics
pipewatch monitor --config pipeline.yaml

# Set an alert threshold for failed records
pipewatch alert --metric failed_records --threshold 100 --notify slack

# View a summary of recent pipeline runs
pipewatch status --last 10
```

Example `pipeline.yaml`:

```yaml
pipeline:
  name: daily-sales-etl
  source: postgres
  destination: snowflake
  checks:
    - metric: row_count
      min: 1000
    - metric: null_rate
      max: 0.05
```

---

## Features

- Real-time health metric tracking for ETL pipelines
- Configurable alerting via Slack, email, or webhooks
- Simple YAML-based pipeline configuration
- Lightweight with minimal dependencies

---

## License

This project is licensed under the [MIT License](LICENSE).