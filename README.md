# Ingestor Reader

Minimal config-first ETL service for incremental dataset ingestion.

## Overview

A clean, production-ready ETL service that:
- Reads data from HTTP, S3, or local files
- Normalizes and validates data according to YAML configs
- Computes incremental deltas using anti-join
- Publishes versions atomically with CAS (Compare-And-Swap)
- Ensures concurrency safety with DynamoDB locks

## Architecture

```
ingestor_reader/
  app/              # CLI entry point
  domain/           # Pure business logic
    entities/       # Pydantic models
    services/       # Domain services
  use_cases/        # Orchestration
    steps/          # Pipeline steps
  infra/            # I/O adapters
    s3_storage.py
    s3_catalog.py
    parquet_io.py
    readers/        # CSV, Excel readers
    locks/          # DynamoDB lock
    event_bus/      # SNS publisher
    configs/        # Config loader
```

## Installation

```bash
pip install -e .
```

## Configuration

Each dataset requires a YAML config file. See `configs/example.yml` for the structure.

### Example Config

```yaml
dataset_id: BCRA_TC_OFICIAL
frequency: D
lag_days: 2
source:
  kind: http
  url: "https://example.com/data.xlsx"
  format: xlsx
  sheet: "Data"
  header_row: 1
parse:
  plugin: bcra_infomondia
normalize:
  plugin: bcra_infomondia
  primary_keys: [obs_time]
  timezone: "America/Argentina/Cordoba"
parse_config:
  file_type: infomondia
  series_map:
    - internal_series_code: BCRA_TC_OFICIAL
      sheet: Data
      header_row: 1
      date_col: A
      value_col: B
notify:
  sns_topic_arn: "arn:aws:sns:us-east-1:123456789012:datasets"
```

## Usage

### Environment Variables

```bash
export S3_BUCKET=my-etl-bucket
export DYNAMODB_TABLE=etl-locks  # Optional
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1
```

### CLI

```bash
# Run pipeline for a dataset
python -m ingestor_reader.app.main run-pipeline --dataset BCRA_TC_OFICIAL

# With custom config path
python -m ingestor_reader.app.main run-pipeline \
  --dataset BCRA_TC_OFICIAL \
  --config configs/custom.yml

# Disable lock (for testing)
python -m ingestor_reader.app.main run-pipeline \
  --dataset BCRA_TC_OFICIAL \
  --no-lock
```

## S3 Layout

Para una descripción detallada de la estructura de datos en S3, ver [docs/S3_STRUCTURE.md](docs/S3_STRUCTURE.md).

```
s3://<bucket>/datasets/<dataset_id>/
  configs/config.yaml              # Dataset config (optional, can use local)
  index/keys.parquet               # Primary key index
  versions/<version_ts>/
    manifest.json                   # Version manifest
  runs/<run_id>/
    raw/...                         # Raw source files
    staging/normalized.parquet      # Normalized data
    delta/added.parquet             # Delta (new rows)
  outputs/<version_ts>/
    data/year=YYYY/month=MM/part-*.parquet  # Published data (partitioned by year/month)
  current/
    manifest.json                   # Pointer to current version (CAS)
```

## Atomic Publish

The service uses Compare-And-Swap (CAS) to ensure atomic version publishing:

1. Write all artifacts under `outputs/<version_ts>/`
2. Write version manifest under `versions/<version_ts>/manifest.json`
3. Update `current/manifest.json` with `If-Match` header (ETag)
4. If CAS fails (412), abort gracefully (pointer unchanged)

This ensures that:
- No partial publishes are visible
- Concurrent runs don't corrupt the pointer
- Failures before publish leave current version unchanged

## Incremental Logic

1. Compute `key_hash = SHA1('|'.join(primary_key_values))` for each row
2. Read `index/keys.parquet` from current version (if exists)
3. Anti-join: keep rows in normalized data not present in index
4. If `rows_added == 0`, skip publish (or publish with 0 if config says so)
5. Update index with new key hashes

## Concurrency Safety

- **DynamoDB Lock**: Per-dataset lock with TTL
  - Acquired before publish step
  - Released in finally block
  - Uses conditional write: `attribute_not_exists OR expires_at < now`
  
- **S3 CAS**: Pointer update with `If-Match` header
  - Prevents concurrent pointer updates
  - One run succeeds, others fail gracefully

## Adding a New Dataset

1. Create YAML config in `configs/<dataset_id>.yml`
2. Run pipeline: `python -m ingestor_reader.app.main run-pipeline --dataset <dataset_id>`
3. Config can also be stored in S3: `s3://<bucket>/datasets/<dataset_id>/configs/config.yaml`

## Cleanup

Old run artifacts under `runs/<run_id>/` can be cleaned up periodically:

```bash
# List old runs (older than 7 days)
aws s3 ls s3://<bucket>/datasets/<dataset_id>/runs/ --recursive

# Delete old runs (be careful!)
aws s3 rm s3://<bucket>/datasets/<dataset_id>/runs/<old_run_id>/ --recursive
```

## Testing

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/
```

## Design Principles

- **Simplicity > Features**: Minimal, focused code
- **Config-First**: Every dataset has a YAML contract
- **Clean Architecture**: Domain logic separate from I/O
- **Atomic Operations**: CAS for pointer updates
- **Incremental**: Append-only with anti-join
- **Concurrency Safe**: Lock + CAS
- **Short Functions**: < 60 lines, clear names
- **Typed**: Type hints everywhere

## License

MIT

