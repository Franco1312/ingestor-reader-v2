# Ingestor Reader

Minimal config-first ETL service for incremental dataset ingestion.

## Overview

A clean, production-ready ETL service that:
- Reads data from HTTP, S3, or local files
- Normalizes and validates data according to YAML configs
- Computes incremental deltas using anti-join
- Uses Event Sourcing + CQRS architecture
- Publishes events atomically with CAS (Compare-And-Swap)
- Consolidates projections by series for efficient consumption

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
```

## S3 Layout

Para una descripción detallada de la estructura de datos en S3, ver [docs/S3_STRUCTURE.md](docs/S3_STRUCTURE.md).

```
s3://<bucket>/datasets/<dataset_id>/
  configs/config.yaml              # Dataset config (optional, can use local)
  index/keys.parquet               # Primary key index
  events/<version_ts>/             # Event Store (Event Sourcing)
    manifest.json                  # Event manifest
    data/year=YYYY/month=MM/part-*.parquet  # Immutable events
  projections/                     # Read Models (CQRS)
    windows/<series_code>/         # Series projections
      year=YYYY/month=MM/data.parquet  # Consolidated projections
  current/
    manifest.json                  # Pointer to current version (CAS)
```

## Architecture: Event Sourcing + CQRS

The service uses **Event Sourcing** for immutable event storage and **CQRS** for optimized read models:

1. **Events** (`events/<version_ts>/`): Immutable incremental events (Event Sourcing)
2. **Projections** (`projections/windows/<series_code>/`): Consolidated read models by series (CQRS)
3. **Atomic Publishing**: CAS (Compare-And-Swap) ensures atomic event publishing

### Atomic Publish

The service uses Compare-And-Swap (CAS) to ensure atomic event publishing:

1. Write all event data under `events/<version_ts>/data/`
2. Write event manifest under `events/<version_ts>/manifest.json`
3. Update `current/manifest.json` with `If-Match` header (ETag)
4. Consolidate projections by series after successful publish
5. If CAS fails (412), abort gracefully (pointer unchanged)

This ensures that:
- No partial publishes are visible
- Concurrent runs don't corrupt the pointer
- Failures before publish leave current version unchanged
- Projections are always consistent with published events

## Incremental Logic

1. Compute `key_hash = SHA1('|'.join(primary_key_values))` for each row
2. Read `index/keys.parquet` from current version (if exists)
3. Anti-join: keep rows in normalized data not present in index
4. If `rows_added == 0`, skip publish (or publish with 0 if config says so)
5. Update index with new key hashes

## Concurrency Safety

- **DynamoDB Locks**: Pipeline-level distributed locks prevent concurrent execution
  - Configure via `DYNAMODB_LOCK_TABLE` environment variable
  - See [docs/LOCKS.md](docs/LOCKS.md) for details
- **S3 CAS**: Pointer update with `If-Match` header
  - Prevents concurrent pointer updates
  - One run succeeds, others fail gracefully

## Adding a New Dataset

1. Create YAML config in `configs/<dataset_id>.yml`
2. Run pipeline: `python -m ingestor_reader.app.main run-pipeline --dataset <dataset_id>`
3. Config can also be stored in S3: `s3://<bucket>/datasets/<dataset_id>/configs/config.yaml`

## Documentation

- [S3 Structure](docs/S3_STRUCTURE.md): Complete S3 folder structure
- [Storage Flow](docs/S3_STORAGE_FLOW.md): What, how, and when data is saved
- [Flow Sequence](docs/FLOW_SEQUENCE.md): Step-by-step pipeline sequence
- [Locks](docs/LOCKS.md): Distributed locking mechanism

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
- **Concurrency Safe**: CAS for pointer updates
- **Short Functions**: < 60 lines, clear names
- **Typed**: Type hints everywhere

## License

MIT

