# polygon-pos-indexer

A blockchain indexerthat follows functional data engineer principles for reliable and idempotent data processing.

## Architecture & Design Principles
The application parses relevant events from a target contract and generates parquet files. DuckDB is the SQL access layer.

### Immutable Data Processing
- Defines table partitions as immutable objects to ensure idempotent runs
- Partitions are stored as Parquet files, organized by block ranges
- Pure task execution model to avoid side effects
- Data overwrites instead of modifications for consistent results
- Unit of work is aligned to a single partition for reproducibility

### Extensible Contract Processing
- Structured data modeling and parsing strategy
- Flexible contract class design for easy addition of new event processors
- Data validation through data spec definitions
- Strong typing enforced through Parquet schema

## Prerequisites

Before running the indexer, you need to:

1. Create an `indexer/.env` file with required configuration. See `indexer/sample.env`
2. Have Docker installed on your system

## Usage

### Building the Container
```bash
make build
```

### Loading Data
```bash
make run
```

### Development Environment
```bash
make shell
```

This launches a shell into the container for development or to access the DuckDB server.

### Querying Data

The DuckDB database file is located at `delegator_rewards.duckdb`. To query the data:

1. From within the container shell open the DuckDB CLI:
```bash
duckdb delegator_rewards.duckdb
```

2. You can now run SQL queries against the indexed data.
```sql
SELECT * FROM delegator_claimed_rewards LIMIT 10
```

## Data Model

The data is organized in Parquet files partitioned by block ranges, providing:
- Efficient querying of specific time periods (DBs like Trino and DuckDB support predicate pushdown and query pruning to limit data accessed)
- [A persistent immutable staging area](https://medium.com/p/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a?source=social.tw )
- Easy parallel processing capabilities for real time and backfills


