# polygon-pos-indexer

A blockchain indexer that follows functional data engineer principles for reliable and idempotent data processing.

## Architecture & Design Principles
The application parses relevant events from a target contract and generates parquet files. DuckDB is the SQL access layer which directly reads the parquet files. DuckDB supports predicate pushdown and query pruning to limit data accessed. Polygon delegrate reward claims has been implemented.

### Data Model
Partitions are stored as Parquet files, organized by event type, wallet address and block ranges, providing:
- [A persistent immutable staging area](https://medium.com/p/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a?source=social.tw )
- Easy parallel processing capabilities for real time and backfills
- Defines table partitions as immutable objects to ensure idempotent runs
- Pure task execution model to avoid side effects
- Unit of work is aligned to a single partition for reproducibility
- Efficient querying of specific time periods (DBs like Trino and DuckDB support predicate pushdown and query pruning to limit data accessed)

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
You may run the application multiple times with different wallet addresses and blockranges. Data will be appended to the same table.
Running the same wallet will overwrite previous results. Becareful running the same wallet twice with different block increment or start blocks.
```bash
make run
```
<img width="394" alt="image" src="https://github.com/user-attachments/assets/1c9e6f7b-b339-4357-b77e-fb0606b4db94" />


### Development Environment
```bash
make shell
```

This launches a shell into the container for development or to access the DuckDB server.

### Querying Data

The DuckDB database file is located at `duckdb polygon_pos.duckdb`. To query the data:

1. From within the container shell open the DuckDB CLI:
```bash
duckdb duckdb polygon_pos.duckdb
```

2. You can now run SQL queries against the indexed data.
```sql
SELECT * FROM delegator_claimed_rewards LIMIT 10
```
<img width="875" alt="image" src="https://github.com/user-attachments/assets/ebf117f8-8446-429a-bb4b-432646d29235" />


