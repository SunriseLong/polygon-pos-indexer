# polygon-pos-indexer

A blockchain indexer that takes a wallet address and parses relevant events from a collection of contracts.

## Architecture & Design Principles

The application prioritizes functional data engineering principles of determinism and idempotency. Work is modeled as pure tasks, ensuring that re-executing a task with the same parameters is safe and does not lead to double-counting.

The app provides a structured way for users to define contracts, events of interest, data validation, and serialization strategies in code. Polygon delegate reward claim events have been implemented.

DuckDB serves as the SQL access layer, which directly reads the parquet files. DuckDB supports predicate pushdown and query pruning to limit the data accessed.

### Data Model
To enforce pure tasks, we model the data in terms of immutable partitions. A task should always overwrite a table partition.

Our partition scheme serializes data as parquet files, organized by event type, wallet address, and block ranges. This provides a manageable and intuitive granularity where it is clear what data is stored in each partition.

The unit of work in a task should directly align with a partition. This makes it easy to map partitions to compute logic. For brevity, tasks in the app are not explicitly defined as singular units of work but are instead bundled through a serial generator. However, the code is designed to allow for more explicit task modeling with minimal modifications if needed.

Partitioning on block ranges and modeling a unit of work that is aligned to a partition allows for:

- Reproducibility
- Efficient rebuilds when business logic changes
- Parallel processing patterns for backfills
- Partial retries from failure

The service follows an ELT approach where data is extracted from a source and loaded into a datastore with minimal processing. Further transformations can be applied downstream, allowing for easy rebuilds of tables without needing to re-extract the same source data.

### Extensible Contract Processing
The application provides a generic interface to process events. Business logic for data access and parsing is kept separate. Contracts are modeled in terms of ABI, parsing strategy, and serialization schema.

Decoupling event processing from business logic ensures flexibility, allowing the application to support multiple contracts and data formats without modifying the core processing logic, making it more maintainable.

## Prerequisites

Before running the indexer, you need to:

1. Create an indexer/.env file to specify the wallet address and RPC endpoint. See indexer/sample.env
2. Have Docker installed on your system

### Usage

Building the Container
```
make build
```

### Loading Data
You can run the application multiple times with different wallet addresses and block ranges. Data will be appended to the same table. Running the same wallet address will overwrite previous results. Be careful when running the same wallet twice with different block increments or start blocks.

```
make run
```
<img width="394" alt="image" src="https://github.com/user-attachments/assets/1c9e6f7b-b339-4357-b77e-fb0606b4db94" />

### Development Environment
```
make shell
```
This launches a shell into the container for development or to access the DuckDB server.

### Querying Data
The DuckDB database file is located at `duckdb polygon_pos.duckdb`. To query the data:

1. From within the container shell, open the DuckDB CLI:
```
duckdb duckdb polygon_pos.duckdb
```
2. You can now run SQL queries against the indexed data:
```
SELECT * FROM delegator_claimed_rewards LIMIT 10
```
<img width="875" alt="image" src="https://github.com/user-attachments/assets/ebf117f8-8446-429a-bb4b-432646d29235" />

### Run Tests
From within the container shell:
```bash
python -m pytest tests/ -v
```

OR from the terminal:
```bash
make test
```

