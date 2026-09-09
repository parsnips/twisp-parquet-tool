# Twisp Parquet → DuckDB

A standalone Go command that lists a tenant's warehouse Parquet files through the
Twisp Files API, downloads them, and imports them into a persistent DuckDB file.
It creates Redshift-style current and history views, including `account` and
`account_history`. Once imported, all queries run offline.

The importer embeds DuckDB through the [official Go driver](https://github.com/duckdb/duckdb-go).
It requires no Twisp SDK, AWS credentials, Redshift, ClickHouse server, or separate
DuckDB installation to import data. A DuckDB client is only needed to explore the
result afterward.

## Build and run

Build with Go 1.25+ and a C/C++ toolchain for CGO. The pinned driver is
`github.com/duckdb/duckdb-go/v2 v2.10505.0`, embedding DuckDB 1.5.5. The first build
downloads its dependencies and native libraries. Supported targets are listed in
the driver's documentation; the binary built here is for macOS Apple Silicon.

```bash
cd ~/projects/parsnips/twisp-parquet-tool
go build -o twisp-parquet-tool .

export TWISP_TOKEN='your-bearer-token'
export TWISP_TENANT='your-twisp-account-id'
./twisp-parquet-tool -db ./ledger.duckdb
```

The already-built `./twisp-parquet-tool` can be run directly on this machine.
You can also use `go run .` with the same arguments. A local `go.work` keeps this
module independent of any enclosing Go workspace.

`-tenant` is the Twisp **account ID** used in the `x-twisp-account-id` HTTP header
(for example, `Sandbox0d6af983`), not the internal UUID in `record_tenantid`.
`-token` accepts a JWT with or without the `Bearer ` prefix. Environment variables
avoid putting the token in command-line arguments. Tokens are never persisted in
the database.

The default endpoint is the US East 1 cloud endpoint. Override it for your region
or environment:

```bash
./twisp-parquet-tool \
  -token "$TWISP_TOKEN" \
  -tenant "$TWISP_TENANT" \
  -endpoint https://api.us-west-2.cloud.twisp.com/financial/v1/graphql \
  -db ./ledger.duckdb
```

You can also set `TWISP_ENDPOINT`.

## How import works

The default prefix is `warehouse`, covering every retained partition exposed by
the Files API. The query uses pagination and obtains download links inline:

```graphql
query ListParquetFiles($prefix: String!, $size: Int!, $token: String, $downloads: Boolean!) {
  files {
    listPage(keyPrefix: $prefix, pageSize: $size, pageToken: $token) {
      nextPageToken
      keys {
        key
        download @include(if: $downloads) {
          downloadURL
          downloadURLExpiration
          downloadHeaders
        }
      }
    }
  }
}
```

The importer follows continuation tokens, including tokens on empty pages, until
the final page. Listing, downloads, and imports run as a bounded pipeline across
page boundaries. By default, 16 files can download while up to four entity
types import concurrently on separate DuckDB connections. Each entity has one
active import at a time, so its schema and views can be updated safely. Queued
files for a busy entity do not consume the other entities' import workers.

```bash
./twisp-parquet-tool -db ledger.duckdb -workers 32 -import-workers 4 -batch-size 128
```

`-workers` controls downloads; `-import-workers` controls concurrent entity
imports. Use `-import-workers 1` for serialized imports. If only one entity is
available in the bounded queue, only one import runs; downloads can still overlap
that import. The first nonempty import establishes the tenant identity before
other entity transactions start. DuckDB shares its memory/CPU resources across
connections, so increasing concurrency does not guarantee a speedup.
See [DuckDB concurrency](https://duckdb.org/docs/current/connect/concurrency)
for its support for concurrent writers within one process.

Downloads use the signed headers. Expired links are renewed through
`files.createDownload`. Transient
HTTP errors are retried with backoff; authentication and GraphQL errors stop the
run with a nonzero exit status.

Each entity imports batches of up to 128 files or 64 MiB of compressed Parquet,
flushing partial batches after one second when an import worker is available.
Files with matching physical schemas share a DuckDB read; different schemas are
normalized separately within the same transaction. Schema information is cached,
views are only rebuilt on schema changes, and checkpoints are written in bulk.
One oversized file can form a batch on its own.

Every batch commits its rows, schema updates, views, and individual file
checkpoints atomically. If a batch fails or the process dies, that entire batch
is retried on resume. Previously committed batches remain available. Downloads are
temporary and are removed after import; the resulting views reference local
DuckDB tables, not remote URLs or Parquet files.

Rerun the same command to resume or pick up new files. Each run lists the prefix
from the beginning and checks `_twisp.imported_files` once per page. Resume listing
initially requests keys without download URLs, so already imported files do not
need URL signing or downloading. Missing links are generated in groups of up to
100; subsequent wholly new pages use inline links again. This also
finds newly delivered files in older partitions. Keys are assumed immutable;
overwriting an object under an already-imported key will not reload it.

Each database is bound to one endpoint and account ID. The first nonempty file
also binds its internal `record_tenantid`; subsequent files must match. Use a
separate database file for another tenant or environment.

## Views and Redshift semantics

For each entity encountered, these objects are created:

| Object | Contents |
| --- | --- |
| `root.parquet_account` | All imported CDC rows, including duplicate deliveries |
| `public.account_history` / `account_history` | One row per record version, resolving duplicate deliveries and status transitions |
| `public.account` / `account` | The highest version of each record from history |
| `_twisp.imported_files` | Imported keys, entities, raw row counts, bytes, and import timestamps |

The same naming applies to `journal`, `account_context`, `account_set`,
`account_set_member`, `tran_code`, `transaction`, `entry`, `balance`, `calculation`,
`velocity_control`, `velocity_limit`, `transaction_exception`, and
`workflow_execution`. Entities are discovered from keys. An entity with no files
has no views because its schema has not been discovered.

The view logic follows Twisp's `tools/protoutil/redshift_schema_gen.go` and
`services/db/warehouse/consumer/datashare/main.go`:

1. History partitions by record ID and version and prefers `DELETE` over `EOL`
   over `ALIVE`. This resolves status changes for the same version as well as
   seed/live overlap. The implementation also partitions by tenant ID and uses
   `record_begin` to break ties within a status.
2. The current view selects the highest `record_version` from history.
3. **Current views retain deletion and EOL markers**, just like Redshift. Add
   `WHERE record_status = 'ALIVE'` when you want only live records. Filtering before
   selecting the latest version could incorrectly resurrect deleted records.

Warehouse columns are preserved, including all five `record_*` metadata columns.
UUID identifiers are normalized to canonical text: older files contain raw
16-byte UUIDs while newer files contain UTF-8 strings. This applies to record IDs,
tenant IDs, entity/reference IDs, `void_of`, and `voided_by`. Empty optional binary
UUIDs stay empty, NULLs stay NULL, and non-UUID binary columns such as
`balance.dimension` remain binary. External and correlation identifiers retain
their original values.

On startup, an existing version 1 database is automatically upgraded in a single
transaction. Its binary UUID columns and saved tenant identity are converted to
the same text representation, its views are rebuilt, and **all imported-file
checkpoints are preserved**. Just rerun the command against the same `-db`; no
manual SQL or re-download is required. A migration error rolls back the upgrade.
The upgraded database requires this version of the tool.

Other Parquet types are preserved. Additive schema changes are supported: new columns
are added, inserts match by name, and older rows/files get NULL for absent
columns. A change to an existing column's type fails the batch transaction instead
of applying a potentially lossy cast.

These views match the Redshift record-selection semantics; this is not a general
Redshift SQL compatibility layer. DuckDB types and JSON/binary functions can
differ. `f_sql_status_int(status)` is provided in both `main` and `public` for
queries that use Twisp's status helper.

## Offline analysis

After the importer exits, open the file in the [DuckDB CLI](https://duckdb.org/docs/current/clients/cli/overview)
or another DuckDB client. Use a current DuckDB 1.5.x or newer client.

```bash
duckdb -readonly ./ledger.duckdb
```

```sql
-- Discover imported entities and schemas.
SHOW TABLES;
DESCRIBE account;

-- Current live accounts.
SELECT account_id, name, code
FROM account
WHERE record_status = 'ALIVE'
ORDER BY code;

-- Every retained version and its resolved CDC status.
SELECT account_id, record_version, record_status, record_begin, name
FROM account_history
WHERE account_id = 'your-account-uuid'
ORDER BY record_version;

-- Import inventory. Row counts include raw duplicate deliveries.
SELECT entity, count(*) AS files, sum(row_count) AS imported_rows,
       sum(byte_count) AS imported_bytes
FROM _twisp.imported_files
GROUP BY entity
ORDER BY entity;

-- Financial values stored as money strings can be converted explicitly.
-- Keep currencies separate; choose decimal precision/scale for your data.
SELECT currency,
       sum(CAST(split_part(settled_dr_balance, ' ', 1) AS DECIMAL(38, 18))) AS debits
FROM balance
WHERE record_status = 'ALIVE'
GROUP BY currency;
```

Python works as well, with its DuckDB package installed:

```python
import duckdb

with duckdb.connect("ledger.duckdb", read_only=True) as db:
    print(db.sql("SELECT account_id, name FROM account WHERE record_status = 'ALIVE'").fetchall())
```

Close other DuckDB clients before running another import. DuckDB's file locking
does not allow a separate writer process alongside those readers. User-created
objects should use their own names/schema; the tool manages `root.parquet_*`,
the entity views in `main` and `public`, and `_twisp`.

## Options

```text
-token          TWISP_TOKEN; required
-tenant         TWISP_TENANT; required
-endpoint       TWISP_ENDPOINT, or the US East 1 cloud GraphQL endpoint
-db             twisp.duckdb
-prefix         warehouse
-page-size      1000 (1–1000)
-workers        16 (1–128; concurrent downloads)
-import-workers 4 (1–64; concurrent entities, one import per entity)
-batch-size     128 (1–4096; files per entity transaction)
-batch-bytes    67108864 (64 MiB compressed; one oversized file allowed)
-batch-wait     1s (partial batch collection time when a worker is available)
-buffer-files   1024 (1–65536; pending downloaded files)
-buffer-bytes   268435456 (256 MiB pending compressed download threshold)
-retries        4 (0–10; retries after the first attempt)
-timeout        10m per HTTP request
-memory-limit   1GB for DuckDB; queries can spill to disk
-temp-dir       OS temporary directory; parent directory must already exist
```

For example, use a narrower UTC date prefix when only a subset is needed:

```bash
./twisp-parquet-tool -db september.duckdb \
  -prefix warehouse/parquet/2026/09/ -page-size 1000
```

Allow disk space for the database, DuckDB spill files, and concurrent temporary
downloads. The pending queue applies backpressure at either the file count or
byte threshold. Disk usage also includes active batches, the download channel,
and in-flight downloads; the byte threshold can be exceeded by one received file.
Buffer pressure flushes partial batches to keep the pipeline moving. HTTP
connections are pooled for the configured download concurrency. An automatic
encoding migration may temporarily need extra disk space for rewritten columns.
`-memory-limit` controls DuckDB, not the entire process's memory.
Paths containing `?` or `%` are rejected because of the Go driver's DSN handling.

## Small-file performance

For a warehouse with many tiny files, start with the defaults or increase downloads
to `-workers 32`. For downloads limited by per-request latency, try 64 or 128
workers and compare throughput after resume scanning finishes. Keep
`-import-workers 4` while tuning downloads; the HTTP connection pool automatically
scales with `-workers`. Batches can be tuned independently with `-batch-size 256` or
`-batch-size 512`. Larger batches amortize transaction and query planning costs,
but require more temporary space and repeat more work after an interrupted batch.
Use `-batch-size 1` when diagnosing a particular file failure.

On an Apple M1 Max, a local benchmark of 256 five-row Parquet files improved from
about **248 files/s before batching to 2,559 files/s with batches of 128** (median
of three runs, durable file checkpoints included). This measures database ingestion
only; API listing, URL signing, and downloads are excluded. Full-run throughput
will depend on those services, file sizes, and entity distribution.

```bash
go test -run '^$' -bench 'Benchmark.*SmallFiles' -benchtime=1x -count=3
```

To use an updated binary with an import already running, interrupt the old process
with Ctrl-C, wait for it to exit, and rerun against the same database. Replacing
the binary does not change the code in an already running process. Existing
checkpoints work with batching; no reimport or database migration is needed.

## Coverage and completeness

This imports **all available files exposed to the tenant**, not an on-demand
historical export. The warehouse pipeline must be enabled, and complete history
requires a historical seed plus all subsequent CDC files. The tool cannot recover
files that expired before import or data that was never seeded. A date prefix
restricts coverage accordingly. An empty listing is reported explicitly.

Listing a live warehouse is not a point-in-time snapshot. New files can arrive
during a run; run the command again to pick them up. Views reflect the data
successfully imported so far, including after a partially completed run.

See Twisp's [Parquet pipeline tutorial](https://www.twisp.com/docs/tutorials/advanced/parquet-pipeline)
for file layout and seed/CDC behavior, and the [warehouse export tutorial](https://www.twisp.com/docs/tutorials/advanced/export-data)
for on-demand exports.

## Verification

```bash
go test ./...
go test -race ./...
go vet ./...
```

Tests generate real Parquet files with DuckDB and exercise the importer against
an HTTP test server. They cover pagination (including empty intermediate pages),
inline downloads, expired-link refresh, signed headers, authentication isolation,
transient errors, cancellation, failed-run resume, duplicate CDC records,
DELETE/EOL precedence, schema evolution, tenant binding, mixed binary/text UUIDs,
automatic database migration with resume, overlapping entity imports, per-entity
serialization, batch size/byte limits and partial flushing, atomic batch rollback,
checkpoint filtering before URL signing, pipeline cancellation, and offline persistence.
The optional DuckDB CLI test runs when `duckdb` is on PATH. No live Twisp token or
tenant is needed to run the tests.

## License

[MIT](LICENSE) © 2026 Michael Parsons.
