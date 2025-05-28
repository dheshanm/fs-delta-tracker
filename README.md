# fs-delta-tracker

`fs-delta-tracker` is a high-performance filesystem crawler and change-tracker for large storage trees. It lets you detect and catalog new, modified, and deleted files at scale (200 TB+), store metadata and content fingerprints in PostgreSQL, and generate daily/weekly change reports with minimal overhead.

---

## Features

- Fast, parallel directory scanning using `os.scandir()` and a worker pool  
- Bulk‐load of file metadata into an **unlogged**, partitionable staging table via `COPY`  
- Single-shot SQL merge (upsert + delete) that  
  - Inserts new files  
  - Updates modified files  
  - Deletes removed files  
  - Records every change in a `file_changes` audit table  
- Optional file fingerprinting (BLAKE3) for integrity checks  (planned)
- Hierarchical path indexing via `ltree` for subtree queries  
- JSONB-configurable scans with arbitrary metadata  

---

## Quickstart

1. Clone this repo:  
   ```bash
   git clone https://github.com/dheshanm/fs-delta-tracker.git
   cd fs-delta-tracker
   ```

2. Install dependencies.

3. Create and initialize your PostgreSQL database:  

4. Copy and edit the example config:  
   ```bash
   cp sample.config.ini config.ini
   vim config.ini
   ```

4. Initialize the database schema:  
   ```bash
   python scripts/init_db.py
   ```
   Ensure your PostgreSQL server is running and the credentials in `config.ini` are correct.
   This will create the necessary tables in your database.

5. Run a scan:  
   ```bash
   python scripts/speed_scan.py
   ```

---

## Prerequisites

- Python 3.8+  
- PostgreSQL 12+  
- (Optional) BLAKE3 or xxHash3 native library for fast checksums  

## Configuration

All settings live in an INI-style file (e.g. `config/local.ini`):
```ini
[general]
repo_root=/path/to/fs-delta-tracker

[crawler]
data_root=/path/to/scan/root

[sql]
templates_root=templates/sql

[postgresql]
host=localhost
port=5432
database=fs_tracker_db
user=fs_tracker_user
password=your_password

[logging]
log_root=data/logs

init_db=data/logs/init_db.log
scan=data/logs/scan.log
speed_scan=data/logs/speed_scan.log
```

---

## Database Schema

The `scripts/init_db.py` script creates three core tables in the `filesystem` schema:

1. **scan_runs**  
   Tracks each crawl: start/finish times, path counts, bytes added/modified/deleted, free‐form JSON metadata.

2. **files**  
   Current view of the filesystem: path, size, mtime, fingerprint, last_seen_scan, optional `ltree` for subtree queries.

3. **file_changes**  
   Audit log of every file‐level change per scan: `new` / `modified` / `removed`, with old/new size, mtime, fingerprint.

An **unlogged** `staging_files` table serves as the bulk-COPY target for raw stat data and is truncated (or partition-dropped) each run.

---

## How It Works

1. **Stat Walk & Staging**  
   A multiprocessing pool scans directories with `os.scandir()`, emits TSV rows `(scan_id, path, size, mtime)` into a `COPY` stream, and loads them into `staging_files`.

2. **Merge & Change Capture**  
   A single, multi-CTE SQL merges staging into `files`:
   - `INSERT … ON CONFLICT … DO UPDATE` to detect **new** vs. **modified**  
   - `DELETE … RETURNING` to capture **removed**  
   All changes flow into `file_changes` in the same transaction.

3. **Finalize**  
   Aggregates per-change counts and volume, updates `scan_runs.finished_at`, and records elapsed times + metadata.

4. **Clean Up**  
   Truncate or drop partitions of `staging_files` so the next run starts fresh.

