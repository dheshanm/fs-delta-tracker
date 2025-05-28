BEGIN;

-- This script processes the staging files for a given scan.
-- It marks files as deleted, inserts new files, updates modified files,
-- and updates the last seen scan for unchanged files.

-- It assumes that the staging files have already been populated
-- and that the `scan_id` parameter is provided.

-- Parameters:
-- :scan_id - The ID of the scan being processed.

WITH -- 1) Mark “deleted” files (and capture name/type too)
deleted AS (
    DELETE FROM
        filesystem.files AS f
    WHERE
        NOT EXISTS (
            SELECT
                1
            FROM
                filesystem.staging_files s
            WHERE
                s.scan_id = :scan_id
                AND s.file_path = f.file_path
        ) RETURNING f.file_path,
        f.file_name AS old_file_name,
        f.file_type AS old_file_type,
        f.file_size_bytes AS old_size,
        f.file_mtime AS old_mtime
),
ins_deleted AS (
    INSERT INTO
        filesystem.file_changes (
            scan_id,
            file_path,
            change_type,
            old_size_bytes,
            old_mtime
        )
    SELECT
        :scan_id,
        file_path,
        'deleted',
        old_size,
        old_mtime
    FROM
        deleted
),
-- 2) Find brand‐new files
new_files AS (
    SELECT
        s.file_path,
        s.file_name,
        s.file_type,
        s.file_size_bytes,
        s.file_mtime
    FROM
        filesystem.staging_files s
    WHERE
        s.scan_id = :scan_id
        AND NOT EXISTS (
            SELECT
                1
            FROM
                filesystem.files f
            WHERE
                f.file_path = s.file_path
        )
),
ins_new AS (
    INSERT INTO
        filesystem.files (
            file_name,
            file_type,
            file_size_bytes,
            file_path,
            file_mtime,
            file_fingerprint,
            last_seen_scan
        )
    SELECT
        nf.file_name,
        nf.file_type,
        nf.file_size_bytes,
        nf.file_path,
        nf.file_mtime,
        NULL, -- fingerprint is not known yet
        :scan_id
    FROM
        new_files nf RETURNING file_path,
        file_size_bytes AS new_size,
        file_mtime AS new_mtime
),
rec_new AS (
    INSERT INTO
        filesystem.file_changes (
            scan_id,
            file_path,
            change_type,
            new_size_bytes,
            new_mtime
        )
    SELECT
        :scan_id,
        file_path,
        'added',
        new_size,
        new_mtime
    FROM
        ins_new
),
-- 3) Find modified files (size/mtime/name/type)
mod AS (
    SELECT
        s.file_path,
        s.file_name AS new_file_name,
        s.file_type AS new_file_type,
        s.file_size_bytes AS new_size,
        s.file_mtime AS new_mtime,
        f.file_name AS old_file_name,
        f.file_type AS old_file_type,
        f.file_size_bytes AS old_size,
        f.file_mtime AS old_mtime
    FROM
        filesystem.staging_files s
        JOIN filesystem.files f USING (file_path)
    WHERE
        s.scan_id = :scan_id
        AND (
            s.file_size_bytes <> f.file_size_bytes
            OR s.file_mtime <> f.file_mtime
        )
),
ins_mod AS (
    INSERT INTO
        filesystem.file_changes (
            scan_id,
            file_path,
            change_type,
            old_size_bytes,
            new_size_bytes,
            old_mtime,
            new_mtime
        )
    SELECT
        :scan_id,
        file_path,
        'modified',
        old_size,
        new_size,
        old_mtime,
        new_mtime
    FROM
        mod
),
upd_mod AS (
    UPDATE
        filesystem.files f
    SET
        file_name = m.new_file_name,
        file_type = m.new_file_type,
        file_size_bytes = m.new_size,
        file_mtime = m.new_mtime,
        last_seen_scan = :scan_id,
        file_fingerprint = NULL, -- fingerprint should be recalculated
        last_updated = now()
    FROM
        mod m
    WHERE
        f.file_path = m.file_path
),
-- 4) Bump last_seen_scan on unchanged files
upd_unchanged AS (
    UPDATE
        filesystem.files f
    SET
        last_seen_scan = :scan_id,
        last_updated = now()
    FROM
        filesystem.staging_files s
    WHERE
        s.scan_id = :scan_id
        AND s.file_path = f.file_path
        AND s.file_size_bytes = f.file_size_bytes
        AND s.file_mtime = f.file_mtime
) -- Just to drive the CTEs:
SELECT
    1;

COMMIT;
