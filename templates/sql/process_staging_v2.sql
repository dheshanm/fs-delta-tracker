-- process_staging.sql
-- Assumes parameter :scan_id is passed in.
BEGIN;

WITH -- 1) pull in the scan_root and turn it into an ltree
scan_info AS (
    SELECT
        scan_root,
        replace(btrim(scan_root, '/'), '/', '.')::ltree AS root_ltree
    FROM
        filesystem.scan_runs
    WHERE
        scan_id = :scan_id
),
-- 2) alias the staging rows for convenience
staged AS (
    SELECT
        s.*
    FROM
        filesystem.staging_files AS s
    WHERE
        s.scan_id = :scan_id
),
-- 3) delete any files in 'filesystem.files' under this root that did NOT show up in staging
deleted AS (
    DELETE FROM
        filesystem.files AS f USING scan_info
    WHERE
        -- only delete things under this scan_root
        f.path_ltree <@ scan_info.root_ltree
        AND NOT EXISTS (
            SELECT
                1
            FROM
                staged AS s2
            WHERE
                s2.file_path = f.file_path
        ) RETURNING f.file_path AS file_path,
        f.file_name AS old_file_name,
        f.file_type AS old_file_type,
        f.file_size_bytes AS old_size_bytes,
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
        old_size_bytes,
        old_mtime
    FROM
        deleted
),
-- 4) find brand-new files in staging (no existing row in filesystem.files)
new_files AS (
    SELECT
        s.file_name,
        s.file_type,
        s.file_size_bytes,
        s.file_path,
        s.file_mtime
    FROM
        staged AS s
        LEFT JOIN filesystem.files AS f ON f.file_path = s.file_path
    WHERE
        f.file_path IS NULL
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
            last_seen_scan,
            last_updated
        )
    SELECT
        nf.file_name,
        nf.file_type,
        nf.file_size_bytes,
        nf.file_path,
        nf.file_mtime,
        NULL,
        -- fingerprint to be calculated later
        :scan_id,
        now()
    FROM
        new_files AS nf RETURNING file_path,
        file_size_bytes AS new_size_bytes,
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
        new_size_bytes,
        new_mtime
    FROM
        ins_new
),
-- 5) modified files (same path exists but size or mtime changed)
mods AS (
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
        staged AS s
        JOIN filesystem.files AS f ON f.file_path = s.file_path
    WHERE
        (s.file_size_bytes <> f.file_size_bytes)
        OR (s.file_mtime <> f.file_mtime)
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
        mods
),
upd_mod AS (
    UPDATE
        filesystem.files AS f
    SET
        file_name = m.new_file_name,
        file_type = m.new_file_type,
        file_size_bytes = m.new_size,
        file_mtime = m.new_mtime,
        last_seen_scan = :scan_id,
        file_fingerprint = NULL,
        -- force re-hash
        last_updated = now()
    FROM
        mods AS m
    WHERE
        f.file_path = m.file_path
),
-- 6) untouched files: just bump last_seen_scan
upd_unchanged AS (
    UPDATE
        filesystem.files AS f
    SET
        last_seen_scan = :scan_id,
        last_updated = now()
    FROM
        staged AS s
    WHERE
        s.file_path = f.file_path
        AND s.file_size_bytes = f.file_size_bytes
        AND s.file_mtime = f.file_mtime
) -- kick off the CTEs
SELECT
    1;

COMMIT;