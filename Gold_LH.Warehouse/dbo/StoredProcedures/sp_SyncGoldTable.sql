-- =============================================================================
-- Gold star schema build script v3 -- SELF-HEALING, NAME-SAFE VERSION
--
-- Fixes the "Column name or number of supplied values does not match table
-- definition" error: v2's TRUNCATE + INSERT INTO real SELECT * FROM staging
-- matches columns by POSITION. If a real table (like DimDate) predates this
-- script and has a different column layout than staging now produces, that
-- blind positional INSERT breaks immediately.
--
-- v3 fix, via one reusable procedure (dbo.sp_SyncGoldTable):
--   1. If the real table doesn't exist yet, OR its column set doesn't match
--      staging's column set -> DROP + recreate it fresh (self-heals any
--      stale/mismatched table automatically, no manual DROP needed).
--   2. If the column sets DO match -> TRUNCATE + INSERT with an EXPLICIT
--      column list on both sides, matched by NAME. This can never hit a
--      positional mismatch again, even if column order ever differs.
--
-- Paste this ENTIRE script into the Gold_Transform Script activity.
-- =============================================================================

-- ============================================================
-- Reusable sync procedure — create once, used by every table below
-- ============================================================
CREATE   PROCEDURE dbo.sp_SyncGoldTable
    @staging_table NVARCHAR(128),
    @real_table NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @real_exists BIT = CASE WHEN OBJECT_ID('dbo.' + @real_table) IS NULL THEN 0 ELSE 1 END;
    DECLARE @schema_match BIT = 0;
    DECLARE @col_list NVARCHAR(MAX);
    DECLARE @sql NVARCHAR(MAX);

    IF @real_exists = 1
    BEGIN
        -- Column sets match (same names, order doesn't matter) only if
        -- neither side has a column the other one lacks.
        IF NOT EXISTS (
            SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = @staging_table
            EXCEPT
            SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = @real_table
        )
        AND NOT EXISTS (
            SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = @real_table
            EXCEPT
            SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = @staging_table
        )
            SET @schema_match = 1;
    END

    IF @real_exists = 0 OR @schema_match = 0
    BEGIN
        -- First run, OR the existing table is stale/mismatched: rebuild it fresh.
        -- (This is the one-time identity reset — happens automatically now,
        -- instead of needing a manual DROP each time a mismatch is found.)
        SET @sql = 'DROP TABLE IF EXISTS dbo.' + @real_table + ';' +
                   'CREATE TABLE dbo.' + @real_table + ' AS SELECT * FROM dbo.' + @staging_table + ';';
        EXEC sp_executesql @sql;
    END
    ELSE
    BEGIN
        -- Schemas already match: sync by NAME, never by position.
        SELECT @col_list = STRING_AGG(QUOTENAME(column_name), ', ')
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = @real_table;

        SET @sql = 'TRUNCATE TABLE dbo.' + @real_table + ';' +
                   'INSERT INTO dbo.' + @real_table + ' (' + @col_list + ') ' +
                   'SELECT ' + @col_list + ' FROM dbo.' + @staging_table + ';';
        EXEC sp_executesql @sql;
    END

    SET @sql = 'DROP TABLE IF EXISTS dbo.' + @staging_table + ';';
    EXEC sp_executesql @sql;
END

GO