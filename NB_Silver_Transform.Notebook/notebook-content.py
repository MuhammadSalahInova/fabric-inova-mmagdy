# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "aa124c78-1e88-46a9-b10d-1d1b63d5c6fc",
# META       "default_lakehouse_name": "Silver_LH",
# META       "default_lakehouse_workspace_id": "b37d6e7d-3c40-4ab5-9c25-3ef4b055be87",
# META       "known_lakehouses": [
# META         {
# META           "id": "20795e0d-c2e4-44e1-854b-1208823b8a5c"
# META         },
# META         {
# META           "id": "aa124c78-1e88-46a9-b10d-1d1b63d5c6fc"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================
# CELL 1: Parameters + Safety Check
# ============================================================

bronze_base_path = ""
core_tables_param = ""
run_id = ""

import json
import re

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

from delta.tables import DeltaTable

run_ts = datetime.now(timezone.utc)
run_ts_iso = run_ts.isoformat()

if not bronze_base_path:

    bronze_base_path = (
        "abfss://b37d6e7d-3c40-4ab5-9c25-3ef4b055be87"
        "@onelake.dfs.fabric.microsoft.com/"
        "20795e0d-c2e4-44e1-854b-1208823b8a5c/Files"
    )

if not run_id:
    run_id = "manual-test-run"

if not core_tables_param:

    core_tables_param = (
        "Categories,"
        "Customers,"
        "Employees,"
        "Order_Details,"
        "Orders,"
        "Products,"
        "Regions,"
        "Shippers,"
        "Suppliers,"
        "Territories"
    )

print(f"bronze_base_path = {bronze_base_path}")
print(f"run_id = {run_id}")
print(f"run_timestamp = {run_ts_iso}")

# Safety: this notebook must run with Silver_LH as default
existing_tables = [
    t.name
    for t in spark.catalog.listTables()
]

if "bronze_validation_log" in existing_tables:

    raise Exception(
        "STOP: Bronze_LH appears to be the default Lakehouse. "
        "Set Silver_LH as the default Lakehouse and restart the session."
    )

print("✅ Silver Lakehouse safety check passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 1b: Interactive Fallback
# ============================================================

if not bronze_base_path:
    bronze_base_path = (
        "abfss://b37d6e7d-3c40-4ab5-9c25-3ef4b055be87"
        "@onelake.dfs.fabric.microsoft.com/"
        "20795e0d-c2e4-44e1-854b-1208823b8a5c/Files"
    )

if not run_id:
    run_id = "manual-test-run"

if not core_tables_param:
    core_tables_param = (
        "Categories,"
        "Customers,"
        "Employees,"
        "Order_Details,"
        "Orders,"
        "Products,"
        "Regions,"
        "Shippers,"
        "Suppliers,"
        "Territories"
    )

print("Interactive fallback values loaded.")
print(f"bronze_base_path = {bronze_base_path}")
print(f"run_id = {run_id}")
print(f"core_tables_param = {core_tables_param}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 2: Imports + Dynamic Table Plan (no hardcoded PK/name/mode lists)
# ============================================================

import json
from itertools import combinations
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

silver_log = []
dq_log = []
silver_frames = {}

core_tables = [t.strip() for t in core_tables_param.split(",") if t.strip()]

def silver_name(table_name):
    """Dynamic naming -- no manual dict to keep in sync with core_tables_param."""
    return f"silver_{table_name.lower()}"

def infer_primary_key(df, max_composite_size=3):
    """Infers the business key from the data itself: any single *ID column
    that's unique and non-null, else the smallest combination of *ID columns
    that is. No table-name lookup needed -- works for any table."""
    if df is None:
        return []
    total = df.count()
    if total == 0:
        return []
    id_cols = [c for c in df.columns if c.lower().endswith("id")]
    if not id_cols:
        return [df.columns[0]] if df.columns else []
    for c in id_cols:
        non_null_df = df.filter(F.col(c).isNotNull())
        if non_null_df.count() == total and non_null_df.select(c).distinct().count() == total:
            return [c]
    for size in range(2, min(max_composite_size, len(id_cols)) + 1):
        for combo in combinations(id_cols, size):
            subset = df.select(*combo).na.drop()
            if subset.count() == total and subset.distinct().count() == total:
                return list(combo)
    return id_cols  # fallback: couldn't prove single-column uniqueness

print("Table plan will be built dynamically per-table in Cell 8.")
print(f"Core tables (from pipeline parameter): {core_tables}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 3: Bronze Loaders (PK/silver-name logic now lives in Cell 2 only)
# ============================================================

EXCEL_NAME_MAP = {
    "Customers": "Customers",
    "Products": "Products",
    "Orders": "Orders",
    "Order_Details": "OrderDetails",
}

TECHNICAL_COLUMNS = {
    "source_system", "row_hash", "effective_start", "effective_end",
    "is_current", "_run_id", "created_at", "modified_at", "operation", "ingested_at",
}

def drop_odata_metadata(df):
    if df is None:
        return None
    odata_cols = [c for c in df.columns if c.startswith("@odata")]
    if odata_cols:
        df = df.drop(*odata_cols)
    return df

def load_delta(path):
    try:
        return spark.read.format("delta").load(path)
    except Exception:
        return None

def load_odata_json(path):
    try:
        raw = spark.read.option("multiLine", "true").json(path)
        if "value" in raw.columns:
            df = raw.select(F.explode("value").alias("r")).select("r.*")
        else:
            df = raw
        return drop_odata_metadata(df)
    except Exception:
        return None

def load_api_bronze(table_name):
    if not table_name:
        return None
    possible_paths = [
        f"{bronze_base_path.rstrip('/')}/bronze_{table_name}",
        f"{bronze_base_path.rstrip('/')}/bronze_{table_name.lower()}",
    ]
    for path in possible_paths:
        df = load_delta(path)
        if df is not None:
            df = drop_odata_metadata(df)
            print(f"[{table_name}] API Bronze loaded from Delta: {path} | rows={df.count():,}")
            return df
        df = load_odata_json(path)
        if df is not None:
            print(f"[{table_name}] API Bronze loaded from JSON: {path} | rows={df.count():,}")
            return df
    print(f"[{table_name}] API Bronze NOT FOUND")
    return None

def load_bronze_excel(table_name):
    if not table_name:
        return None
    folder_name = EXCEL_NAME_MAP.get(table_name, table_name)
    possible_paths = [
        f"{bronze_base_path.rstrip('/')}/bronze_excel_{folder_name}",
        f"{bronze_base_path.rstrip('/')}/bronze_excel_{table_name}",
        f"{bronze_base_path.rstrip('/')}/bronze_excel_{folder_name.lower()}",
    ]
    for path in possible_paths:
        df = load_delta(path)
        if df is not None:
            print(f"[{table_name}] Excel Bronze loaded: {path} | rows={df.count():,}")
            return df
        df = load_odata_json(path)
        if df is not None:
            print(f"[{table_name}] Excel Bronze loaded as JSON: {path} | rows={df.count():,}")
            return df
    print(f"[{table_name}] Excel Bronze NOT FOUND")
    return None

print("Bronze loaders ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 4: Cleaning Helpers (consolidated -- was duplicated across old Cell 4 + Cell 7)
# ============================================================

def trim_all_strings(df):
    if df is None:
        return None
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                F.when(F.trim(F.col(field.name)) == "", F.lit(None))
                 .otherwise(F.trim(F.col(field.name)))
            )
    return df

def get_date_columns(df):
    """FIXED: previously 'order' as a keyword matched OrderID (a numeric
    column, not a date) and risked silently corrupting it. Now excludes any
    column ending in ID/Key outright, and only matches genuine date-ish names."""
    if df is None:
        return []
    date_cols = []
    date_keywords = ("date", "time", "datetime", "created", "modified",
                      "updated", "birth", "hire", "shipped", "required")
    for field in df.schema.fields:
        name = field.name
        lname = name.lower()
        if lname.endswith("id") or lname.endswith("key"):
            continue
        if isinstance(field.dataType, (TimestampType, DateType)):
            date_cols.append(name)
            continue
        if isinstance(field.dataType, StringType) and any(k in lname for k in date_keywords):
            date_cols.append(name)
    return list(dict.fromkeys(date_cols))

def standardize_dates(df):
    """Converts every detected date-like STRING column to a real timestamp.
    Tries multiple common formats before giving up, so mixed API/Excel date
    formats don't silently produce nulls. Safe to call on a df that already
    has real date/timestamp columns -- those are skipped."""
    if df is None:
        return None
    for col_name in get_date_columns(df):
        dtype = df.schema[col_name].dataType
        if isinstance(dtype, (TimestampType, DateType)):
            continue
        c = F.col(col_name)
        df = df.withColumn(
            col_name,
            F.coalesce(
                F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss"),
                F.to_timestamp(c, "yyyy-MM-dd"),
                F.to_timestamp(c, "MM/dd/yyyy HH:mm:ss"),
                F.to_timestamp(c, "MM/dd/yyyy"),
                F.to_timestamp(c)  # generic fallback, catches anything else Spark can parse
            )
        )
    return df

def remove_empty_rows(df, pk_cols):
    if df is None:
        return None
    for c in pk_cols:
        if c in df.columns:
            df = df.filter(F.col(c).isNotNull())
    return df

def dedup_on_pk(df, pk_cols):
    if df is None or not pk_cols:
        return df
    if "is_current" in df.columns:
        return df  # never deduplicate an SCD2 table -- history must remain
    return df.dropDuplicates(pk_cols)

def fill_nulls(df):
    if df is None:
        return None
    protected = TECHNICAL_COLUMNS | {"row_hash"}
    for field in df.schema.fields:
        c = field.name
        if c in protected or c.startswith("source_"):
            continue
        if isinstance(field.dataType, StringType):
            df = df.withColumn(c, F.coalesce(F.col(c), F.lit("Unknown")))
    return df

def add_ingestion_timestamp(df):
    if df is None:
        return None
    if "ingested_at" not in df.columns:
        df = df.withColumn("ingested_at", F.current_timestamp())
    return df

def apply_generic_cleaning(df, pk_cols, table_name):
    """Full cleaning path for freshly-loaded API-only tables."""
    if df is None:
        return None
    df = trim_all_strings(df)
    df = remove_empty_rows(df, pk_cols)
    df = dedup_on_pk(df, pk_cols)
    df = standardize_dates(df)
    df = add_ingestion_timestamp(df)
    df = fill_nulls(df)
    return df

print("Consolidated cleaning helpers loaded (trim, dates, dedup, nulls, ingestion timestamp).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 5: Table-Specific Transformations
# ============================================================

def transform_table(
    df,
    table_name
):

    if df is None:
        return None

    # --------------------------------------------------------
    # Customers
    # --------------------------------------------------------

    if table_name == "Customers":

        if "CustomerID" in df.columns:

            df = df.withColumn(
                "CustomerID",
                F.col("CustomerID").cast("string")
            )

    # --------------------------------------------------------
    # Products
    # --------------------------------------------------------

    elif table_name == "Products":

        numeric_columns = [
            "ProductID",
            "SupplierID",
            "CategoryID",
            "UnitsInStock",
            "UnitsOnOrder",
            "ReorderLevel"
        ]

        for c in numeric_columns:

            if c in df.columns:

                df = df.withColumn(
                    c,
                    F.col(c).cast("long")
                )

        if "UnitPrice" in df.columns:

            df = df.withColumn(
                "UnitPrice",
                F.col("UnitPrice").cast("double")
            )

    # --------------------------------------------------------
    # Orders
    # --------------------------------------------------------

    elif table_name == "Orders":

        numeric_columns = [
            "OrderID",
            "CustomerID",
            "EmployeeID",
            "ShipVia"
        ]

        for c in numeric_columns:

            if c in df.columns:

                df = df.withColumn(
                    c,
                    F.col(c).cast("long")
                )

        if "Freight" in df.columns:

            df = df.withColumn(
                "Freight",
                F.col("Freight").cast("double")
            )

    # --------------------------------------------------------
    # Order Details
    # --------------------------------------------------------

    elif table_name == "Order_Details":

        numeric_columns = [
            "OrderID",
            "ProductID",
            "Quantity"
        ]

        for c in numeric_columns:

            if c in df.columns:

                df = df.withColumn(
                    c,
                    F.col(c).cast("long")
                )

        if "UnitPrice" in df.columns:

            df = df.withColumn(
                "UnitPrice",
                F.col("UnitPrice").cast("double")
            )

        if "Discount" in df.columns:

            df = df.withColumn(
                "Discount",
                F.col("Discount").cast("double")
            )

    # --------------------------------------------------------
    # Categories
    # --------------------------------------------------------

    elif table_name == "Categories":

        if "CategoryID" in df.columns:

            df = df.withColumn(
                "CategoryID",
                F.col("CategoryID").cast("long")
            )

    # --------------------------------------------------------
    # Employees
    # --------------------------------------------------------

    elif table_name == "Employees":

        if "EmployeeID" in df.columns:

            df = df.withColumn(
                "EmployeeID",
                F.col("EmployeeID").cast("long")
            )

        if "ReportsTo" in df.columns:

            df = df.withColumn(
                "ReportsTo",
                F.col("ReportsTo").cast("long")
            )

    # --------------------------------------------------------
    # Regions
    # --------------------------------------------------------

    elif table_name == "Regions":

        if "RegionID" in df.columns:

            df = df.withColumn(
                "RegionID",
                F.col("RegionID").cast("long")
            )

    # --------------------------------------------------------
    # Shippers
    # --------------------------------------------------------

    elif table_name == "Shippers":

        if "ShipperID" in df.columns:

            df = df.withColumn(
                "ShipperID",
                F.col("ShipperID").cast("long")
            )

    # --------------------------------------------------------
    # Suppliers
    # --------------------------------------------------------

    elif table_name == "Suppliers":

        if "SupplierID" in df.columns:

            df = df.withColumn(
                "SupplierID",
                F.col("SupplierID").cast("long")
            )

    # --------------------------------------------------------
    # Territories
    # --------------------------------------------------------

    elif table_name == "Territories":

        if "TerritoryID" in df.columns:

            df = df.withColumn(
                "TerritoryID",
                F.col("TerritoryID").cast("string")
            )

        if "RegionID" in df.columns:

            df = df.withColumn(
                "RegionID",
                F.col("RegionID").cast("long")
            )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 6: Data Quality Helpers
# ============================================================

def check_nulls(
    df,
    pk_cols
):

    result = {}

    if df is None:
        return result

    total = df.count()

    if total == 0:
        return result

    for c in df.columns:

        if c in TECHNICAL_COLUMNS:
            continue

        count = (
            df
            .filter(
                F.col(c).isNull()
            )
            .count()
        )

        if count > 0:

            result[c] = {
                "null_count": count,
                "null_pct": round(
                    count / total * 100,
                    2
                )
            }

    return result


def check_duplicates(
    df,
    pk_cols
):

    if df is None or not pk_cols:
        return 0

    total = df.count()

    distinct_count = (
        df
        .select(*pk_cols)
        .distinct()
        .count()
    )

    return total - distinct_count


def check_current_duplicates(
    df,
    pk_cols
):

    if (
        df is None
        or not pk_cols
        or "is_current" not in df.columns
    ):
        return 0

    current_df = (
        df
        .filter(
            F.col("is_current") == True
        )
    )

    total = current_df.count()

    distinct_count = (
        current_df
        .select(*pk_cols)
        .distinct()
        .count()
    )

    return total - distinct_count


def build_dq_result(
    df,
    table_name,
    pk_cols
):

    return {

        "table": table_name,

        "primary_key": pk_cols,

        "row_count": (
            df.count()
            if df is not None
            else 0
        ),

        "nulls": check_nulls(
            df,
            pk_cols
        ),

        "duplicate_pk_count":
            check_duplicates(
                df,
                pk_cols
            ),

        "current_duplicate_pk_count":
            check_current_duplicates(
                df,
                pk_cols
            )
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 8: FINAL SILVER BUILD (fully dynamic)
#
# Mode is no longer a hardcoded set -- it's detected per table:
#   if NB_Excel_Merge already built silver_<table>, this notebook PRESERVES
#   it (SCD2_MERGED); otherwise it builds fresh from Bronze API (API_ONLY).
#
# FIX (this version): SCD2_MERGED tables were only getting trim_all_strings()
# and standardize_dates() -- fill_nulls() was never called for them, which is
# exactly why ShipRegion (and any other string column) still showed raw NULL
# instead of "Unknown". Both branches now call the full null-fill step.
# ============================================================

print("\n" + "=" * 90)
print("FINAL SILVER BUILD")
print("=" * 90)

pipeline_state = {}
silver_frames = {}

for table_name in core_tables:
    silver_table_name = silver_name(table_name)
    already_exists = spark.catalog.tableExists(silver_table_name)
    mode = "SCD2_MERGED" if already_exists else "API_ONLY"

    print("\n" + "-" * 80)
    print(f"PROCESSING: {table_name} -> {silver_table_name}  (mode: {mode})")
    print("-" * 80)

    try:
        # ========================================================
        # A. EXISTING SCD2 TABLE (built earlier by NB_Excel_Merge)
        # ========================================================
        if mode == "SCD2_MERGED":
            final_df = spark.table(silver_table_name)

            if "is_current" not in final_df.columns:
                raise Exception(f"{silver_table_name} exists but is missing the SCD2 "
                                 f"column 'is_current' -- was it really built by NB_Excel_Merge?")

            pk_cols = infer_primary_key(final_df.filter(F.col("is_current") == True))  # noqa: E712

            # Trim strings, fix dates, AND fill string nulls with "Unknown" --
            # this last call was missing before and is the actual fix.
            final_df = trim_all_strings(final_df)
            final_df = standardize_dates(final_df)
            final_df = fill_nulls(final_df)   # <-- NEW: this is the fix

            (final_df.write.format("delta").mode("overwrite")
                .option("overwriteSchema", "true").saveAsTable(silver_table_name))

            final_df = spark.table(silver_table_name)
            current_df = final_df.filter(F.col("is_current") == True)  # noqa: E712
            total_rows = final_df.count()
            current_rows = current_df.count()
            historical_rows = final_df.filter(F.col("is_current") == False).count()  # noqa: E712

            api_rows = final_df.filter(F.col("source_system") == "API").count() if "source_system" in final_df.columns else 0
            excel_rows = final_df.filter(F.col("source_system") == "Excel").count() if "source_system" in final_df.columns else 0

            merge_result = {
                "status": "SUCCESS", "api_rows": api_rows, "excel_rows": excel_rows,
                "unchanged_rows": 0, "changed_rows": historical_rows,
                "new_rows": max(0, current_rows - api_rows), "total_rows": total_rows,
                "current_rows": current_rows, "historical_rows": historical_rows,
            }

            pipeline_state[table_name] = {
                "clean_df": final_df, "pk_cols": pk_cols, "silver_table": silver_table_name,
                "error": None, "merge_result": merge_result,
            }
            silver_frames[table_name] = current_df

            print(f"Preserved existing SCD2 table -- Total={total_rows:,} Current={current_rows:,} Historical={historical_rows:,}")
            print(f"Inferred PK: {pk_cols}")
            continue

        # ========================================================
        # B. API-ONLY TABLE -- build fresh from Bronze
        # ========================================================
        api_df = load_api_bronze(table_name)
        if api_df is None:
            raise Exception(f"API Bronze not found for {table_name}")

        api_count = api_df.count()
        pk_cols = infer_primary_key(api_df)
        print(f"[{table_name}] API Bronze rows: {api_count:,} | inferred PK: {pk_cols}")

        clean_df = apply_generic_cleaning(api_df, pk_cols, table_name)

        if "source_system" not in clean_df.columns:
            clean_df = clean_df.withColumn("source_system", F.lit("API"))

        clean_df = (
            clean_df
            .withColumn("effective_start", F.lit(run_ts_iso).cast("timestamp"))
            .withColumn("effective_end", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .withColumn("created_at", F.lit(run_ts_iso).cast("timestamp"))
            .withColumn("modified_at", F.lit(run_ts_iso).cast("timestamp"))
            .withColumn("operation", F.lit("I"))
            .withColumn("_run_id", F.lit(run_id))
        )

        hash_cols = [c for c in clean_df.columns if c not in TECHNICAL_COLUMNS]
        hash_expr = (F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in hash_cols])
                     if hash_cols else F.lit("EMPTY_RECORD"))
        clean_df = clean_df.withColumn("row_hash", F.sha2(hash_expr, 256))

        (clean_df.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true").saveAsTable(silver_table_name))

        final_df = spark.table(silver_table_name)
        current_df = final_df.filter(F.col("is_current") == True)  # noqa: E712
        total_rows = final_df.count()
        current_rows = current_df.count()

        merge_result = {
            "status": "SUCCESS", "api_rows": api_count, "excel_rows": 0,
            "unchanged_rows": 0, "changed_rows": 0, "new_rows": api_count,
            "total_rows": total_rows, "current_rows": current_rows, "historical_rows": 0,
        }

        pipeline_state[table_name] = {
            "clean_df": final_df, "pk_cols": pk_cols, "silver_table": silver_table_name,
            "error": None, "merge_result": merge_result,
        }
        silver_frames[table_name] = current_df

        print(f"Created {silver_table_name} -- Current={current_rows:,}")

    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)[:500]}"
        pipeline_state[table_name] = {
            "clean_df": None, "pk_cols": [], "silver_table": silver_table_name,
            "error": error_msg,
            "merge_result": {"status": "FAILED", "api_rows": 0, "excel_rows": 0,
                              "unchanged_rows": 0, "changed_rows": 0, "new_rows": 0,
                              "total_rows": 0, "current_rows": 0, "historical_rows": 0},
        }
        print(f"FAILED {silver_table_name}: {error_msg}")

# ============================================================
# FINAL VERIFICATION
# ============================================================
print("\n" + "=" * 90)
print("CELL 8 FINAL VERIFICATION")
print("=" * 90)

all_ok = True
for table_name in core_tables:
    silver_table_name = silver_name(table_name)
    if spark.catalog.tableExists(silver_table_name):
        df = spark.table(silver_table_name)
        total = df.count()
        if "is_current" in df.columns:
            current = df.filter(F.col("is_current") == True).count()  # noqa: E712
            historical = df.filter(F.col("is_current") == False).count()  # noqa: E712
        else:
            current, historical = total, 0
        print(f"OK  {silver_table_name:<30} Total={total:,} Current={current:,} Historical={historical:,}")
    else:
        all_ok = False
        print(f"MISSING: {silver_table_name}")

print("=" * 90)
print("ALL TABLES PRESENT" if all_ok else "SOME TABLES MISSING/FAILED")
print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 9: Data Quality Validation
# ============================================================

silver_log = []
dq_log = []

print("\n")
print("=" * 80)
print("SILVER DATA QUALITY VALIDATION")
print("=" * 80)

for table_name, state in pipeline_state.items():

    silver_table_name = state["silver_table"]

    print("\n" + "-" * 70)
    print(f"VALIDATING: {table_name}")
    print("-" * 70)

    if state.get("error"):

        print(
            f"❌ Skipping validation because transformation failed: "
            f"{state['error']}"
        )

        dq_log.append({
            "table": table_name,
            "run_id": run_id,
            "run_timestamp": run_ts_iso,
            "result": {
                "primary_key": state.get("pk_cols", []),
                "row_count": 0,
                "duplicate_pk_count": 0,
                "current_duplicate_pk_count": 0,
                "nulls": {},
                "status": "FAILED",
                "error": state["error"]
            }
        })

        continue

    try:

        df = spark.table(
            silver_table_name
        )

        pk_cols = state.get(
            "pk_cols",
            []
        )

        # ----------------------------------------------------
        # Duplicate PKs (across ALL rows, including SCD2 history)
        # ----------------------------------------------------

        duplicate_pk_count = check_duplicates(
            df,
            pk_cols
        )

        # ----------------------------------------------------
        # Current duplicate PKs (is_current = True rows only)
        # ----------------------------------------------------

        current_duplicate_pk_count = (
            check_current_duplicates(
                df,
                pk_cols
            )
        )

        # ----------------------------------------------------
        # Row count
        # ----------------------------------------------------

        row_count = df.count()

        # ----------------------------------------------------
        # Nulls
        # ----------------------------------------------------

        nulls = check_nulls(
            df,
            pk_cols
        )

        # ----------------------------------------------------
        # SCD2 check
        # ----------------------------------------------------

        if "is_current" in df.columns:

            current_rows = (
                df
                .filter(
                    F.col("is_current") == True
                )
                .count()
            )

            historical_rows = (
                df
                .filter(
                    F.col("is_current") == False
                )
                .count()
            )

        else:

            current_rows = row_count
            historical_rows = 0

        # ----------------------------------------------------
        # Status
        # ----------------------------------------------------

        status = "PASSED"

        reasons = []

        if duplicate_pk_count > 0:

            status = "FAILED"

            reasons.append(
                f"{duplicate_pk_count} duplicate PK rows"
            )

        if current_duplicate_pk_count > 0:

            status = "FAILED"

            reasons.append(
                f"{current_duplicate_pk_count} duplicate current PK rows"
            )

        dq_result = {

            "primary_key":
                pk_cols,

            "row_count":
                row_count,

            "current_rows":
                current_rows,

            "historical_rows":
                historical_rows,

            "duplicate_pk_count":
                duplicate_pk_count,

            "current_duplicate_pk_count":
                current_duplicate_pk_count,

            "nulls":
                nulls,

            "status":
                status,

            "reasons":
                reasons
        }

        dq_log.append({

            "table":
                table_name,

            "run_id":
                run_id,

            "run_timestamp":
                run_ts_iso,

            "result":
                dq_result
        })

        silver_log.append((

            silver_table_name,

            row_count,

            current_rows,

            historical_rows,

            status,

            "; ".join(reasons)
            if reasons
            else None,

            run_ts_iso
        ))

        print(
            f"Rows:        {row_count:,}"
        )

        print(
            f"Current:     {current_rows:,}"
        )

        print(
            f"Historical:  {historical_rows:,}"
        )

        print(
            f"Duplicate PK (all rows):     {duplicate_pk_count:,}"
        )

        print(
            f"Duplicate PK (current only): {current_duplicate_pk_count:,}"
        )

        print(
            f"Status:      {status}"
        )

    except Exception as e:

        error_message = (
            f"{type(e).__name__}: "
            f"{str(e)[:500]}"
        )

        print(
            f"❌ DQ validation failed: "
            f"{error_message}"
        )

        silver_log.append((

            silver_table_name,

            0,

            0,

            0,

            "FAILED",

            error_message,

            run_ts_iso
        ))

        dq_log.append({

            "table":
                table_name,

            "run_id":
                run_id,

            "run_timestamp":
                run_ts_iso,

            "result": {

                "primary_key":
                    state.get(
                        "pk_cols",
                        []
                    ),

                "row_count":
                    0,

                "status":
                    "FAILED",

                "error":
                    error_message
            }
        })

print("\n")
print("=" * 80)
print("DQ VALIDATION COMPLETE")
print("=" * 80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 9b: Foreign Key / Gold Readiness Helpers
# ============================================================

all_pks = {
    table_name: state.get("pk_cols", [])
    for table_name, state in pipeline_state.items()
    if state.get("pk_cols")
}


def infer_foreign_keys(
    table_name,
    df,
    pk_cols,
    all_pks
):

    if df is None:
        return []

    candidates = []

    for column in df.columns:

        if column in pk_cols:
            continue

        for ref_table, ref_pk_cols in all_pks.items():

            if ref_table == table_name:
                continue

            for ref_col in ref_pk_cols:

                if column.lower() == ref_col.lower():

                    candidates.append(
                        (
                            column,
                            ref_table,
                            ref_col
                        )
                    )

    return candidates


def check_referential_integrity(
    df,
    fk_col,
    ref_df,
    ref_col
):

    if (
        df is None
        or ref_df is None
        or fk_col not in df.columns
        or ref_col not in ref_df.columns
    ):

        return 0

    ref_values = (
        ref_df
        .select(
            F.col(ref_col).alias("_ref_value")
        )
        .filter(
            F.col("_ref_value").isNotNull()
        )
        .distinct()
    )

    orphan_count = (
        df
        .filter(
            F.col(fk_col).isNotNull()
        )
        .join(
            ref_values,
            F.col(fk_col) ==
            F.col("_ref_value"),
            "left_anti"
        )
        .count()
    )

    return orphan_count


def gold_readiness(
    dq_result
):

    reasons = []

    if dq_result.get(
        "duplicate_pk_count",
        0
    ) > 0:

        reasons.append(
            "Duplicate primary keys found"
        )

    if dq_result.get(
        "current_duplicate_pk_count",
        0
    ) > 0:

        reasons.append(
            "Duplicate current SCD2 keys found"
        )

    return {
        "gold_ready":
            len(reasons) == 0,

        "reasons":
            reasons
    }


print(
    "✅ FK and Gold-readiness helpers loaded."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 10: FK Validation + Gold Readiness
# ============================================================

print("\n")
print("=" * 80)
print("FOREIGN KEY / GOLD READINESS VALIDATION")
print("=" * 80)

for entry in dq_log:

    table_name = entry["table"]

    if (
        table_name not in pipeline_state
        or
        pipeline_state[table_name].get("error")
    ):

        entry["result"]["referential_integrity"] = {}

        entry["result"]["gold_readiness"] = {
            "gold_ready": False,
            "reasons": [
                "Silver transformation failed"
            ]
        }

        continue

    state = pipeline_state[
        table_name
    ]

    df = spark.table(
        state["silver_table"]
    )

    pk_cols = state.get(
        "pk_cols",
        []
    )

    fk_list = infer_foreign_keys(
        table_name,
        df,
        pk_cols,
        all_pks
    )

    fk_results = {}

    for (
        fk_col,
        ref_table,
        ref_col
    ) in fk_list:

        if (
            ref_table in pipeline_state
            and
            pipeline_state[
                ref_table
            ].get("error") is None
        ):

            ref_df = spark.table(
                pipeline_state[
                    ref_table
                ]["silver_table"]
            )

            orphan_count = (
                check_referential_integrity(
                    df,
                    fk_col,
                    ref_df,
                    ref_col
                )
            )

            fk_results[
                f"{fk_col} -> "
                f"{ref_table}.{ref_col}"
            ] = orphan_count

    entry["result"][
        "referential_integrity"
    ] = fk_results

    # Add FK failures to readiness
    fk_failures = [
        key
        for key, value
        in fk_results.items()
        if value > 0
    ]

    if fk_failures:

        entry["result"].setdefault(
            "gold_readiness",
            {
                "gold_ready": True,
                "reasons": []
            }
        )

        entry["result"][
            "gold_readiness"
        ]["gold_ready"] = False

        entry["result"][
            "gold_readiness"
        ]["reasons"].append(
            f"Referential integrity failures: "
            f"{fk_failures}"
        )

    else:

        entry["result"][
            "gold_readiness"
        ] = gold_readiness(
            entry["result"]
        )

    gold_ready = entry[
        "result"
    ]["gold_readiness"]["gold_ready"]

    if gold_ready:

        print(
            f"✅ {table_name}: Gold READY"
        )

    else:

        print(
            f"⚠️ {table_name}: Gold NOT READY"
        )

        print(
            entry["result"][
                "gold_readiness"
            ]["reasons"]
        )

# ------------------------------------------------------------
# Write DQ log
# ------------------------------------------------------------

dq_rows = []

for entry in dq_log:

    result = entry["result"]

    dq_rows.append({

        "run_id":
            entry["run_id"],

        "run_timestamp":
            entry["run_timestamp"],

        "table_name":
            entry["table"],

        "primary_key":
            json.dumps(
                result.get(
                    "primary_key",
                    []
                )
            ),

        "gold_ready":
            result.get(
                "gold_readiness",
                {}
            ).get(
                "gold_ready",
                False
            ),

        "reasons":
            json.dumps(
                result.get(
                    "gold_readiness",
                    {}
                ).get(
                    "reasons",
                    []
                )
            ),

        "details_json":
            json.dumps(
                result,
                default=str
            )
    })

if dq_rows:

    dq_schema = StructType([

        StructField(
            "run_id",
            StringType(),
            True
        ),

        StructField(
            "run_timestamp",
            StringType(),
            True
        ),

        StructField(
            "table_name",
            StringType(),
            True
        ),

        StructField(
            "primary_key",
            StringType(),
            True
        ),

        StructField(
            "gold_ready",
            BooleanType(),
            True
        ),

        StructField(
            "reasons",
            StringType(),
            True
        ),

        StructField(
            "details_json",
            StringType(),
            True
        )
    ])

    dq_df = spark.createDataFrame(
        dq_rows,
        schema=dq_schema
    )

    (
        dq_df
        .write
        .format("delta")
        .mode("append")
        .option(
            "mergeSchema",
            "true"
        )
        .saveAsTable(
            "silver_dq_log"
        )
    )

    display(dq_df)

    print(
        f"✅ Wrote {len(dq_rows)} DQ records."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 10b: API vs Excel SCD2 Provenance
# ============================================================

source_breakdown = {}

print("\n")
print("=" * 80)
print("SCD2 SOURCE PROVENANCE")
print("=" * 80)

for table_name, state in pipeline_state.items():

    if state.get("error"):
        source_breakdown[
            table_name
        ] = {}

        continue

    silver_table = state[
        "silver_table"
    ]

    frame = spark.table(
        silver_table
    )

    if "source_system" in frame.columns:

        # ALL rows
        all_counts = (
            frame
            .groupBy(
                "source_system"
            )
            .count()
            .collect()
        )

        # CURRENT rows only
        if "is_current" in frame.columns:

            current_counts = (
                frame
                .filter(
                    F.col("is_current") == True
                )
                .groupBy(
                    "source_system"
                )
                .count()
                .collect()
            )

        else:

            current_counts = all_counts

        source_breakdown[
            table_name
        ] = {

            "all": {
                row["source_system"]:
                    row["count"]
                for row in all_counts
            },

            "current": {
                row["source_system"]:
                    row["count"]
                for row in current_counts
            }
        }

    else:

        source_breakdown[
            table_name
        ] = {
            "all": {},
            "current": {}
        }


print("\n--- Source Breakdown ---")

for table_name, counts in source_breakdown.items():

    all_counts = counts.get(
        "all",
        {}
    )

    current_counts = counts.get(
        "current",
        {}
    )

    print(
        f"{table_name}: "
        f"ALL "
        f"API={all_counts.get('API', 0):,}, "
        f"Excel={all_counts.get('Excel', 0):,} | "
        f"CURRENT "
        f"API={current_counts.get('API', 0):,}, "
        f"Excel={current_counts.get('Excel', 0):,}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 11: Final Summary
# ============================================================

print("\n")
print("=" * 80)
print("FINAL SILVER TRANSFORMATION SUMMARY")
print("=" * 80)

succeeded = [
    r
    for r in silver_log
    if r[4] == "SUCCESS"
]

failed_rows = [
    r
    for r in silver_log
    if r[4] != "SUCCESS"
]

not_gold_ready = [

    e["table"]

    for e in dq_log

    if not e["result"]
    .get(
        "gold_readiness",
        {}
    )
    .get(
        "gold_ready",
        False
    )
]

summary = {

    "run_id":
        run_id,

    "run_timestamp":
        run_ts_iso,

    "overall_status":
        "FAILED"
        if failed_rows
        else "SUCCESS",

    "tables_discovered":
        core_tables,

    "tables_succeeded": [

        {
            "table":
                r[0],

            "final_rows":
                r[1],

            "current_rows":
                r[2],

            "historical_rows":
                r[3],

            "status":
                r[4]
        }

        for r in succeeded
    ],

    "tables_failed": [

        {
            "table":
                r[0],

            "error":
                r[5]
        }

        for r in failed_rows
    ],

    "tables_not_gold_ready":
        not_gold_ready,

    "total_succeeded":
        len(succeeded),

    "total_failed":
        len(failed_rows),

    "silver_tables": {

        table_name:
            state["silver_table"]

        for table_name, state
        in pipeline_state.items()
    }
}

print(
    json.dumps(
        summary,
        indent=2,
        default=str
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 11b: Human-Readable Silver Summary + Pipeline Exit
# ============================================================

summary_table_rows = []

for r in silver_log:

    table_name = r[0]

    # Find the corresponding source breakdown
    logical_name = None

    for name, state in pipeline_state.items():

        if state["silver_table"] == table_name:

            logical_name = name
            break

    if logical_name is None:
        logical_name = table_name

    counts = source_breakdown.get(
        logical_name,
        {}
    )

    all_counts = counts.get(
        "all",
        {}
    )

    current_counts = counts.get(
        "current",
        {}
    )

    gold_ready = None

    for entry in dq_log:

        if entry["table"] == logical_name:

            gold_ready = (
                entry["result"]
                .get(
                    "gold_readiness",
                    {}
                )
                .get(
                    "gold_ready",
                    False
                )
            )

            break

    summary_table_rows.append({

        "table":
            table_name,

        "status":
            r[4],

        "final_rows":
            r[1],

        "current_rows":
            r[2],

        "historical_rows":
            r[3],

        "all_api_rows":
            all_counts.get(
                "API",
                0
            ),

        "all_excel_rows":
            all_counts.get(
                "Excel",
                0
            ),

        "current_api_rows":
            current_counts.get(
                "API",
                0
            ),

        "current_excel_rows":
            current_counts.get(
                "Excel",
                0
            ),

        "gold_ready":
            gold_ready,

        "error":
            (
                r[5][:300] + "..."
                if r[5]
                and len(r[5]) > 300
                else r[5]
            )
    })


summary_table_schema = StructType([

    StructField(
        "table",
        StringType(),
        True
    ),

    StructField(
        "status",
        StringType(),
        True
    ),

    StructField(
        "final_rows",
        LongType(),
        True
    ),

    StructField(
        "current_rows",
        LongType(),
        True
    ),

    StructField(
        "historical_rows",
        LongType(),
        True
    ),

    StructField(
        "all_api_rows",
        LongType(),
        True
    ),

    StructField(
        "all_excel_rows",
        LongType(),
        True
    ),

    StructField(
        "current_api_rows",
        LongType(),
        True
    ),

    StructField(
        "current_excel_rows",
        LongType(),
        True
    ),

    StructField(
        "gold_ready",
        BooleanType(),
        True
    ),

    StructField(
        "error",
        StringType(),
        True
    )
])


summary_table_df = spark.createDataFrame(
    summary_table_rows,
    schema=summary_table_schema
)

display(
    summary_table_df
)

print("\n")
print("=" * 80)
print("NB_Silver_Transform COMPLETE")
print("=" * 80)

print(
    json.dumps(
        summary,
        indent=2,
        default=str
    )
)

notebookutils.notebook.exit(
    json.dumps(
        summary,
        default=str
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
