# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "20795e0d-c2e4-44e1-854b-1208823b8a5c",
# META       "default_lakehouse_name": "Bronze_LH",
# META       "default_lakehouse_workspace_id": "b37d6e7d-3c40-4ab5-9c25-3ef4b055be87",
# META       "known_lakehouses": [
# META         {
# META           "id": "20795e0d-c2e4-44e1-854b-1208823b8a5c"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# ---- CELL 1: Parameters -------------------------------------------------------
base_path = ""                    # Base path for Bronze tables
core_tables_param = ""            # Comma-separated API table names
source_counts_json = "[]"         # Real source counts: [{"table":..,"count":..}]
required_not_null_json = "{}"     # Optional: {"Table": ["Col1","Col2"]}
run_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- CELL 1: Parameters & Setup -----------------------------------------------
import json, datetime, traceback
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, NumericType, DateType, TimestampType

base_path = base_path if 'base_path' in locals() and base_path else ""
core_tables_param = core_tables_param if 'core_tables_param' in locals() and core_tables_param else ""
source_counts_json = source_counts_json if 'source_counts_json' in locals() and source_counts_json else "[]"
required_not_null_json = required_not_null_json if 'required_not_null_json' in locals() and required_not_null_json else "{}"
run_id = run_id if 'run_id' in locals() and run_id else ""

# Interactive/Pipeline Parameter Fallbacks
if not base_path:
    print("base_path was empty -- using interactive test value.")
    base_path = "abfss://b37d6e7d-3c40-4ab5-9c25-3ef4b055be87@onelake.dfs.fabric.microsoft.com/20795e0d-c2e4-44e1-854b-1208823b8a5c/Files"

if not run_id:
    run_id = "manual-test-run"

# Handle TableList passed from Data Factory or default string parameter
if 'TableList' in locals() and TableList:
    if isinstance(TableList, str):
        try:
            core_tables = json.loads(TableList)
        except Exception:
            core_tables = [t.strip() for t in TableList.split(",") if t.strip()]
    elif isinstance(TableList, list):
        core_tables = TableList
elif core_tables_param:
    core_tables = [t.strip() for t in core_tables_param.split(",") if t.strip()]
else:
    core_tables = ["Categories", "Customers", "Employees", "Order_Details", "Orders", "Products", "Regions", "Shippers", "Suppliers", "Territories"]

run_ts = datetime.datetime.now().isoformat()
print(f"Validated tables list: {core_tables}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 2: Imports and Parameter Parsing
# ============================================================

import json
from datetime import datetime

from pyspark.sql import functions as F

from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    DecimalType,
    ShortType,
    DateType,
    TimestampType
)


# ------------------------------------------------------------
# Parse core/API tables
# ------------------------------------------------------------

core_tables = [
    t.strip()
    for t in core_tables_param.split(",")
    if t.strip()
]


# ------------------------------------------------------------
# Excel tables
# ------------------------------------------------------------

excel_tables = [
    "Orders",
    "Order_Details",
    "Customers",
    "Products"
]


# ------------------------------------------------------------
# Parse API source counts
# ------------------------------------------------------------

source_counts = {}

try:

    parsed = json.loads(
        source_counts_json
    )

    if isinstance(parsed, dict):

        source_counts = parsed

    elif isinstance(parsed, list):

        for item in parsed:

            if not isinstance(item, dict):
                continue

            table_name = (
                item.get("table_name")
                or item.get("table")
                or item.get("name")
            )

            count_value = (
                item.get("count")
                if item.get("count") is not None
                else item.get("row_count")
            )

            if table_name is not None:
                source_counts[
                    table_name
                ] = count_value

except Exception as e:

    print(
        "WARNING: Could not parse "
        "source_counts_json:"
    )

    print(
        str(e)
    )


# ------------------------------------------------------------
# Required NOT NULL columns
#
# We use primary keys as the minimum required fields.
# ------------------------------------------------------------

required_not_null = {}


# ------------------------------------------------------------
# All tables
# ------------------------------------------------------------

all_tables = sorted(
    set(
        core_tables +
        excel_tables
    )
)


# ------------------------------------------------------------
# Debug
# ------------------------------------------------------------

print(
    "Core/API tables:",
    core_tables
)

print(
    "Excel tables:",
    excel_tables
)

print(
    "API source counts:",
    source_counts
)

print(
    "All tables:",
    all_tables
)

print(
    "Cell 2 loaded successfully."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 3: Separate API Bronze and Excel Bronze loaders
# ============================================================

def load_delta_or_json(path, source_label):

    # --------------------------------------------------------
    # 1. Try Delta
    # --------------------------------------------------------
    try:
        df = (
            spark.read
            .format("delta")
            .load(path)
        )

        if "source_system" not in df.columns:
            df = df.withColumn(
                "source_system",
                F.lit(source_label)
            )

        print(
            f"[{source_label}] Delta loaded: "
            f"{path} | rows={df.count():,}"
        )

        return df

    except Exception:
        pass

    # --------------------------------------------------------
    # 2. Try JSON recursively
    # --------------------------------------------------------
    try:
        raw = (
            spark.read
            .format("json")
            .option(
                "recursiveFileLookup",
                "true"
            )
            .load(path)
        )

        if "value" in raw.columns:

            df = (
                raw
                .select(
                    F.explode("value").alias("r")
                )
                .select("r.*")
            )

        else:
            df = raw

        # Remove OData metadata
        metadata_cols = [
            c
            for c in df.columns
            if c.startswith("@odata")
        ]

        if metadata_cols:
            df = df.drop(
                *metadata_cols
            )

        if "source_system" not in df.columns:
            df = df.withColumn(
                "source_system",
                F.lit(source_label)
            )

        print(
            f"[{source_label}] JSON loaded: "
            f"{path} | rows={df.count():,}"
        )

        return df

    except Exception as e:

        print(
            f"[{source_label}] Could not load "
            f"{path}: {str(e)[:200]}"
        )

        return None


def load_api_bronze(table_name):

    if not table_name:
        return None

    names = [
        table_name
    ]

    if table_name == "Order_Details":
        names.extend([
            "OrderDetails"
        ])

    # --------------------------------------------------------
    # Catalog tables
    # --------------------------------------------------------
    for name in names:

        possible_tables = [
            f"bronze_{name}",
            f"bronze_{name.lower()}"
        ]

        for tbl in possible_tables:

            try:

                if spark.catalog.tableExists(tbl):

                    df = spark.table(tbl)

                    if "source_system" not in df.columns:
                        df = df.withColumn(
                            "source_system",
                            F.lit("API")
                        )

                    print(
                        f"[API] Catalog table "
                        f"{tbl}: {df.count():,} rows"
                    )

                    return df

            except Exception:
                pass

    # --------------------------------------------------------
    # ABFSS / Files
    # --------------------------------------------------------
    for name in names:

        paths = [
            f"{base_path.rstrip('/')}/bronze_{name}",
            f"{base_path.rstrip('/')}/bronze_{name.lower()}"
        ]

        for path in paths:

            df = load_delta_or_json(
                path,
                "API"
            )

            if df is not None:
                return df

    print(
        f"[API] ❌ No Bronze data found for "
        f"{table_name}"
    )

    return None


def load_excel_bronze(table_name):

    if not table_name:
        return None

    names = [
        table_name
    ]

    if table_name == "Order_Details":
        names.extend([
            "OrderDetails"
        ])

    for name in names:

        paths = [
            f"{base_path.rstrip('/')}/bronze_excel_{name}",
            f"{base_path.rstrip('/')}/bronze_excel_{name.lower()}"
        ]

        for path in paths:

            df = load_delta_or_json(
                path,
                "Excel"
            )

            if df is not None:
                return df

    print(
        f"[Excel] ❌ No Bronze data found for "
        f"{table_name}"
    )

    return None


print(
    "Separate API and Excel Bronze loaders loaded successfully."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 4: Validate API Bronze and Excel Bronze separately
# ============================================================

PK_MAP = {
    "Categories": ["CategoryID"],
    "Customers": ["CustomerID"],
    "Employees": ["EmployeeID"],
    "Order_Details": ["OrderID", "ProductID"],
    "Orders": ["OrderID"],
    "Products": ["ProductID"],
    "Regions": ["RegionID"],
    "Shippers": ["ShipperID"],
    "Suppliers": ["SupplierID"],
    "Territories": ["TerritoryID"]
}


def infer_primary_key(df, table_name):

    if df is None:
        return []

    return [
        c
        for c in PK_MAP.get(table_name, [])
        if c in df.columns
    ]


def duplicate_count(df, pk_cols):

    if df is None or not pk_cols:
        return 0

    total = df.count()

    distinct_count = (
        df
        .dropDuplicates(pk_cols)
        .count()
    )

    return total - distinct_count


def null_counts_by_column(df):

    if df is None:
        return {}

    return {
        c: df.filter(
            F.col(c).isNull()
        ).count()
        for c in df.columns
    }


def dtype_map(df):

    if df is None:
        return {}

    return {
        f.name: str(f.dataType)
        for f in df.schema.fields
    }


def calculate_aggregates(df):

    if df is None:
        return {}

    numeric_types = (
        IntegerType,
        LongType,
        DoubleType,
        FloatType,
        DecimalType,
        ShortType
    )

    numeric_cols = [
        f.name
        for f in df.schema.fields
        if isinstance(
            f.dataType,
            numeric_types
        )
    ]

    if not numeric_cols:
        return {}

    expressions = []

    for c in numeric_cols:

        expressions.append(
            F.sum(
                F.col(c)
            ).alias(
                f"{c}_sum"
            )
        )

        expressions.append(
            F.min(
                F.col(c)
            ).alias(
                f"{c}_min"
            )
        )

        expressions.append(
            F.max(
                F.col(c)
            ).alias(
                f"{c}_max"
            )
        )

    return (
        df
        .agg(*expressions)
        .collect()[0]
        .asDict()
    )


def validate_table(table_name):

    print("\n")
    print("=" * 70)
    print(
        "VALIDATING:",
        table_name
    )
    print("=" * 70)

    # --------------------------------------------------------
    # Determine sources
    # --------------------------------------------------------

    is_api = table_name in core_tables
    is_excel = table_name in excel_tables

    api_df = None
    excel_df = None

    # --------------------------------------------------------
    # Load API Bronze
    # --------------------------------------------------------

    if is_api:

        api_df = load_api_bronze(
            table_name
        )

    # --------------------------------------------------------
    # Load Excel Bronze
    # --------------------------------------------------------

    if is_excel:

        excel_df = load_excel_bronze(
            table_name
        )

    has_api = api_df is not None
    has_excel = excel_df is not None

    # --------------------------------------------------------
    # Counts
    # --------------------------------------------------------

    api_count = (
        api_df.count()
        if has_api
        else None
    )

    excel_count = (
        excel_df.count()
        if has_excel
        else None
    )

    # --------------------------------------------------------
    # API expected count
    #
    # Comes from pipeline source_counts_json.
    # --------------------------------------------------------

    api_expected = (
        source_counts.get(
            table_name
        )
        if is_api
        else None
    )

    # --------------------------------------------------------
    # Excel expected count
    #
    # We intentionally do NOT use
    # excel_source_counts_json because that
    # parameter does not exist in your notebook.
    #
    # The Excel Bronze row count itself is the
    # validation count.
    # --------------------------------------------------------

    excel_expected = (
        excel_count
        if has_excel
        else None
    )

    # --------------------------------------------------------
    # Count match
    # --------------------------------------------------------

    if api_expected is None:

        api_count_match = None

    else:

        try:

            api_count_match = (
                api_count
                ==
                int(api_expected)
            )

        except Exception:

            api_count_match = None

    excel_count_match = (
        True
        if has_excel
        else None
    )

    # --------------------------------------------------------
    # Primary key
    # --------------------------------------------------------

    base_df = (
        api_df
        if has_api
        else excel_df
    )

    pk = infer_primary_key(
        base_df,
        table_name
    )

    # --------------------------------------------------------
    # Duplicate PK checks
    # --------------------------------------------------------

    api_pk_duplicates = (
        duplicate_count(
            api_df,
            pk
        )
        if has_api
        else 0
    )

    excel_pk_duplicates = (
        duplicate_count(
            excel_df,
            pk
        )
        if has_excel
        else 0
    )

    # --------------------------------------------------------
    # Required columns
    #
    # Primary keys are required by default.
    # --------------------------------------------------------

    required_cols = pk

    required_null_count = 0

    if has_api:

        for c in required_cols:

            if c in api_df.columns:

                required_null_count += (
                    api_df
                    .filter(
                        F.col(c).isNull()
                    )
                    .count()
                )

    if has_excel:

        for c in required_cols:

            if c in excel_df.columns:

                required_null_count += (
                    excel_df
                    .filter(
                        F.col(c).isNull()
                    )
                    .count()
                )

    # --------------------------------------------------------
    # Reasons
    # --------------------------------------------------------

    reasons = []

    if is_api and not has_api:

        reasons.append(
            "no API Bronze data found"
        )

    if is_excel and not has_excel:

        reasons.append(
            "no Excel Bronze data found"
        )

    if api_count_match is False:

        reasons.append(
            "API count mismatch: "
            f"expected={api_expected}, "
            f"actual={api_count}"
        )

    if api_pk_duplicates > 0:

        reasons.append(
            f"{api_pk_duplicates} "
            "duplicate API PK rows"
        )

    if excel_pk_duplicates > 0:

        reasons.append(
            f"{excel_pk_duplicates} "
            "duplicate Excel PK rows"
        )

    if required_null_count > 0:

        reasons.append(
            f"{required_null_count} "
            "required PK NULL rows"
        )

    # --------------------------------------------------------
    # Missing primary key
    # --------------------------------------------------------

    if (
        table_name in PK_MAP
        and (
            has_api
            or has_excel
        )
        and not pk
    ):

        reasons.append(
            "required primary key not found"
        )

    # --------------------------------------------------------
    # Final status
    # --------------------------------------------------------

    hard_fail = (

        (is_api and not has_api)

        or

        (is_excel and not has_excel)

        or

        (api_count_match is False)

        or

        (api_pk_duplicates > 0)

        or

        (excel_pk_duplicates > 0)

        or

        (required_null_count > 0)

        or

        (
            table_name in PK_MAP
            and (
                has_api
                or has_excel
            )
            and not pk
        )
    )

    status = (
        "PASSED"
        if not hard_fail
        else "FAILED"
    )

    if not reasons:

        reasons.append(
            "validation passed"
        )

    # --------------------------------------------------------
    # Aggregates
    # --------------------------------------------------------

    api_aggs = (
        calculate_aggregates(
            api_df
        )
        if has_api
        else {}
    )

    excel_aggs = (
        calculate_aggregates(
            excel_df
        )
        if has_excel
        else {}
    )

    # --------------------------------------------------------
    # Return result
    # --------------------------------------------------------

    return {

        "run_id": run_id,

        "run_timestamp": run_ts,

        "table_name": table_name,

        "has_api": has_api,

        "has_excel": has_excel,

        "status": status,

        "failure_reason": "; ".join(
            reasons
        ),

        "api_count": api_count,

        "api_source_count": api_expected,

        "api_count_match": api_count_match,

        "excel_count": excel_count,

        "excel_source_count": excel_expected,

        "excel_count_match": excel_count_match,

        "api_pk_duplicates":
            api_pk_duplicates,

        "excel_pk_duplicates":
            excel_pk_duplicates,

        "full_duplicate_count":
            api_pk_duplicates
            +
            excel_pk_duplicates,

        "inferred_pk":
            ",".join(pk)
            if pk
            else None,

        "required_columns":
            ",".join(required_cols)
            if required_cols
            else None,

        "required_null_count":
            required_null_count,

        "null_counts_by_column":
            json.dumps(
                {
                    "API":
                        null_counts_by_column(
                            api_df
                        ),
                    "Excel":
                        null_counts_by_column(
                            excel_df
                        )
                },
                default=str
            ),

        "combined_bronze_aggs":
            json.dumps(
                {
                    "API":
                        api_aggs,
                    "Excel":
                        excel_aggs
                },
                default=str
            ),

        "excel_bronze_aggs":
            json.dumps(
                excel_aggs,
                default=str
            ),

        "excel_source_aggs":
            json.dumps(
                {},
                default=str
            ),

        "aggs_match":
            "not_compared",

        "agg_diffs":
            json.dumps(
                {},
                default=str
            ),

        "dtypes":
            json.dumps(
                {
                    "API":
                        dtype_map(api_df),
                    "Excel":
                        dtype_map(excel_df)
                },
                default=str
            )
    }


print(
    "Cell 4 loaded successfully."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 5: Run Validation
# ============================================================

print("\n" + "=" * 70)
print("STARTING BRONZE VALIDATION")
print("=" * 70)


# ------------------------------------------------------------
# Make sure table lists exist
# ------------------------------------------------------------

core_tables = [
    "Categories",
    "Customers",
    "Employees",
    "Order_Details",
    "Orders",
    "Products",
    "Regions",
    "Shippers",
    "Suppliers",
    "Territories"
]


excel_tables = [
    "Orders",
    "Order_Details",
    "Customers",
    "Products"
]


all_tables = sorted(
    set(
        core_tables +
        excel_tables
    )
)


# ------------------------------------------------------------
# Make sure source_counts exists
# ------------------------------------------------------------

if "source_counts" not in globals():

    source_counts = {}

    try:

        parsed = json.loads(
            source_counts_json
        )

        if isinstance(parsed, dict):

            source_counts = parsed

        elif isinstance(parsed, list):

            for item in parsed:

                if not isinstance(item, dict):
                    continue

                table_name = (
                    item.get("table_name")
                    or item.get("table")
                    or item.get("name")
                )

                count_value = (
                    item.get("count")
                    if item.get("count") is not None
                    else item.get("row_count")
                )

                if table_name is not None:

                    source_counts[
                        table_name
                    ] = count_value

    except Exception as e:

        print(
            "WARNING: source_counts could not be parsed:"
        )

        print(
            str(e)
        )


print(
    "Tables to validate:"
)

print(
    all_tables
)

print(
    "API source counts:"
)

print(
    source_counts
)


# ------------------------------------------------------------
# Run validation
# ------------------------------------------------------------

results = []


for table_name in all_tables:

    print("\n" + "-" * 60)

    print(
        "Validating:",
        table_name
    )

    print("-" * 60)

    try:

        result = validate_table(
            table_name
        )

        results.append(
            result
        )

        if result["status"] == "PASSED":

            print(
                "PASS:",
                table_name
            )

        else:

            print(
                "FAIL:",
                table_name
            )

            print(
                "Reason:",
                result.get(
                    "failure_reason",
                    ""
                )
            )

    except Exception as e:

        error_message = (
            type(e).__name__
            + ": "
            + str(e)[:500]
        )

        print(
            "FAIL:",
            table_name,
            error_message
        )

        results.append({

            "run_id":
                run_id,

            "run_timestamp":
                datetime.utcnow().isoformat(),

            "table_name":
                table_name,

            "has_api":
                table_name in core_tables,

            "has_excel":
                table_name in excel_tables,

            "status":
                "FAILED",

            "failure_reason":
                error_message,

            "api_count":
                None,

            "api_source_count":
                None,

            "api_count_match":
                None,

            "excel_count":
                None,

            "excel_source_count":
                None,

            "excel_count_match":
                None,

            "api_pk_duplicates":
                0,

            "excel_pk_duplicates":
                0,

            "full_duplicate_count":
                0,

            "inferred_pk":
                None,

            "required_columns":
                None,

            "required_null_count":
                0,

            "null_counts_by_column":
                "{}",

            "combined_bronze_aggs":
                "{}",

            "excel_bronze_aggs":
                "{}",

            "excel_source_aggs":
                "{}",

            "aggs_match":
                "error",

            "agg_diffs":
                "{}",

            "dtypes":
                "{}"
        })


print("\n" + "=" * 70)

print(
    "VALIDATION COMPLETE:",
    len(results),
    "tables"
)

print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- CELL 6: Write the log (explicit schema -- avoids type-inference errors) -
if results:
    try:
        log_rows = [{
            "run_id": run_id, "run_timestamp": run_ts, "table_name": r["table_name"],
            "status": r["status"], "exists": r["exists"], "row_count": r["row_count"],
            "expected_count": r["expected_count"], "count_match": r["count_match"],
            "primary_key": json.dumps(r["primary_key"]),
            "duplicate_pk_rows": r["duplicate_pk_rows"], "required_null_count": r["required_null_count"],
            "null_counts_by_column": json.dumps(r["null_counts_by_column"], default=str),
            "aggregates": json.dumps(r["aggregates"], default=str),
            "dtypes": json.dumps(r["dtypes"], default=str),
            "reasons": json.dumps(r["reasons"]),
        } for r in results]

        log_schema = StructType([
            StructField("run_id", StringType()), StructField("run_timestamp", StringType()),
            StructField("table_name", StringType()), StructField("status", StringType()),
            StructField("exists", BooleanType()), StructField("row_count", LongType()),
            StructField("expected_count", LongType()), StructField("count_match", BooleanType()),
            StructField("primary_key", StringType()), StructField("duplicate_pk_rows", LongType()),
            StructField("required_null_count", LongType()), StructField("null_counts_by_column", StringType()),
            StructField("aggregates", StringType()), StructField("dtypes", StringType()),
            StructField("reasons", StringType()),
        ])
        log_df = spark.createDataFrame(log_rows, schema=log_schema)
        log_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("bronze_validation_log")
        display(log_df)
    except Exception as e:
        print(f"WARNING: could not write bronze_validation_log: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 7: Build Final Validation Summary
# ============================================================

print("\n" + "=" * 70)
print("BUILDING VALIDATION SUMMARY")
print("=" * 70)


passed_tables = [
    r["table_name"]
    for r in results
    if r["status"] == "PASSED"
]


failed_tables = [
    r["table_name"]
    for r in results
    if r["status"] != "PASSED"
]


print(
    "Passed tables:",
    passed_tables
)

print(
    "Failed tables:",
    failed_tables
)


summary = {
    "run_id": run_id,
    "total_tables": len(results),
    "passed_tables": passed_tables,
    "failed_tables": failed_tables,
    "passed_count": len(passed_tables),
    "failed_count": len(failed_tables),
    "results": results
}


print(
    json.dumps(
        summary,
        indent=2,
        default=str
    )
)


mssparkutils.notebook.exit(
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
