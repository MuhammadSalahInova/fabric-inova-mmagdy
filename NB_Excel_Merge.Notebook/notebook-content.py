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
# BRONZE API PAGINATION DIAGNOSTIC
# DO NOT CHANGE ANY OTHER CELL
# ============================================================

import json
from json import JSONDecoder

print("\n")
print("=" * 90)
print("BRONZE API PAGINATION DIAGNOSTIC")
print("=" * 90)


# ============================================================
# PATH
# ============================================================

BRONZE_ROOT = (
    "abfss://b37d6e7d-3c40-4ab5-9c25-3ef4b055be87"
    "@onelake.dfs.fabric.microsoft.com/"
    "20795e0d-c2e4-44e1-854b-1208823b8a5c/Files"
)


TABLES = {
    "Customers": "bronze_Customers",
    "Products": "bronze_Products",
    "Orders": "bronze_Orders",
    "OrderDetails": "bronze_Order_Details"
}


EXPECTED = {
    "Customers": 91,
    "Products": 77,
    "Orders": 830,
    "OrderDetails": 2155
}


# ============================================================
# JSON OBJECT PARSER
# ============================================================

def parse_json_objects(text):

    text = text.lstrip("\ufeff")

    decoder = JSONDecoder()

    objects = []

    pos = 0

    while pos < len(text):

        while (
            pos < len(text)
            and text[pos].isspace()
        ):
            pos += 1

        if pos >= len(text):
            break

        try:

            obj, end_pos = decoder.raw_decode(
                text,
                pos
            )

            objects.append(obj)

            pos = end_pos

        except Exception as e:

            print(
                f"JSON parsing stopped at character {pos}: "
                f"{e}"
            )

            break

    return objects


# ============================================================
# DIAGNOSTIC
# ============================================================

for table_name, folder_name in TABLES.items():

    print("\n")
    print("#" * 90)
    print(
        f"CHECKING BRONZE: {table_name}"
    )
    print("#" * 90)

    path = (
        f"{BRONZE_ROOT}/{folder_name}"
    )

    print(
        f"Path:\n{path}"
    )

    try:

        # ----------------------------------------------------
        # Read physical files exactly as stored
        # ----------------------------------------------------

        files = (
            spark.sparkContext
            .wholeTextFiles(path)
            .collect()
        )

        print(
            f"\nPhysical files found: {len(files)}"
        )

        total_objects = 0
        total_records = 0
        page_sizes = []
        next_links = []

        # ----------------------------------------------------
        # Inspect every physical file
        # ----------------------------------------------------

        for file_path, content in files:

            print(
                f"\nFile:"
            )

            print(
                file_path
            )

            print(
                f"File characters: "
                f"{len(content):,}"
            )

            objects = parse_json_objects(
                content
            )

            print(
                f"JSON objects found: "
                f"{len(objects)}"
            )

            total_objects += len(objects)

            # ------------------------------------------------
            # Inspect every JSON response object
            # ------------------------------------------------

            for i, obj in enumerate(objects, 1):

                if not isinstance(
                    obj,
                    dict
                ):

                    print(
                        f"  Object {i}: NOT A JSON OBJECT"
                    )

                    continue

                values = obj.get(
                    "value"
                )

                if isinstance(
                    values,
                    list
                ):

                    page_count = len(
                        values
                    )

                    page_sizes.append(
                        page_count
                    )

                    total_records += (
                        page_count
                    )

                    print(
                        f"  Page {i}: "
                        f"{page_count:,} records"
                    )

                else:

                    print(
                        f"  Page {i}: "
                        f"NO value array"
                    )

                next_link = obj.get(
                    "@odata.nextLink"
                )

                if next_link:

                    next_links.append(
                        next_link
                    )

        # ----------------------------------------------------
        # Spark's normal JSON reader
        # ----------------------------------------------------

        try:

            spark_raw = (
                spark.read
                .format("json")
                .option(
                    "recursiveFileLookup",
                    "true"
                )
                .load(path)
            )

            spark_objects = spark_raw.count()

            if "value" in spark_raw.columns:

                spark_value_count = (
                    spark_raw
                    .select(
                        F.explode(
                            F.col("value")
                        ).alias("record")
                    )
                    .count()
                )

            else:

                spark_value_count = spark_objects

        except Exception as e:

            spark_objects = -1
            spark_value_count = -1

            print(
                f"Spark JSON read failed: {e}"
            )

        # ----------------------------------------------------
        # RESULT
        # ----------------------------------------------------

        expected = EXPECTED[
            table_name
        ]

        print("\n")
        print(
            "=" * 70
        )

        print(
            f"{table_name} RESULT"
        )

        print(
            "=" * 70
        )

        print(
            f"Physical files:       {len(files):,}"
        )

        print(
            f"JSON page objects:     {total_objects:,}"
        )

        print(
            f"Page sizes:            {page_sizes}"
        )

        print(
            f"Total records in JSON: {total_records:,}"
        )

        print(
            f"@odata.nextLink count: {len(next_links):,}"
        )

        print(
            f"Spark JSON objects:    {spark_objects:,}"
        )

        print(
            f"Spark exploded rows:   {spark_value_count:,}"
        )

        print(
            f"Expected API rows:     {expected:,}"
        )

        # ----------------------------------------------------
        # Diagnosis
        # ----------------------------------------------------

        if total_records == expected:

            print(
                f"\n✅ {table_name}: BRONZE IS COMPLETE"
            )

        elif total_records < expected:

            print(
                f"\n❌ {table_name}: BRONZE IS INCOMPLETE"
            )

            print(
                f"Missing approximately "
                f"{expected - total_records:,} rows"
            )

        elif total_records > expected:

            print(
                f"\n⚠️ {table_name}: BRONZE HAS MORE "
                f"ROWS THAN EXPECTED"
            )

        print(
            "=" * 70
        )

    except Exception as e:

        print(
            f"\n❌ {table_name} DIAGNOSTIC FAILED"
        )

        print(
            f"{type(e).__name__}: "
            f"{str(e)}"
        )


print("\n")
print("=" * 90)
print("BRONZE DIAGNOSTIC FINISHED")
print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# ============================================================
# CELL 1: Parameters
# ============================================================

FS_SOURCE_FILE = ""
bronze_base_path = ""
run_id = ""
api_base_url = ""
excel_file_name = ""
table_list_json = "[]"

print("Excel Merge parameters loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not bronze_base_path:
    bronze_base_path = (
        "abfss://b37d6e7d-3c40-4ab5-9c25-3ef4b055be87"
        "@onelake.dfs.fabric.microsoft.com/"
        "20795e0d-c2e4-44e1-854b-1208823b8a5c/Files"
    )

if not FS_SOURCE_FILE:
    if excel_file_name:
        FS_SOURCE_FILE = f"{bronze_base_path.rstrip('/')}/{excel_file_name}"
    else:
        FS_SOURCE_FILE = f"{bronze_base_path.rstrip('/')}/FakeStore_Orders_OrderDetails_Customers_Products_Linked.xlsx"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 2: Imports and Run Setup
# ============================================================

import json
import os
import pandas as pd

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

from delta.tables import DeltaTable

merge_log = []

run_ts = datetime.now(timezone.utc)
run_ts_iso = run_ts.isoformat()

print(f"Run timestamp: {run_ts_iso}")
print("✅ Imports loaded, including Window.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 3: Configuration
# ============================================================

FS_CONFIG = [
    {
        "sheet": "Customers",
        "target_table": "Silver_Customers",
        "keys": ["CustomerID"],
        "columns": [
            "CompanyName",
            "ContactName",
            "ContactTitle",
            "Address",
            "City",
            "Region",
            "PostalCode",
            "Country",
            "Phone",
            "Fax"
        ]
    },

    {
        "sheet": "Products",
        "target_table": "Silver_Products",
        "keys": ["ProductID"],
        "columns": [
            "ProductName",
            "SupplierID",
            "CategoryID",
            "QuantityPerUnit",
            "UnitPrice",
            "UnitsInStock",
            "UnitsOnOrder",
            "ReorderLevel",
            "Discontinued"
        ]
    },

    {
        "sheet": "Orders",
        "target_table": "Silver_Orders",
        "keys": ["OrderID"],
        "columns": [
            "CustomerID",
            "EmployeeID",
            "OrderDate",
            "RequiredDate",
            "ShippedDate",
            "ShipVia",
            "Freight",
            "ShipName",
            "ShipAddress",
            "ShipCity",
            "ShipRegion",
            "ShipPostalCode",
            "ShipCountry"
        ]
    },

    {
        "sheet": "OrderDetails",
        "target_table": "Silver_Order_Details",
        "keys": ["OrderID", "ProductID"],
        "columns": [
            "Quantity",
            "UnitPrice",
            "Discount"
        ]
    }
]

# Excel sheet -> API Bronze folder
BRONZE_FOLDER_MAP = {
    "OrderDetails": "Order_Details"
}

# Technical columns NEVER included in SHA2
TECHNICAL_COLUMNS = {
    "source_system",
    "row_hash",
    "effective_start",
    "effective_end",
    "is_current",
    "_run_id",
    "created_at",
    "modified_at",
    "operation",
    "ingested_at"
}

# Source audit columns from Excel
SOURCE_AUDIT_COLUMNS = {
    "CreatedDate",
    "ModifiedDate",
    "Operation"
}

print("Configuration loaded.")
print(f"Configured Excel entities: {len(FS_CONFIG)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 4: COMPLETE BRONZE JSON PARSER + SHA2 SCD2 ENGINE
# ============================================================

import json
from json import JSONDecoder

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timezone


# ============================================================
# DROP ODATA METADATA
# ============================================================

def drop_odata_metadata(df):

    if df is None:
        return None

    metadata_columns = [
        c
        for c in df.columns
        if c.startswith("@odata")
    ]

    if metadata_columns:
        df = df.drop(*metadata_columns)

    return df


# ============================================================
# NORMALIZE DATE COLUMNS
# ============================================================

def normalize_date_columns(df, cfg):

    if df is None:
        return None

    for c in cfg["columns"]:

        if (
            c in df.columns
            and "date" in c.lower()
        ):

            df = df.withColumn(
                c,
                F.date_format(
                    F.to_timestamp(
                        F.col(c).cast("string")
                    ),
                    "yyyy-MM-dd HH:mm:ss"
                )
            )

    return df


# ============================================================
# READ COMPLETE API BRONZE
#
# Bronze contains ONE physical file with MULTIPLE
# concatenated OData JSON objects.
#
# Example:
#
# JSON object 1 → 20 rows
# JSON object 2 → 20 rows
# JSON object 3 → 20 rows
# ...
#
# We parse every JSON object and every value[] array.
# ============================================================

def read_bronze_api(sheet_name, cfg):

    folder_name = BRONZE_FOLDER_MAP.get(
        sheet_name,
        sheet_name
    )

    path = (
        f"{bronze_base_path.rstrip('/')}"
        f"/bronze_{folder_name}"
    )

    print("\n")
    print("=" * 80)
    print(
        f"[{sheet_name}] READING COMPLETE API BRONZE"
    )
    print("=" * 80)

    print(
        f"Bronze path:\n{path}"
    )

    # ========================================================
    # READ PHYSICAL FILE AS RAW TEXT
    # ========================================================

    try:

        files = (
            spark.sparkContext
            .wholeTextFiles(path)
            .collect()
        )

    except Exception as e:

        raise RuntimeError(
            f"[{sheet_name}] Could not read Bronze files: "
            f"{str(e)}"
        )

    print(
        f"Physical Bronze files: {len(files)}"
    )

    if not files:

        raise RuntimeError(
            f"[{sheet_name}] No Bronze files found."
        )

    # ========================================================
    # PARSE ALL JSON OBJECTS
    # ========================================================

    decoder = JSONDecoder()

    all_records = []

    total_json_objects = 0

    for file_path, content in files:

        print("\n")
        print(
            "Reading physical file:"
        )
        print(
            file_path
        )

        # Remove UTF-8 BOM if present
        content = content.lstrip("\ufeff")

        position = 0

        while position < len(content):

            # ------------------------------------------------
            # Skip whitespace
            # ------------------------------------------------

            while (
                position < len(content)
                and content[position].isspace()
            ):
                position += 1

            if position >= len(content):
                break

            # ------------------------------------------------
            # Decode ONE JSON object
            # ------------------------------------------------

            try:

                obj, next_position = (
                    decoder.raw_decode(
                        content,
                        position
                    )
                )

            except Exception as e:

                raise RuntimeError(
                    f"[{sheet_name}] Failed parsing JSON "
                    f"at character {position}: {str(e)}"
                )

            position = next_position

            total_json_objects += 1

            # ------------------------------------------------
            # Validate JSON object
            # ------------------------------------------------

            if not isinstance(
                obj,
                dict
            ):
                continue

            # ------------------------------------------------
            # Extract value[]
            # ------------------------------------------------

            values = obj.get(
                "value",
                []
            )

            if not isinstance(
                values,
                list
            ):
                continue

            page_count = len(values)

            print(
                f"  JSON page "
                f"{total_json_objects}: "
                f"{page_count:,} rows"
            )

            all_records.extend(
                values
            )

    # ========================================================
    # COMPLETE RECORD COUNT
    # ========================================================

    print("\n")
    print(
        f"[{sheet_name}] JSON objects: "
        f"{total_json_objects}"
    )

    print(
        f"[{sheet_name}] TOTAL API RECORDS: "
        f"{len(all_records):,}"
    )

    if not all_records:

        raise RuntimeError(
            f"[{sheet_name}] No API records found "
            f"inside Bronze JSON."
        )

    # ========================================================
    # NORMALIZE PYTHON TYPES BEFORE SPARK INFERENCE
    #
    # This is especially important for OrderDetails.
    #
    # Some JSON records can contain:
    #
    #   Quantity → int
    #   Quantity → float
    #
    # Spark cannot infer LongType + DoubleType together.
    #
    # We force the expected business types before creating
    # the DataFrame.
    # ========================================================

    normalized_records = []

    for record in all_records:

        if not isinstance(
            record,
            dict
        ):
            continue

        r = dict(record)

        # ----------------------------------------------------
        # ORDER DETAILS
        # ----------------------------------------------------

        if sheet_name == "OrderDetails":

            if r.get("OrderID") is not None:

                r["OrderID"] = int(
                    r["OrderID"]
                )

            if r.get("ProductID") is not None:

                r["ProductID"] = int(
                    r["ProductID"]
                )

            if r.get("Quantity") is not None:

                r["Quantity"] = int(
                    r["Quantity"]
                )

            if r.get("UnitPrice") is not None:

                r["UnitPrice"] = float(
                    r["UnitPrice"]
                )

            if r.get("Discount") is not None:

                r["Discount"] = float(
                    r["Discount"]
                )

        # ----------------------------------------------------
        # PRODUCTS
        # ----------------------------------------------------

        elif sheet_name == "Products":

            for c in [
                "ProductID",
                "SupplierID",
                "CategoryID",
                "UnitsInStock",
                "UnitsOnOrder",
                "ReorderLevel"
            ]:

                if r.get(c) is not None:

                    r[c] = int(
                        r[c]
                    )

            if r.get("UnitPrice") is not None:

                r["UnitPrice"] = float(
                    r["UnitPrice"]
                )

        # ----------------------------------------------------
        # ORDERS
        # ----------------------------------------------------

        elif sheet_name == "Orders":

            for c in [
                "OrderID",
                "EmployeeID",
                "ShipVia"
            ]:

                if r.get(c) is not None:

                    r[c] = int(
                        r[c]
                    )

            if r.get("Freight") is not None:

                r["Freight"] = float(
                    r["Freight"]
                )

        # ----------------------------------------------------
        # CUSTOMERS
        # ----------------------------------------------------

        elif sheet_name == "Customers":

            # Customer fields are strings.
            pass

        normalized_records.append(
            r
        )

    # ========================================================
    # CREATE COMPLETE SPARK DATAFRAME
    # ========================================================

    try:

        df = spark.createDataFrame(
            normalized_records
        )

    except Exception as e:

        raise RuntimeError(
            f"[{sheet_name}] Could not convert complete "
            f"API JSON to Spark DataFrame: "
            f"{type(e).__name__}: {str(e)}"
        )

    print("\n")
    print(
        f"✅ [{sheet_name}] COMPLETE API JSON "
        f"CONVERTED TO TABLE"
    )

    print(
        f"Rows before dedup: "
        f"{df.count():,}"
    )

    print(
        f"Columns: "
        f"{len(df.columns)}"
    )

    # ========================================================
    # REMOVE ODATA METADATA
    # ========================================================

    df = drop_odata_metadata(
        df
    )

    # ========================================================
    # REQUIRED BUSINESS COLUMNS
    # ========================================================

    required_columns = (
        cfg["keys"] +
        cfg["columns"]
    )

    for c in required_columns:

        if c not in df.columns:

            df = df.withColumn(
                c,
                F.lit(None).cast("string")
            )

    df = df.select(
        *required_columns
    )

    # ========================================================
    # NORMALIZE DATE COLUMNS
    # ========================================================

    df = normalize_date_columns(
        df,
        cfg
    )

    # ========================================================
    # DEDUPLICATE BUSINESS KEYS
    # ========================================================

    before_dedup = df.count()

    df = (
        df
        .dropDuplicates(
            cfg["keys"]
        )
    )

    after_dedup = df.count()

    print(
        f"[{sheet_name}] API rows before dedup: "
        f"{before_dedup:,}"
    )

    print(
        f"[{sheet_name}] API rows after dedup: "
        f"{after_dedup:,}"
    )

    print(
        f"✅ [{sheet_name}] COMPLETE API TABLE READY"
    )

    return df


# ============================================================
# READ EXCEL BRONZE
# ============================================================

def read_excel_data(sheet_name, cfg):

    excel_path = (
        f"{bronze_base_path.rstrip('/')}"
        f"/bronze_excel_{sheet_name}"
    )

    print("\n")
    print(
        f"[{sheet_name}] Excel Bronze path:"
    )

    print(
        excel_path
    )

    try:

        df = (
            spark.read
            .format("delta")
            .load(excel_path)
        )

    except Exception as e:

        print(
            f"[{sheet_name}] Excel Bronze Delta read failed."
        )

        print(
            str(e)[:300]
        )

        print(
            f"[{sheet_name}] Falling back to source Excel."
        )

        pdf = pd.read_excel(
            FS_SOURCE_FILE,
            sheet_name=sheet_name,
            engine="openpyxl"
        )

        required_columns = (
            cfg["keys"] +
            cfg["columns"]
        )

        for c in required_columns:

            if c not in pdf.columns:

                pdf[c] = None

        pdf = pdf[
            required_columns
        ]

        pdf = (
            pdf
            .astype(object)
            .where(
                pd.notnull(pdf),
                None
            )
        )

        df = spark.createDataFrame(
            pdf
        )

    # ========================================================
    # REQUIRED COLUMNS
    # ========================================================

    required_columns = (
        cfg["keys"] +
        cfg["columns"]
    )

    for c in required_columns:

        if c not in df.columns:

            df = df.withColumn(
                c,
                F.lit(None).cast("string")
            )

    df = df.select(
        *required_columns
    )

    # ========================================================
    # NORMALIZE DATES
    # ========================================================

    df = normalize_date_columns(
        df,
        cfg
    )

    # ========================================================
    # DEDUPLICATE
    # ========================================================

    df = (
        df
        .dropDuplicates(
            cfg["keys"]
        )
    )

    # ========================================================
    # SOURCE
    # ========================================================

    df = df.withColumn(
        "source_system",
        F.lit("Excel")
    )

    print(
        f"[{sheet_name}] Excel Bronze rows: "
        f"{df.count():,}"
    )

    return df


# ============================================================
# ALIGN EXCEL TYPES TO API TYPES
# ============================================================

def align_excel_to_api_schema(
    excel_df,
    api_df,
    cfg
):

    if excel_df is None:
        return None

    for field in api_df.schema.fields:

        c = field.name

        if c in excel_df.columns:

            excel_df = excel_df.withColumn(
                c,
                F.col(c).cast(
                    field.dataType
                )
            )

    columns_to_keep = (
        cfg["keys"] +
        cfg["columns"] +
        ["source_system"]
    )

    columns_to_keep = [
        c
        for c in columns_to_keep
        if c in excel_df.columns
    ]

    return excel_df.select(
        *columns_to_keep
    )


# ============================================================
# SHA2 HASH
# ============================================================

def compute_row_hash(
    df,
    cfg
):

    business_columns = (
        cfg["keys"] +
        cfg["columns"]
    )

    hash_columns = [
        c
        for c in business_columns
        if c in df.columns
    ]

    hash_expression = F.concat_ws(
        "||",
        *[
            F.coalesce(
                F.col(c).cast("string"),
                F.lit("<NULL>")
            )
            for c in hash_columns
        ]
    )

    return df.withColumn(
        "row_hash",
        F.sha2(
            hash_expression,
            256
        )
    )


# ============================================================
# PREPARE API BASELINE
# ============================================================

def prepare_api_baseline(
    api_df,
    cfg
):

    api_df = (
        api_df

        .withColumn(
            "source_system",
            F.lit("API")
        )

        .withColumn(
            "effective_start",
            F.lit(
                "2026-01-01 00:00:00"
            ).cast("timestamp")
        )

        .withColumn(
            "effective_end",
            F.lit(None).cast("timestamp")
        )

        .withColumn(
            "is_current",
            F.lit(True)
        )

        .withColumn(
            "created_at",
            F.lit(
                "2026-01-01 00:00:00"
            ).cast("timestamp")
        )

        .withColumn(
            "modified_at",
            F.lit(
                "2026-01-01 00:00:00"
            ).cast("timestamp")
        )

        .withColumn(
            "operation",
            F.lit("I")
        )
    )

    return compute_row_hash(
        api_df,
        cfg
    )


# ============================================================
# PREPARE EXCEL
# ============================================================

def prepare_excel(
    excel_df,
    cfg,
    run_ts_iso
):

    excel_df = (
        excel_df

        .withColumn(
            "effective_start",
            F.lit(
                run_ts_iso
            ).cast("timestamp")
        )

        .withColumn(
            "effective_end",
            F.lit(None).cast("timestamp")
        )

        .withColumn(
            "is_current",
            F.lit(True)
        )

        .withColumn(
            "created_at",
            F.lit(
                run_ts_iso
            ).cast("timestamp")
        )

        .withColumn(
            "modified_at",
            F.lit(
                run_ts_iso
            ).cast("timestamp")
        )

        .withColumn(
            "operation",
            F.lit("U")
        )
    )

    return compute_row_hash(
        excel_df,
        cfg
    )


# ============================================================
# SCD2 MERGE
#
# API = BASELINE
#
# Same PK + same hash:
#     API remains current
#
# Same PK + different hash:
#     API becomes historical
#     Excel becomes current
#
# Excel PK not in API:
#     Excel becomes current
#
# API-only:
#     API remains current
# ============================================================

def scd2_merge_api_excel(
    target_table_name,
    api_df,
    excel_df,
    cfg,
    run_ts_iso
):

    pk_cols = cfg["keys"]

    print("\n")
    print("=" * 70)
    print(
        f"SCD2 MERGE → {target_table_name}"
    )
    print("=" * 70)

    if api_df is None:

        raise Exception(
            "API dataframe is missing."
        )

    if excel_df is None:

        raise Exception(
            "Excel dataframe is missing."
        )

    # ========================================================
    # ALIGN EXCEL TYPES
    # ========================================================

    excel_df = align_excel_to_api_schema(
        excel_df,
        api_df,
        cfg
    )

    # ========================================================
    # PREPARE API
    # ========================================================

    api_prepared = prepare_api_baseline(
        api_df,
        cfg
    )

    # ========================================================
    # PREPARE EXCEL
    # ========================================================

    excel_prepared = prepare_excel(
        excel_df,
        cfg,
        run_ts_iso
    )

    api_count = api_prepared.count()
    excel_count = excel_prepared.count()

    print(
        f"API rows:   {api_count:,}"
    )

    print(
        f"Excel rows: {excel_count:,}"
    )

    # ========================================================
    # MATCH EXCEL TO API
    # ========================================================

    matched = (
        excel_prepared.alias("e")
        .join(
            api_prepared.alias("a"),
            on=pk_cols,
            how="inner"
        )
    )

    # ========================================================
    # UNCHANGED
    # ========================================================

    unchanged_count = (
        matched
        .filter(
            F.col("e.row_hash") ==
            F.col("a.row_hash")
        )
        .select(
            *[
                F.col(
                    f"e.{c}"
                ).alias(c)
                for c in pk_cols
            ]
        )
        .distinct()
        .count()
    )

    # ========================================================
    # CHANGED
    # ========================================================

    changed_excel = (
        matched
        .filter(
            F.col("e.row_hash") !=
            F.col("a.row_hash")
        )
        .select(
            "e.*"
        )
        .dropDuplicates(
            pk_cols
        )
    )

    changed_keys = (
        changed_excel
        .select(
            *pk_cols
        )
        .distinct()
    )

    changed_count = (
        changed_keys.count()
    )

    # ========================================================
    # NEW
    # ========================================================

    new_excel = (
        excel_prepared.alias("e")
        .join(
            api_prepared.alias("a"),
            on=pk_cols,
            how="left_anti"
        )
        .dropDuplicates(
            pk_cols
        )
    )

    new_count = new_excel.count()

    print(
        f"Unchanged: {unchanged_count:,}"
    )

    print(
        f"Changed:   {changed_count:,}"
    )

    print(
        f"New:       {new_count:,}"
    )

    # ========================================================
    # API UNCHANGED
    # ========================================================

    api_unchanged = (
        api_prepared.alias("a")
        .join(
            changed_keys.alias("c"),
            on=pk_cols,
            how="left_anti"
        )
    )

    # ========================================================
    # API HISTORICAL
    # ========================================================

    if changed_count > 0:

        api_historical = (
            api_prepared.alias("a")
            .join(
                changed_keys.alias("c"),
                on=pk_cols,
                how="left_semi"
            )
            .withColumn(
                "is_current",
                F.lit(False)
            )
            .withColumn(
                "effective_end",
                F.lit(
                    run_ts_iso
                ).cast("timestamp")
            )
            .withColumn(
                "modified_at",
                F.lit(
                    run_ts_iso
                ).cast("timestamp")
            )
        )

    else:

        api_historical = (
            api_prepared
            .limit(0)
        )

    # ========================================================
    # EXCEL CHANGED CURRENT
    # ========================================================

    excel_changed_current = (
        changed_excel

        .withColumn(
            "source_system",
            F.lit("Excel")
        )

        .withColumn(
            "is_current",
            F.lit(True)
        )

        .withColumn(
            "effective_end",
            F.lit(None).cast("timestamp")
        )

        .withColumn(
            "created_at",
            F.lit(
                run_ts_iso
            ).cast("timestamp")
        )

        .withColumn(
            "modified_at",
            F.lit(
                run_ts_iso
            ).cast("timestamp")
        )

        .withColumn(
            "operation",
            F.lit("U")
        )
    )

    # ========================================================
    # EXCEL NEW CURRENT
    # ========================================================

    excel_new_current = (
        new_excel

        .withColumn(
            "source_system",
            F.lit("Excel")
        )

        .withColumn(
            "is_current",
            F.lit(True)
        )

        .withColumn(
            "effective_end",
            F.lit(None).cast("timestamp")
        )

        .withColumn(
            "created_at",
            F.lit(
                run_ts_iso
            ).cast("timestamp")
        )

        .withColumn(
            "modified_at",
            F.lit(
                run_ts_iso
            ).cast("timestamp")
        )

        .withColumn(
            "operation",
            F.lit("I")
        )
    )

    # ========================================================
    # FINAL COMPLETE SCD2 TABLE
    # ========================================================

    final_df = (
        api_unchanged
        .unionByName(
            api_historical
        )
        .unionByName(
            excel_changed_current
        )
        .unionByName(
            excel_new_current
        )
    )

    final_df = (
        final_df
        .dropDuplicates()
    )

    # ========================================================
    # WRITE SILVER
    # ========================================================

    (
        final_df
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            target_table_name
        )
    )

    # ========================================================
    # FINAL COUNTS
    # ========================================================

    total_rows = final_df.count()

    current_rows = (
        final_df
        .filter(
            F.col("is_current") == True
        )
        .count()
    )

    historical_rows = (
        final_df
        .filter(
            F.col("is_current") == False
        )
        .count()
    )

    print("\n")
    print(
        f"FINAL {target_table_name}"
    )

    print(
        f"API:          {api_count:,}"
    )

    print(
        f"Excel:        {excel_count:,}"
    )

    print(
        f"Unchanged:    {unchanged_count:,}"
    )

    print(
        f"Changed:      {changed_count:,}"
    )

    print(
        f"New:          {new_count:,}"
    )

    print(
        f"Total:        {total_rows:,}"
    )

    print(
        f"Current:      {current_rows:,}"
    )

    print(
        f"Historical:   {historical_rows:,}"
    )

    return {
        "status": "SUCCESS",
        "api_rows": api_count,
        "excel_rows": excel_count,
        "unchanged_rows": unchanged_count,
        "changed_rows": changed_count,
        "new_rows": new_count,
        "total_rows": total_rows,
        "current_rows": current_rows,
        "historical_rows": historical_rows
    }


print(
    "✅ Complete Bronze JSON + API/Excel SHA2 SCD2 engine loaded."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 5: API + EXCEL SHA2 SCD TYPE 2 MERGE
# ============================================================

print("\n")
print("=" * 90)
print("STARTING API + EXCEL SHA2 SCD TYPE 2 MERGE")
print("=" * 90)


# ------------------------------------------------------------
# Reset execution log
# ------------------------------------------------------------

merge_log = []


# ------------------------------------------------------------
# Read Excel workbook
# ------------------------------------------------------------

try:

    import uuid
    local_excel_path = f"/tmp/_excel_merge_{uuid.uuid4().hex}.xlsx"
    mssparkutils.fs.cp(FS_SOURCE_FILE, f"file://{local_excel_path}", True)

    excel_book = pd.ExcelFile(
        local_excel_path,
        engine="openpyxl"
    )

    excel_sheets = excel_book.sheet_names

    print(
        f"✅ Excel workbook loaded"
    )

    print(
        f"Sheets: {excel_sheets}"
    )

except Exception as e:

    raise Exception(
        f"Could not open Excel workbook: {e}"
    )


# ------------------------------------------------------------
# Verify FS_CONFIG
# ------------------------------------------------------------

print("\n")
print("=" * 90)
print("CONFIGURATION")
print("=" * 90)

for cfg in FS_CONFIG:

    print(
        f"✅ {cfg['sheet']}"
        f" → {cfg['target_table']}"
        f" | PK={cfg['keys']}"
    )


# ------------------------------------------------------------
# Process all four entities
# ------------------------------------------------------------

for cfg in FS_CONFIG:

    sheet = cfg["sheet"]

    target_table = cfg["target_table"]

    pk_cols = cfg["keys"]


    print("\n")
    print("=" * 85)
    print(
        f"PROCESSING: {sheet} → {target_table}"
    )
    print(
        f"PK: {pk_cols}"
    )
    print("=" * 85)


    # --------------------------------------------------------
    # Check Excel sheet
    # --------------------------------------------------------

    if sheet not in excel_sheets:

        error_msg = (
            f"Excel sheet '{sheet}' not found."
        )

        print(
            f"❌ {error_msg}"
        )

        merge_log.append(
            (
                target_table,
                sheet,
                0,
                0,
                "FAILED",
                error_msg,
                run_ts
            )
        )

        continue


    try:

        # ====================================================
        # 1. LOAD COMPLETE API BASELINE
        # ====================================================

        api_df = read_bronze_api(
            sheet,
            cfg
        )


        if api_df is None:

            raise Exception(
                f"API Bronze could not be loaded "
                f"for {sheet}"
            )


        api_count = api_df.count()


        print(
            f"✅ API baseline: "
            f"{api_count:,} rows"
        )


        # ====================================================
        # 2. LOAD EXCEL INCOMING DATA
        # ====================================================

        excel_df = read_excel_data(
            sheet,
            cfg
        )


        if excel_df is None:

            raise Exception(
                f"Excel data could not be loaded "
                f"for {sheet}"
            )


        excel_count = excel_df.count()


        print(
            f"✅ Excel incoming: "
            f"{excel_count:,} rows"
        )


        # ====================================================
        # 3. CALL SCD2 ENGINE POSITIONALLY
        #
        # This intentionally avoids keyword arguments.
        #
        # Expected function signature:
        #
        # scd2_merge_api_excel(
        #     target_table_name,
        #     api_df,
        #     excel_df,
        #     pk_cols,
        #     run_ts_iso
        # )
        # ====================================================

        result = scd2_merge_api_excel(
            target_table,
            api_df,
            excel_df,
            cfg,
            run_ts
        )


        # ====================================================
        # 4. VERIFY TABLE WAS CREATED
        # ====================================================

        if not spark.catalog.tableExists(
            target_table
        ):

            raise Exception(
                f"SCD2 merge returned without creating "
                f"'{target_table}'."
            )


        # ====================================================
        # 5. READ FINAL SCD2 TABLE
        # ====================================================

        final_df = spark.table(
            target_table
        )


        total_rows = final_df.count()


        current_rows = (
            final_df
            .filter(
                F.col("is_current") == True
            )
            .count()
        )


        historical_rows = (
            final_df
            .filter(
                F.col("is_current") == False
            )
            .count()
        )


        # ====================================================
        # 6. SOURCE COUNTS
        # ====================================================

        api_final_rows = 0
        excel_final_rows = 0

        if "source_system" in final_df.columns:

            api_final_rows = (
                final_df
                .filter(
                    F.col("source_system") == "API"
                )
                .count()
            )

            excel_final_rows = (
                final_df
                .filter(
                    F.col("source_system") == "Excel"
                )
                .count()
            )


        # ====================================================
        # 7. PRINT FINAL RESULT
        # ====================================================

        print("\n")
        print(
            f"✅ {target_table} SUCCESS"
        )

        print(
            f"   API baseline:      {api_count:,}"
        )

        print(
            f"   Excel incoming:    {excel_count:,}"
        )

        print(
            f"   Unchanged:         "
            f"{result.get('unchanged_rows', 0):,}"
        )

        print(
            f"   Changed:           "
            f"{result.get('changed_rows', 0):,}"
        )

        print(
            f"   New:               "
            f"{result.get('new_rows', 0):,}"
        )

        print(
            f"   Total:             {total_rows:,}"
        )

        print(
            f"   Current:           {current_rows:,}"
        )

        print(
            f"   Historical:        {historical_rows:,}"
        )

        print(
            f"   API source:        {api_final_rows:,}"
        )

        print(
            f"   Excel source:      {excel_final_rows:,}"
        )


        # ====================================================
        # 8. EXECUTION LOG
        # ====================================================

        merge_log.append(
            (
                target_table,
                sheet,
                int(excel_count),
                int(total_rows),
                "SUCCESS",
                None,
                run_ts
            )
        )


    except Exception as e:

        error_msg = (
            f"{type(e).__name__}: "
            f"{str(e)[:1000]}"
        )


        print("\n")
        print(
            f"❌ {target_table} FAILED"
        )

        print(
            error_msg
        )


        merge_log.append(
            (
                target_table,
                sheet,
                0,
                0,
                "FAILED",
                error_msg,
                run_ts
            )
        )


# ============================================================
# FINAL SUMMARY
# ============================================================

success_count = sum(
    1
    for r in merge_log
    if r[4] == "SUCCESS"
)

failed_count = sum(
    1
    for r in merge_log
    if r[4] == "FAILED"
)


print("\n")
print("=" * 90)
print("FINAL SCD2 MERGE SUMMARY")
print("=" * 90)


for r in merge_log:

    print(
        f"{r[0]:<30} "
        f"{r[4]:<8} "
        f"Excel={r[2]:>5} "
        f"Total={r[3]:>5}"
    )


print("\n")
print(
    f"SUCCESS : {success_count}"
)

print(
    f"FAILED  : {failed_count}"
)

print(
    f"TOTAL   : {len(merge_log)}"
)

print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 6: Write SCD2 Merge Execution Logs
# ============================================================

print("\n")
print("=" * 70)
print("WRITING SCD2 MERGE LOGS")
print("=" * 70)

if merge_log:

    # --------------------------------------------------------
    # IMPORTANT:
    # Current merge_log rows contain 13 fields:
    #
    # 0  target_table
    # 1  sheet
    # 2  excel_rows
    # 3  net_change
    # 4  status
    # 5  error
    # 6  run_timestamp
    # 7  api_rows
    # 8  unchanged_rows
    # 9  changed_rows
    # 10 new_rows
    # 11 current_rows
    # 12 historical_rows
    #
    # Build the log dataframe explicitly from those 13 fields.
    # --------------------------------------------------------

    log_rows = []

    for log in merge_log:

        # Protect against old/short log records
        padded = list(log) + [None] * (
            13 - len(log)
        )

        log_rows.append(
            (
                str(padded[0])
                if padded[0] is not None
                else None,

                str(padded[1])
                if padded[1] is not None
                else None,

                int(padded[2])
                if padded[2] is not None
                else 0,

                int(padded[3])
                if padded[3] is not None
                else 0,

                str(padded[4])
                if padded[4] is not None
                else "UNKNOWN",

                str(padded[5])
                if padded[5] is not None
                else None,

                str(padded[6])
                if padded[6] is not None
                else None,

                int(padded[7])
                if padded[7] is not None
                else 0,

                int(padded[8])
                if padded[8] is not None
                else 0,

                int(padded[9])
                if padded[9] is not None
                else 0,

                int(padded[10])
                if padded[10] is not None
                else 0,

                int(padded[11])
                if padded[11] is not None
                else 0,

                int(padded[12])
                if padded[12] is not None
                else 0
            )
        )

    # --------------------------------------------------------
    # Explicit 13-column schema
    # --------------------------------------------------------

    log_schema = StructType([

        StructField(
            "target_table",
            StringType(),
            True
        ),

        StructField(
            "sheet",
            StringType(),
            True
        ),

        StructField(
            "excel_rows",
            LongType(),
            True
        ),

        StructField(
            "net_row_change",
            LongType(),
            True
        ),

        StructField(
            "status",
            StringType(),
            True
        ),

        StructField(
            "error",
            StringType(),
            True
        ),

        StructField(
            "run_timestamp",
            StringType(),
            True
        ),

        StructField(
            "api_rows",
            LongType(),
            True
        ),

        StructField(
            "unchanged_rows",
            LongType(),
            True
        ),

        StructField(
            "changed_rows",
            LongType(),
            True
        ),

        StructField(
            "new_rows",
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
        )
    ])

    # --------------------------------------------------------
    # Create DataFrame
    # --------------------------------------------------------

    log_df = spark.createDataFrame(
        log_rows,
        schema=log_schema
    )

    # --------------------------------------------------------
    # Write log
    # --------------------------------------------------------

    (
        log_df.write
        .format("delta")
        .mode("append")
        .option(
            "mergeSchema",
            "true"
        )
        .saveAsTable(
            "excel_merge_log"
        )
    )

    display(
        log_df
    )

    print(
        "✅ SCD2 merge log written successfully."
    )

else:

    print(
        "⚠️ No merge logs generated."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 7: Final Merge Summary + Safe Pipeline Exit
# ============================================================

print("\n")
print("=" * 70)
print("FINAL SCD2 MERGE SUMMARY")
print("=" * 70)


# ============================================================
# SAFE LOG NORMALIZATION
#
# Expected merge_log structure:
#
# 0 = target_table
# 1 = sheet
# 2 = excel_rows
# 3 = net_row_change
# 4 = status
# 5 = error
# 6 = run_timestamp
#
# If any record is shorter, pad it safely.
# ============================================================

safe_logs = []

for log in merge_log:

    values = list(log)

    while len(values) < 7:
        values.append(None)

    safe_logs.append(
        values[:7]
    )


# ============================================================
# COUNTS
# ============================================================

success_count = sum(
    1
    for log in safe_logs
    if log[4] == "SUCCESS"
)

failed_count = sum(
    1
    for log in safe_logs
    if log[4] not in (
        "SUCCESS",
        "SKIPPED"
    )
)

skipped_count = sum(
    1
    for log in safe_logs
    if log[4] == "SKIPPED"
)


# ============================================================
# TABLE RESULTS
# ============================================================

table_results = []

for log in safe_logs:

    table_results.append(
        {
            "target_table": (
                str(log[0])
                if log[0] is not None
                else None
            ),

            "sheet": (
                str(log[1])
                if log[1] is not None
                else None
            ),

            "excel_rows": (
                int(log[2])
                if log[2] is not None
                else 0
            ),

            "net_row_change": (
                int(log[3])
                if log[3] is not None
                else 0
            ),

            "status": (
                str(log[4])
                if log[4] is not None
                else "UNKNOWN"
            ),

            "error": (
                str(log[5])
                if log[5] is not None
                else None
            )
        }
    )


# ============================================================
# OVERALL STATUS
# ============================================================

overall_status = (
    "SUCCESS"
    if failed_count == 0
    else "FAILED"
)


# ============================================================
# FINAL SUMMARY
# ============================================================

summary = {

    "run_id": run_id,

    "run_timestamp": run_ts,

    "overall_status": overall_status,

    "success_count": success_count,

    "failed_count": failed_count,

    "skipped_count": skipped_count,

    "total_tables_processed": len(
        safe_logs
    ),

    "tables": table_results
}


# ============================================================
# PRINT SUMMARY
# ============================================================

print(
    json.dumps(
        summary,
        indent=2,
        default=str
    )
)


print("\n")
print("=" * 70)

print(
    f"SUCCESS : {success_count}"
)

print(
    f"FAILED  : {failed_count}"
)

print(
    f"SKIPPED : {skipped_count}"
)

print(
    f"TOTAL   : {len(safe_logs)}"
)

print("=" * 70)


# ============================================================
# PIPELINE EXIT
# ============================================================

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

# CELL ********************

# ============================================================
# CELL 8: SAFE DIAGNOSTIC - CHECK MERGED SILVER TABLES
# ============================================================

print("\n")
print("=" * 80)
print("DIAGNOSTIC - CHECKING SILVER TABLES")
print("=" * 80)


MERGED_SILVER_TABLES = [
    "Silver_Customers",
    "Silver_Products",
    "Silver_Orders",
    "Silver_Order_Details"
]


diagnostic_results = []


# ============================================================
# CHECK EACH TABLE
# ============================================================

for table_name in MERGED_SILVER_TABLES:

    print("\n")
    print("-" * 70)
    print(f"CHECKING: {table_name}")
    print("-" * 70)

    try:

        # ----------------------------------------------------
        # Check Spark catalog
        # ----------------------------------------------------

        exists = spark.catalog.tableExists(
            table_name
        )

        if not exists:

            print(
                f"⚠️ {table_name}: "
                f"Table does not exist in Spark catalog."
            )

            diagnostic_results.append({
                "table": table_name,
                "exists": False,
                "rows": 0,
                "current_rows": 0,
                "historical_rows": 0,
                "status": "MISSING"
            })

            continue

        # ----------------------------------------------------
        # Load table
        # ----------------------------------------------------

        df = spark.table(
            table_name
        )

        total_rows = df.count()

        print(
            f"✅ Table exists"
        )

        print(
            f"Total rows: {total_rows:,}"
        )

        # ----------------------------------------------------
        # Check SCD2 columns
        # ----------------------------------------------------

        required_scd2_columns = [
            "row_hash",
            "effective_start",
            "effective_end",
            "is_current",
            "source_system"
        ]

        missing_columns = [
            c
            for c in required_scd2_columns
            if c not in df.columns
        ]

        if missing_columns:

            print(
                "⚠️ Missing SCD2 columns:"
            )

            print(
                missing_columns
            )

            diagnostic_results.append({
                "table": table_name,
                "exists": True,
                "rows": total_rows,
                "current_rows": 0,
                "historical_rows": 0,
                "status": "INVALID_SCHEMA"
            })

            continue

        # ----------------------------------------------------
        # Current rows
        # ----------------------------------------------------

        current_rows = (
            df
            .filter(
                F.col("is_current") == True
            )
            .count()
        )

        # ----------------------------------------------------
        # Historical rows
        # ----------------------------------------------------

        historical_rows = (
            df
            .filter(
                F.col("is_current") == False
            )
            .count()
        )

        # ----------------------------------------------------
        # Source breakdown
        # ----------------------------------------------------

        print(
            f"Current rows:    {current_rows:,}"
        )

        print(
            f"Historical rows: {historical_rows:,}"
        )

        print(
            "\nSource breakdown:"
        )

        (
            df
            .groupBy("source_system")
            .count()
            .orderBy(
                F.desc("count")
            )
            .show(
                truncate=False
            )
        )

        # ----------------------------------------------------
        # Final diagnostic record
        # ----------------------------------------------------

        diagnostic_results.append({
            "table": table_name,
            "exists": True,
            "rows": total_rows,
            "current_rows": current_rows,
            "historical_rows": historical_rows,
            "status": "OK"
        })

    except Exception as e:

        print(
            f"❌ {table_name}: diagnostic failed"
        )

        print(
            f"{type(e).__name__}: "
            f"{str(e)[:500]}"
        )

        diagnostic_results.append({
            "table": table_name,
            "exists": False,
            "rows": 0,
            "current_rows": 0,
            "historical_rows": 0,
            "status": "ERROR"
        })


# ============================================================
# EXPECTED API BASELINE
# ============================================================

expected_api_counts = {
    "Silver_Customers": 91,
    "Silver_Products": 77,
    "Silver_Orders": 830,
    "Silver_Order_Details": 2155
}


# ============================================================
# SUMMARY
# ============================================================

print("\n")
print("=" * 80)
print("SILVER TABLE DIAGNOSTIC SUMMARY")
print("=" * 80)

for result in diagnostic_results:

    table_name = result["table"]

    expected = expected_api_counts.get(
        table_name,
        0
    )

    print("\n")

    print(
        f"{table_name}"
    )

    print(
        f"Status:           {result['status']}"
    )

    print(
        f"Exists:           {result['exists']}"
    )

    print(
        f"Total rows:       {result['rows']:,}"
    )

    print(
        f"Current rows:     {result['current_rows']:,}"
    )

    print(
        f"Historical rows:  {result['historical_rows']:,}"
    )

    print(
        f"Expected API:     {expected:,}"
    )


# ============================================================
# DATAFRAME SUMMARY
# ============================================================

diagnostic_df = spark.createDataFrame(
    diagnostic_results
)

display(
    diagnostic_df
)


print("\n")
print("=" * 80)
print("DIAGNOSTIC COMPLETE")
print("=" * 80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.catalog.listTables()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
