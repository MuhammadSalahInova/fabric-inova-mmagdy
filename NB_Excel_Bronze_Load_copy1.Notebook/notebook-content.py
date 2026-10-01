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

# ---- CELL 1: Parameters ------------------------------------------------------
excel_file_path = ""        # abfss path to the source .xlsx in OneLake
bronze_base_path = ""       # Base path for Bronze tables
run_id = ""                 # Unique run identifier

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- CELL 1b: Interactive testing fallback (safe for pipeline runs) --------
# This only fires when running manually (parameters are empty)
if not excel_file_path:
    print("excel_file_path was empty -- using interactive test value (this only happens outside the pipeline).")
    # The Excel file is in BronzeLH/Files
    excel_file_path = "abfss://b37d6e7d-3c40-4ab5-9c25-3ef4b055be87@onelake.dfs.fabric.microsoft.com/20795e0d-c2e4-44e1-854b-1208823b8a5c/Files/FakeStore_Orders_OrderDetails_Customers_Products_Linked.xlsx"
    
if not bronze_base_path:
    print("bronze_base_path was empty -- using interactive test value (this only happens outside the pipeline).")
    bronze_base_path = "abfss://b37d6e7d-3c40-4ab5-9c25-3ef4b055be87@onelake.dfs.fabric.microsoft.com/20795e0d-c2e4-44e1-854b-1208823b8a5c/Files/"
    
if not run_id:
    run_id = "manual-test-run"

print(f"excel_file_path: {excel_file_path}")
print(f"bronze_base_path: {bronze_base_path}")
print(f"run_id: {run_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- CELL 2: Imports ----------------------------------------------------------
import json
import re
import uuid
import pandas as pd
from datetime import datetime, timezone
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.functions import min as spark_min, max as spark_max, sum as spark_sum

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- CELL 3: Column name sanitizer -------------------------------------------
# Delta rejects spaces and characters like ,;{}()\n\t= in column names.
# This strips anything invalid BEFORE the DataFrame ever reaches Spark, so
# every downstream step (write, profiling, aggregates) uses clean names.
def sanitize_column_name(name):
    clean = re.sub(r'[^0-9a-zA-Z_]', '_', str(name))   # invalid char -> "_"
    clean = re.sub(r'_+', '_', clean).strip('_')          # collapse/trim "_"
    return clean if clean else "unnamed_column"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- CELL 4: Copy workbook locally and discover sheets -----------------------
# Validate that paths are not empty
if not excel_file_path:
    raise ValueError("excel_file_path is empty! Please provide a valid path to the Excel file.")

if not bronze_base_path:
    raise ValueError("bronze_base_path is empty! Please provide a valid base path for Bronze tables.")

print(f"Source Excel file: {excel_file_path}")
print(f"Bronze base path: {bronze_base_path}")

# Check if the file exists before trying to copy
try:
    file_exists = mssparkutils.fs.exists(excel_file_path)
    print(f"File exists: {file_exists}")
    
    if not file_exists:
        # Try to list the directory to see what's available
        try:
            parent_path = "/".join(excel_file_path.split("/")[:-1])
            print(f"\nListing directory: {parent_path}")
            files = mssparkutils.fs.ls(parent_path)
            print("Available files in this directory:")
            for f in files:
                if not f.isDir:
                    print(f"  📄 {f.name} (size: {f.size} bytes)")
                else:
                    print(f"  📁 {f.name}/")
        except Exception as ls_e:
            print(f"Could not list directory: {ls_e}")
        raise FileNotFoundError(f"File not found: {excel_file_path}")
    else:
        print("✅ File found!")
        # Get file size using available method
        try:
            # Try to get file info using ls on the specific file
            file_list = mssparkutils.fs.ls(excel_file_path)
            for f in file_list:
                if not f.isDir:
                    print(f"File size: {f.size} bytes")
                    break
        except Exception as size_e:
            print(f"Could not get file size: {size_e}")
        
except Exception as e:
    print(f"ERROR checking file: {e}")
    raise

# Ensure the /tmp directory exists
try:
    mssparkutils.fs.mkdirs("file:///tmp")
    print("✅ Created /tmp directory")
except Exception as e:
    print(f"⚠️ Could not create /tmp directory: {e}")

local_tmp_path = f"/tmp/excel_source_{uuid.uuid4().hex}.xlsx"
print(f"\nCopying to local: {local_tmp_path}")

try:
    # Copy the file
    mssparkutils.fs.cp(excel_file_path, f"file://{local_tmp_path}", True)
    print("✅ File copied successfully")
    
    # Verify the local file exists
    import os
    if os.path.exists(local_tmp_path):
        file_size = os.path.getsize(local_tmp_path)
        print(f"✅ Local file size: {file_size:,} bytes")
    else:
        raise Exception("Local file does not exist after copy")
        
except Exception as e:
    print(f"❌ ERROR copying file: {e}")
    raise

# Load the Excel file
try:
    workbook = pd.ExcelFile(local_tmp_path, engine="openpyxl")
    sheet_names = workbook.sheet_names  # fully dynamic, discovered at run time
    print(f"\n✅ Discovered {len(sheet_names)} sheet(s): {sheet_names}")
except Exception as e:
    print(f"❌ ERROR reading Excel file: {e}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

numeric_pandas_kinds = {"i", "u", "f"}
excel_source_counts = []
source_counts_json_list = []

for raw_sheet_name in sheet_names:
    # Standardize sheet name to Order_Details if coming in as OrderDetails
    sheet_name = "Order_Details" if raw_sheet_name in ["OrderDetails", "Order_Details"] else raw_sheet_name
    
    print(f"\nProcessing sheet: '{raw_sheet_name}' mapped to table: '{sheet_name}'")

    try:
        pdf = workbook.parse(raw_sheet_name)
        if len(pdf) == 0:
            excel_source_counts.append({
                "table": sheet_name, "count": 0, "columns": [], "dtypes": {}, "aggs": {}, "error": "Empty sheet"
            })
            continue

        pdf = pdf.where(pd.notnull(pdf), None)
        pdf.columns = [sanitize_column_name(c) for c in pdf.columns]

        sdf = spark.createDataFrame(pdf)
        row_count = sdf.count()
        sdf = sdf.withColumn("source_system", F.lit("Excel"))

        bronze_path = f"{bronze_base_path}/bronze_excel_{sheet_name}"
        sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(bronze_path)

        columns = sdf.columns
        dtypes = {f.name: f.dataType.simpleString() for f in sdf.schema.fields}

        aggregates = {}
        numeric_cols = [c for c in pdf.columns if pdf[c].dtype.kind in numeric_pandas_kinds]
        
        for c in numeric_cols:
            try:
                agg_row = sdf.agg(spark_sum(c).alias("sum"), spark_min(c).alias("min"), spark_max(c).alias("max")).first()
                aggregates[c] = {
                    "sum": float(agg_row["sum"]) if agg_row["sum"] is not None else None,
                    "min": float(agg_row["min"]) if agg_row["min"] is not None else None,
                    "max": float(agg_row["max"]) if agg_row["max"] is not None else None
                }
            except Exception as e:
                aggregates[c] = {"sum": None, "min": None, "max": None}

        source_info = {
            "table": sheet_name, "count": row_count, "columns": columns, "dtypes": dtypes, "aggs": aggregates
        }
        excel_source_counts.append(source_info)
        source_counts_json_list.append({"table": sheet_name, "count": row_count, "aggs": aggregates})

        print(f"  ✅ Successfully loaded {sheet_name} ({row_count:,} rows)")

    except Exception as e:
        print(f"  ❌ ERROR loading {sheet_name}: {e}")
        excel_source_counts.append({"table": sheet_name, "count": None, "error": str(e)[:200]})

try:
    mssparkutils.fs.rm(f"file://{local_tmp_path}", True)
except Exception:
    pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- CELL 6: Write audit log (SIMPLIFIED) -----------------------------------
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType

# Build audit rows
audit_rows = []
for item in excel_source_counts:
    row_count = item.get("count")
    if row_count is not None:
        try:
            row_count = int(row_count)
        except (ValueError, TypeError):
            row_count = None
    
    error_msg = item.get("error")
    if error_msg is not None:
        error_msg = str(error_msg)[:500]
    
    audit_rows.append({
        "run_id": str(run_id) if run_id else "unknown",
        "table_name": str(item.get("table", "unknown")),
        "row_count": row_count,
        "column_count": int(len(item.get("columns", []))) if item.get("columns") is not None else 0,
        "numeric_column_count": int(len(item.get("aggs", {}))) if item.get("aggs") is not None else 0,
        "loaded_at": datetime.now(timezone.utc).isoformat(),
        "error": error_msg
    })

if audit_rows:
    # Define schema explicitly
    audit_schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("row_count", LongType(), True),
        StructField("column_count", IntegerType(), True),
        StructField("numeric_column_count", IntegerType(), True),
        StructField("loaded_at", StringType(), True),
        StructField("error", StringType(), True)
    ])
    
    audit_df = spark.createDataFrame(audit_rows, schema=audit_schema)
    audit_log_path = f"{bronze_base_path}/bronze_excel_load_log"
    
    try:
        # Try to read existing data
        existing_df = spark.read.format("delta").load(audit_log_path)
        print(f"✅ Existing audit log found")
        
        # Union with new data (allow missing columns)
        combined_df = existing_df.unionByName(audit_df, allowMissingColumns=True)
        
        # Overwrite the table (this preserves all data)
        combined_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(audit_log_path)
        print(f"\n✅ Audit log updated at: {audit_log_path}")
        print(f"   Total rows: {combined_df.count()}")
        print(f"   New rows added: {len(audit_rows)}")
        
    except Exception as e:
        # Table doesn't exist or can't be read
        print(f"ℹ️ Creating new audit log (or error: {e})")
        
        try:
            # Try to create the table
            audit_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(audit_log_path)
            print(f"\n✅ Audit log created at: {audit_log_path}")
            print(f"   Rows written: {len(audit_rows)}")
        except Exception as create_e:
            print(f"❌ Could not create audit log: {create_e}")
            
            # Fallback: Save as JSON
            try:
                import tempfile
                audit_json = json.dumps(audit_rows, default=str, indent=2)
                audit_path = f"{bronze_base_path}/bronze_excel_load_log_{run_id}.json"
                
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    f.write(audit_json)
                    temp_path = f.name
                
                mssparkutils.fs.cp(f"file://{temp_path}", audit_path, True)
                print(f"   ✅ Audit data saved as JSON: {audit_path}")
                
                try:
                    mssparkutils.fs.rm(f"file://{temp_path}", True)
                except:
                    pass
                    
            except Exception as e2:
                print(f"   ❌ Could not save backup: {e2}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

successful = [item for item in excel_source_counts if item.get("count") is not None]
failed = [item for item in excel_source_counts if item.get("error")]

output_payload = {
    "run_id": run_id,
    "excel_tables": [item["table"] for item in successful],
    "excel_source_counts": source_counts_json_list,
    "total_tables": len(sheet_names),
    "successful_loads": len(successful),
    "failed_loads": len(failed)
}

print(json.dumps(output_payload, indent=2, default=str))
mssparkutils.notebook.exit(json.dumps(output_payload, default=str))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- CELL 8: Quick diagnostic to verify load --------------------------------
print("\n" + "=" * 60)
print("VERIFYING LOADED TABLES")
print("=" * 60)

for sheet_name in sheet_names:
    try:
        bronze_path = f"{bronze_base_path}/bronze_excel_{sheet_name}"
        df = spark.read.format("delta").load(bronze_path)
        count = df.count()
        has_source = "source_system" in df.columns
        
        print(f"✅ {sheet_name}: {count} rows, source_system column: {has_source}")
        
        if has_source:
            source_counts = df.groupBy("source_system").count().collect()
            for row in source_counts:
                print(f"    {row['source_system']}: {row['count']} rows")
                
        # Show sample columns
        print(f"    Columns: {df.columns[:5]}..." if len(df.columns) > 5 else f"    Columns: {df.columns}")
        
    except Exception as e:
        print(f"❌ {sheet_name}: Failed to verify - {str(e)[:100]}")

print("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
