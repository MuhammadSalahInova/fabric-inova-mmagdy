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

# MARKDOWN ********************

# #### **** Parameter Cell

# CELL ********************

success_list = ""
failed_list = ""
run_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from datetime import datetime

# Parse the JSON-string arrays back into real Python lists
success_tables = json.loads(success_list) if success_list else []
failed_tables = json.loads(failed_list) if failed_list else []

rows = []
now = datetime.utcnow().isoformat()

for t in success_tables:
    rows.append({
        "pipeline_run_id": run_id,
        "table_name": t,
        "status": "SUCCESS",
        "run_timestamp": now
    })

for t in failed_tables:
    rows.append({
        "pipeline_run_id": run_id,
        "table_name": t,
        "status": "FAILED",
        "run_timestamp": now
    })

if rows:
    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("append").saveAsTable("bronze_copy_log")
    print(f"Logged {len(success_tables)} success, {len(failed_tables)} failed to bronze_copy_log")
else:
    print("No tables were processed — nothing to log. Check upstream variables.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(json.dumps({
    "success_count": len(success_tables),
    "failed_count": len(failed_tables)
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
