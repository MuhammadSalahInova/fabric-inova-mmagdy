-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "aa124c78-1e88-46a9-b10d-1d1b63d5c6fc",
-- META       "default_lakehouse_name": "Silver_LH",
-- META       "default_lakehouse_workspace_id": "b37d6e7d-3c40-4ab5-9c25-3ef4b055be87",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "aa124c78-1e88-46a9-b10d-1d1b63d5c6fc"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC for t in [t.name for t in spark.catalog.listTables() if t.name.lower().startswith("silver_")]:
-- MAGIC     spark.sql(f"DROP TABLE IF EXISTS {t}")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC print([t.name for t in spark.catalog.listTables() if t.name.lower().startswith("silver_")])

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- Type a Spark SQL query to get started.


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
