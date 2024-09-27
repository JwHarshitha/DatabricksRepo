# Databricks notebook source
file_type = dbutils.widgets.get("file_type")
target_type = dbutils.widgets.get("target_type")
run_id = dbutils.widgets.get("run_id")

if target_type.lower() == "sqlserver":
    output_table_name = dbutils.widgets.get("output_table_name")
elif file_type.lower() == "sqlserver":
    input_table_name = dbutils.widgets.get("input_table_name")

# COMMAND ----------

# MAGIC %run ./ReadingOps

# COMMAND ----------

# MAGIC %run ./WritingOps

# COMMAND ----------

df = read_parquet_file(f"/mnt/mlworks-staging-container/{run_id}/{file_type}/")

# COMMAND ----------

if target_type.lower() == "csv":
    write_to_adls_csv(df, f'/mnt/mlworks-archival-container/Processed/CSV/{run_id}/', mode="overwrite")
elif target_type.lower() == "json":
    write_to_adls_json(df, f'/mnt/mlworks-archival-container/Processed/JSON/{run_id}/', mode="overwrite")
elif target_type.lower() == "parquet":
    write_to_adls_parquet(df, f'/mnt/mlworks-archival-container/Processed/PARQUET/{run_id}/', mode="overwrite")
elif target_type.lower() == "sqlserver":
    write_to_sql_server(df, output_table_name,mode='append')

display(df)
