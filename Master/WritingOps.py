# Databricks notebook source
# MAGIC %run ./DatabaseOps

# COMMAND ----------

def write_to_adls_csv(df, path, mode="overwrite"):
    """
    Write DataFrame to ADLS in CSV format.

    Parameters:
    df   : DataFrame to be written
    path : Path in ADLS (e.g., '/mnt/your_mount_point/folder_name')
    mode : Write mode ('overwrite', 'append', 'ignore', 'errorifexists')
    """
    df.write.format("csv") \
            .option("header", True) \
            .mode(mode) \
            .save(path)
    print(f"Data successfully written to {path} in CSV format.")


# COMMAND ----------

def write_to_adls_json(df, path, mode="overwrite"):
    """
    Write DataFrame to ADLS in JSON format.

    Parameters:
    df   : DataFrame to be written
    path : Path in ADLS (e.g., '/mnt/your_mount_point/folder_name')
    mode : Write mode ('overwrite', 'append', 'ignore', 'errorifexists')
    """
    df.write.format("json") \
            .mode(mode) \
            .save(path)
    print(f"Data successfully written to {path} in JSON format.")


# COMMAND ----------

def write_to_adls_parquet(df, path, mode="overwrite"):
    """
    Write DataFrame to ADLS in Parquet format.

    Parameters:
    df   : DataFrame to be written
    path : Path in ADLS (e.g., '/mnt/your_mount_point/folder_name')
    mode : Write mode ('overwrite', 'append', 'ignore', 'errorifexists')
    """
    df.write.format("parquet") \
            .mode(mode) \
            .save(path)
    print(f"Data successfully written to {path} in Parquet format.")


# COMMAND ----------

def write_to_sql_server(df, table_name,mode='append'):
    jdbc_url, connection_properties = get_jdbc_connection()

    # Write the DataFrame to SQL Server in append mode
    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode=mode,
        properties=connection_properties
    )
    print(f"Data successfully appended to {table_name}")

