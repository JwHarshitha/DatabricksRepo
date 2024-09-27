# Databricks notebook source
# MAGIC %run ./DatabaseOps

# COMMAND ----------

# MAGIC %md
# MAGIC Read CSV File Function

# COMMAND ----------

def read_csv_file(file_path):
    spark_df = spark.read.format("csv") \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("sep", ",") \
                    .option("quote", '"') \
                    .option("escape", "\\") \
                    .option("multiLine", "true") \
                    .option("nullValue", "null") \
                    .option("dateFormat", "yyyy-MM-dd") \
                    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss") \
                    .load(file_path)
    return spark_df

# COMMAND ----------

# MAGIC %md
# MAGIC Read JSON File Function

# COMMAND ----------

def read_json_file(file_path):
    spark_df = spark.read.format("json") \
                        .option("multiLine", "true") \
                        .option("primitivesAsString", "true") \
                        .option("allowUnquotedFieldNames", "true") \
                        .option("allowNumericLeadingZeros", "true") \
                        .option("dropFieldIfAllNull", "true") \
                        .load(file_path)
    return spark_df


# COMMAND ----------

# MAGIC %md
# MAGIC Read Parquet File Function

# COMMAND ----------

def read_parquet_file(file_path):
    spark_df  = spark.read.format("parquet") \
                        .option("mergeSchema", "true") \
                        .load(file_path)
    return spark_df
        


# COMMAND ----------

# MAGIC %md
# MAGIC Read ORC File Function

# COMMAND ----------

def read_orc_file(file_path):
    spark_df =  spark.read.format("orc") \
                        .option("mergeSchema", "true") \
                        .load(file_path)
    return spark_df


# COMMAND ----------

def read_from_sql_server(table_name):
    jdbc_url, connection_properties = get_jdbc_connection()

    # Read the entire table
    df = spark.read.jdbc(
        url=jdbc_url,
        table=table_name,
        properties=connection_properties
    )
    print(f"Data successfully read from table: {table_name}")

    return df

