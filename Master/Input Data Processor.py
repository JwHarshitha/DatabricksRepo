# Databricks notebook source
# MAGIC %run ./ReadingOps

# COMMAND ----------

# MAGIC %run ./WritingOps

# COMMAND ----------

file_type = dbutils.widgets.get("file_type")
target_type = dbutils.widgets.get("target_type")
run_id = dbutils.widgets.get("run_id")
if target_type.lower() == "sqlserver":
    output_table_name = dbutils.widgets.get("output_table_name")
elif file_type.lower() == "sqlserver":
    input_table_name = dbutils.widgets.get("input_table_name")

# COMMAND ----------

if file_type.lower() == "csv":
    df = read_csv_file("/mnt/mlworks-input-container/FlatFiles/CSV/")
elif file_type.lower() == "json":
    df = read_json_file("/mnt/mlworks-input-container/FlatFiles/JSON/")
elif file_type.lower() == "parquet":
    df = read_parquet_file("/mnt/mlworks-input-container/FlatFiles/PARQUET/")
elif file_type.lower() == "sqlserver":
    df = read_from_sql_server(input_table_name)

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
# from pyspark.sql.functions import col, concat, lit, year, month, datediff, current_date, when, length
# 1. Full name concatenation
# Create a full name by concatenating the first and last names
df = df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))

# 2. Age calculation from date_of_birth
# Calculate the age of each employee based on the current date
df = df.withColumn("age", (datediff(current_date(), col("date_of_birth")) / 365.25).cast("integer"))

# 3. Gender normalization
# Normalize gender values (assuming 'M' and 'F' or similar abbreviations)
df = df.withColumn("gender", when(col("gender") == "M", "Male")
                              .when(col("gender") == "F", "Female")
                              .otherwise("Other"))

# 4. Hire date formatting and seniority calculation
# Calculate years of service/seniority
df = df.withColumn("years_of_service", (datediff(current_date(), col("hire_date")) / 365.25).cast("integer"))

# 5. Department mapping (joining department names based on department_id)
# Assuming you have another DataFrame `departments_df` with department info
departments_df = spark.createDataFrame([
    (1, "Sales"), 
    (2, "Marketing"), 
    (3, "IT"), 
    (4, "HR")
], ["department_id", "department_name"])

df = df.join(departments_df, on="department_id", how="left")

# 6. Email domain extraction
# Extract the domain part of the employee's email
# df = df.withColumn("email_domain", col("email").substr(col("email").instr("@") + 1, 100))

# 7. Masking phone number for privacy
# Mask the phone number to show only the last 4 digits
df = df.withColumn("masked_phone", concat(lit("***-***-"), col("phone").substr(-4, 4)))

# 8. Address splitting (assuming the format 'Street, City, State, ZIP')
# Extract street, city, state, and ZIP from the address field
split_col = split(col("address"), ",")
df = df.withColumn("street", split_col.getItem(0)) \
       .withColumn("city", split_col.getItem(1)) \
       .withColumn("state", split_col.getItem(2)) \
       .withColumn("zip_code", split_col.getItem(3))

# 9. Position Title Cleaning (e.g., trim extra spaces or fix cases)
df = df.withColumn("position_cleaned", trim(initcap(col("position"))))

# 10. Flagging employees with senior positions
# Assuming that employees with titles containing "Manager" or "Lead" are in senior positions
df = df.withColumn("is_senior", when(col("position").rlike("(Manager|Lead)"), "Yes").otherwise("No"))

# 11. Add a column for birth month (for analytics purposes like birthday campaigns)
df = df.withColumn("birth_month", month(col("date_of_birth")))

# 12. Identify employees with incomplete profiles
# Checking if any key information is missing (e.g., email or phone)
df = df.withColumn("incomplete_profile", when(col("email").isNull() | (length(col("phone")) < 10), "Yes").otherwise("No"))

# 13. Add a risk category based on seniority and age (just an example)
df = df.withColumn("risk_category", when((col("years_of_service") > 10) & (col("age") > 50), "High Risk")
                                  .when((col("years_of_service") > 5) & (col("age") > 40), "Medium Risk")
                                  .otherwise("Low Risk"))

# 14. Extract year of hire from the hire_date for tenure analysis
df = df.withColumn("hire_year", year(col("hire_date")))

# Show the resulting DataFrame
display(df)


# COMMAND ----------

# Write to Parquet
write_to_adls_parquet(df, f"/mnt/mlworks-staging-container/{run_id}/{file_type}/", mode="overwrite")
# we are saving the temporary output to a staging location
