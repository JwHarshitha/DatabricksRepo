# Databricks notebook source
sql_server_name = 'mlworks-sql.database.windows.net'
username = 'mlworks-admin'
password = 'Tredence12345'
database_name = 'mlworks_dev'
port = 1433

# COMMAND ----------

def get_jdbc_connection():
    jdbc_url = f"jdbc:sqlserver://{sql_server_name}:{port};database={database_name}"
    connection_properties = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    return jdbc_url, connection_properties

