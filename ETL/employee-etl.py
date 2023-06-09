# Databricks notebook source
# read the csv files
df_employee_Personal = spark.read.load("dbfs:/user/hive/warehouse/employee_personal_table",
                     format="delta", sep=";", inferSchema="true", header="true")
# display(df_employee_Personal)

# COMMAND ----------

# def getSqlData(): 
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

database_host = "myplus-dbx.database.windows.net"
database_port = "1433" # update if you use a non-default port
database_name = "myplus"
table = "Employee_Personal"
user = "sbhandari"
password = "vDUwaSAGnZGb5rqHIrrhdQaoLPL7fyxR"

url = f"jdbc:sqlserver://{database_host}:{database_port};database={database_name}"

employee_Personal_tbl = (spark.read
.format("jdbc")
.option("driver", driver)
.option("url", url)
.option("dbtable", table)
.option("user", user)
.option("password", password)
.load()
)
#return employee_Personal_tbl
display(employee_Personal_tbl.select('Worker_ID')) 

# COMMAND ----------


