# Databricks notebook source
emp = [('SQL_DATABASE_HOST','myplus-dbx.database.windows.net'), \
    ('SQL_DATABASE_PORT','1433'), \
    ('SQL_DATABASE_NAME','myplus'), \
    ('SQL_DATABASE_USER','sbhandari'), \
    ('SQL_DATABASE_PASSWORD','vDUwaSAGnZGb5rqHIrrhdQaoLPL7fyxR'), \
    ('MONGO_DATABASE_HOST','myplus-dbx.database.windows.net'), \
    ('MONGO_DATABASE_NAME','myplus'), \
    ('MONGO_DATABASE_USER','sbhandari'), \
    ('MONGO_DATABASE_PASSWORD','vDUwaSAGnZGb5rqHIrrhdQaoLPL7fyxR') \
  ]
empColumns = ["key","value"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)

# COMMAND ----------

( empDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .json('settings.json')
  )

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

#DEPT DataFrame
deptData = [("Finance",10), ("Marketing",20),
    ("Sales",30),("IT",40)
  ]
deptColumns = ["dept_name","dept_id"]
deptDF=spark.createDataFrame(deptData,deptColumns)  
deptDF.show()

# COMMAND ----------

#Address DataFrame
addData=[(1,"1523 Main St","SFO","CA"),
    (2,"3453 Orange St","SFO","NY"),
    (3,"34 Warner St","Jersey","NJ"),
    (4,"221 Cavalier St","Newark","DE"),
    (5,"789 Walnut St","Sandiago","CA")
  ]
addColumns = ["emp_id","addline1","city","state"]
addDF = spark.createDataFrame(addData,addColumns)
addDF.show()

# COMMAND ----------

#Join two DataFrames
empDF.join(addDF,empDF["emp_id"] == addDF["emp_id"]).show()

# COMMAND ----------

#Drop duplicate column
empDF.join(addDF,["emp_id"]).show()
