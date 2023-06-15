# Databricks notebook source
import pandas as pd
import Library.sql_data as sqldata
from pyspark.sql.functions import col
from pyspark.sql.functions import col,when,count

# COMMAND ----------

personal = sqldata.getSqlData(spark, "Employee_Personal")
job = sqldata.getSqlData(spark, "Employment_Job")

# COMMAND ----------

empCols = [col("Worker_Id").alias("workdayId"), col("Party_Id").alias("partyId"), col("Guid").alias("guid"),col("Legal_First_Name").alias("firstName"), col("Legal_Last_Name").alias("lastName"), col("Preferred_First_Name").alias("preferredFirstName"), col("Preferred_Last_Name").alias("preferredLastName"),col("email_address_workemail").alias("email") ]
employee = personal[empCols].where(col("partyId").isNotNull()).toPandas()
# display(employee.count())

# COMMAND ----------

jobCols = [col("Worker_Id").alias("workdayId"), col("Management_Level_Descr").alias("managementLevel"), col("Department_ID").alias("costCenter"), col  ("Sub_Line_Of_Service_1_Name").alias("constCenterLevel4"), col("Business_Unit_Descr_Short").alias("segment"), col("Employee_Status_Descr").alias("employeeStatus"), col("Employee_Status").alias("employeeStatusCode"), col("Business_Title").alias("businessTitle"), col("Employee_Type_Descr").alias("employeeType"), col("Line_of_Service_Descr").alias("globalLOS"), col("Hire_Date").alias("hireDate"), col("Job_Profile_Descr").alias("jobProfile"), col("Job_Profile_Level_Descr").alias("jobLevel"), col("Job_Family_Descr").alias("jobFamily"), col("Location_ID").alias("location"), col("WD_Position_Time_Type_ID").alias("positionTimeTypeId"), col("WD_Coach_ID").alias("coachId"), col("Primary_HR_Manager_Supervisory").alias("hrManagerId")]
activeEmployeeStatus = ['A', 'L', 'P', 'S']
employeeJobs = job[jobCols].filter(col("employeeStatusCode").isin(activeEmployeeStatus)).toPandas()
# employeeJobs.head()

# COMMAND ----------

dfEmployeeWithJobs=employee.merge(employeeJobs["workdayId"],on='workdayId',how='inner')
# dfEmployeeWithJobs = employee.join(employeeJobs["workdayId"], ["workdayId"], "inner")
# display(dfEmployeeWithJobs.count())
# dfEmployeeWithJobs.head()

# COMMAND ----------

df= dfEmployeeWithJobs.merge(employeeJobs,how='left',on='workdayId').sort_values(by=['hireDate']).drop_duplicates(subset=['partyId'], keep='last')
# df.head()

# COMMAND ----------

df['isRelationshipLeader'] = df['coachId'].isin(df['workdayId'])
df['isTalentConsultant'] = df['hrManagerId'].isin(df['workdayId'])

# COMMAND ----------

connectionString='mongodb+srv://sbhandari:vDUwaSAGnZGb5rqHIrrhdQaoLPL7fyxR@cluster0.ov0d2xd.mongodb.net'
database="myplus"
collection="employee"
pipeline="[ { '$project': { 'partyId': 1, 'createdDate': 1}}]"
mongoemployee = spark.read.format("mongo").option("database", database).option("collection", collection).option("pipeline", pipeline).option("spark.mongodb.input.uri", connectionString).load()
mongoemployee = mongoemployee.drop('_id')

# COMMAND ----------

if mongoemployee.count() > 0: 
    df = df.merge(mongoemployee.toPandas(),on='partyId', how='left')
    df['createdDate'].fillna(pd.Timestamp.now())
else: 
    df['createdDate'] = pd.Timestamp.now()
df['updatedDate'] = pd.Timestamp.now()
df['isDeleted'] = False
df.head()

# COMMAND ----------

spark.createDataFrame(df).write.format("mongo") \
    .option("spark.mongodb.output.uri", connectionString) \
    .option("database",database) \
    .option("collection",collection) \
    .option("idFieldList","partyId") \
    .mode("overwrite").save()
