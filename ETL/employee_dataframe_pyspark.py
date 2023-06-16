# Databricks notebook source
import pandas as pd
import Library.sql_data as sqldata
from pyspark.sql.functions import col,when,count, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Sql Data

# COMMAND ----------

personal = sqldata.getSqlData(spark, "Employee_Personal")
job = sqldata.getSqlData(spark, "Employment_Job")
jobFamily = sqldata.getSqlData(spark, "Job_Families_Group_lkp")

# COMMAND ----------

empCols = [col("Worker_Id").alias("workdayId"), col("Party_Id").alias("partyId"), col("Guid").alias("guid"),col("Legal_First_Name").alias("firstName"), col("Legal_Last_Name").alias("lastName"), col("Preferred_First_Name").alias("preferredFirstName"), col("Preferred_Last_Name").alias("preferredLastName"),col("email_address_workemail").alias("email") ]
employee = personal[empCols].where(col("partyId").isNotNull())
# display(employee.count())

# COMMAND ----------

jobCols = [col("Worker_Id").alias("workdayId"), col("Management_Level_Descr").alias("managementLevel"), col("Department_ID").alias("costCenter"), col  ("Sub_Line_Of_Service_1_Name").alias("constCenterLevel4"), col("Business_Unit_Descr_Short").alias("segment"), col("Employee_Status_Descr").alias("employeeStatus"), col("Employee_Status").alias("employeeStatusCode"), col("Business_Title").alias("businessTitle"), col("Employee_Type_Descr").alias("employeeType"), col("Line_of_Service_Descr").alias("globalLOS"), col("Hire_Date").alias("hireDate"), col("Job_Profile_Descr").alias("jobProfile"), col("Job_Profile_Level_Descr").alias("jobLevel"), col("Job_Family_Descr").alias("jobFamily"), col("Location_ID").alias("location"), col("WD_Position_Time_Type_ID").alias("positionTimeTypeId"), col("WD_Coach_ID").alias("coachId"), col("Primary_HR_Manager_Supervisory").alias("hrManagerId"),col("Job_Profile_ID").alias("jobProfileId") ]
activeEmployeeStatus = ['A', 'L', 'P', 'S']
employeeJobs = job[jobCols].filter(col("employeeStatusCode").isin(activeEmployeeStatus))
# employeeJobs.head()

# COMMAND ----------

jobFamilyCols = [col("Job_Profile_ID").alias("jobProfileId"), col("Job_Family_Group_ID").alias("jobFamilyGroup")]
employeeJobs = employeeJobs.join(jobFamily[jobFamilyCols], on= "jobProfileId", how= "left").drop("jobProfileId")

# COMMAND ----------

dfEmployeeWithJobs=employee.join(employeeJobs.select(["workdayId"]),on='workdayId',how='inner')

# COMMAND ----------

df= dfEmployeeWithJobs.join(employeeJobs,how='left',on='workdayId').sort(col('hireDate').desc()).drop_duplicates(subset=['partyId'])
# display(df2.where(df2.partyId == '0133dbbd-e2fd-4869-9eeb-e6804aeb2aa9').select(['workdayId', 'partyId', 'hireDate']))
# df.toPandas()[df.toPandas().duplicated(['partyId'])]

# COMMAND ----------

df = df.withColumn('isRelationshipLeader', when(df['coachId'].isin(df['workdayId']), True).otherwise(False))
df = df.withColumn('isTalentConsultant', when(df['hrManagerId'].isin(df['workdayId']), True).otherwise(False))

# COMMAND ----------

connectionString='mongodb+srv://sbhandari:vDUwaSAGnZGb5rqHIrrhdQaoLPL7fyxR@cluster0.ov0d2xd.mongodb.net'
database="myplus"
collection="employee"
mongoemployee = spark.read.format("mongo").option("database", database).option("collection", collection).option("spark.mongodb.input.uri", connectionString).load()
mongoemployee = mongoemployee.drop('_id')

# COMMAND ----------

# MAGIC %md 
# MAGIC ## update isDeleted, createdDate and updatedDate field
# MAGIC ### set isDeleted to false for all employees from sql
# MAGIC ### set updatedDate to currentTime
# MAGIC ### set created date to currentTime if there is no data in mongodb, else for all null values fill createdDate with currentTime

# COMMAND ----------

if mongoemployee.count() > 0: 
    df = df.join(mongoemployee.select(['partyId', 'createdDate']),on='partyId', how='left')
    df= df.fillna(str(pd.Timestamp.now()), subset=['createdDate'])
else: 
    df = df.withColumn('createdDate', lit(str(pd.Timestamp.now())))
df = df.withColumn('updatedDate', lit(str(pd.Timestamp.now())))
df =df.withColumn('isDeleted', lit(False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## update sql data

# COMMAND ----------

df.write.format("mongo") \
    .option("spark.mongodb.output.uri", connectionString) \
    .option("database",database) \
    .option("collection",collection) \
    .option("idFieldList","partyId") \
    .mode("overwrite").save()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## find and update deleted employee

# COMMAND ----------

if mongoemployee.count() > 0: 
    deletedEmployee = mongoemployee.join(df.select(['partyId']), on='partyId', how='left_anti')
    deletedEmployee = deletedEmployee.withColumn('updatedDate', lit(str(pd.Timestamp.now())))
    deletedEmployee = deletedEmployee.withColumn('isDeleted', lit(True))
    #display(deletedEmployee.filter(col('partyId') == 'f8f97313-fa68-44ed-81d4-892d86d9ec61').select(['partyId','updatedDate', 'isDeleted']))
    print(deletedEmployee.count())
    deletedEmployee.write.format("mongo") \
    .option("spark.mongodb.output.uri", connectionString) \
    .option("database",database) \
    .option("collection",collection) \
    .option("idFieldList","partyId") \
    .mode("overwrite").save()
