# Databricks notebook source
import Library.sql_data as sqldata

# COMMAND ----------

personal = sqldata.getSqlData(spark, "Employee_Personal")
job = sqldata.getSqlData(spark, "Employment_Job")
personal.createOrReplaceTempView("temp_personal")
job.createOrReplaceTempView("temp_job")

# COMMAND ----------

df_emp_job = personal.join(job, personal.Worker_ID == job.Worker_ID, "inner").select(
    "Legal_First_Name","Legal_Last_Name","Business_Unit_Descr_Short","Management_Level_ID","Guid",
    personal.Worker_ID.alias('job_Worker_ID'),personal.Party_ID.alias('job_Party_ID'))
df_emp_job.createOrReplaceTempView('temp_emp_job')

# COMMAND ----------

# MAGIC %sql
# MAGIC select ep.Worker_Id,
# MAGIC   ep.Party_Id,
# MAGIC   ep.Guid guid,
# MAGIC   ep.Legal_First_Name,
# MAGIC   ep.Preferred_First_Name,
# MAGIC   ep.Legal_Last_Name,
# MAGIC   ep.Preferred_Last_Name,
# MAGIC   ep.Email_Address_WorkEmail,
# MAGIC   ej.Management_Level_Descr,
# MAGIC   ej.Business_Unit_Descr_Short,
# MAGIC   ej.Employee_Status_Descr,
# MAGIC   ej.Employee_Status,
# MAGIC   --rl.Party_ID relationshipLeaderPartyId,
# MAGIC   case
# MAGIC     when (
# MAGIC       Select
# MAGIC         count(m.worker_id)
# MAGIC       from
# MAGIC         temp_job m
# MAGIC       where
# MAGIC         m.WD_Coach_ID = ej.Worker_ID
# MAGIC     ) > 1 then 'true'
# MAGIC     else 'false'
# MAGIC   end as isRelationshipLeader --Is employee an RL? 
# MAGIC ,
# MAGIC   case
# MAGIC     when (
# MAGIC       Select
# MAGIC         count(m.worker_id)
# MAGIC       from
# MAGIC         temp_job m
# MAGIC       where
# MAGIC         m.Primary_HR_Manager_Supervisory = ej.worker_id
# MAGIC     ) > 1 then 'true'
# MAGIC     else 'false'
# MAGIC   end as isTalentConsultant,
# MAGIC   ej.Business_Title,
# MAGIC   Department_ID costCenter,
# MAGIC   ej.Sub_Line_Of_Service_1_Name constCenterLevel4,
# MAGIC   ej.Employee_Type_Descr employeeType,
# MAGIC   Line_of_Service_Descr globalLOS,
# MAGIC   ej.Hire_Date hireDate ,
# MAGIC   ej.Job_Profile_Descr jobProfile,
# MAGIC   ej.Job_Profile_Level_Descr jobLevel,
# MAGIC   ej.Job_Family_Descr jobFamily,
# MAGIC /*(
# MAGIC     Select
# MAGIC       Job_family_group_ID
# MAGIC     from
# MAGIC       Job_Families_Group_lkp
# MAGIC     where
# MAGIC       Job_Profile_ID = ej.Job_Profile_ID
# MAGIC   ) as 'jobFamilyGroup',*/
# MAGIC   ej.Location_ID,
# MAGIC   ej.Location_Descr location,
# MAGIC   ej.WD_Position_Time_Type_ID,
# MAGIC   '' timeInGrade,
# MAGIC   GETDATE() updatedDate
# MAGIC from  temp_personal ep
# MAGIC join temp_job ej on ep.Worker_ID = ej.Worker_ID
# MAGIC left join temp_emp_job eman on ej.WD_Manager_ID = eman.job_Worker_ID
# MAGIC left join temp_emp_job ecoach on ej.WD_Coach_ID = ecoach.job_Worker_ID
# MAGIC left join temp_emp_job esuper on ej.Primary_HR_Manager_Supervisory = esuper.job_Worker_ID

# COMMAND ----------

connectionString='mongodb+srv://sbhandari:vDUwaSAGnZGb5rqHIrrhdQaoLPL7fyxR@cluster0.ov0d2xd.mongodb.net'
database="myplus"
collection="employee"

# COMMAND ----------

_sqldf.write.format("mongo") \
    .option("spark.mongodb.output.uri", connectionString) \
    .option("database",database) \
    .option("collection",collection) \
    .option("spark.mongodb.idFieldList","Party_Id") \
    .mode("append") \
    .save()

# COMMAND ----------

cols = ["Worker_ID", "Party_ID"]
df_emp_job = personal.join(job, ["Worker_ID"], "inner")

# COMMAND ----------

cols = ["Worker_Id","Party_Id","Guid","Legal_First_Name","Legal_Last_Name","Preferred_First_Name","Preferred_Last_Name","PPID","Management_Level_ID","Management_Level_Descr","Hire_Date","Rehire_Date","Business_Unit_Descr_Short","Business_Title","Employee_Type_ID","Employee_Type_Descr","Job_Profile_ID","Job_Profile_Descr", "WD_Job_Profile_Level_ID","Job_Family_Descr","Job_Family_ID","WD_Position_Time_Type_ID","Manager_Id","Global_Network_Id","Professional_Experience_Date","Primary_HR_Manager_Supervisory","Location_ID","Global_Network_Id","Global_Network_Descr" ]
# emp_with_jobs = personal.join(job,"Worker_ID").select(cols)
[col for col in personal.columns]

# COMMAND ----------


