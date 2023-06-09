# Databricks notebook source
# read the csv files
df_employee_Personal = spark.read.load("dbfs:/user/hive/warehouse/employee_personal_table",
                     format="delta", sep=";", inferSchema="true", header="true")
# display(df_employee_Personal)

# COMMAND ----------

df_employee_Job = spark.read.load("dbfs:/user/hive/warehouse/employment_job_table",
                     format="delta", sep=";", inferSchema="true", header="true")
# display(df_employee_Job)

# COMMAND ----------

df_emp_job = df_employee_Personal.join(df_employee_Job, df_employee_Personal.Worker_ID == df_employee_Job.Worker_ID, "inner").select(df_employee_Personal.Worker_ID.alias('job_Worker_ID'),df_employee_Personal.Party_ID.alias('job_Party_ID'))

# COMMAND ----------

df_emp_job.createOrReplaceTempView('temp_emp_job')

# COMMAND ----------

# DBTITLE 1,SQL_Join_Query
# MAGIC %sql
# MAGIC select ep.Worker_Id
# MAGIC ,ep.Party_Id
# MAGIC ,ep.Guid
# MAGIC ,ep.Legal_First_Name
# MAGIC ,ep.Legal_Last_Name	
# MAGIC ,ep.Preferred_First_Name
# MAGIC ,ep.Preferred_Last_Name
# MAGIC ,ep.PPID
# MAGIC ,ej.Management_Level_ID
# MAGIC ,ej.Management_Level_Descr
# MAGIC --,ej.Email_Address_WorkEmail	
# MAGIC ,ej.Hire_Date
# MAGIC ,ej.Rehire_Date
# MAGIC ,ej.Business_Unit_Descr_Short	--Segment
# MAGIC --,ej.Sub_Line_Of_Service_1_Name	
# MAGIC --,ej.Employee_Status
# MAGIC ,ej.Business_Title
# MAGIC ,ej.Employee_Type_ID
# MAGIC ,ej.Employee_Type_Descr
# MAGIC ,ej.Job_Profile_ID
# MAGIC ,ej.Job_Profile_Descr
# MAGIC --,(Select Job_family_group_ID from Job_Families_Group_lkp where Job_Profile_ID = ej.Job_Profile_ID) as 'Job_family_group' -- Job Family Group
# MAGIC ,ej.WD_Job_Profile_Level_ID	
# MAGIC ,ej.Job_Family_Descr
# MAGIC ,ej.Job_Family_ID
# MAGIC ,ej.WD_Position_Time_Type_ID
# MAGIC ,ej.Manager_Id	
# MAGIC ,ej.Global_Network_Id
# MAGIC ,ej.Professional_Experience_Date
# MAGIC ,ej.Primary_HR_Manager_Supervisory
# MAGIC ,ej.Location_ID
# MAGIC ,ej.Global_Network_Id --PwC Global Network
# MAGIC ,ej.Global_Network_Descr --PwC Global Network
# MAGIC --, (Select top 1 Concat(Company_Level_1_Descr,'>', Company_Level_2_Descr,'>',	Company_Level_3_Descr,'>', Company_Level_4_Descr,'>',	Company_Level_5_Descr) from Company_Hierarchy_lkp where Effective_Status = 'A' and Company_ID = ej.Company_ID) as CompanyHierarchy  --Company Hierarchy
# MAGIC , Manager_ID --Manager
# MAGIC --, case when (Select count(m.worker_id) from Employment_Job m where m.WD_Coach_ID = ej.Worker_ID ) > 1 then 'true' else 'false' end as IsRelationshipLeader --Is employee an RL? 
# MAGIC --, case when (Select count(m.worker_id) from Employment_Job m where m.Primary_HR_Manager_Supervisory = ej.worker_id ) > 1 then 'true' else 'false' end as IsTalentConsultant --Is employee a TC?
# MAGIC , Compensation_Grade_Profile_Descr --Comp Grade Profile
# MAGIC , Professional_Experience_Date --Professional Experience Date (PED)
# MAGIC , Compensation_Grade_Profile_Descr Cohort --Cohort           (compensation band a person is tied to)
# MAGIC , Sub_line_of_service_1_name --Sub LOS 1 Desc
# MAGIC , Department_ID --Cost Center
# MAGIC , Sub_line_of_service_4 --Cost Center Level 1
# MAGIC , Sub_Line_Of_Service_4_Name
# MAGIC , Sub_line_of_service_3 --Cost Center Level 2
# MAGIC , Sub_line_of_service_3_Name 
# MAGIC , Sub_line_of_service_2 --Cost Center Level 3
# MAGIC , Sub_Line_Of_Service_2_Name
# MAGIC , Sub_line_of_service_1 --Cost Center Level 4
# MAGIC , Line_of_Service_Descr 
# MAGIC , Line_of_Service_ID --Cost Center Level 5
# MAGIC , Location_ID
# MAGIC , Location_Descr
# MAGIC , Country_Alpha_3_WorkAddr --Country code
# MAGIC , Supervisory_Org --Supervisory organization
# MAGIC , Supervisory_Org_Descr
# MAGIC
# MAGIC --, ej.WD_Manager_ID, eman.job_Party_ID as Manager_Party_ID,  ej.WD_Coach_ID, ej.Primary_HR_Manager_Supervisory as Supervisor_ID
# MAGIC
# MAGIC , Global_Competency_Network_Descr --Global Competency Network Description
# MAGIC from  employee_personal_table ep
# MAGIC join employment_job_table ej on ep.Worker_ID = ej.Worker_ID
# MAGIC left join temp_emp_job eman on ej.WD_Manager_ID = eman.job_Worker_ID
# MAGIC left join temp_emp_job ecoach on ej.WD_Coach_ID = ecoach.job_Worker_ID
# MAGIC left join temp_emp_job esuper on ej.Primary_HR_Manager_Supervisory = esuper.job_Worker_ID

# COMMAND ----------


