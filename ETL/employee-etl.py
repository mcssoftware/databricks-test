# Databricks notebook source
# read the csv files
df_employee_Personal = spark.read.load("dbfs:/user/hive/warehouse/employee_personal_table",
                     format="delta", sep=";", inferSchema="true", header="true")
# display(df_employee_Personal)
