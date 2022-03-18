-- Databricks notebook source
-- MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU013/COVID-19-SEVERITY-PHENOTYPING/CCU013_00_helper_functions

-- COMMAND ----------

SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort LIMIT 5

-- COMMAND ----------

 SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_paper_cohort LIMIT 5

-- COMMAND ----------

 SELECT distinct covid_phenotype FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Therefore we're basically going to use the method of creating the events table only instead of pivotting on events, will pivot on data source

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import databricks.koalas
-- MAGIC 
-- MAGIC events = spark.sql(f"""
-- MAGIC SELECT 
-- MAGIC   person_id_deid, 
-- MAGIC   source,
-- MAGIC   1 as value
-- MAGIC FROM
-- MAGIC   dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
-- MAGIC """) \
-- MAGIC   .to_koalas() \
-- MAGIC   .pivot(index='person_id_deid',
-- MAGIC          columns='source', 
-- MAGIC          values='value') \
-- MAGIC   .fillna(0) \
-- MAGIC   .reset_index() \
-- MAGIC   .to_spark()
-- MAGIC 
-- MAGIC display(events)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC events.count()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Join metadata

-- COMMAND ----------

 SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort LIMIT 5

-- COMMAND ----------

-- MAGIC %py
-- MAGIC X = spark.sql("""
-- MAGIC SELECT
-- MAGIC   person_id_deid,
-- MAGIC   date_first,
-- MAGIC   sex,
-- MAGIC   ethnic_group,
-- MAGIC   IMD_quintile,
-- MAGIC   age
-- MAGIC FROM 
-- MAGIC   dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
-- MAGIC   """)
-- MAGIC 
-- MAGIC cohort = events.join(X, 
-- MAGIC                 "person_id_deid", 
-- MAGIC                 "left") \
-- MAGIC           .fillna(0)
-- MAGIC 
-- MAGIC # Rename columns with spaces in them
-- MAGIC cohort = cohort.withColumnRenamed("HES APC", "HES_APC") \
-- MAGIC   .withColumnRenamed("HES CC", "HES_CC")
-- MAGIC 
-- MAGIC display(cohort)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC cohort.createOrReplaceGlobalTempView("ccu013_01_paper_upset")
-- MAGIC drop_table("ccu013_01_paper_upset")
-- MAGIC create_table("ccu013_01_paper_upset")

-- COMMAND ----------

SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_01_paper_upset

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Queries

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## NIV

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import databricks.koalas
-- MAGIC 
-- MAGIC NIV = spark.sql(f"""
-- MAGIC SELECT 
-- MAGIC   person_id_deid, 
-- MAGIC   source,
-- MAGIC   1 as value
-- MAGIC FROM
-- MAGIC   dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
-- MAGIC WHERE
-- MAGIC   covid_phenotype = "03_NIV_treatment"
-- MAGIC """) \
-- MAGIC   .to_koalas() \
-- MAGIC   .pivot(index='person_id_deid',
-- MAGIC          columns='source', 
-- MAGIC          values='value') \
-- MAGIC   .fillna(0) \
-- MAGIC   .reset_index() \
-- MAGIC   .to_spark()
-- MAGIC 
-- MAGIC # Rename columns with spaces in them
-- MAGIC NIV = NIV.withColumnRenamed("HES APC", "HES_APC") \
-- MAGIC   .withColumnRenamed("HES CC", "HES_CC")
-- MAGIC 
-- MAGIC NIV.createOrReplaceGlobalTempView("ccu013_01_upset_NIV")
-- MAGIC 
-- MAGIC display(spark.sql("""
-- MAGIC SELECT
-- MAGIC   -- HES CC with OPCS-4
-- MAGIC   SUM(HES_CC) as cc,
-- MAGIC   SUM(CASE WHEN HES_CC = 1 and (HES_APC = 1 OR SUS = 1) THEN 1 else 0 end) as cc_opcs,
-- MAGIC   ROUND(SUM(CASE WHEN HES_CC = 1 and (HES_APC = 1 OR SUS = 1) THEN 1 else 0 end)/SUM(HES_CC)*100, 2) as cc_opcs_percent,
-- MAGIC   -- CHESS with OPCS_4
-- MAGIC   SUM(CHESS) as chess,
-- MAGIC   SUM(CASE WHEN CHESS = 1 and (HES_APC = 1 OR SUS = 1) THEN 1 else 0 end) as chess_opcs,
-- MAGIC   ROUND(SUM(CASE WHEN CHESS = 1 and (HES_APC = 1 OR SUS = 1) THEN 1 else 0 end)/SUM(CHESS)*100, 2) as chess_opcs_percent
-- MAGIC FROM
-- MAGIC   global_temp.ccu013_01_upset_NIV
-- MAGIC """))

-- COMMAND ----------

-- WRONG as doesn't include those where ICU admission was detected from CHESS
SELECT
  COUNT(*),
  SUM(CASE WHEN HES_CC = 0 then 1 else 0 end) as niv_out_ICU
FROM
  global_temp.ccu013_01_upset_NIV

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## IMV

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import databricks.koalas
-- MAGIC 
-- MAGIC IMV = spark.sql(f"""
-- MAGIC SELECT 
-- MAGIC   person_id_deid, 
-- MAGIC   source,
-- MAGIC   1 as value
-- MAGIC FROM
-- MAGIC   dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
-- MAGIC WHERE
-- MAGIC   covid_phenotype = "03_IMV_treatment"
-- MAGIC """) \
-- MAGIC   .to_koalas() \
-- MAGIC   .pivot(index='person_id_deid',
-- MAGIC          columns='source', 
-- MAGIC          values='value') \
-- MAGIC   .fillna(0) \
-- MAGIC   .reset_index() \
-- MAGIC   .to_spark()
-- MAGIC 
-- MAGIC # Rename columns with spaces in them
-- MAGIC IMV = IMV.withColumnRenamed("HES APC", "HES_APC") \
-- MAGIC   .withColumnRenamed("HES CC", "HES_CC")
-- MAGIC 
-- MAGIC IMV.createOrReplaceGlobalTempView("ccu013_01_upset_IMV")
-- MAGIC 
-- MAGIC display(spark.sql("""
-- MAGIC SELECT
-- MAGIC   -- HES CC with OPCS-4
-- MAGIC   SUM(HES_CC) as cc,
-- MAGIC   SUM(CASE WHEN HES_CC = 1 and (HES_APC = 1 OR SUS = 1) THEN 1 else 0 end) as cc_opcs,
-- MAGIC   ROUND(SUM(CASE WHEN HES_CC = 1 and (HES_APC = 1 OR SUS = 1) THEN 1 else 0 end)/SUM(HES_CC)*100, 2) as cc_opcs_percent,
-- MAGIC   -- CHESS with OPCS_4
-- MAGIC   SUM(CHESS) as chess,
-- MAGIC   SUM(CASE WHEN CHESS = 1 and (HES_APC = 1 OR SUS = 1) THEN 1 else 0 end) as chess_opcs,
-- MAGIC   ROUND(SUM(CASE WHEN CHESS = 1 and (HES_APC = 1 OR SUS = 1) THEN 1 else 0 end)/SUM(CHESS)*100, 2) as chess_opcs_percent
-- MAGIC FROM
-- MAGIC   global_temp.ccu013_01_upset_IMV
-- MAGIC """))
