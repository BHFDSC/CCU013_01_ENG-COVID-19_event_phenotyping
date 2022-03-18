# Databricks notebook source
# MAGIC %md
# MAGIC # COVID-19 Events - Demographics
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook runs a list of `SQL` queries to:  
# MAGIC 
# MAGIC <br>
# MAGIC   
# MAGIC 1. Join demographics created in [`CCU013/0_shared_resources/1_demographics`](https://db.core.data.digital.nhs.uk/#notebook/3284853/command/3284854) and stored in `ccu013_master_demographics`    
# MAGIC     1.2 Calculate age (at time of covid_events phenotype)   
# MAGIC 2. Add additional variables  
# MAGIC     2.1 High risk (shielding)  
# MAGIC     2.2 Long COVID 
# MAGIC 3. Ceate & optimise output table `ccu013_covid_events_demographics` 1 row per patient  
# MAGIC   
# MAGIC   
# MAGIC **Project(s)** CCU013
# MAGIC  
# MAGIC **Author(s)** Chris Tomlinson
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2021-09-10
# MAGIC  
# MAGIC **Date last reviewed** *NA*
# MAGIC  
# MAGIC **Date last run** 2022-01-22 
# MAGIC  
# MAGIC **Data input** `ccu013_covid_events` *See cell 4*
# MAGIC   
# MAGIC **Data output** `ccu013_covid_events_demographics` *See cell 4*
# MAGIC 
# MAGIC **Software and versions** `SQL`, `Python`
# MAGIC  
# MAGIC **Packages and versions** *See cell 3*

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU013/COVID-19-SEVERITY-PHENOTYPING/CCU013_00_helper_functions

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql.functions import lit, to_date, col, udf, substring, datediff, floor, when

# COMMAND ----------

# MAGIC %md
# MAGIC # Check master demographics rebuilt if updating prior to running!
# MAGIC Dependencies:
# MAGIC * [`1_demographics`](https://db.core.data.digital.nhs.uk/#notebook/3284853/command/3602362)  Check production date
# MAGIC * [`2-1_CALIBER_codelist`](https://db.core.data.digital.nhs.uk/#notebook/3286046/command/3286047) does NOT need to be updated as codelists are static
# MAGIC * [`2-2_CALIBER_skinny`](https://db.core.data.digital.nhs.uk/#notebook/3286010/command/3286011) Check production date
# MAGIC * [`2-3_CALIBER_comorbidities_pre2020`](https://db.core.data.digital.nhs.uk/#notebook/3286218/command/3286219) No params
# MAGIC * [`2-4_CALIBER-categories_pre2020`](https://db.core.data.digital.nhs.uk/#notebook/3285967/command/3285968) No params

# COMMAND ----------

# Params
production_date = "2022-01-20 14:58:52.353312" # Notebook CCU03_01_create_table_aliases   Cell 8

# COVID-19 events
events_table = "dars_nic_391419_j3w9t_collab.ccu013_covid_events"
# Demographics table
demographics_table = "dars_nic_391419_j3w9t_collab.ccu013_master_demographics"
# GDPPR (for high risk)
gdppr_table = "dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive"
# Long COVID codelist (table)
long_covid_table = "dars_nic_391419_j3w9t_collab.ccu013_codelist_long_covid"

# Output table (no dars_.. prefix)
output_table = "ccu013_covid_events_demographics"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Join demographics from [`ccu013_master_demographics`](https://db.core.data.digital.nhs.uk/#notebook/3284853/command/3284854)
# MAGIC Created in notebook [`CCU013/01_shared_resources/1_demographics`](https://db.core.data.digital.nhs.uk/#notebook/3284853/command/3284854)

# COMMAND ----------

cohort = spark.table(events_table)
demographics = spark.table(demographics_table)

cohort = cohort.join(demographics, "person_id_deid", "LEFT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Calculate age
# MAGIC Calculate age at time of first covid event (`date_first` in `ccu013_covid_events`)

# COMMAND ----------

cohort = cohort \
  .withColumn('dob', to_date(col('dob'))) \
  .withColumn('age', floor(datediff(col('date_first'),col('dob'))/365.25))

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Additional variables

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 High risk (shielding)
# MAGIC investigate shielded patients
# MAGIC should be 'tagged' 1300561000000107 
# MAGIC https://digital.nhs.uk/coronavirus/shielded-patient-list/guidance-for-general-practice#how-to-flag-patients-as-high-risk
# MAGIC 
# MAGIC You should flag any patients you identify as high risk by adding the SNOMED CT code to their patient record.
# MAGIC 
# MAGIC > 1300561000000107 - **High risk** category for developing complication from coronavirus disease caused by severe acute respiratory syndrome coronavirus infection (finding)
# MAGIC 
# MAGIC If you decide the patient should no longer be included in the Shielded Patient List, you should revise their high risk flag to moderate or low risk (do not delete the high risk flag) to remove them from the national list and prevent them from receiving continuing national advice and guidance.

# COMMAND ----------

high_risk = spark.sql(f"""
SELECT
  DISTINCT NHS_NUMBER_DEID as person_id_deid,
  (case when CODE = 1300561000000107 THEN 1 Else 0 End) as high_risk
FROM
  {gdppr_table}
WHERE
  CODE = 1300561000000107
AND
  ProductionDate == "{production_date}"
""")

cohort = cohort.join(high_risk, "person_id_deid", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Long COVID
# MAGIC **NB not in analysis owing to coding issues as highlighted by OpenSAFELY paper**

# COMMAND ----------

codelist_long_covid = spark.table(long_covid_table) \
  .toPandas()['code'] \
  .tolist()

gdppr = spark.sql(f"""
SELECT 
  NHS_NUMBER_DEID as person_id_deid,
  CODE as code
FROM
  {gdppr_table}
WHERE
  ProductionDate == "{production_date}"
""")

# Filter GDPPR to only long covid codes
pts_long_covid = gdppr \
  .filter(f.col('code') \
  .isin(codelist_long_covid)) \
  .withColumnRenamed("code", 'long_covid')


# Clean to change code -> flag
pts_long_covid = pts_long_covid \
  .withColumn('long_covid', when(pts_long_covid.long_covid.isNotNull(), 1))

# DISTINCT as want only 1 flag per patient
pts_long_covid = pts_long_covid.distinct()

# LEFT join to add a comorbidity to cohort
cohort = cohort.join(pts_long_covid, "person_id_deid", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Create table

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Tests
# MAGIC This will break the notebook, thereby preventing the table being generated if failures

# COMMAND ----------

assert cohort.select('person_id_deid').count() == cohort.select('person_id_deid').dropDuplicates(['person_id_deid']).count(), "Cohort contains duplicate ids when should be mutually exclusive"
assert cohort.count() == spark.table(events_table).count(), "Input length does not match output length"
print("Passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Commit & optimise

# COMMAND ----------

cohort.createOrReplaceGlobalTempView(output_table)
drop_table(output_table) 
create_table(output_table) 

# COMMAND ----------

spark.sql(f"OPTIMIZE dars_nic_391419_j3w9t_collab.{output_table} ZORDER BY person_id_deid")

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Queries

# COMMAND ----------

# Old value pre @22.01.2020 update: 5044357
display(spark.sql(f"SELECT COUNT(*), COUNT(DISTINCT person_id_deid) FROM {events_table}"))

# COMMAND ----------

# Old value pre @22.01.2020 update: 5044357
display(spark.sql(f"SELECT COUNT(*), COUNT(DISTINCT person_id_deid) FROM dars_nic_391419_j3w9t_collab.{output_table}"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{output_table} LIMIT 10"))
