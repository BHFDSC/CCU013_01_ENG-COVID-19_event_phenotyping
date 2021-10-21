# Databricks notebook source
# MAGIC %md
# MAGIC # COVID-19 Severity Phenotypes: Demographics for 'Table 1'
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook runs a list of `SQL` queries to:
# MAGIC 1. Extract demographics from central `curr302_patient_skinny_record`  
# MAGIC     1.2 Check coverage  
# MAGIC     1.3 Calculate age (at time of covid_events phenotype)  
# MAGIC     1.4 Map ethnicity categories  
# MAGIC 2. Add geographical & demographic data  
# MAGIC     2.1 Extracts Lower layer super output areas (LSOA) from GDPPR and SGSS
# MAGIC     2.2 Extracts useful geographic & deprivation variables from dss_corporate.english_indices_of_dep_v02
# MAGIC     2.3 Joins the geogrphic & deprivation data from 3.2, via the LSOA from 3.1
# MAGIC     2.4 Calculates IMD quintiles
# MAGIC 3. Comorbidities
# MAGIC     3.1 High Risk (Shielding)
# MAGIC     3.2 Long COVID
# MAGIC     3.3 Descriptive paper comorbidity phenotypes
# MAGIC 4. Cleaning
# MAGIC 5. Create output table `ccu013_covid_events_demographics_paper_cohort` 1 row per patient
# MAGIC 
# MAGIC **Project(s)** CCU013
# MAGIC  
# MAGIC **Author(s)** Chris Tomlinson
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2021-09-07
# MAGIC  
# MAGIC **Date last reviewed** *NA*
# MAGIC  
# MAGIC **Date last run** 2021-10-06
# MAGIC  
# MAGIC **Data input**  
# MAGIC * **`dars_nic_391419_j3w9t_collab.ccu013_covid_events_paper_cohort`**
# MAGIC * `dars_nic_391419_j3w9t_collab.curr302_patient_skinny_record`
# MAGIC * `bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu003_direfcts_dataprep_1_gdppr_frzon28may_mm_210528`
# MAGIC * Geographic & demographic data:
# MAGIC     * `dars_nic_391419_j3w9t_collab.ccu003_direfcts_dataprep_1_gdppr_frzon28may_mm_210528`: link `person_id_deid` -> `lsoa`
# MAGIC     * `dss_corporate.english_indices_of_dep_v02`[https://db.core.data.digital.nhs.uk/#table/dss_corporate/english_indices_of_dep_v02] Geographic & deprivation data
# MAGIC * Comorbidities from:
# MAGIC     * High Risk & Long COVID
# MAGIC     *
# MAGIC 
# MAGIC 
# MAGIC **Data output**  
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort` 1 row per patient
# MAGIC 
# MAGIC **Software and versions** `SQL`, `Python`
# MAGIC  
# MAGIC **Packages and versions** `pyspark`

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU013/COVID-19-SEVERITY-PHENOTYPING/CCU013_00_helper_functions

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql.functions import lit, to_date, col, udf, substring, datediff, floor, when

# COMMAND ----------

# Params
production_date = "2021-07-29 13:39:04.161949"

# COVID-19 events
events_table = "dars_nic_391419_j3w9t_collab.ccu013_covid_events_paper_cohort"
# Demographics table
demographics_table = "dars_nic_391419_j3w9t_collab.ccu013_master_demographics"
# GDPPR (for high risk)
gdppr_table = "dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive"
# Long COVID codelist (table)
long_covid_table = "dars_nic_391419_j3w9t_collab.ccu013_codelist_long_covid"
# Comorbidities table
comorbidities_table = "dars_nic_391419_j3w9t_collab.ccu013_caliber_categories_pre2020"

# Output table (no dars_.. prefix)
output_table = "ccu013_covid_events_demographics_paper_cohort"

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
# MAGIC # 3. Add comorbidities from [`ccu013_master_demographics`](https://db.core.data.digital.nhs.uk/#notebook/3284853/command/3284854)
# MAGIC Created in notebook [`CCU013/01_shared_resources/1_demographics`](https://db.core.data.digital.nhs.uk/#notebook/3284853/command/3284854)

# COMMAND ----------

comorbidities = spark.table(comorbidities_table)
cohort = cohort.join(comorbidities, "person_id_deid", "left") \
  .fillna(0)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4 Create Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Tests
# MAGIC This will break the notebook, thereby preventing the table being generated if failures.  
# MAGIC Takes time to run, but worth the headaches saved.  

# COMMAND ----------

assert cohort.select('person_id_deid').count() == cohort.select('person_id_deid').dropDuplicates(['person_id_deid']).count(), "Cohort contains duplicate ids when should be mutually exclusive"
assert cohort.count() == spark.table(events_table).count(), "Input length does not match output length"
print("Passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Commit & optimise

# COMMAND ----------

display(cohort, 10)

# COMMAND ----------

cohort.createOrReplaceGlobalTempView(output_table)
drop_table(output_table) 
create_table(output_table) 

# COMMAND ----------

spark.sql(f"OPTIMIZE dars_nic_391419_j3w9t_collab.{output_table} ZORDER BY person_id_deid")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3469528
# MAGIC SELECT COUNT(*), COUNT(DISTINCT person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

# COMMAND ----------

# MAGIC %md
# MAGIC # Comments/previous issues now resolved
