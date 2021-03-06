# Databricks notebook source
# MAGIC %md # 3_dp_skinny_patient_01_01_2020: make a single version of the truth for each patient

# COMMAND ----------

# MAGIC %md
# MAGIC **Description** Populates the table `dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020` with the following for **each patient alive** on **23rd Jan 2020**:
# MAGIC - SEX
# MAGIC - RAW_ETHNICITY
# MAGIC - CATEGORISED_ETHNICITY
# MAGIC - DATE_OF_BIRTH
# MAGIC - AGE_AT_COHORT_START
# MAGIC - DATE_OF_DEATH
# MAGIC 
# MAGIC Uses records from GDPPR and HES, but only from **before 23rd Jan 2020**. All death records are used. Multiple records for each patient are reconciled into a single version of the truth using the following algorithm:
# MAGIC - Non-NULL (including coded NULLs) taken first
# MAGIC - Primary (GDPPR) data is preferred next
# MAGIC - Finally, most recent record is chosen.
# MAGIC 
# MAGIC The records are taken from the `dars_nic_391419_j3w9t_collab.ccu013_patient_skinny_unassembled` which is made in notebook `ccu013_01_dp_skinny_record_unassebled`. This is simply a list of all the records for each patient from all the datasets.
# MAGIC  
# MAGIC **Project(s)** All
# MAGIC  
# MAGIC **Author(s)** Sam Hollings, adopted to project ccu013 by Johan Thygesen
# MAGIC  
# MAGIC **Reviewer(s)** Angela Wood
# MAGIC  
# MAGIC **Date last updated** 2021-01-22
# MAGIC  
# MAGIC **Date last run** `1/22/2022, 12:52:31 PM`
# MAGIC  
# MAGIC **Data input** [**table**: `dars_nic_391419_j3w9t_collab.ccu013_dp_patient_skinny_unassembled`]
# MAGIC 
# MAGIC **Data output** `dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020`
# MAGIC 
# MAGIC **Software and versions** Databricks (Python and SQL)
# MAGIC  
# MAGIC **Packages and versions** Databricks runtime 6.4 ML

# COMMAND ----------

# MAGIC %md ## QA working Document - See box
# MAGIC This notebook gets the demographic features of each patient as described in the QA working document in the box - https://app.box.com/file/751034783514
# MAGIC 
# MAGIC **Cohort start -> 2020-01-01 **
# MAGIC 
# MAGIC 
# MAGIC  - PATIENT_IDENTIFIER - GDPPR 
# MAGIC  - SEX - GDPPR : most recent prior to COHORT START
# MAGIC  - DATE_OF_BIRTH - GDPPR  : most recent prior to COHORT START
# MAGIC  - AGE_AT_COHORT_START - calculated from DATE_OF_BIRTH
# MAGIC  - RAW_ETHNICITY - taken from GDPPR SNOMED CODE ethnicity primarily, then HES secondarily. most recent prior to COHORT START
# MAGIC  - CATEGORISED_ETHNICITY - RAW_ETHNICITY categorised into ONS Categories
# MAGIC  - DATE_OF_DEATH - civil registrations of deaths. most recent prior to COHORT START

# COMMAND ----------

cohort_start = '2020-01-23'

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU013/COVID-19-SEVERITY-PHENOTYPING/CCU013_00_helper_functions

# COMMAND ----------

# MAGIC %md ### First mark records as after or before the corhor start 23 Jan 2000

# COMMAND ----------

# MAGIC %sql
# MAGIC --- old value pre August update 106,011,394
# MAGIC --- old value @ 17.08.2021      107,194,954
# MAGIC --- Current value @ 22.01.2022  109,858,924
# MAGIC SELECT count(DISTINCT NHS_NUMBER_DEID) from dars_nic_391419_j3w9t_collab.ccu013_dp_patient_skinny_unassembled

# COMMAND ----------

spark.sql(
f"""CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_dp_patient_skinny_unassembled_beyond_2020 as
SELECT *, 
CASE WHEN RECORD_DATE > '{cohort_start}' THEN True ELSE False END as Beyond_Jan_2020
FROM dars_nic_391419_j3w9t_collab.ccu013_dp_patient_skinny_unassembled""")

# COMMAND ----------

# MAGIC %md ### Rank the results for the patients, only keeping records which were before Jan 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_dp_patient_fields_ranked_pre_cutoff AS
# MAGIC SELECT * --NHS_NUMBER_DEID, DATE_OF_DEATH
# MAGIC FROM (
# MAGIC       SELECT *, row_number() OVER (PARTITION BY NHS_NUMBER_DEID 
# MAGIC                                     ORDER BY death_table desc,  RECORD_DATE DESC) as death_recency_rank,
# MAGIC                 row_number() OVER (PARTITION BY NHS_NUMBER_DEID 
# MAGIC                                     ORDER BY date_of_birth_null asc, primary desc, RECORD_DATE DESC) as birth_recency_rank,
# MAGIC                 row_number() OVER (PARTITION BY NHS_NUMBER_DEID 
# MAGIC                                     ORDER BY sex_null asc, primary desc, RECORD_DATE DESC) as sex_recency_rank,
# MAGIC                 row_number() OVER (PARTITION BY NHS_NUMBER_DEID 
# MAGIC                                     ORDER BY ethnic_null asc, primary desc, RECORD_DATE DESC) as ethnic_recency_rank
# MAGIC                               
# MAGIC       FROM global_temp.ccu013_dp_patient_skinny_unassembled_beyond_2020
# MAGIC       WHERE Beyond_Jan_2020 = False -- we only want records from before the cohort start date
# MAGIC             Or Death_table = 1 -- but we want all deaths (in case death was recorded after 1st Jan 2020)
# MAGIC       ) 

# COMMAND ----------

drop_table("ccu013_dp_patient_fields_ranked_pre_cutoff")
create_table("ccu013_dp_patient_fields_ranked_pre_cutoff")

# COMMAND ----------

# MAGIC %sql
# MAGIC --- old value pre August update  99,531,011
# MAGIC --- old value @ 17.08.2021       99,934,779
# MAGIC --- Current value @ 22.01.2022  100,118,189
# MAGIC SELECT count(DISTINCT NHS_NUMBER_DEID) from dars_nic_391419_j3w9t_collab.ccu013_dp_patient_fields_ranked_pre_cutoff

# COMMAND ----------

# MAGIC %md Make ethnicity group lookup

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_ethnicity_lookup AS
# MAGIC SELECT *, 
# MAGIC       CASE WHEN ETHNICITY_CODE IN ('1','2','3','N','M','P') THEN "Black or Black British"
# MAGIC            WHEN ETHNICITY_CODE IN ('0','A','B','C') THEN "White"
# MAGIC            WHEN ETHNICITY_CODE IN ('4','5','6','L','K','J','H') THEN "Asian or Asian British"
# MAGIC            WHEN ETHNICITY_CODE IN ('7','8','W','T','S','R') THEN "Other Ethnic Groups"
# MAGIC            WHEN ETHNICITY_CODE IN ('D','E','F','G') THEN "Mixed"
# MAGIC            WHEN ETHNICITY_CODE IN ('9','Z','X') THEN "Unknown"
# MAGIC            ELSE 'Unknown' END as ETHNIC_GROUP  
# MAGIC FROM (
# MAGIC   SELECT ETHNICITY_CODE, ETHNICITY_DESCRIPTION FROM dss_corporate.hesf_ethnicity
# MAGIC   UNION ALL
# MAGIC   SELECT Value as ETHNICITY_CODE, Label as ETHNICITY_DESCRIPTION FROM dss_corporate.gdppr_ethnicity WHERE Value not in (SELECT ETHNICITY_CODE FROM FROM dss_corporate.hesf_ethnicity))

# COMMAND ----------

# MAGIC %md assemble skinny record

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_dp_skinny_patient_23_01_2020 AS
# MAGIC SELECT pat.NHS_NUMBER_DEID,
# MAGIC       eth.ETHNIC as RAW_ETHNICITY,
# MAGIC       eth_group.ETHNIC_GROUP as CATEGORISED_ETHNICITY,
# MAGIC       sex.SEX,
# MAGIC       dob.DATE_OF_BIRTH,
# MAGIC       dod.DATE_OF_DEATH,
# MAGIC       pres.deaths as deaths_table,
# MAGIC       pres.sgss as sgss,
# MAGIC       pres.gdppr as primary,
# MAGIC       pres.hes as secondary,
# MAGIC       pres.hes_apc,
# MAGIC       pres.hes_op,
# MAGIC       pres.hes_ae
# MAGIC FROM (SELECT DISTINCT NHS_NUMBER_DEID FROM dars_nic_391419_j3w9t_collab.ccu013_dp_patient_fields_ranked_pre_cutoff) pat 
# MAGIC         LEFT JOIN (SELECT NHS_NUMBER_DEID, ETHNIC FROM dars_nic_391419_j3w9t_collab.ccu013_dp_patient_fields_ranked_pre_cutoff WHERE ethnic_recency_rank = 1) eth ON pat.NHS_NUMBER_DEID = eth.NHS_NUMBER_DEID
# MAGIC         LEFT JOIN (SELECT NHS_NUMBER_DEID, SEX FROM dars_nic_391419_j3w9t_collab.ccu013_dp_patient_fields_ranked_pre_cutoff WHERE sex_recency_rank = 1) sex ON pat.NHS_NUMBER_DEID = sex.NHS_NUMBER_DEID
# MAGIC         LEFT JOIN (SELECT NHS_NUMBER_DEID, DATE_OF_BIRTH FROM dars_nic_391419_j3w9t_collab.ccu013_dp_patient_fields_ranked_pre_cutoff WHERE birth_recency_rank = 1) dob ON pat.NHS_NUMBER_DEID = dob.NHS_NUMBER_DEID
# MAGIC         LEFT JOIN (SELECT NHS_NUMBER_DEID, DATE_OF_DEATH FROM dars_nic_391419_j3w9t_collab.ccu013_dp_patient_fields_ranked_pre_cutoff WHERE death_recency_rank = 1) dod ON pat.NHS_NUMBER_DEID = dod.NHS_NUMBER_DEID
# MAGIC         LEFT JOIN dars_nic_391419_j3w9t_collab.ccu013_dp_patient_dataset_presence_lookup pres ON pat.NHS_NUMBER_DEID = pres.NHS_NUMBER_DEID
# MAGIC         LEFT JOIN global_temp.ccu013_ethnicity_lookup eth_group ON eth.ETHNIC = eth_group.ETHNICITY_CODE
# MAGIC WHERE pres.gdppr = 1
# MAGIC 
# MAGIC ---- pat.NHS_NUMBER_DEID IN (SELECT NHS_NUMBER_DEID 
# MAGIC ----                               FROM global_temp.patient_skinny_unassembled_beyond_2020
# MAGIC ----                               GROUP BY NHS_NUMBER_DEID HAVING MAX(primary) = 1) -- we only want patients which have ever appered in primary care

# COMMAND ----------

# MAGIC %sql
# MAGIC --- old value pre August update 55,988,064
# MAGIC --- old value @ 17.08.2021      56,721,158
# MAGIC --- Current value @             57,175,620
# MAGIC SELECT count(DISTINCT NHS_NUMBER_DEID) FROM global_temp.ccu013_dp_skinny_patient_23_01_2020

# COMMAND ----------

import pyspark.sql.functions as f
(spark.table("global_temp.ccu013_dp_skinny_patient_23_01_2020")
      .selectExpr("*", 
                  f"floor(float(months_between('{cohort_start}', DATE_OF_BIRTH))/12.0) as AGE_AT_COHORT_START")
      .createOrReplaceGlobalTempView("ccu013_dp_skinny_patient_23_01_2020_age"))

# COMMAND ----------

# MAGIC %sql
# MAGIC --- old value pre August update 55,988,064
# MAGIC --- old value @ 17.08.2021      56,721,158
# MAGIC --- Current value @ 22.01.2022  57,175,620
# MAGIC SELECT count(DISTINCT NHS_NUMBER_DEID) FROM global_temp.ccu013_dp_skinny_patient_23_01_2020_age

# COMMAND ----------

# MAGIC %md filter out the dead people - so we only have people which are **alive** on **23rd Jan 2020**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_alive_patients_2020 AS
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM global_temp.ccu013_dp_skinny_patient_23_01_2020_age
# MAGIC WHERE COALESCE(DATE_OF_DEATH, '2199-01-01') > '2020-01-23'

# COMMAND ----------

# %sql
# SELECT COUNT(DISTINCT NHS_NUMBER_DEID)
# FROM dars_nic_391419_j3w9t_collab.DP_skinny_patient_01_01_2020_4

# COMMAND ----------

# MAGIC %md save to a to a table

# COMMAND ----------

drop_table("ccu013_dp_skinny_patient_23_01_2020")

# COMMAND ----------

#cols = ", ".join(spark.table('dars_nic_391419_j3w9t_collab.dp_skinny_patient_01_01_2020').columns)
create_table("ccu013_dp_skinny_patient_23_01_2020", select_sql_script=f"SELECT * FROM global_temp.ccu013_alive_patients_2020")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020

# COMMAND ----------

# MAGIC %sql
# MAGIC --- old value pre August update 55,876,173
# MAGIC --- old value @ 17.08.2021      56,609,049
# MAGIC --- current value @ 22.01.2022  57,032,174
# MAGIC SELECT count(DISTINCT NHS_NUMBER_DEID) FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020
