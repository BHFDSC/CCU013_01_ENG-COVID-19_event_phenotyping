# Databricks notebook source
# MAGIC %md
# MAGIC # Supplementary Table 2: COVID-19 codes & frequencies
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook runs a list of `SQL` queries to extract the different codelists useed in the `CCU013: COVID-19 Event Phenotypes` work and the frequency with which they occur.
# MAGIC 
# MAGIC The output from these queries produces `Supplementary Table 2: COVID-19 codes & frequencies` within the manuscript `Characterising COVID-19 related events in a nationwide electronic health record cohort of 55.9 million people in England`
# MAGIC 
# MAGIC **Project(s)** CCU013
# MAGIC  
# MAGIC **Author(s)** Chris Tomlinson
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2021-06-23
# MAGIC  
# MAGIC **Date last reviewed** *NA*
# MAGIC  
# MAGIC **Date last run** 2021-06-16
# MAGIC  
# MAGIC **Data input**  
# MAGIC * `ccu013_covid_trajectory_paper_cohort`
# MAGIC 
# MAGIC **Data output**  
# MAGIC Export of this notebook.
# MAGIC 
# MAGIC **Software and versions** `python`
# MAGIC  
# MAGIC **Packages and versions** `pyspark`

# COMMAND ----------

# MAGIC %md
# MAGIC # All COVID-19 event phenotypes and their codelists + counts
# MAGIC 
# MAGIC This returns all codes used in `ccu013_covid_trajectory` and the frequencies with which they occur.
# MAGIC   
# MAGIC Please note:
# MAGIC * This *excludes* those codes searched for but returning 0 results  
# MAGIC   * A complete list of all codelists is available at [`https://github.com/BHFDSC/CCU013_01_ENG-COVID-19_event_phenotyping`](https://github.com/BHFDSC/CCU013_01_ENG-COVID-19_event_phenotyping)   
# MAGIC * Counts < 5 are reported as `<5` as per NHS Digital disclosure policy  
# MAGIC * `description` field included to allow for non-code rules, e.g. `ARRESSUPDAYS > 0 ` in HES CC
# MAGIC   

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

event_codes = spark.sql("""
SELECT
  DISTINCT covid_phenotype, 
  clinical_code, 
  code as terminology, 
  description, 
  covid_status,
  source, 
  COUNT(clinical_code) as n
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
GROUP BY
  covid_phenotype, clinical_code, code, description, covid_status, source
ORDER BY
  covid_phenotype, n DESC
""")

display(
  # Mask counts < 5
event_codes.withColumn('n', regexp_replace('n', '^[1-4]$', '<5'))
)
