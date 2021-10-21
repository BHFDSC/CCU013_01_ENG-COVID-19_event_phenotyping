# Databricks notebook source
# MAGIC %md
# MAGIC # Supplementary Table 3: CALIBER phenotype frequencies
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook produces the number of distinct individuals with a CALIBER phenotype prior to 01/01/2020, as used when defining comorbidities for `CCU013: COVID-19 Event Phenotypes`.
# MAGIC 
# MAGIC The output from these queries produces `Supplement table 3: 269 CALIBER phenotypes, aggregated into 16 categories, and the number of individuals within the study cohort identified from GDPPR (SNOMED-CT) and HES APC (ICD-10, OPCS-4).` within the manuscript `Characterising COVID-19 related events in a nationwide electronic health record cohort of 55.9 million people in England`
# MAGIC 
# MAGIC **Project(s)** CCU013
# MAGIC  
# MAGIC **Author(s)** Chris Tomlinson
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2021-10-04
# MAGIC  
# MAGIC **Date last reviewed** *NA*
# MAGIC  
# MAGIC **Date last run** 2021-10-04
# MAGIC  
# MAGIC **Data input**  
# MAGIC * `ccu013_covid_events_paper_cohort`  
# MAGIC * `ccu013_caliber_comorbidities_pre2020`  
# MAGIC * `ccu013_caliber_category_mapping`
# MAGIC 
# MAGIC **Data output**  
# MAGIC Export of this notebook.
# MAGIC 
# MAGIC **Software and versions** `python`
# MAGIC  
# MAGIC **Packages and versions** `pyspark`

# COMMAND ----------

import databricks.koalas as ks

# COMMAND ----------

# COVID-19 events
events_table = "dars_nic_391419_j3w9t_collab.ccu013_covid_events_paper_cohort"

# CALIBER phenotypes table
phenos_table = "dars_nic_391419_j3w9t_collab.ccu013_caliber_comorbidities_pre2020"

# COMMAND ----------

patients = spark.sql(f"""SELECT person_id_deid FROM {events_table}""")
phenos = spark.table(phenos_table)
# Subset to cohort
df = patients.join(phenos, 'person_id_deid', 'left') \
  .fillna(0) \
  .drop('person_id_deid')

# Col sums
df = df.to_koalas() \
  .sum(axis=0) \
  .reset_index()

# Renaming operations prior to join
df.columns = df.columns.fillna('count')
df = df.rename(columns={'index': 'phenotype'})

# COMMAND ----------

category_dictionary_table = spark.table("dars_nic_391419_j3w9t_collab.ccu013_caliber_category_mapping") \
  .drop('cat') \
  .to_koalas()

# COMMAND ----------

df.merge(category_dictionary_table, on='phenotype', how='left')
