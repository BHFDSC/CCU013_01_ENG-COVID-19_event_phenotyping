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
# MAGIC **Date last updated** 2022-01-24
# MAGIC  
# MAGIC **Date last reviewed** *NA*
# MAGIC  
# MAGIC **Date last run** `1/24/2022, 11:34:40 AM`
# MAGIC 
# MAGIC **Last export requested** `1/24/2022`
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
import pandas as pd

# COMMAND ----------

# COVID-19 events
events_table = "dars_nic_391419_j3w9t_collab.ccu013_covid_events_paper_cohort"

# CALIBER phenotypes table
phenos_table = "dars_nic_391419_j3w9t_collab.ccu013_caliber_comorbidities_pre2020"

# COMMAND ----------

patients = spark.sql(f"""SELECT person_id_deid FROM {events_table}""")
phenos = spark.table(phenos_table)

# Subset to cohort
counts = patients.join(phenos, 'person_id_deid', 'left') \
  .fillna(0) \
  .drop('person_id_deid')

# Col sums
counts = counts.to_koalas() \
  .sum(axis=0) \
  .reset_index()

# Renaming operations prior to join
counts.columns = counts.columns.fillna('count')
counts = counts.rename(columns={'index': 'Phenotype',
                       'count': 'Individuals'})

# COMMAND ----------

# Get phenotype-category mapping
category_dictionary_table = spark.table("dars_nic_391419_j3w9t_collab.ccu013_caliber_category_mapping") \
  .drop('cat') \
  .to_koalas() \
  .rename(columns={'phenotype': 'Phenotype'})

# Apply mapping with merge
df = counts.merge(category_dictionary_table, on='Phenotype', how='left') \
  .rename(columns={'category': 'Category'}) \
  .sort_values(by=['Category', 'Individuals'], ascending=[True, False])
# Mask counts < 5. Do this last as will change count to string so then can't sort by it
df['Individuals'] = df['Individuals'].astype('str').str.replace('^[1-4]$', '<5')
# Process text
df = df.to_pandas()
df['Category'] = df['Category'].str.capitalize()
df['Phenotype'] = df['Phenotype'].str.capitalize()
df = df.replace(regex='_', value=" ")
# Manual corrections
df = df.replace({'Benign neoplasm cin': 'Benign neoplasm/CIN',
                'Hiv': 'HIV',
                'Bacterial diseases excl tb': 'Bacterial diseases excl TB',
                'Copd': 'COPD',
                'Vitamin b12 deficiency anaemia': 'Vitamin B12 deficiency anaemia',
                'Rheumatic valve dz': 'Rheumatic valve disease',
                'Venous thromboembolic disease excl pe': 'Venous thromboembolic disease excl PE',
                'Stroke nos': 'Stroke NOS',
                'Secondary malignancy brain other cns and intracranial': 'Secondary malignancy brain other CNS and intracranial',
                'Primary malignancy brain other cns and intracranial': 'Primary malignancy brain other CNS and intracranial',
                'Monoclonal gammopathy of undetermined significance mgus': 'Monoclonal gammopathy of undetermined significance',
                'Viral diseases excluding chronic hepatitis hiv': 'Viral diseases excluding chronic hepatitis or HIV'})
# excl
df = df.replace(regex='\sexcl\s', value=" excluding ")
df = df.replace(regex='\sincl\s', value=" including ")
# Reorder
df = df[['Category', 'Phenotype', 'Individuals']]
# Set to display full Phenotype text
pd.set_option('display.max_colwidth', None)

display(df)
