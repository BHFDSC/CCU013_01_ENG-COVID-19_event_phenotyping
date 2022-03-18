# Databricks notebook source
# MAGIC %md
# MAGIC # Master Demographics
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook runs a list of `SQL` queries to:  
# MAGIC   
# MAGIC 1. Extract demographics from central `curr302_patient_skinny_record`  
# MAGIC     1.1 Map ethnicity categories  
# MAGIC 2. Add geographical & demographic data  
# MAGIC     2.1 Extracts Lower layer super output areas (LSOA) from GDPPR and SGSS  
# MAGIC     2.2 Extracts useful geographic & deprivation variables from dss_corporate.english_indices_of_dep_v02  
# MAGIC     2.3 Joins the geogrphic & deprivation data from 3.2, via the LSOA from 3.1  
# MAGIC     2.4 Calculates IMD quintiles   
# MAGIC 3. Cleaning  
# MAGIC 4. Create output table `X` 1 row per patient  
# MAGIC   
# MAGIC   
# MAGIC **Project(s)** CCU013
# MAGIC  
# MAGIC **Author(s)** Chris Tomlinson
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2022-01-22
# MAGIC  
# MAGIC **Date last reviewed** *NA*
# MAGIC  
# MAGIC **Date last run** 2022-01-22
# MAGIC 
# MAGIC **Changelog**   
# MAGIC * **2021-10-06**: Reverted from `curr302_patient_skinny_record` to `curr302_patient_skinny_record_archive WHERE ProductionDate = "2021-07-29 13:39:04.161949"` due to ten-fold higher unknown ethnicity issues
# MAGIC * **2022-01-22** Updated 2022-01-22 for revised manuscript to use latest ProductionDate `2022-01-20 14:58:52.353312`
# MAGIC  
# MAGIC **Data input**   
# MAGIC * **Key demographics: `dars_nic_391419_j3w9t_collab.curr302_patient_skinny_record` **
# MAGIC * LSOA, comorbidity code search: `dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive`  
# MAGIC * Geographic & deprivation data: `dss_corporate.english_indices_of_dep_v02`[https://db.core.data.digital.nhs.uk/#table/dss_corporate/english_indices_of_dep_v02] 
# MAGIC   
# MAGIC   
# MAGIC   
# MAGIC **Data output**  
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu013_master_demographics` 1 row per patient
# MAGIC 
# MAGIC **Software and versions** `SQL`, `Python`
# MAGIC  
# MAGIC **Packages and versions** `pyspark`

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU013/COVID-19-SEVERITY-PHENOTYPING/CCU013_00_helper_functions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct ProductionDate FROM dars_nic_391419_j3w9t_collab.curr302_patient_skinny_record

# COMMAND ----------

# Params
production_date = "2022-01-20 14:58:52.353312" # Notebook CCU03_01_create_table_aliases   Cell 8

# Table names
skinny_record = "dars_nic_391419_j3w9t_collab.curr302_patient_skinny_record_archive"
gdppr = "dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive"
deprivation = "dss_corporate.english_indices_of_dep_v02"

# without dars_nic_391419_j3w9t_collab. prefix
output = "ccu013_master_demographics"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Extract demographics from `curr302_patient_skinny_record`
# MAGIC This is a core table maintained by Sam Hollings from NHS Digital

# COMMAND ----------

demographics = spark.sql(f""" 
  SELECT
    NHS_NUMBER_DEID as person_id_deid,
    ETHNIC as ethnic_cat,
    SEX as sex,
    DATE_OF_BIRTH as dob
  FROM 
    {skinny_record}
  WHERE
    ProductionDate == '{production_date}'
""")
display(demographics)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Calculate age
# MAGIC Calculate age at time of first covid event (`date_first` in `ccu013_covid_events`)

# COMMAND ----------

# from pyspark.sql.functions import lit, to_date, col, udf, substring, datediff, floor

# cohort = cohort \
#   .withColumn('dob', to_date(col('dob'))) \
#   .withColumn('age', floor(datediff(col('date_first'),col('dob'))/365.25))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Map Ethnicity as per data dictionary
# MAGIC From HES APC Data Dictionary which uses the ONS 2011 census categories

# COMMAND ----------

# From HES APC Data Dictionary (ONS 2011 census categories)
dict_ethnic = """
Code,Group,Description
A,White,British (White)
B,White,Irish (White)
C,White,Any other White background
D,Mixed,White and Black Caribbean (Mixed)
E,Mixed,White and Black African (Mixed)
F,Mixed,White and Asian (Mixed)
G,Mixed,Any other Mixed background
H,Asian or Asian British,Indian (Asian or Asian British)
J,Asian or Asian British,Pakistani (Asian or Asian British)
K,Asian or Asian British,Bangladeshi (Asian or Asian British)
L,Asian or Asian British,Any other Asian background
M,Black or Black British,Caribbean (Black or Black British)
N,Black or Black British,African (Black or Black British)
P,Black or Black British,Any other Black background
R,Chinese,Chinese (other ethnic group)
S,Other,Any other ethnic group
Z,Unknown,Unknown
X,Unknown,Unknown
99,Unknown,Unknown
0,White,White
1,Black or Black British,Black - Caribbean
2,Black or Black British,Black - African
3,Black or Black British,Black - Other
4,Asian or Asian British,Indian
5,Asian or Asian British,Pakistani
6,Asian or Asian British,Bangladeshi
7,Chinese,Chinese
8,Other,Any other ethnic group
9,Unknown,Unknown
"""

import io
import pandas as pd

dict_ethnic = pd.read_csv(io.StringIO(dict_ethnic))
dict_ethnic = dict_ethnic.rename(columns={"Code" : "code",
                                          "Group" : "ethnic_group",
                                          "Description" : "ethnicity"})

# COMMAND ----------

# Convert to dictionary for mapping
mapping_ethnic_group = dict(zip(dict_ethnic['code'], dict_ethnic['ethnic_group']))
mapping_ethnicity = dict(zip(dict_ethnic['code'], dict_ethnic['ethnicity']))

# Build mapping expressions
from pyspark.sql.functions import col, create_map, lit
from itertools import chain

mapping_expr_ethnic_group = create_map([lit(x) for x in chain(*mapping_ethnic_group.items())])
mapping_expr_ethnicity = create_map([lit(x) for x in chain(*mapping_ethnicity.items())])

# COMMAND ----------

# Map
demographics = demographics \
  .withColumn("ethnic_group", mapping_expr_ethnic_group[col("ethnic_cat")]) \
  .withColumn("ethnicity", mapping_expr_ethnicity[col("ethnic_cat")]) \
  .drop("ethnic_cat")

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Deprivation data  

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Extract `lsoa` from GDPPR

# COMMAND ----------

lsoas = spark.sql(f'''
SELECT * FROM (SELECT
  NHS_NUMBER_DEID as person_id_deid,
  MAX(DATE) as date
FROM
  {gdppr}
WHERE 
  ProductionDate == "{production_date}"
GROUP BY
  NHS_NUMBER_DEID
  ) as tab1
LEFT JOIN
  (SELECT 
    NHS_NUMBER_DEID as person_id_deid,
    DATE as date,
    LSOA as lsoa 
  FROM 
    {gdppr}
  WHERE 
    ProductionDate == "{production_date}"
    ) as tab2
ON
  tab1.person_id_deid = tab2.person_id_deid
  AND
  tab1.date = tab2.date
''')
# Unsure why duplicate persist so crude approach = 
lsoas = lsoas.dropDuplicates(['person_id_deid'])
# Remove duplicate cols by only selecting necessary and using tab1. prefix
lsoas = lsoas.select("tab1.person_id_deid", "lsoa")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2. Select deprivation variables from [`dss_corporate.english_indices_of_dep_v02`](https://db.core.data.digital.nhs.uk/#table/dss_corporate/english_indices_of_dep_v02)
# MAGIC NB selected from where IMD year = 2019 so most up to date as database contains earlier years. As we're looking at COVID we want the most recent.

# COMMAND ----------

imd = spark.sql(f"""
SELECT
  LSOA_CODE_2011 as lsoa,
  DECI_IMD
FROM
  {deprivation}
WHERE
  IMD_YEAR = 2019
  """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Join!

# COMMAND ----------

# Join with Geographic data from GDPPR/SGSS to get LSOA
# LEFT join to retain all patients -> nulls where no LSOA
demographics = demographics.join(lsoas, "person_id_deid", "LEFT")

# Use LSOA to join with deprivation/geography
demographics = demographics \
  .join(imd, "lsoa", "LEFT") \
  .drop("lsoa")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Calculate IMD quintiles
# MAGIC ** NB technically fifths as quintiles refers to the breakpoints **  
# MAGIC This seems to be a popular measure, easy to do given that we're supplied with deciles

# COMMAND ----------

from pyspark.sql.functions import col, create_map, lit
from itertools import chain

mapping = {
    '1': '1',
    '2': '1', 
    '3': '2', 
    '4': '2', 
    '5': '3', 
    '6': '3', 
    '7': '4', 
    '8': '4', 
    '9': '5', 
    '10': '5'}

mapping_expr = create_map([lit(x) for x in chain(*mapping.items())])

demographics = demographics \
  .withColumn("IMD_quintile", mapping_expr[col("DECI_IMD")]) \
  .drop("DECI_IMD")

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Cleaning

# COMMAND ----------

# Regexp cleaning
from pyspark.sql.functions import regexp_replace
demographics = demographics \
  .withColumn('sex', regexp_replace('sex', '0', 'Unknown')) \
  .withColumn('sex', regexp_replace('sex', '9', 'Unknown'))
# NB Sex: 8 = unspecified, 9 = "Home Leave" See: https://datadictionary.nhs.uk/attributes/sex_of_patients.html

# COMMAND ----------

# Coalescing nulls
from pyspark.sql.functions import col, coalesce, lit
demographics = demographics \
  .withColumn('ethnic_group', 
              coalesce(col('ethnic_group'), 
                       lit("Unknown"))) \
  .withColumn('ethnicity', 
              coalesce(col('ethnicity'), 
                       lit("Unknown"))) \
  .withColumn('dob', 
              coalesce(col('dob'), 
                       lit("Unknown"))) \
  .withColumn('IMD_quintile', 
              coalesce(col('IMD_quintile'), 
                       lit("Unknown"))) \
  .fillna(0)
# NA fill (0) with remaining to convert the comorbidities into binary flags

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Create table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Check no duplicates
# MAGIC This will break the notebook if duplicates, thereby preventing the table being generated if duplicates!

# COMMAND ----------

assert demographics.select('person_id_deid').count() == demographics.select('person_id_deid').dropDuplicates(['person_id_deid']).count(), "Cohort contains duplicate ids when should be mutually exclusive"
print("Passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Commit & optimise

# COMMAND ----------

demographics.createOrReplaceGlobalTempView(output)
drop_table(output) 
create_table(output) 

# COMMAND ----------

spark.sql(f"OPTIMIZE dars_nic_391419_j3w9t_collab.{output} ZORDER BY person_id_deid")

# COMMAND ----------

display(
  spark.sql(f"""
  SELECT COUNT(*), COUNT(DISTINCT person_id_deid) FROM dars_nic_391419_j3w9t_collab.{output}
  """)
)

# COMMAND ----------

display(
  spark.sql(f"""
  SELECT * FROM dars_nic_391419_j3w9t_collab.{output}
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check `ethnicity='unknown'`
# MAGIC * Issue noted 05/10/21 whereby out of 56 million in Table 1 of manuscript ~17% had a missing ethnicity
# MAGIC   * This was derived from `dars_nic_391419_j3w9t_collab.curr302_patient_skinny_record WHERE ProductionDate = '2021-08-18 14:47:00.887883'`
# MAGIC     * SELECT COUNT(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_master_demographics WHERE ethnicity='Unknown' = 25,935,198
# MAGIC   * Using `dars_nic_391419_j3w9t_collab.curr302_patient_skinny_record_archive WHERE ProductionDate = "2021-07-29 13:39:04.161949"`  
# MAGIC     * = 21,070,900
# MAGIC * Note that this is on the denominator of ~109 Million, i.e. includes records that don't meet minimum data quality and are filtered out in cohort process. Maximally comprehensive to allow for other studies using different cohort definitions.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_master_demographics
# MAGIC WHERE ethnicity='Unknown'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM dars_nic_391419_j3w9t_collab.ccu013_master_demographics
