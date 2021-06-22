# Databricks notebook source
# MAGIC %md
# MAGIC # COVID-19 Severity Phenotypes: Demographics for 'Table 1'
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook runs a list of `SQL` queries to:  
# MAGIC   
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
# MAGIC 5. Create output table `ccu013_covid_events_demographics` 1 row per patient  
# MAGIC   
# MAGIC   
# MAGIC **Project(s)** CCU013
# MAGIC  
# MAGIC **Author(s)** Chris Tomlinson
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2021-06-08
# MAGIC  
# MAGIC **Date last reviewed** *NA*
# MAGIC  
# MAGIC **Date last run** 2021-06-15
# MAGIC  
# MAGIC **Data input**  
# MAGIC * **Cohort: `dars_nic_391419_j3w9t_collab.ccu013_covid_events`**  
# MAGIC * Key demographics: `dars_nic_391419_j3w9t_collab.curr302_patient_skinny_record`  
# MAGIC * LSOA, comorbidity code search: `dars_nic_391419_j3w9t_collab.ccu003_direfcts_dataprep_1_gdppr_frzon28may_mm_210528`  
# MAGIC * Geographic & deprivation data: `dss_corporate.english_indices_of_dep_v02`[https://db.core.data.digital.nhs.uk/#table/dss_corporate/english_indices_of_dep_v02]  
# MAGIC * Long-COVID codelist: `ccu013_codelist_long_covid`  
# MAGIC * Prev. Stroke: `descriptive_stroke_previous_distinct_table`  
# MAGIC * Prev. MI: `descriptive_mi_previous_distinct_table`  
# MAGIC * Prev. Obesity `covariate_flags_diabetes`  
# MAGIC * Prev. Diabetes: `covariate_flags_obesity`  
# MAGIC   
# MAGIC   
# MAGIC   
# MAGIC **Data output**  
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics` 1 row per patient
# MAGIC 
# MAGIC **Software and versions** `SQL`, `Python`
# MAGIC  
# MAGIC **Packages and versions** `pyspark`

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU013/COVID-19-SEVERITY-PHENOTYPING/CCU013_00_helper_functions

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Extract demographics from `curr302_patient_skinny_record`
# MAGIC This is a core table maintained by Sam Hollings from NHS Digital

# COMMAND ----------

demog_data = spark.table('dars_nic_391419_j3w9t_collab.curr302_patient_skinny_record') \
  .withColumnRenamed("NHS_NUMBER_DEID", "person_id_deid") \
  .withColumnRenamed("ETHNIC", "ethnic_cat") \
  .withColumnRenamed("SEX", "sex") \
  .withColumnRenamed("DATE_OF_BIRTH", "dob") \
  .select("person_id_deid", 
          'dob', 
          'sex', 
          "ethnic_cat")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Check coverage
# MAGIC This is just an inner join to test the coverage  
# MAGIC !The actual cohort is created in a cell below using a LEFT join between events & demog_data

# COMMAND ----------

# Check coverage
events = spark.table('dars_nic_391419_j3w9t_collab.ccu013_covid_events')

cohort = demog_data.join(events, "person_id_deid", "inner")

print("N. individuals with events Phenotypes", 
      events.select('person_id_deid').distinct().count()
     )
print("N. individuals with events Phenotypes AND demographics", 
      cohort.select('person_id_deid').distinct().count()
     )
print("Discrepancy:", 
      events.select('person_id_deid').distinct().count() 
      - 
      cohort.select('person_id_deid').distinct().count()
     )
print("Representing:", 
      (events.select('person_id_deid').distinct().count() 
       - 
       cohort.select('person_id_deid').distinct().count()) 
      / 
      events.select('person_id_deid').distinct().count() * 100, 
      "%" 
     )
print("NB this doesn't include cases where ther record exists but the variable is null/unknown")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Build cohort
# MAGIC Build cohort by joining to our events table (1 row per patient)  
# MAGIC We will do a LEFT join to ensure that we keep all patients and just have missing demographics

# COMMAND ----------

events = spark.table('dars_nic_391419_j3w9t_collab.ccu013_covid_events')
cohort = events.join(demog_data, "person_id_deid", "LEFT")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Calculate age
# MAGIC Calculate age at time of first covid event (`date_first` in `ccu013_covid_events`)

# COMMAND ----------

from pyspark.sql.functions import lit, to_date, col, udf, substring, datediff, floor

cohort = cohort \
  .withColumn('dob', to_date(col('dob'))) \
  .withColumn('age', floor(datediff(col('date_first'),col('dob'))/365.25))

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
mapping = dict(zip(dict_ethnic['code'], dict_ethnic['ethnic_group']))

# COMMAND ----------

# Map
from pyspark.sql.functions import col, create_map, lit
from itertools import chain

mapping_expr = create_map([lit(x) for x in chain(*mapping.items())])

cohort = cohort.withColumn("ethnic_group", mapping_expr[col("ethnic_cat")]).drop("ethnic_cat")

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Deprivation data  

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Extract `lsoa` from GDPPR

# COMMAND ----------

lsoas = spark.sql("""
SELECT * FROM (SELECT
  NHS_NUMBER_DEID as person_id_deid,
  MAX(DATE) as date
FROM
  dars_nic_391419_j3w9t_collab.ccu003_direfcts_dataprep_1_gdppr_frzon28may_mm_210528
GROUP BY
  NHS_NUMBER_DEID ) as tab1
LEFT JOIN
  (SELECT 
    NHS_NUMBER_DEID as person_id_deid,
    DATE as date,
    LSOA as lsoa 
  FROM dars_nic_391419_j3w9t_collab.ccu003_direfcts_dataprep_1_gdppr_frzon28may_mm_210528) as tab2
ON
  tab1.person_id_deid = tab2.person_id_deid
  AND
  tab1.date = tab2.date
""")
# Unsure why duplicate persist so crude approach = 
lsoas = lsoas.dropDuplicates(['person_id_deid'])
# Remove duplicate cols by only selecting necessary and using tab1. prefix
lsoas = lsoas.select("tab1.person_id_deid", "lsoa")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2. Select deprivation variables from [`dss_corporate.english_indices_of_dep_v02`](https://db.core.data.digital.nhs.uk/#table/dss_corporate/english_indices_of_dep_v02)
# MAGIC NB selected from where IMD year = 2019 so most up to date as database contains earlier years. As we're looking at COVID we want the most recent.

# COMMAND ----------

deprivation = spark.sql("""
SELECT
  LSOA_CODE_2011,
  DECI_IMD
FROM
  dss_corporate.english_indices_of_dep_v02
WHERE
  IMD_YEAR = 2019
  """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Join!

# COMMAND ----------

# Join with Geographic data from GDPPR/SGSS to get LSOA
# LEFT join to retain all patients -> nulls where no LSOA
cohort = cohort.join(lsoas, "person_id_deid", "LEFT")

# Use LSOA to join with deprivation/geography
cohort = cohort.join(deprivation, cohort.lsoa == deprivation.LSOA_CODE_2011, "LEFT")
cohort = cohort.drop('LSOA_CODE_2011')

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

cohort = cohort.withColumn("IMD_quintile", mapping_expr[col("DECI_IMD")])

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Comorbidities

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1 High risk (shielding)
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

# Other options:
# (case when CODE = 1300571000000100 THEN 1 Else 0 End) as mod_risk,
# (case when CODE = 1300591000000101 THEN 1 Else 0 End) as low_risk

high_risk = spark.sql("""
SELECT
  DISTINCT NHS_NUMBER_DEID as person_id_deid,
  (case when CODE = 1300561000000107 THEN 1 Else 0 End) as high_risk
FROM
   dars_nic_391419_j3w9t_collab.ccu003_direfcts_dataprep_1_gdppr_frzon28may_mm_210528
WHERE
  CODE = 1300561000000107
""")

# LEFT join to add a comorbidity to cohort
cohort = cohort.join(high_risk, "person_id_deid", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Long covid
# MAGIC 
# MAGIC Using codelist: `dars_nic_391419_j3w9t_collab.ccu013_codelist_long_covid`

# COMMAND ----------

codelist_long_covid = spark.table("dars_nic_391419_j3w9t_collab.ccu013_codelist_long_covid") \
  .toPandas()['code'].tolist()

# COMMAND ----------

gdppr = spark.sql("""
SELECT 
  NHS_NUMBER_DEID as person_id_deid,
  CODE as code
FROM
   dars_nic_391419_j3w9t_collab.ccu003_direfcts_dataprep_1_gdppr_frzon28may_mm_210528""")

import pyspark.sql.functions as f

# Filter GDPPR to only long covid codes
pts_long_covid = gdppr.filter(f.col('code').isin(codelist_long_covid)).withColumnRenamed("code", 'long_covid')

from pyspark.sql.functions import when
# Clean to change code -> flag
pts_long_covid = pts_long_covid.withColumn('long_covid', when(pts_long_covid.long_covid.isNotNull(), 1))

# DISTINCT as want only 1 flag per patient
pts_long_covid = pts_long_covid.distinct()

# LEFT join to add a comorbidity to cohort
cohort = cohort.join(pts_long_covid, "person_id_deid", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Add descriptive paper comorbidities
# MAGIC 1. Prev Stroke/TIA
# MAGIC 2. Prev MI
# MAGIC 3. Prev Diabetes
# MAGIC 4. Prev Obesity  
# MAGIC [https://db.core.data.digital.nhs.uk/#notebook/1153457/command/1153458]

# COMMAND ----------

prev_stroke = spark.sql("""
SELECT
  DISTINCT person_id_deid,
  (case when Stroke_previous = '1' THEN 1 Else 0 End) as stroke_prev
FROM 
  dars_nic_391419_j3w9t_collab.descriptive_stroke_previous_distinct_table
WHERE
  Stroke_previous = 1
  """)

cohort = cohort.join(prev_stroke, "person_id_deid", "left")

# MI

prev_mi = spark.sql("""
SELECT
  DISTINCT person_id_deid,
  (case when MI_previous = '1' THEN 1 Else 0 End) as MI_prev
FROM 
  dars_nic_391419_j3w9t_collab.descriptive_mi_previous_distinct_table
WHERE
  MI_previous = 1
  """) 

cohort = cohort.join(prev_mi, "person_id_deid", "left")

# Diabetes

prev_dm = spark.sql("""
SELECT
  DISTINCT ID as person_id_deid,
  (case when EVER_DIAB = '1' THEN 1 Else 0 End) as diabetes_prev
FROM 
  dars_nic_391419_j3w9t_collab.covariate_flags_diabetes
WHERE
  EVER_DIAB = 1
  """) 

cohort = cohort.join(prev_dm, "person_id_deid", "left")

# Obesity

prev_obese = spark.sql("""
SELECT
  DISTINCT ID as person_id_deid,
  (case when EVER_OBESE = '1' THEN 1 Else 0 End) as obesity_prev
FROM 
  dars_nic_391419_j3w9t_collab.covariate_flags_obesity
WHERE
  EVER_OBESE = 1
  """) 

cohort = cohort.join(prev_obese, "person_id_deid", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Cleaning

# COMMAND ----------

# Regexp cleaning
from pyspark.sql.functions import regexp_replace
cohort = cohort \
  .withColumn('sex', regexp_replace('sex', '0', 'Unknown')) \
  .withColumn('sex', regexp_replace('sex', '9', 'Unknown'))
# NB Sex: 8 = unspecified, 9 = "Home Leave" See: https://datadictionary.nhs.uk/attributes/sex_of_patients.html

# COMMAND ----------

# Coalescing nulls
from pyspark.sql.functions import col, coalesce, lit
cohort = cohort \
  .withColumn('ethnic_group', coalesce(col('ethnic_group'), lit("Unknown"))) \
  .withColumn('age', coalesce(col('age'), lit("Unknown"))) \
  .withColumn('imd_quintile', coalesce(col('imd_quintile'), lit("Unknown"))) \
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

# Test no duplicates
# Updated to just select id column to improve computation speed
if cohort.select('person_id_deid').count() > cohort.select('person_id_deid').dropDuplicates(['person_id_deid']).count():
    raise ValueError('Cohort contains duplicate ids when should be mutually exclusive')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Commit & optimise

# COMMAND ----------

cohort.createOrReplaceGlobalTempView("ccu013_covid_events_demographics")
drop_table("ccu013_covid_events_demographics") 
create_table("ccu013_covid_events_demographics") 

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics ZORDER BY person_id_deid

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), COUNT(DISTINCT person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics
