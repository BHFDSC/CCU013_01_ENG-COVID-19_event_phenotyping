# Databricks notebook source
# MAGIC %md
# MAGIC # COVID-19 Severity Phenotypes: Build Events Table
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook runs a list of pyspark operations to produce a master table `ccu013_covid_events`  
# MAGIC <br>
# MAGIC   
# MAGIC 1. Extracts distinct COVID events, per individual, from `trajectory_table`, producing binary outcome matrix
# MAGIC 2. Extract from `trajectory_table`:
# MAGIC     1. `date_first` = Date of first COVID event, i.e. best estimate of onset
# MAGIC     2. `first_event` = Which event was the first event to occur for that patient
# MAGIC     2. `date_death`  
# MAGIC 3. Add additional flags:  
# MAGIC     3.1. `0_Covid_infection` as catch-all  
# MAGIC     3.2. `death_covid` if COVID-19 on death certificate, at any position  
# MAGIC     3.3. `severity` mutually exclusive worst healthcare event, not including death unless death the only dataset from which patient is ascertained  
# MAGIC     3.4. `critical_care` aggregated variable  
# MAGIC 4. Joins to produce cohort  
# MAGIC     4.2. Tests no duplicates  
# MAGIC     4.3. Creates delta table & optimises  
# MAGIC 
# MAGIC **Project(s)** CCU013
# MAGIC  
# MAGIC **Author(s)** Chris Tomlinson
# MAGIC  
# MAGIC **Reviewer(s)** ⚠ UNREVIEWED
# MAGIC  
# MAGIC **Date last updated** 2021-09-09
# MAGIC  
# MAGIC **Date last reviewed** *NA*
# MAGIC  
# MAGIC **Date last run** 2021-09-09
# MAGIC  
# MAGIC **Data input**  
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory` Specified in cell 4 below  
# MAGIC 
# MAGIC **Data output**  
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu013_covid_events` 1 row per patient
# MAGIC 
# MAGIC **Software and versions** `SQL`, `Python`
# MAGIC  
# MAGIC **Packages and versions** `pyspark`, `koalas`

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU013/COVID-19-SEVERITY-PHENOTYPING/CCU013_00_helper_functions

# COMMAND ----------

import databricks.koalas as ks
from pyspark.sql.functions import lit

# COMMAND ----------

# Input tables
trajectory_table = "dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory"
# Output table
# NB no dars_.. prefix
output_table = "ccu013_covid_events"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1) Extract distinct COVID events, per individual

# COMMAND ----------

events = spark.sql(f"""
SELECT 
  person_id_deid, 
  covid_phenotype as event,
  1 as value
FROM
  {trajectory_table}
""") \
  .to_koalas() \
  .pivot(index='person_id_deid',
         columns='event', 
         values='value') \
  .fillna(0) \
  .reset_index() \
  .to_spark()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2) Extract dates

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Date of first covid event
# MAGIC * NB could be any type of event: test/hospital/death etc.
# MAGIC * `first_event` variable for event type added 16/06/21 @ 09:27 at request of Spiros, see Slack

# COMMAND ----------

# Select date of first covid event + event type
date_first = spark.sql(f"""
SELECT
  person_id_deid,
    --- Use first instead of grouping with covid_phenotype as that will produce multiples!
  FIRST(covid_phenotype) as first_event,
  MIN(date) as date_first
FROM 
  {trajectory_table}
GROUP BY 
  person_id_deid
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2) Date of Death

# COMMAND ----------

# Select death date
date_death = spark.sql(f"""
SELECT
  person_id_deid,
  1 as death,
  -- min() so first mention of death in case of multiples (shouldn't occur)
  MIN(date) as date_death
FROM 
  {trajectory_table}
WHERE 
  covid_phenotype like '04%'
GROUP BY 
  person_id_deid
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Add additional flags

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 `0_Covid_infection` catch-all 
# MAGIC Suggested by Alex

# COMMAND ----------

events = events.withColumn("0_Covid_infection", lit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Create `death_covid` to indicate if COVID-19 on the death certificate
# MAGIC Flag added 29/06/21 @ 16:37 following request from Spiros, see Slack

# COMMAND ----------

death_covid = spark.sql(f"""
SELECT
  distinct person_id_deid,
  1 as death_covid
FROM 
  {trajectory_table}
WHERE 
  covid_phenotype = '04_Fatal_with_covid_diagnosis'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 `severity` (mutually-exclusive)

# COMMAND ----------

# Easiest to just create temporary events table so can use existing SQL
events.createOrReplaceGlobalTempView("ccu013_covid_events_tmp_for_severity")
severity = spark.sql("""
SELECT
  person_id_deid,
  CASE WHEN 
    03_ECMO_treatment = 1
    OR 03_ICU_admission = 1
    OR 03_IMV_treatment = 1
    OR 03_NIV_treatment = 1
    THEN '3_critical_care' 
  WHEN
    02_Covid_admission = 1
    THEN '2_hospitalised'
  WHEN
    01_GP_covid_diagnosis = 1
    THEN '1_gp'
  WHEN
    01_Covid_positive_test = 1
    THEN '0_positive'
  ELSE '4_death_only' END as severity
FROM
  global_temp.ccu013_covid_events_tmp_for_severity
  """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 `critical_care` aggregate variable
# MAGIC Chris: I find myself implementing this frequently in SQL for R analysis therefore will incorporate here

# COMMAND ----------

critical_care = spark.sql(f"""
SELECT
  distinct person_id_deid,
  1 as critical_care
FROM 
  {trajectory_table}
WHERE 
  covid_phenotype like "03_%"
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # 4) Join to produce cohort

# COMMAND ----------

cohort  = date_first \
          .join(date_death, 
                "person_id_deid", 
                "left") \
          .join(death_covid, 
                "person_id_deid", 
                "left") \
          .join(events, 
                "person_id_deid", 
                "left") \
          .join(severity, 
                "person_id_deid", 
                "left") \
          .join(critical_care, 
                "person_id_deid", 
                "left") \
          .fillna(0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Test no duplicates

# COMMAND ----------

# Test no duplicates
# Updated to just select id column to improve computation speed?
if cohort.select('person_id_deid').count() > cohort.select('person_id_deid').dropDuplicates(['person_id_deid']).count():
    raise ValueError('Cohort contains duplicate ids when should be mutually exclusive')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Write & optimise table

# COMMAND ----------

cohort.createOrReplaceGlobalTempView(output_table)
drop_table(output_table)
create_table(output_table)

# COMMAND ----------

spark.sql(f"OPTIMIZE dars_nic_391419_j3w9t_collab.{output_table} ZORDER BY person_id_deid")

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(distinct person_id_deid)
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t_collab.ccu013_covid_events

# COMMAND ----------

# Get schema
display(spark.sql(f"SHOW COLUMNS FROM dars_nic_391419_j3w9t_collab.{output_table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Depreciated
# MAGIC Creation of survival data, abandoned as will just present the raw dates and others can take their own approaches
# MAGIC 
# MAGIC 2. Create `fu_time` for survival analysis
# MAGIC     1. Extracts date of last death from table
# MAGIC     2. Coalesces this with `date_death` if that occurred first -> `fu_date`
# MAGIC     3. Computes the difference between `fu_date` & `date_first` -> `fu_time`

# COMMAND ----------

# from pyspark.sql.functions import coalesce, unix_timestamp, to_timestamp, col, lit

# cohort = cohort \
#       .withColumn('fu_date', 
#                   # death as first argument so takes priority over fu_date
#                   coalesce('date_death', 
#                            'fu_date')
#                  ) \
#       .withColumn('fu_time',
#                   (
#                     (unix_timestamp(to_timestamp(col('fu_date')))) - 
#                     (unix_timestamp(to_timestamp(col('date_first'))))
#                   ) /lit(86400) # Convert to days
#                   )
