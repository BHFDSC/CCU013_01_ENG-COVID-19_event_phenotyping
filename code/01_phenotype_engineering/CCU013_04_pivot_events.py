# Databricks notebook source
# MAGIC %md
# MAGIC # COVID-19 Severity Phenotypes: Build Events Table
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook runs a list of pyspark operations to produce a master table `ccu013_covid_events`  
# MAGIC   
# MAGIC   
# MAGIC 1. Extracts distinct COVID events, per individual, from `dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory`
# MAGIC     1. Produces binary outcome matrix
# MAGIC 2. Extract from `dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory`:
# MAGIC     1. `date_first` = Date of first COVID event, i.e. best estimate of onset
# MAGIC     2. `first_event` = Which event was the first event to occur for that patient
# MAGIC     2. `date_death`
# MAGIC 3. Joins to produce cohort
# MAGIC 4. Creates delta table & optimises
# MAGIC 
# MAGIC **Project(s)** CCU013
# MAGIC  
# MAGIC **Author(s)** Chris Tomlinson
# MAGIC  
# MAGIC **Reviewer(s)** ⚠ UNREVIEWED
# MAGIC  
# MAGIC **Date last updated** 2021-05-26
# MAGIC  
# MAGIC **Date last reviewed** *NA*
# MAGIC  
# MAGIC **Date last run** 2021-06-15
# MAGIC  
# MAGIC **Data input**  
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory`  
# MAGIC 
# MAGIC **Data output**  
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu013_covid_events` 1 row per patient
# MAGIC 
# MAGIC **Software and versions** `SQL`, `Python`
# MAGIC  
# MAGIC **Packages and versions** `pyspark`, `koalas`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Extract distinct COVID events, per individual

# COMMAND ----------

# Use koalas as pivots nicelyas in 
import databricks.koalas as ks

events = spark.sql("""
SELECT 
  person_id_deid, 
  covid_phenotype as event,
  1 as value
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
""") \
  .to_koalas() \
  .pivot(index='person_id_deid',
         columns='event', 
         values='value') \
  .fillna(0) \
  . reset_index() \
  .to_spark()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Extract dates
# MAGIC ### 2.1 Date of first covid event
# MAGIC NB could be any type of event: test/hospital/death etc.

# COMMAND ----------

# Select date of first covid event + event type
date_first = spark.sql("""
SELECT
  person_id_deid,
  MIN(date) as date_first,
  --- Use first instead of grouping with covid_phenotype as that will produce multiples!
  FIRST(covid_phenotype) as first_event
FROM 
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
GROUP BY 
  person_id_deid
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2) Date of Death

# COMMAND ----------

# Select death date
date_death = spark.sql("""
SELECT
  person_id_deid,
  -- min() so first mention of death in case of multiples (shouldn't occur)
  MIN(date) as date_death,
  1 as death
FROM 
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
WHERE 
  covid_phenotype like '04%'
GROUP BY 
  person_id_deid
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Join to produce cohort

# COMMAND ----------

# Join to create cohort
cohort  = date_first \
          .join(date_death, 
                "person_id_deid", 
                "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add a catch-all `0_Covid_infection`

# COMMAND ----------

# Add 0_Covid_infection
from pyspark.sql.functions import lit

cohort = cohort.withColumn("0_Covid_infection", lit(1))

# COMMAND ----------

# Add on events
cohort  = cohort \
          .join(events, 
                "person_id_deid", 
                "left") \
          .fillna(0)

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU013/COVID-19-SEVERITY-PHENOTYPING/CCU013_00_helper_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test no duplicates

# COMMAND ----------

# Test no duplicates
# Updated to just select id column to improve computation speed?
if cohort.select('person_id_deid').count() > cohort.select('person_id_deid').dropDuplicates(['person_id_deid']).count():
    raise ValueError('Cohort contains duplicate ids when should be mutually exclusive')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Write & optimise table

# COMMAND ----------

cohort.createOrReplaceGlobalTempView("ccu013_covid_events")
drop_table("ccu013_covid_events")
create_table("ccu013_covid_events")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_covid_events ZORDER BY person_id_deid

# COMMAND ----------

# MAGIC %md
# MAGIC # Future plans
# MAGIC Intention to add date column for each event here.  
# MAGIC 
# MAGIC 
# MAGIC Plan:
# MAGIC 1. Pivot with min(date) pre-fix values with date -> `event_dates`
# MAGIC 2. Join to the above `events` to give dates for each event
# MAGIC   
# MAGIC Currently not implemented due to koalas inability to pivot values of class dates.

# COMMAND ----------

# phenos = spark.sql("""
# SELECT 
#   DISTINCT covid_phenotype
# FROM
#   dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
# """)
# # To List
# phenos = [row[0] for row in phenos.collect()]

# COMMAND ----------

# event_dates = spark.sql("""
# SELECT 
#   person_id_deid, 
#   covid_phenotype as event,
#   MIN(date) as date
# FROM
#   dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
# GROUP BY
#   person_id_deid,
#   covid_phenotype
# """) \
#   .to_koalas() \
#   .pivot(index='person_id_deid',
#          columns='event', 
#          values='date') \
#   .fillna(0) \
#   . reset_index() \
#   .to_spark()

# display(event_dates)

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
