# Databricks notebook source
# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU013/COVID-19-SEVERITY-PHENOTYPING/CCU013_00_helper_functions

# COMMAND ----------

from pyspark.sql.functions import lit, to_date, col, udf, substring, regexp_replace, max, when, date_add
from pyspark.sql import functions as f
from datetime import datetime
from pyspark.sql.types import DateType

start_date = '2020-01-01'
end_date = '2021-07-29' # The maximal date covered by all sources.
# NB common cut-off data across all data sources is implemented in CCU013_13_paper_subset_data_to_cohort

# COMMAND ----------

# MAGIC %md
# MAGIC # All Samples

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Get first occurence of each phenotype

# COMMAND ----------

date_first = spark.sql("""
SELECT
  person_id_deid,
  min(date) as date_first
FROM 
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
GROUP BY
  person_id_deid
  """)

test_first = spark.sql("""
SELECT 
  person_id_deid, 
  1 as test,
  min(date) as test_first
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
WHERE
  covid_phenotype = '01_Covid_positive_test'
GROUP BY
  person_id_deid
  """)

gp_first = spark.sql("""
SELECT 
  person_id_deid, 
  1 as gp,
  min(date) as gp_first
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
WHERE
  covid_phenotype = '01_GP_covid_diagnosis'
GROUP BY
  person_id_deid
""")

hosp_first = spark.sql("""
SELECT 
  person_id_deid, 
  1 as hosp,
  min(date) as hosp_first
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
WHERE
  covid_phenotype = '02_Covid_admission'
GROUP BY
  person_id_deid
""")

icu_first = spark.sql("""
SELECT 
  person_id_deid, 
  1 as icu,
  min(date) as icu_first
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
WHERE
  covid_phenotype = '03_ICU_admission'
GROUP BY
  person_id_deid
""")

date_death = spark.sql("""
SELECT 
  person_id_deid, 
  1 as death,
  min(date) as date_death
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
WHERE
  covid_phenotype like '04_%'
GROUP BY
  person_id_deid
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Use left joins because we have `date_first` which every person is present in

# COMMAND ----------

events = date_first \
  .join(date_death, "person_id_deid", "left") \
  .fillna(0) # to get binary flags, won't work for dates as different type, which is fine

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Add mutex severity phenotype

# COMMAND ----------

severity = spark.sql("""
SELECT
  person_id_deid,
  CASE WHEN
    03_ICU_admission = 1
    THEN '4_icu_admission'
  WHEN
    03_ICU_admission = 0
    AND
    (03_ECMO_treatment = 1
    OR 03_IMV_treatment = 1
    OR 03_NIV_treatment = 1)
    THEN '03_critical_care_outside_ICU' 
  WHEN
    02_Covid_admission = 1
    THEN '2_hospitalised'
  WHEN
    01_GP_covid_diagnosis = 1
    THEN '1_gp'
  WHEN
    01_Covid_positive_test = 1
    THEN '0_positive'
  ELSE '5_death_only' END as severity
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_paper_cohort
""")

# COMMAND ----------

cohort = events \
  .join(severity, "person_id_deid", "left")

# COMMAND ----------

display(cohort)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Calculate Survival Times
# MAGIC Easiest to just do this using SQL CASE WHEN + DATEDIFF, therefore commit to temp. table. 

# COMMAND ----------

cohort.createOrReplaceGlobalTempView("ccu013_covid_trajectory_paper_cohort_date_severity")

cohort = spark.sql("""
SELECT
  *,
  (CASE WHEN time_to_death is not null then time_to_death WHEN time_to_death is null then DATEDIFF('2021-05-31', date_first) else 0 end) AS death_fu_time
FROM
(
SELECT 
  *,
  DATEDIFF(date_death, date_first) as time_to_death
FROM 
  global_temp.ccu013_covid_trajectory_paper_cohort_date_severity
)  
""")

cohort.createOrReplaceGlobalTempView("ccu013_covid_trajectory_paper_cohort_survival")
drop_table("ccu013_covid_trajectory_paper_cohort_survival")
create_table("ccu013_covid_trajectory_paper_cohort_survival")
optimise_table("ccu013_covid_trajectory_paper_cohort_survival")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_survival

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   severity,
# MAGIC   COUNT(distinct person_id_deid) as individuals,
# MAGIC   SUM(death) as deaths,
# MAGIC   ROUND(SUM(death) / COUNT(distinct person_id_deid) * 100, 2) as mortality
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_survival
# MAGIC GROUP BY 
# MAGIC   severity

# COMMAND ----------

# MAGIC %md
# MAGIC # Wave 1

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1. Subset trajectory table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query to get all events for individuals with the wave
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_trajectory_paper_cohort_wave1_tmp AS
# MAGIC SELECT DISTINCT t.person_id_deid, date, covid_phenotype
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort as t
# MAGIC INNER JOIN (SELECT distinct person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort WHERE date >= "2020-02-20" AND date <= "2020-05-29" group by person_id_deid) as w
# MAGIC ON t.person_id_deid = w.person_id_deid
# MAGIC WHERE date >= "2020-02-20"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query to get a minimum of 28 days followup for those individuals who present within the wave - AND exclude future events for individual who do have 28 days followup
# MAGIC --SELECT *
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_trajectory_paper_cohort_wave1
# MAGIC AS
# MAGIC SELECT person_id_deid, date, covid_phenotype, earliest_event, min_followup
# MAGIC FROM (
# MAGIC   SELECT t.person_id_deid, date, covid_phenotype, earliest_event, followup_time, min_followup, datediff(date, earliest_event) as date_diff,
# MAGIC   CASE WHEN datediff(date, earliest_event) <= 28 THEN 1 ELSE 0 END AS keep_time
# MAGIC   FROM global_temp.ccu013_covid_trajectory_paper_cohort_wave1_tmp AS t
# MAGIC   INNER JOIN (
# MAGIC     SELECT person_id_deid, min(date) AS earliest_event, DATEDIFF("2020-05-29", min(date)) AS followup_time,  
# MAGIC     CASE WHEN DATEDIFF("2020-05-29", min(date)) > 28 THEN 1 ELSE 0 END AS min_followup 
# MAGIC     FROM global_temp.ccu013_covid_trajectory_paper_cohort_wave1_tmp
# MAGIC     GROUP BY person_id_deid) as w
# MAGIC   ON t.person_id_deid = w.person_id_deid
# MAGIC )
# MAGIC WHERE 
# MAGIC (min_followup == 1 AND date <= "2020-05-29") --- Exclude future events for individual who do have 28 days followup
# MAGIC OR 
# MAGIC (min_followup ==0 AND keep_time == 1) --- Get a minimum of 28 days followup for those individuals who do not have 28 days of followup days to the end date
# MAGIC ORDER BY person_id_deid, date

# COMMAND ----------

wave = spark.sql('''SELECT *, TO_DATE('2020-05-29') as end_date FROM global_temp.ccu013_covid_trajectory_paper_cohort_wave1''')
# Get correct followup end date for sample which are followed additionally 28 days.
wave = wave.withColumn("followup_end", when(col("min_followup") == 1, col('end_date')).otherwise(date_add(col('earliest_event'),28)))
wave = wave.select('person_id_deid', 'date', 'covid_phenotype', 'followup_end')
wave.createOrReplaceGlobalTempView("ccu013_covid_trajectory_paper_cohort_wave1")
drop_table("ccu013_covid_trajectory_paper_cohort_wave1")
create_table("ccu013_covid_trajectory_paper_cohort_wave1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(date), max(date), min(followup_end), max(followup_end)  FROM global_temp.ccu013_covid_trajectory_paper_cohort_wave1

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2. Create wave 1 events table 

# COMMAND ----------

# Use koalas as pivots nicely
import databricks.koalas as ks

events = spark.sql("""
SELECT 
  person_id_deid, 
  covid_phenotype as event,
  1 as value
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_wave1
""") \
  .to_koalas() \
  .pivot(index='person_id_deid',
         columns='event', 
         values='value') \
  .fillna(0) \
  . reset_index() \
  .to_spark()

date_first = spark.sql("""
SELECT
  person_id_deid,
  min(date) as date_first
FROM 
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_wave1
GROUP BY
  person_id_deid
  """)

date_death = spark.sql("""
SELECT 
  person_id_deid, 
  1 as death,
  min(date) as date_death
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_wave1
WHERE
  covid_phenotype like '04_%'
GROUP BY
  person_id_deid
""")

# Join to create cohort
cohort  = date_first \
          .join(date_death, 
                "person_id_deid", 
                "left")

# Add on events
cohort  = cohort \
          .join(events, 
                "person_id_deid", 
                "left") \
          .fillna(0)

display(cohort)


# COMMAND ----------

cohort.createOrReplaceGlobalTempView("ccu013_covid_events_paper_cohort_wave1")
drop_table("ccu013_covid_events_paper_cohort_wave1")
create_table("ccu013_covid_events_paper_cohort_wave1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Add mutex severity phenotype

# COMMAND ----------

severity = spark.sql("""
SELECT
  person_id_deid,
  CASE WHEN
    03_ICU_admission = 1
    THEN '4_icu_admission'
  WHEN
    03_ICU_admission = 0
    AND
    (03_ECMO_treatment = 1
    OR 03_IMV_treatment = 1
    OR 03_NIV_treatment = 1)
    THEN '03_critical_care_outside_ICU' 
  WHEN
    02_Covid_admission = 1
    THEN '2_hospitalised'
  WHEN
    01_GP_covid_diagnosis = 1
    THEN '1_gp'
  WHEN
    01_Covid_positive_test = 1
    THEN '0_positive'
  ELSE '5_death_only' END as severity
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_paper_cohort_wave1
""")
display(severity)

# COMMAND ----------

cohort = cohort \
  .join(severity, "person_id_deid", "left")
cohort = cohort.select("person_id_deid", "date_first", "death", "date_death", "severity")
cohort.createOrReplaceGlobalTempView("ccu013_covid_trajectory_paper_cohort_date_severity_wave1")

display(cohort)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Calculate Survival

# COMMAND ----------

cohort = spark.sql("""
SELECT
  *,
  (CASE WHEN time_to_death is not null then time_to_death WHEN time_to_death is null then DATEDIFF(followup_end, date_first) else 0 end) AS death_fu_time
FROM
(
SELECT 
  s.person_id_deid, date_first, death, date_death, severity, followup_end, 
  DATEDIFF(date_death, date_first) as time_to_death
FROM 
  global_temp.ccu013_covid_trajectory_paper_cohort_date_severity_wave1 as s
INNER JOIN (SELECT DISTINCT person_id_deid, followup_end FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_wave1) as t
ON s.person_id_deid = t.person_id_deid
)
""")

cohort.createOrReplaceGlobalTempView("ccu013_covid_trajectory_paper_cohort_survival_wave1")
drop_table("ccu013_covid_trajectory_paper_cohort_survival_wave1")
create_table("ccu013_covid_trajectory_paper_cohort_survival_wave1")
optimise_table("ccu013_covid_trajectory_paper_cohort_survival_wave1")
display(cohort)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Wave 2

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1. Subset trajectory table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query to get all events for individuals with the wave
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_trajectory_paper_cohort_wave2_tmp AS
# MAGIC SELECT DISTINCT t.person_id_deid, date, covid_phenotype
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort as t
# MAGIC INNER JOIN (SELECT distinct person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort WHERE date >= "2020-09-30" AND date <= "2021-02-12" group by person_id_deid) as w
# MAGIC ON t.person_id_deid = w.person_id_deid
# MAGIC WHERE date >= "2020-09-30"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query to get a minimum of 28 days followup for those individuals who present within the wave - AND exclude future events for individual who do have 28 days followup
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_trajectory_paper_cohort_wave2
# MAGIC AS
# MAGIC SELECT person_id_deid, date, covid_phenotype, earliest_event, min_followup
# MAGIC FROM (
# MAGIC   SELECT t.person_id_deid, date, covid_phenotype, earliest_event, followup_time, min_followup, datediff(date, earliest_event) as date_diff,
# MAGIC   CASE WHEN datediff(date, earliest_event) <= 28 THEN 1 ELSE 0 END AS keep_time
# MAGIC   FROM global_temp.ccu013_covid_trajectory_paper_cohort_wave2_tmp AS t
# MAGIC   INNER JOIN (
# MAGIC     SELECT person_id_deid, min(date) AS earliest_event, DATEDIFF("2021-02-12", min(date)) AS followup_time,  
# MAGIC     CASE WHEN DATEDIFF("2021-02-12", min(date)) > 28 THEN 1 ELSE 0 END AS min_followup 
# MAGIC     FROM global_temp.ccu013_covid_trajectory_paper_cohort_wave2_tmp
# MAGIC     GROUP BY person_id_deid) as w
# MAGIC   ON t.person_id_deid = w.person_id_deid
# MAGIC )
# MAGIC WHERE 
# MAGIC (min_followup == 1 AND date <= "2021-02-12") --- Exclude future events for individual who do have 28 days followup
# MAGIC OR 
# MAGIC (min_followup ==0 AND keep_time == 1) --- Get a minimum of 28 days followup for those individuals who do not have 28 days of followup days to the end date
# MAGIC --- WHERE t.person_id_deid = "005FAFO75GJPVWI" ---  Example of person with min 28 days followup AND events happening after wave 2
# MAGIC --- WHERE t.person_id_deid == "2ZLQ8T35T4811VV" --- Example of person with out min 28 days followyup AND events happening after wave 2
# MAGIC ORDER BY person_id_deid, date

# COMMAND ----------

wave = spark.sql('''SELECT *, TO_DATE('2021-02-12') as end_date FROM global_temp.ccu013_covid_trajectory_paper_cohort_wave2''')
# Get correct followup end date for sample which are followed additionally 28 days.
wave = wave.withColumn("followup_end", when(col("min_followup") == 1, col('end_date')).otherwise(date_add(col('earliest_event'),28)))
wave = wave.select('person_id_deid', 'date', 'covid_phenotype', 'followup_end')
wave.createOrReplaceGlobalTempView("ccu013_covid_trajectory_paper_cohort_wave2")
drop_table("ccu013_covid_trajectory_paper_cohort_wave2")
create_table("ccu013_covid_trajectory_paper_cohort_wave2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min (date), max(date), min(followup_end), max(followup_end) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_wave2

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2. Create wave 2 events table 

# COMMAND ----------

# Use koalas as pivots nicely
import databricks.koalas as ks

events = spark.sql("""
SELECT 
  person_id_deid, 
  covid_phenotype as event,
  1 as value
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_wave2
""") \
  .to_koalas() \
  .pivot(index='person_id_deid',
         columns='event', 
         values='value') \
  .fillna(0) \
  . reset_index() \
  .to_spark()

date_first = spark.sql("""
SELECT
  person_id_deid,
  min(date) as date_first
FROM 
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_wave2
GROUP BY
  person_id_deid
  """)

date_death = spark.sql("""
SELECT 
  person_id_deid, 
  1 as death,
  min(date) as date_death
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_wave2
WHERE
  covid_phenotype like '04_%'
GROUP BY
  person_id_deid
""")

# Join to create cohort
cohort  = date_first \
          .join(date_death, 
                "person_id_deid", 
                "left")

# Add on events
cohort  = cohort \
          .join(events, 
                "person_id_deid", 
                "left") \
          .fillna(0)

display(cohort)


# COMMAND ----------

cohort.createOrReplaceGlobalTempView("ccu013_covid_events_paper_cohort_wave2")
drop_table("ccu013_covid_events_paper_cohort_wave2")
create_table("ccu013_covid_events_paper_cohort_wave2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Add mutex severity phenotype

# COMMAND ----------

severity = spark.sql("""
SELECT
  person_id_deid,
  CASE WHEN
    03_ICU_admission = 1
    THEN '4_icu_admission'
  WHEN
    03_ICU_admission = 0
    AND
    (03_ECMO_treatment = 1
    OR 03_IMV_treatment = 1
    OR 03_NIV_treatment = 1)
    THEN '03_critical_care_outside_ICU' 
  WHEN
    02_Covid_admission = 1
    THEN '2_hospitalised'
  WHEN
    01_GP_covid_diagnosis = 1
    THEN '1_gp'
  WHEN
    01_Covid_positive_test = 1
    THEN '0_positive'
  ELSE '5_death_only' END as severity
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_paper_cohort_wave2
""")
display(severity)

# COMMAND ----------

cohort = cohort \
  .join(severity, "person_id_deid", "left")
cohort = cohort.select("person_id_deid", "date_first", "death", "date_death", "severity")
cohort.createOrReplaceGlobalTempView("ccu013_covid_trajectory_paper_cohort_date_severity_wave2")
display(cohort)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Calculate Survival

# COMMAND ----------

cohort = spark.sql("""
SELECT
  *,
  (CASE WHEN time_to_death is not null then time_to_death WHEN time_to_death is null then DATEDIFF(followup_end, date_first) else 0 end) AS death_fu_time
FROM
(
SELECT 
  s.person_id_deid, date_first, death, date_death, severity, followup_end, 
  DATEDIFF(date_death, date_first) as time_to_death
FROM 
  global_temp.ccu013_covid_trajectory_paper_cohort_date_severity_wave2 as s
INNER JOIN (SELECT DISTINCT person_id_deid, followup_end FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_wave2) as t
ON s.person_id_deid = t.person_id_deid
)  
""")
cohort.createOrReplaceGlobalTempView("ccu013_covid_trajectory_paper_cohort_survival_wave2")
drop_table("ccu013_covid_trajectory_paper_cohort_survival_wave2")
create_table("ccu013_covid_trajectory_paper_cohort_survival_wave2")
optimise_table("ccu013_covid_trajectory_paper_cohort_survival_wave2")
display(cohort)

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Test to see if anyone in the wave 2 cohort have had covid events prior to wave 2
# MAGIC SELECT min(date), max(date) FROM 
# MAGIC dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort as a
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_survival_wave2 as b
# MAGIC ON a.person_id_deid = b.person_id_deid

# COMMAND ----------

# MAGIC %md
# MAGIC # Wave 2 - vax/unvax
# MAGIC - We are working with four groups
# MAGIC   1. Ever unvaccinated - No dose 1 or 2 (N = 1,117,322; 39%)
# MAGIC   2. Ever vaccinated with dose 2 before event (N = 964 ; 0.03%)
# MAGIC   3. Ever vaccinated with dose 2 after event 
# MAGIC   4. Others (vaccinated only with dose 1)
# MAGIC - Of those groups our plot will compare group 1 and 2 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_wave2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_survival_wave2

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Wave 2 samples only!
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_trajectory_paper_cohort_wave2
# MAGIC as
# MAGIC SELECT a.person_id_deid, earliest_event, covid_death, dose2, SEX, ETHNIC, DATE_OF_BIRTH, DATE_OF_DEATH,
# MAGIC CASE WHEN DATEDIFF(earliest_event, dose2) > 14 THEN 1 ELSE 0 END as dose2_prior_to_event
# MAGIC FROM (SELECT DISTINCT NHS_NUMBER_DEID as person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as a
# MAGIC LEFT JOIN (SELECT person_id_deid, min(date) as earliest_event FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_wave2 group by person_id_deid) as t
# MAGIC ON a.person_id_deid = t.person_id_deid
# MAGIC LEFT JOIN (SELECT person_id_deid, min(date) as dose2 FROM dars_nic_391419_j3w9t_collab.ccu013_vaccine_status_temp WHERE DOSE_SEQUENCE == 2 GROUP BY person_id_deid) as v 
# MAGIC ON a.person_id_deid = v.person_id_deid
# MAGIC LEFT JOIN (SELECT person_id_deid, death as covid_death FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_survival_wave2) as d
# MAGIC ON a.person_id_deid = d.person_id_deid
# MAGIC LEFT JOIN (SELECT NHS_NUMBER_DEID as person_id_deid, SEX, ETHNIC, DATE_OF_BIRTH, DATE_OF_DEATH FROM dars_nic_391419_j3w9t_collab.curr302_patient_skinny_record) as j
# MAGIC ON a.person_id_deid = j.person_id_deid
# MAGIC WHERE DATE_OF_DEATH is NULL or DATE_OF_DEATH >= "2020-09-30"

# COMMAND ----------

# MAGIC %sql
# MAGIC select (SUM(dose2_prior_to_event)) FROM global_temp.ccu013_covid_trajectory_paper_cohort_wave2

# COMMAND ----------

# MAGIC  %sql
# MAGIC  SELECT count(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_survival_wave2 a
# MAGIC  LEFT ANTI JOIN  dars_nic_391419_j3w9t_collab.ccu013_vaccine_status_temp as b ON a.person_id_deid = b.person_id_deid
