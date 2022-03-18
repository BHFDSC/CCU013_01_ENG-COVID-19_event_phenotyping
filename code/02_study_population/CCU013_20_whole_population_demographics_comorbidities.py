# Databricks notebook source
# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU013/COVID-19-SEVERITY-PHENOTYPING/CCU013_00_helper_functions

# COMMAND ----------

# MAGIC %md
# MAGIC # 1) The STUDY population
# MAGIC From `ccu013_dp_skinny_patient_23_01_2020`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*), COUNT(Distinct NHS_NUMBER_DEID)
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1) The population without COVID

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), COUNT(Distinct NHS_NUMBER_DEID) FROM
# MAGIC (SELECT DISTINCT NHS_NUMBER_DEID FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as pop
# MAGIC ANTI JOIN
# MAGIC (SELECT DISTINCT person_id_deid as NHS_NUMBER_DEID FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort) as covid
# MAGIC ON pop.NHS_NUMBER_DEID = covid.NHS_NUMBER_DEID

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2) COVID cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), COUNT(Distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3) COVID cohort + non-COVID population

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   COUNT(*), COUNT(distinct person_id_deid)
# MAGIC FROM
# MAGIC (SELECT * FROM
# MAGIC   (SELECT DISTINCT NHS_NUMBER_DEID as person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as pop
# MAGIC   ANTI JOIN
# MAGIC   (SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort) as covid
# MAGIC   ON pop.person_id_deid = covid.person_id_deid
# MAGIC UNION ALL
# MAGIC SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4) Adding severity

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM
# MAGIC   (SELECT
# MAGIC     person_id_deid,
# MAGIC     severity
# MAGIC   FROM
# MAGIC     (SELECT 
# MAGIC       DISTINCT NHS_NUMBER_DEID as person_id_deid,
# MAGIC       "no_covid" as severity
# MAGIC     FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as pop
# MAGIC     ANTI JOIN
# MAGIC     (SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort) as covid
# MAGIC     ON pop.person_id_deid = covid.person_id_deid
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     person_id_deid, 
# MAGIC     severity 
# MAGIC   FROM 
# MAGIC     dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT severity, COUNT(*) FROM
# MAGIC   (SELECT
# MAGIC     person_id_deid,
# MAGIC     severity
# MAGIC   FROM
# MAGIC     (SELECT 
# MAGIC       DISTINCT NHS_NUMBER_DEID as person_id_deid,
# MAGIC       "no_covid" as severity
# MAGIC     FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as pop
# MAGIC     ANTI JOIN
# MAGIC     (SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort) as covid
# MAGIC     ON pop.person_id_deid = covid.person_id_deid
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     person_id_deid, 
# MAGIC     severity 
# MAGIC   FROM 
# MAGIC     dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
# MAGIC   )
# MAGIC GROUP BY severity

# COMMAND ----------

56609049-53139521

# COMMAND ----------

2163734 + 60184 + 931126 + 304081 + 10403

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5) Add date_first
# MAGIC * For the COVID-19 patients this will be the date of their first COVID-19 event
# MAGIC * For the non-COVID individuals this will be 01/01/2020

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM
# MAGIC   (SELECT
# MAGIC     person_id_deid,
# MAGIC     severity,
# MAGIC     date_first
# MAGIC   FROM
# MAGIC     (SELECT 
# MAGIC       DISTINCT NHS_NUMBER_DEID as person_id_deid,
# MAGIC       "no_covid" as severity,
# MAGIC       "2020-01-01" as date_first
# MAGIC     FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as pop
# MAGIC     ANTI JOIN
# MAGIC     (SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort) as covid
# MAGIC     ON pop.person_id_deid = covid.person_id_deid
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     person_id_deid, 
# MAGIC     severity,
# MAGIC     date_first
# MAGIC   FROM 
# MAGIC     dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 Add COVID-19 death

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM
# MAGIC   (SELECT
# MAGIC     person_id_deid,
# MAGIC     severity,
# MAGIC     date_first,
# MAGIC     death_covid
# MAGIC   FROM
# MAGIC     (SELECT 
# MAGIC       DISTINCT NHS_NUMBER_DEID as person_id_deid,
# MAGIC       "no_covid" as severity,
# MAGIC       "2020-01-01" as date_first,
# MAGIC       0 as death_covid
# MAGIC     FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as pop
# MAGIC     ANTI JOIN
# MAGIC     (SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort) as covid
# MAGIC     ON pop.person_id_deid = covid.person_id_deid
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     person_id_deid, 
# MAGIC     severity,
# MAGIC     date_first,
# MAGIC     death as death_covid
# MAGIC   FROM 
# MAGIC     dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC # 2) Build pyspark cohort

# COMMAND ----------

def test(table):
  assert table.count() == spark.table("dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020").count(), "Cohort has diverged in number from descriptive paper skinny table"
  assert table.select('person_id_deid').count() == table.select('person_id_deid').dropDuplicates(['person_id_deid']).count(), "Cohort contains duplicate ids when should be mutually exclusive"
  print("Tests passed")

# COMMAND ----------

cohort = spark.sql("""
SELECT * FROM
  (SELECT
    person_id_deid,
    severity,
    date_first,
    death_covid
  FROM
    (SELECT 
      DISTINCT NHS_NUMBER_DEID as person_id_deid,
      "no_covid" as severity,
      "2020-01-01" as date_first,
      0 as death_covid
    FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as pop
    ANTI JOIN
    (SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort) as covid
    ON pop.person_id_deid = covid.person_id_deid
  UNION ALL
  SELECT 
    person_id_deid, 
    severity,
    date_first,
    death as death_covid
  FROM 
    dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
  )
""")

test(cohort)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3) Add covariates

# COMMAND ----------

# Params
production_date = "2022-01-20 14:58:52.353312"
cohort_start = '2020-01-23' # For deaths
cohort_end = '2021-11-30' # For deaths MANUALLY CALCULATED FOR NOW across datasets

# Population table i.e. the denominator for our work
population_table = "dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020"

# COVID-19 events
covid_table = "dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort"
# Demographics table
demographics_table = "dars_nic_391419_j3w9t_collab.ccu013_master_demographics"
# GDPPR (for high risk)
gdppr_table = "dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive"
# Comorbidities table
comorbidities_table = "dars_nic_391419_j3w9t_collab.ccu013_caliber_categories_pre2020"
# Deaths table
deaths_table = "dars_nic_391419_j3w9t.deaths_dars_nic_391419_j3w9t"

# Output table (no dars_.. prefix)
output_table = "ccu013_paper_table_one_56million_denominator"# "ccu013_covid_events_demographics_paper_cohort"

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.1 Join demographics from [`ccu013_master_demographics`](https://db.core.data.digital.nhs.uk/#notebook/3284853/command/3284854)
# MAGIC Created in notebook [`CCU013/01_shared_resources/1_demographics`](https://db.core.data.digital.nhs.uk/#notebook/3284853/command/3284854)

# COMMAND ----------

demographics = spark.table(demographics_table)

cohort = cohort.join(demographics, "person_id_deid", "LEFT")

test(cohort)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2. Calculate age
# MAGIC For COVID patients we calculate age at time of first covid event (`date_first` in `ccu013_covid_events`)  
# MAGIC For non-COVID patients will calculate age at `01/01/2020`  

# COMMAND ----------

from pyspark.sql.functions import to_date, col, floor, datediff

cohort = cohort \
  .withColumn('dob', to_date(col('dob'))) \
  .withColumn('date_first', to_date(col('date_first'))) \
  .withColumn('age', floor(datediff(col('date_first'), col('dob'))/365.25)) \
  .drop('dob')

test(cohort)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 High risk (shielding)
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
  1 as high_risk
FROM
  {gdppr_table}
WHERE
  CODE = 1300561000000107
AND
  ProductionDate == "{production_date}"
""")

cohort = cohort.join(high_risk, "person_id_deid", "left") \
  .fillna(0, 'high_risk')

test(cohort)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4. Add comorbidities from [`ccu013_master_demographics`](https://db.core.data.digital.nhs.uk/#notebook/3284853/command/3284854)
# MAGIC Created in notebook [`CCU013/01_shared_resources/1_demographics`](https://db.core.data.digital.nhs.uk/#notebook/3284853/command/3284854)

# COMMAND ----------

comorbidities = spark.table(comorbidities_table)
comorbidities_names = comorbidities.schema.names[1:]
cohort = cohort.join(comorbidities, "person_id_deid", "left") \
  .fillna(0, subset = comorbidities_names)

test(cohort)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Add all cause mortality

# COMMAND ----------

import re

deaths_all = spark.sql(f"""
  SELECT
    distinct DEC_CONF_NHS_NUMBER_CLEAN_DEID as person_id_deid,
    1 as death_all
  FROM
    {deaths_table}
  WHERE 
    REG_DATE_OF_DEATH >= {re.sub('-','',cohort_start)}
  AND
    REG_DATE_OF_DEATH <= {re.sub('-','',cohort_end)}
  AND
    DEC_CONF_NHS_NUMBER_CLEAN_DEID is not null
  """)
cohort = cohort.join(deaths_all, 'person_id_deid', 'left') \
  .fillna(0, 'death_all')

test(cohort)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Commit table
# MAGIC cohort.createOrReplaceGlobalTempView(output_table)
# MAGIC drop_table(output_table) 
# MAGIC create_table(output_table) 

# COMMAND ----------

cohort.createOrReplaceGlobalTempView(output_table)
drop_table(output_table) 
create_table(output_table) 

# COMMAND ----------

spark.sql(f"OPTIMIZE dars_nic_391419_j3w9t_collab.{output_table} ZORDER BY person_id_deid")

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Checks
# MAGIC Ugly code but designed to test previous errors

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*),
# MAGIC   COUNT(distinct person_id_deid),
# MAGIC   SUM(CASE WHEN severity != 'no_covid' then 1 else 0 end) as NOT_no_covid,
# MAGIC   SUM(CASE WHEN severity = '0_positive'
# MAGIC       OR severity = '1_gp'
# MAGIC       OR severity = '2_hospitalised'
# MAGIC       OR severity = '3_critical_care'
# MAGIC       OR severity = '4_death_only' 
# MAGIC       then 1 else 0 end) as covid  
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t_collab.ccu013_paper_table_one_56million_denominator

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(distinct person_id_deid) as population,
# MAGIC   SUM(CASE WHEN severity = '0_positive'
# MAGIC       OR severity = '1_gp'
# MAGIC       OR severity = '2_hospitalised'
# MAGIC       OR severity = '3_critical_care'
# MAGIC       OR severity = '4_death_only' 
# MAGIC       then 1 else 0 end) as covid,
# MAGIC   round(SUM(CASE WHEN severity = '0_positive'
# MAGIC       OR severity = '1_gp'
# MAGIC       OR severity = '2_hospitalised'
# MAGIC       OR severity = '3_critical_care'
# MAGIC       OR severity = '4_death_only' 
# MAGIC       then 1 else 0 end) / COUNT(distinct person_id_deid) * 100, 2) as percent_covid
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t_collab.ccu013_paper_table_one_56million_denominator
