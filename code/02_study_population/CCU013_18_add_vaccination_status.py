# Databricks notebook source
# MAGIC %md
# MAGIC  # Add vaccination status
# MAGIC  
# MAGIC **Description** 
# MAGIC   
# MAGIC This notebook identifies vaccination status for our paper cohort  
# MAGIC 
# MAGIC **Project(s)** CCU013
# MAGIC  
# MAGIC **Author(s)** Johan Thygesen, Chris Tomlinson
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2021-08-18
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** 2021-08-18
# MAGIC  
# MAGIC **Data input** 
# MAGIC 
# MAGIC **Data output**
# MAGIC 
# MAGIC **Software and versions** `sql`, `python`
# MAGIC  
# MAGIC **Packages and versions** `pyspark`

# COMMAND ----------

from pyspark.sql.functions import lit, to_date, col, udf, substring, regexp_replace, max
from pyspark.sql import functions as f
from datetime import datetime
from pyspark.sql.types import DateType

start_date = '2020-01-01'
end_date = '2021-05-31' # The study end date

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU013/COVID-19-SEVERITY-PHENOTYPING/CCU013_00_helper_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0 Subset vaccination table to our cohort

# COMMAND ----------

# Get the production date from the GDPPR table - set in notebook _01
# [production_date] is destructuring, expecting spark.sql to return a list of exactly one value similar to result = spark.sql...; result[0].value
[production_date] = spark.sql("SELECT DISTINCT ProductionDate as value from dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory  LIMIT 1").collect()

# COMMAND ----------

# Get the vaccine data using the production date
vaccine = spark.sql(f'''SELECT PERSON_ID_DEID, DOSE_SEQUENCE, DATE_AND_TIME, ProductionDate FROM dars_nic_391419_j3w9t_collab.vaccine_status_dars_nic_391419_j3w9t_archive 
                        WHERE ProductionDate == "{production_date.value}"''')
vaccine = vaccine.withColumnRenamed('PERSON_ID_DEID', 'person_id_deid')
vaccine = vaccine.withColumn('date',substring('DATE_AND_TIME', 0,8))
vaccine = vaccine.withColumn('date', to_date(vaccine['date'], "yyyyMMdd"))
## Remove vaccination data before 8th of december - first official vaccination date
vaccine = vaccine.filter((vaccine['date'] > '2020-12-08') & (vaccine['date'] <= end_date))
vaccine = vaccine.withColumn('date_is', lit('DATE_AND_TIME'))
vaccine = vaccine.select('person_id_deid', 'date', 'DOSE_SEQUENCE', 'ProductionDate')
vaccine.createOrReplaceGlobalTempView('ccu013_vaccine_status_temp')
drop_table("ccu013_vaccine_status_temp")
create_table("ccu013_vaccine_status_temp")
#display(vaccine)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate date difference for vaccine dosages for our covid_trajectory sample 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_vaccine_status_paper_cohort AS
# MAGIC SELECT t.person_id_deid, earliest_event, dose1, dose2, 
# MAGIC DATEDIFF(earliest_event, dose1) as date_diff_dose1,
# MAGIC DATEDIFF(earliest_event, dose2) as date_diff_dose2,
# MAGIC CASE WHEN DATEDIFF(earliest_event, dose1) > 14 THEN 1 ELSE 0 END as dose1_prior_to_event,
# MAGIC CASE WHEN DATEDIFF(earliest_event, dose2) > 14 THEN 1 ELSE 0 END as dose2_prior_to_event
# MAGIC FROM (SELECT person_id_deid, min(date) as earliest_event FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory group by person_id_deid) as t
# MAGIC LEFT JOIN (SELECT person_id_deid, min(date) as dose1 FROM dars_nic_391419_j3w9t_collab.ccu013_vaccine_status_temp WHERE DOSE_SEQUENCE == 1 GROUP BY person_id_deid) as v 
# MAGIC ON t.person_id_deid = v.person_id_deid
# MAGIC LEFT JOIN (SELECT person_id_deid, min(date) as dose2 FROM dars_nic_391419_j3w9t_collab.ccu013_vaccine_status_temp WHERE DOSE_SEQUENCE == 2 GROUP BY person_id_deid) as f
# MAGIC ON t.person_id_deid = f.person_id_deid
# MAGIC INNER JOIN (SELECT person_id_deid, death FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_survival) as d
# MAGIC ON t.person_id_deid = d.person_id_deid

# COMMAND ----------

drop_table("ccu013_vaccine_status_paper_cohort")
create_table("ccu013_vaccine_status_paper_cohort")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_vaccine_status_paper_cohort

# COMMAND ----------

# MAGIC %md
# MAGIC # Compareing outcome in vaccinated vs unvaccinated during wave 2

# COMMAND ----------

# MAGIC %md
# MAGIC - Generates two datasets for exploring the effects of vaccination on covid phenotypes
# MAGIC - This is done for all people who are alive and have not had a covid event prior to the start date of interest (wave 2 start 2020-09-30 or match start 2020-02-01)
# MAGIC    - 1) a dateset capturing vax (minimum 14 days prior to first covid event) vs unvax - through wave 2
# MAGIC    - 2) a dataset of vax vs unvax matched on (age, gender and ethnicity) from 1st feb 2021 and the following 48 days

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0 Dose2 prior to first covid-19 event through wave2 

# COMMAND ----------

# MAGIC %sql
# MAGIC --- N = 56,609,049 - Total population
# MAGIC SELECT count(distinct NHS_NUMBER_DEID) FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Get people who have died before wave2
# MAGIC SELECT count(distinct NHS_NUMBER_DEID) 
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020 AS a
# MAGIC RIGHT JOIN (SELECT person_id_deid, min(death_date) as death_date FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_deaths group by person_id_deid) as b
# MAGIC ON a.NHS_NUMBER_DEID = b.person_id_deid
# MAGIC WHERE death_date < "2020-09-30" OR death_date is null 

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Get all individuals who had a reported COVID-19 event prior to wave 2 start
# MAGIC SELECT count(distinct a.person_id_deid) FROM (SELECT DISTINCT NHS_NUMBER_DEID as person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as a
# MAGIC INNER JOIN (SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort WHERE date < "2020-09-30") as t
# MAGIC ON a.person_id_deid = t.person_id_deid

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Get all individuals WITHOUT a reported COVID-19 event prior to wave 2 AND OR who are not DEAD pre date of interset
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_no_covid_before_wave2 AS
# MAGIC SELECT a.person_id_deid FROM (SELECT DISTINCT NHS_NUMBER_DEID as person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as a
# MAGIC LEFT ANTI JOIN (SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort WHERE date < "2020-09-30") as t
# MAGIC ON a.person_id_deid = t.person_id_deid
# MAGIC LEFT JOIN (SELECT person_id_deid, min(death_date) as death_date FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_deaths group by person_id_deid) as c
# MAGIC ON a.person_id_deid = c.person_id_deid
# MAGIC WHERE death_date > "2020-09-30" OR death_date is null 

# COMMAND ----------

# MAGIC %sql
# MAGIC --- N = 55,774,208 - individuals WITHOUT a reported COVID-19 event prior to date and or All Alive!
# MAGIC SELECT count(distinct person_id_deid) FROM global_temp.ccu013_no_covid_before_wave2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_no_covid_before_wave2_vax_status AS
# MAGIC SELECT *, 
# MAGIC CASE WHEN dose2_prior_to_date == 1 THEN 'Vaccinated'
# MAGIC WHEN (dose1_prior_to_date == 0 OR dose1_prior_to_date IS NULL) AND (dose2_prior_to_date == 0 OR dose2_prior_to_date IS NULL) THEN 'Unvaccinated'
# MAGIC WHEN dose1_prior_to_date == 1 AND (dose2_prior_to_date == 0 OR dose2_prior_to_date IS NULL) then 'Dose1_but_no_dose2' 
# MAGIC else NULL END as vaccine_status
# MAGIC FROM (
# MAGIC SELECT a.person_id_deid, b.date, c.dose2, d.dose1,
# MAGIC DATEDIFF(c.dose2,b.date) as date_diff_dose2,
# MAGIC CASE WHEN DATEDIFF(c.dose2,b.date) <= -14 THEN 1 
# MAGIC WHEN c.dose2 is NULL THEN NULL
# MAGIC WHEN b.date is NULL and c.dose2 is NOT NULL THEN 1
# MAGIC ELSE 0 END as dose2_prior_to_date,
# MAGIC CASE WHEN DATEDIFF(d.dose1,b.date) <= -14 THEN 1 
# MAGIC WHEN d.dose1 is NULL THEN NULL
# MAGIC WHEN b.date is NULL and d.dose1 is NOT NULL THEN 1
# MAGIC ELSE 0 END as dose1_prior_to_date
# MAGIC FROM
# MAGIC global_temp.ccu013_no_covid_before_wave2 as a
# MAGIC LEFT JOIN (SELECT person_id_deid, min(date) as date FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort group by person_id_deid) as b
# MAGIC ON a.person_id_deid = b.person_id_deid
# MAGIC LEFT JOIN (SELECT person_id_deid, min(date) as dose2 FROM dars_nic_391419_j3w9t_collab.ccu013_vaccine_status_temp WHERE DOSE_SEQUENCE == 2 GROUP BY person_id_deid) AS c
# MAGIC ON a.person_id_deid = c.person_id_deid
# MAGIC LEFT JOIN (SELECT person_id_deid, min(date) as dose1 FROM dars_nic_391419_j3w9t_collab.ccu013_vaccine_status_temp WHERE DOSE_SEQUENCE == 1 GROUP BY person_id_deid) AS d
# MAGIC ON a.person_id_deid = d.person_id_deid)

# COMMAND ----------

drop_table("ccu013_no_covid_before_wave2_vax_status")
create_table("ccu013_no_covid_before_wave2_vax_status")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_no_covid_before_wave2_vax_status

# COMMAND ----------

# MAGIC %sql
# MAGIC --- count of vaccinated vs unvaccinated
# MAGIC --- Using 14 days as buffer; 9,562,898 | 19,638,856 | 26,572,454
# MAGIC SELECT vaccine_status, count(vaccine_status) as count
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_no_covid_before_wave2_vax_status
# MAGIC group by vaccine_status

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.0 Vax vs unvax matched on (age, gender and ethnicity) from 1st feb 2021 

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Get people who have died before 1st of feb 2021
# MAGIC SELECT count(distinct NHS_NUMBER_DEID) 
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020 AS a
# MAGIC RIGHT JOIN (SELECT person_id_deid, min(death_date) as death_date FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_deaths group by person_id_deid) as b
# MAGIC ON a.NHS_NUMBER_DEID = b.person_id_deid
# MAGIC WHERE death_date < "2021-02-01" OR death_date is null 

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Get all individuals who had a reported COVID-19 before 1st of feb 2021
# MAGIC SELECT count(distinct a.person_id_deid) FROM (SELECT DISTINCT NHS_NUMBER_DEID as person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as a
# MAGIC INNER JOIN (SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort WHERE date < "2021-02-01") as t
# MAGIC ON a.person_id_deid = t.person_id_deid

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Get all individuals WITHOUT a reported COVID-19 event prior to wave 2 AND OR who are not DEAD pre date of interset
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_no_covid_before_feb_2021 AS
# MAGIC SELECT a.person_id_deid FROM (SELECT DISTINCT NHS_NUMBER_DEID as person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as a
# MAGIC LEFT ANTI JOIN (SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort WHERE date < "2021-02-01") as t
# MAGIC ON a.person_id_deid = t.person_id_deid
# MAGIC LEFT JOIN (SELECT person_id_deid, min(death_date) as death_date FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_deaths group by person_id_deid) as c
# MAGIC ON a.person_id_deid = c.person_id_deid
# MAGIC WHERE death_date > "2021-02-01" OR death_date is null 

# COMMAND ----------

# MAGIC %sql
# MAGIC --- N = 52,969,966 - individuals WITHOUT a reported COVID-19 event prior to date and or All Alive!
# MAGIC SELECT count(distinct person_id_deid) FROM global_temp.ccu013_no_covid_before_feb_2021

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Dose 2
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_no_covid_before_feb_2021_vax_status AS
# MAGIC SELECT *, 
# MAGIC CASE WHEN dose2_prior_to_date == 1 THEN 'Vaccinated'
# MAGIC WHEN (dose1_prior_to_date == 0 OR dose1_prior_to_date IS NULL) AND (dose2_prior_to_date == 0 OR dose2_prior_to_date IS NULL) THEN 'Unvaccinated'
# MAGIC WHEN dose1_prior_to_date == 1 AND (dose2_prior_to_date == 0 OR dose2_prior_to_date IS NULL) then 'Dose1_but_no_dose2' 
# MAGIC else NULL END as vaccine_status
# MAGIC FROM (
# MAGIC SELECT a.person_id_deid, dose2, 
# MAGIC DATEDIFF(dose2,"2021-02-01") as date_diff_dose2,
# MAGIC CASE WHEN DATEDIFF(dose2,"2021-02-01") <= -14 THEN 1 
# MAGIC WHEN dose2 is NULL THEN NULL
# MAGIC ELSE 0 END as dose2_prior_to_date,
# MAGIC CASE WHEN DATEDIFF(dose1,"2021-02-01") <= -14 THEN 1 
# MAGIC WHEN dose1 is NULL THEN NULL
# MAGIC ELSE 0 END as dose1_prior_to_date
# MAGIC FROM global_temp.ccu013_no_covid_before_feb_2021 AS a
# MAGIC LEFT JOIN (SELECT person_id_deid, min(date) as dose2 FROM dars_nic_391419_j3w9t_collab.ccu013_vaccine_status_temp WHERE DOSE_SEQUENCE == 2 GROUP BY person_id_deid) AS b
# MAGIC ON a.person_id_deid = b.person_id_deid
# MAGIC LEFT JOIN (SELECT person_id_deid, min(date) as dose1 FROM dars_nic_391419_j3w9t_collab.ccu013_vaccine_status_temp WHERE DOSE_SEQUENCE == 1 GROUP BY person_id_deid) AS c
# MAGIC ON a.person_id_deid = c.person_id_deid)

# COMMAND ----------

drop_table("ccu013_no_covid_before_feb_2021_vax_status")
create_table("ccu013_no_covid_before_feb_2021_vax_status")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_no_covid_before_feb_2021_vax_status

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Using 14 days as buffer; 3,040,532 | 421,811 | 49,507,623
# MAGIC SELECT vaccine_status, count(vaccine_status) as count
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_no_covid_before_feb_2021_vax_status
# MAGIC group by vaccine_status

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.1 Add demographics to allow for matching

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Query to add data for matching
# MAGIC --- The matching is done in R and then saved back to a table
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_no_covid_before_feb_2021_vax_status_matching AS
# MAGIC SELECT person_id_deid, SEX, 
# MAGIC vaccine_status,
# MAGIC CASE
# MAGIC WHEN ETHNIC == 'A' THEN 'White'
# MAGIC WHEN ETHNIC == 'B' THEN 'White'
# MAGIC WHEN ETHNIC == 'C' THEN 'White'
# MAGIC WHEN ETHNIC == 'D' THEN 'Mixed'
# MAGIC WHEN ETHNIC == 'E' THEN 'Mixed'
# MAGIC WHEN ETHNIC == 'F' THEN 'Mixed'
# MAGIC WHEN ETHNIC == 'G' THEN 'Mixed'
# MAGIC WHEN ETHNIC == 'H' THEN 'Asian' --- Asian or Asian British
# MAGIC WHEN ETHNIC == 'J' THEN 'Asian'
# MAGIC WHEN ETHNIC == 'K' THEN 'Asian'
# MAGIC WHEN ETHNIC == 'L' THEN 'Asian'
# MAGIC WHEN ETHNIC == 'M' THEN 'Black' --- Black or Black British
# MAGIC WHEN ETHNIC == 'N' THEN 'Black'
# MAGIC WHEN ETHNIC == 'P' THEN 'Black'
# MAGIC WHEN ETHNIC == 'R' THEN 'Chinese'
# MAGIC WHEN ETHNIC == 'S' THEN 'Other'
# MAGIC WHEN ETHNIC == 'Z' THEN 'Unknown'
# MAGIC WHEN ETHNIC == 'X' THEN 'Unknown'
# MAGIC WHEN ETHNIC == 99 THEN 'Unknown'
# MAGIC WHEN ETHNIC == 0 THEN 'White'
# MAGIC WHEN ETHNIC == 1 THEN 'Black'
# MAGIC WHEN ETHNIC == 2 THEN 'Black'
# MAGIC WHEN ETHNIC == 3 THEN 'Black'
# MAGIC WHEN ETHNIC == 4 THEN 'Asian'
# MAGIC WHEN ETHNIC == 5 THEN 'Asian'
# MAGIC WHEN ETHNIC == 6 THEN 'Asian'
# MAGIC WHEN ETHNIC == 7 THEN 'Chinese'
# MAGIC WHEN ETHNIC == 8 THEN 'Other'
# MAGIC WHEN ETHNIC == 9 THEN 'Unknown'
# MAGIC WHEN ETHNIC is NULL THEN 'Unknown'
# MAGIC ELSE 'Unknown' END AS ethnicity,
# MAGIC CASE 
# MAGIC WHEN AGE < 5 THEN '-5'
# MAGIC WHEN AGE < 10 THEN '-10'
# MAGIC WHEN AGE < 15 THEN '-15'
# MAGIC WHEN AGE < 20 THEN '-20'
# MAGIC WHEN AGE < 25 THEN '-25'
# MAGIC WHEN AGE < 30 THEN '-30'
# MAGIC WHEN AGE < 35 THEN '-35'
# MAGIC WHEN AGE < 40 THEN '-40'
# MAGIC WHEN AGE < 45 THEN '-45'
# MAGIC WHEN AGE < 50 THEN '-50'
# MAGIC WHEN AGE < 55 THEN '-55'
# MAGIC WHEN AGE < 60 THEN '-60'
# MAGIC WHEN AGE < 65 THEN '-65'
# MAGIC WHEN AGE < 70 THEN '-70'
# MAGIC WHEN AGE < 75 THEN '-75'
# MAGIC WHEN AGE < 80 THEN '-80'
# MAGIC WHEN AGE < 85 THEN '-85'
# MAGIC WHEN AGE < 90 THEN '-90'
# MAGIC ELSE '>90' END as age_grp
# MAGIC FROM (SELECT a.person_id_deid, SEX, ETHNIC, 2021 - LEFT(DATE_OF_BIRTH,4) AS AGE, vaccine_status FROM (
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_no_covid_before_feb_2021_vax_status WHERE (vaccine_status == 'Vaccinated' OR vaccine_status == 'Unvaccinated')) as a
# MAGIC LEFT JOIN (SELECT NHS_NUMBER_DEID as person_id_deid, SEX, ETHNIC, DATE_OF_BIRTH, DATE_OF_DEATH FROM dars_nic_391419_j3w9t_collab.curr302_patient_skinny_record) as b
# MAGIC ON a.person_id_deid = b.person_id_deid)

# COMMAND ----------

# MAGIC %md
# MAGIC This is the pool which matches are drawn from, but the matching process is performed within R

# COMMAND ----------

drop_table("ccu013_no_covid_before_feb_2021_vax_status_matching")
create_table("ccu013_no_covid_before_feb_2021_vax_status_matching")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT vaccine_status, count(vaccine_status) FROM dars_nic_391419_j3w9t_collab.ccu013_no_covid_before_feb_2021_vax_status_matching group by vaccine_status

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_no_covid_before_feb_2021_vax_status_matching
