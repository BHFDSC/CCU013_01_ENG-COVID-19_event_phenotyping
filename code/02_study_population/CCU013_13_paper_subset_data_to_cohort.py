# Databricks notebook source
# MAGIC %md
# MAGIC # CCU013_08 Paper subset data to cohort
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook subsets the covid trajectory, severity and events tables to the cohort used for the phenotype severity paper.
# MAGIC 
# MAGIC **Project(s)** CCU0013
# MAGIC  
# MAGIC **Author(s)** Johan Thygesen, Chris Tomlinson
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2021-08-17
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** 2021-08-17
# MAGIC  
# MAGIC **Data input**  
# MAGIC 1. Descriptive Paper methodology derived cohort
# MAGIC 2. Maximally inclusive COVID-19 related event phenotypes:
# MAGIC   1. `ccu013_covid_trajectory`
# MAGIC   2. `ccu013_covid_events_demographics`
# MAGIC 
# MAGIC **Data output**
# MAGIC 1. `ccu013_covid_trajectory_paper_cohort` - Comprehensive long list of COVID-19 related events, subset to paper cohort
# MAGIC 2. `ccu013_covid_severity_paper_cohort` - Mutually exclusive 'worst' COVID-19 related event, 1 row per patient
# MAGIC 3. `ccu013_covid_events_demographics_paper_cohort`- Binary matrix of COVID-19 related events + demographics, 1 row per patient
# MAGIC 
# MAGIC **Software and versions** SQL, python
# MAGIC  
# MAGIC **Packages and versions** See cell below:
# MAGIC 
# MAGIC **TODO**
# MAGIC * Implement Longcovid search

# COMMAND ----------

# MAGIC %md
# MAGIC # 1 Subset Covid Phenotype data to the cohort population of interest

# COMMAND ----------

from pyspark.sql.functions import lit, col, udf
from functools import reduce
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.types import DateType

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU013/COVID-19-SEVERITY-PHENOTYPING/CCU013_00_helper_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 New approach (current) using the DP definition

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Individuals alive and registred in GDPPR on 23/01/2020
# MAGIC --- Old value      = 55,876,173
# MAGIC --- value @ 170821 = 56,609,049
# MAGIC SELECT count(DISTINCT NHS_NUMBER_DEID) FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020
# MAGIC WHERE DATE_OF_DEATH <= "2020-03-20"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.1.1 Find patients who do not have minimum follow up time
# MAGIC - Participants with non-fatal index events who had less than 28 days of follow up were excluded.

# COMMAND ----------

from pyspark.sql.functions import *

all_fatal = spark.sql("""
SELECT person_id_deid, MIN(date) AS death_date
FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
WHERE covid_phenotype == '04_Fatal_with_covid_diagnosis' OR
covid_phenotype == '04_Fatal_without_covid_diagnosis' OR
covid_phenotype == '04_Covid_inpatient_death'
GROUP BY person_id_deid
""")

# Get all none deaths dates
followup_time = spark.sql("""
SELECT person_id_deid, MIN(date) AS first_covid_event
FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
WHERE covid_phenotype != '04_Fatal_with_covid_diagnosis' OR
covid_phenotype != '04_Fatal_without_covid_diagnosis' OR
covid_phenotype != '04_Covid_inpatient_death'
GROUP BY person_id_deid
""")

# Calculate elapsed number of days between earliest event and study end (except if fatal)
followup_time = followup_time.join(all_fatal, ['person_id_deid'], how='left')
followup_time = followup_time.select(['person_id_deid', 'first_covid_event', 'death_date']) 
followup_time = followup_time.withColumn('study_end', lit(datetime(2021, 3, 31)))
followup_time= followup_time.withColumn('followup_days', 
                                        when(followup_time['death_date'].isNull(), datediff(followup_time['study_end'], followup_time['first_covid_event'])).otherwise(-1))
    
# Mark deaths within 28 days
followup_time = followup_time.withColumn('28d_followup', \
  when((followup_time['followup_days'] >= 28) | (followup_time['followup_days'] == -1), 1).otherwise(0))
#display(followup_time)
followup_time.createOrReplaceGlobalTempView('followup_time')

# COMMAND ----------

# MAGIC %sql -- participants excluded due to lack of 28 days minimal followup time.
# MAGIC 
# MAGIC --- NOTE THIS number also include patients who enter the study after the cutoff time 
# MAGIC SELECT count(DISTINCT person_id_deid) FROM global_temp.followup_time
# MAGIC WHERE 28d_followup == 0

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.1.2 Subset trajectory table
# MAGIC Subset for cohor population - inclusion time and minimal followup

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count (DISTINCT person_id_deid) from dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count (DISTINCT person_id_deid) from dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
# MAGIC WHERE date >= "2020-01-23" AND date <= "2021-05-31"

# COMMAND ----------

# MAGIC %sql 
# MAGIC --- Subset trajectory table to cohort population and cohort timeline
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_trajectory_paper_cohort_tmp AS
# MAGIC SELECT tab1.* FROM  
# MAGIC   dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory tab1
# MAGIC INNER JOIN 
# MAGIC   dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020 tab2 
# MAGIC ON 
# MAGIC tab1.person_id_deid = tab2.NHS_NUMBER_DEID
# MAGIC WHERE date >= "2020-01-23" AND date <= "2021-05-31"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Value @ 150621 3567617
# MAGIC -- Value @ 170821 3705123
# MAGIC SELECT count (DISTINCT person_id_deid) from global_temp.ccu013_covid_trajectory_paper_cohort_tmp

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_trajectory_paper_cohort as
# MAGIC WITH list_patients_to_omit AS (SELECT person_id_deid from global_temp.followup_time WHERE 28d_followup == 0)
# MAGIC SELECT /*+ BROADCAST(list_patients_to_omit) */ t.* FROM global_temp.ccu013_covid_trajectory_paper_cohort_tmp as t
# MAGIC LEFT ANTI JOIN list_patients_to_omit ON t.person_id_deid = list_patients_to_omit.person_id_deid

# COMMAND ----------

drop_table("ccu013_covid_trajectory_paper_cohort")
create_table("ccu013_covid_trajectory_paper_cohort")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort ZORDER BY person_id_deid

# COMMAND ----------

# MAGIC %sql
# MAGIC -- value @ 150621 = 3454653
# MAGIC -- value @ 170821 = 3469528
# MAGIC SELECT count (DISTINCT person_id_deid) from dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC -- value @ 150621 = 8683174
# MAGIC -- value @ 170821 = 8825738
# MAGIC SELECT count (*) from dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT covid_phenotype, count (DISTINCT person_id_deid) as unique_ids, count (person_id_deid) as observations
# MAGIC from dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
# MAGIC group by covid_phenotype
# MAGIC order by covid_phenotype

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.1.3 Recreate severity table using cohort only info

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count (DISTINCT person_id_deid) from dars_nic_391419_j3w9t_collab.ccu013_covid_severity

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_severity_paper_cohort AS
# MAGIC SELECT s.person_id_deid, s.date, s.covid_severity, s.ProductionDate FROM dars_nic_391419_j3w9t_collab.ccu013_covid_severity as s
# MAGIC INNER JOIN dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort as t
# MAGIC ON s.person_id_deid == t.person_id_deid

# COMMAND ----------

drop_table("ccu013_covid_severity_paper_cohort")
create_table("ccu013_covid_severity_paper_cohort")

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH dars_nic_391419_j3w9t_collab.ccu013_covid_severity_paper_cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC -- value @ 150621 = 3454653
# MAGIC -- value @ 170821 = 3469528
# MAGIC SELECT count(DISTINCT person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_severity_paper_cohort

# COMMAND ----------

# MAGIC %md
# MAGIC # 2 Create input for patient trajectory plots
# MAGIC - Create order and simplified phenotype groups for the plots
# MAGIC - Get the first event date from the new simplified trajectory phenotypes and order by id, date and phenotype order.
# MAGIC - Calculate days between events and write to table for further processing in R 
# MAGIC   - see ccu013 R script ccu013_trajectory_finder.R for next steps

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Full study period

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Query to get all events includeing the uneffected event at the start of the pandemic for all individuals in study.
# MAGIC ---SELECT covid_severity, count(covid_severity) FROM (
# MAGIC SELECT person_id_deid, date, covid_phenotype, (CASE WHEN covid_severity IS NULL THEN '00_unaffected' ELSE covid_severity END) AS covid_severity, phenotype_order, trajectory_phenotype FROM (
# MAGIC SELECT NHS_NUMBER_DEID AS person_id_deid, DATE('2020-01-23') AS date, '00_Unaffected' AS covid_phenotype, covid_severity, 0 AS phenotype_order, 'Unaffected' AS trajectory_phenotype FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020 AS a
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu013_covid_severity_paper_cohort AS b ON a.NHS_NUMBER_DEID = b.person_id_deid)
# MAGIC ---)group by covid_severity

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Create an ordered and simplified phenotype groups table
# MAGIC --- This includes all events includeing the uneffected event at the start of the pandemic for all individuals in study.
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_trajectory_plot_data_tmp AS
# MAGIC SELECT * FROM
# MAGIC (SELECT DISTINCT tab1.person_id_deid, tab1.date, tab1.covid_phenotype, tab2.covid_severity, 
# MAGIC (case covid_phenotype 
# MAGIC when "01_Covid_positive_test" then 1 
# MAGIC when "01_GP_covid_diagnosis" then 2
# MAGIC when "02_Covid_admission" then 3
# MAGIC when "03_NIV_treatment" then 4
# MAGIC when "03_ICU_admission" then 4
# MAGIC when "03_IMV_treatment" then 4
# MAGIC when "03_ECMO_treatment" then 4
# MAGIC when "04_Fatal_with_covid_diagnosis" then 5
# MAGIC when "04_Fatal_without_covid_diagnosis" then 5
# MAGIC when "04_Covid_inpatient_death" then 5 ELSE NULL end) as phenotype_order,
# MAGIC (case covid_phenotype
# MAGIC when "01_Covid_positive_test" then "Positive test" 
# MAGIC when "01_GP_covid_diagnosis" then "Primary care diagnosis"
# MAGIC when "02_Covid_admission" then "Hospitalisation"
# MAGIC when "03_NIV_treatment" then "Critical care"
# MAGIC when "03_ICU_admission" then "Critical care"
# MAGIC when "03_IMV_treatment" then "Critical care"
# MAGIC when "03_ECMO_treatment" then "Critical care"
# MAGIC when "04_Fatal_with_covid_diagnosis" then "Death"
# MAGIC when "04_Fatal_without_covid_diagnosis" then "Death"
# MAGIC when "04_Covid_inpatient_death" then "Death" ELSE NULL end) as trajectory_phenotype
# MAGIC FROM 
# MAGIC dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort as tab1
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu013_covid_severity as tab2 ON tab1.person_id_deid = tab2.person_id_deid
# MAGIC UNION ALL
# MAGIC SELECT person_id_deid, date, covid_phenotype, (CASE WHEN covid_severity IS NULL THEN '00_unaffected' ELSE covid_severity END) AS covid_severity, phenotype_order, trajectory_phenotype FROM (
# MAGIC SELECT NHS_NUMBER_DEID AS person_id_deid, DATE('2020-01-23') AS date, '00_Unaffected' AS covid_phenotype, covid_severity, 0 AS phenotype_order, 'Unaffected' AS trajectory_phenotype FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020 AS a
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu013_covid_severity_paper_cohort AS b ON a.NHS_NUMBER_DEID = b.person_id_deid))
# MAGIC ORDER BY person_id_deid, date, phenotype_order

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Exemplar of ordered data
# MAGIC SELECT * from global_temp.ccu013_covid_trajectory_plot_data_tmp
# MAGIC WHERE person_id_deid = '00046L6S0IX8YE1'

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Example of query used below to caclualte time between events per ID
# MAGIC --- Get the event dates from the new trajectory phenotypes and order by id, date and phenotype order.
# MAGIC SELECT DISTINCT person_id_deid, min(date) as date, covid_severity, trajectory_phenotype, phenotype_order from global_temp.ccu013_covid_trajectory_plot_data_tmp
# MAGIC GROUP BY person_id_deid, phenotype_order, trajectory_phenotype, covid_severity
# MAGIC ORDER BY person_id_deid, date, phenotype_order

# COMMAND ----------

## 3) Calculate days between events and write to table for further processing in R 
###   see ccu013 R script ccu013_trajectory_finder.R for next steps

from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.window import Window
traject_data = spark.sql("""
SELECT DISTINCT person_id_deid, min(date) as date, covid_severity, trajectory_phenotype, phenotype_order from global_temp.ccu013_covid_trajectory_plot_data_tmp
GROUP BY person_id_deid, phenotype_order, trajectory_phenotype, covid_severity
ORDER BY person_id_deid, date, phenotype_order
""")

window = Window.partitionBy('person_id_deid').orderBy(['date', 'phenotype_order'])
# Calculate difference in days per ID 
traject_data = traject_data.withColumn("days_passed", f.datediff(traject_data.date, 
                                  f.lag(traject_data.date, 1).over(window)))

#display(traject_data)
traject_data.createOrReplaceGlobalTempView("ccu013_covid_trajectory_graph_data")
drop_table("ccu013_covid_trajectory_graph_data")
create_table("ccu013_covid_trajectory_graph_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Examplar output for one individual
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data
# MAGIC WHERE person_id_deid = '00046L6S0IX8YE1'

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 56609049
# MAGIC SELECT count (distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 wave 1 - trajectory input

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Query to define all pople included in wave 1
# MAGIC --- This is used below to subset the trajectory graph data
# MAGIC --- SELECT * FROM
# MAGIC SELECT count(distinct a.person_id_deid) FROM
# MAGIC (SELECT DISTINCT NHS_NUMBER_DEID as person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as a
# MAGIC --- Remove anyone with a 
# MAGIC LEFT ANTI JOIN (SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort WHERE date < "2020-03-20") as t
# MAGIC ON a.person_id_deid = t.person_id_deid
# MAGIC LEFT JOIN (SELECT person_id_deid, min(death_date) as death_date FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_deaths group by person_id_deid) as c
# MAGIC ON a.person_id_deid = c.person_id_deid
# MAGIC WHERE death_date > "2020-03-20" OR death_date is null 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_trajectory_graph_data_wave1 AS
# MAGIC SELECT * FROM 
# MAGIC (SELECT a.person_id_deid, a.date, a.covid_severity, a.trajectory_phenotype, a.phenotype_order, a.days_passed 
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data as a
# MAGIC INNER JOIN (SELECT j.person_id_deid FROM 
# MAGIC (SELECT DISTINCT NHS_NUMBER_DEID as person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as j
# MAGIC --- Remove anyone who had covid before the wave
# MAGIC LEFT ANTI JOIN (SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort WHERE date < "2020-03-20") as t
# MAGIC ON j.person_id_deid = t.person_id_deid
# MAGIC --- Remove anyone who died before the wave
# MAGIC LEFT JOIN (SELECT person_id_deid, min(death_date) as death_date FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_deaths group by person_id_deid) as c
# MAGIC ON j.person_id_deid = c.person_id_deid
# MAGIC WHERE death_date > "2020-03-20" OR death_date is null ) as b
# MAGIC ON a.person_id_deid == b.person_id_deid
# MAGIC WHERE date <= date_add(TO_DATE("2020-05-29"),28))

# COMMAND ----------

drop_table("ccu013_covid_trajectory_graph_data_wave1")
create_table("ccu013_covid_trajectory_graph_data_wave1")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 56491308
# MAGIC SELECT count(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_wave1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_wave1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count (distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_wave1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count (distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_wave1
# MAGIC ---SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_wave1
# MAGIC WHERE covid_severity != "00_unaffected" ---AND date <= date_add(TO_DATE("2020-05-29"),28)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count (distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_wave1
# MAGIC ---SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_wave1
# MAGIC WHERE covid_severity == "00_unaffected" ---AND date <= date_add(TO_DATE("2020-05-29"),28)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 wave 2 - trajectory input

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Query to define all pople included in wave 2
# MAGIC --- This is used below to subset the trajectory graph data
# MAGIC SELECT count(distinct a.person_id_deid) FROM
# MAGIC (SELECT DISTINCT NHS_NUMBER_DEID as person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as a
# MAGIC LEFT ANTI JOIN (SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort WHERE date < "2020-09-30") as t
# MAGIC ON a.person_id_deid = t.person_id_deid
# MAGIC LEFT JOIN (SELECT person_id_deid, min(death_date) as death_date FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_deaths group by person_id_deid) as c
# MAGIC ON a.person_id_deid = c.person_id_deid
# MAGIC WHERE death_date > "2020-09-30" OR death_date is null 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_trajectory_graph_data_wave2 AS
# MAGIC SELECT * FROM 
# MAGIC (SELECT a.person_id_deid, a.date, a.covid_severity, a.trajectory_phenotype, a.phenotype_order, a.days_passed 
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data as a
# MAGIC INNER JOIN (SELECT j.person_id_deid FROM 
# MAGIC (SELECT DISTINCT NHS_NUMBER_DEID as person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as j
# MAGIC LEFT ANTI JOIN (SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort WHERE date < "2020-09-30") as t
# MAGIC ON j.person_id_deid = t.person_id_deid
# MAGIC LEFT JOIN (SELECT person_id_deid, min(death_date) as death_date FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_deaths group by person_id_deid) as c
# MAGIC ON j.person_id_deid = c.person_id_deid
# MAGIC WHERE death_date > "2020-09-30" OR death_date is null ) as b
# MAGIC ON a.person_id_deid == b.person_id_deid
# MAGIC WHERE date <= date_add(TO_DATE("2021-02-12"),28))

# COMMAND ----------

drop_table("ccu013_covid_trajectory_graph_data_wave2")
create_table("ccu013_covid_trajectory_graph_data_wave2")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Trajectory plot input - ICU only as Critical care.

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Create an ordered and simplified phenotype groups table
# MAGIC --- This includes all events includeing the uneffected event at the start of the pandemic for all individuals in study.
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_trajectory_plot_data_icu_tmp AS
# MAGIC SELECT * FROM
# MAGIC (SELECT DISTINCT tab1.person_id_deid, tab1.date, tab1.covid_phenotype, tab2.covid_severity, 
# MAGIC (case covid_phenotype 
# MAGIC when "01_Covid_positive_test" then 1 
# MAGIC when "01_GP_covid_diagnosis" then 2
# MAGIC when "02_Covid_admission" then 3
# MAGIC when "03_NIV_treatment" then NULL
# MAGIC when "03_ICU_admission" then 4
# MAGIC when "03_IMV_treatment" then NULL
# MAGIC when "03_ECMO_treatment" then NULL
# MAGIC when "04_Fatal_with_covid_diagnosis" then 5
# MAGIC when "04_Fatal_without_covid_diagnosis" then 5
# MAGIC when "04_Covid_inpatient_death" then 5 ELSE NULL end) as phenotype_order,
# MAGIC (case covid_phenotype
# MAGIC when "01_Covid_positive_test" then "Positive test" 
# MAGIC when "01_GP_covid_diagnosis" then "Primary care diagnosis"
# MAGIC when "02_Covid_admission" then "Hospitalisation"
# MAGIC when "03_NIV_treatment" then NULL
# MAGIC when "03_ICU_admission" then "ICU admission"
# MAGIC when "03_IMV_treatment" then NULL
# MAGIC when "03_ECMO_treatment" then NULL
# MAGIC when "04_Fatal_with_covid_diagnosis" then "Death"
# MAGIC when "04_Fatal_without_covid_diagnosis" then "Death"
# MAGIC when "04_Covid_inpatient_death" then "Death" ELSE NULL end) as trajectory_phenotype
# MAGIC FROM 
# MAGIC dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort as tab1
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu013_covid_severity as tab2 ON tab1.person_id_deid = tab2.person_id_deid
# MAGIC UNION ALL
# MAGIC SELECT person_id_deid, date, covid_phenotype, (CASE WHEN covid_severity IS NULL THEN '00_unaffected' ELSE covid_severity END) AS covid_severity, phenotype_order, trajectory_phenotype FROM (
# MAGIC SELECT NHS_NUMBER_DEID AS person_id_deid, DATE('2020-01-23') AS date, '00_Unaffected' AS covid_phenotype, covid_severity, 0 AS phenotype_order, 'Unaffected' AS trajectory_phenotype FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020 AS a
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu013_covid_severity_paper_cohort AS b ON a.NHS_NUMBER_DEID = b.person_id_deid))
# MAGIC WHERE phenotype_order is not NULL
# MAGIC ORDER BY person_id_deid, date, phenotype_order

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Exemplar of ordered data
# MAGIC SELECT * from global_temp.ccu013_covid_trajectory_plot_data_icu_tmp
# MAGIC WHERE person_id_deid = '00046L6S0IX8YE1'

# COMMAND ----------

## 3) Calculate days between events and write to table for further processing in R 
###   see ccu013 R script ccu013_trajectory_finder.R for next steps

from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.window import Window
traject_data = spark.sql("""
SELECT DISTINCT person_id_deid, min(date) as date, covid_severity, trajectory_phenotype, phenotype_order from global_temp.ccu013_covid_trajectory_plot_data_icu_tmp
GROUP BY person_id_deid, phenotype_order, trajectory_phenotype, covid_severity
ORDER BY person_id_deid, date, phenotype_order
""")

window = Window.partitionBy('person_id_deid').orderBy(['date', 'phenotype_order'])
# Calculate difference in days per ID 
traject_data = traject_data.withColumn("days_passed", f.datediff(traject_data.date, 
                                  f.lag(traject_data.date, 1).over(window)))

#display(traject_data)
traject_data.createOrReplaceGlobalTempView("ccu013_covid_trajectory_graph_data_icu")
drop_table("ccu013_covid_trajectory_graph_data_icu")
create_table("ccu013_covid_trajectory_graph_data_icu")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Wave 1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_trajectory_graph_data_wave1_icu AS
# MAGIC SELECT * FROM 
# MAGIC (SELECT a.person_id_deid, a.date, a.covid_severity, a.trajectory_phenotype, a.phenotype_order, a.days_passed 
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_icu as a
# MAGIC INNER JOIN (SELECT j.person_id_deid FROM 
# MAGIC (SELECT DISTINCT NHS_NUMBER_DEID as person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as j
# MAGIC --- Remove anyone who had covid before the wave
# MAGIC LEFT ANTI JOIN (SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort WHERE date < "2020-03-20") as t
# MAGIC ON j.person_id_deid = t.person_id_deid
# MAGIC --- Remove anyone who died before the wave
# MAGIC LEFT JOIN (SELECT person_id_deid, min(death_date) as death_date FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_deaths group by person_id_deid) as c
# MAGIC ON j.person_id_deid = c.person_id_deid
# MAGIC WHERE death_date > "2020-03-20" OR death_date is null ) as b
# MAGIC ON a.person_id_deid == b.person_id_deid
# MAGIC WHERE date <= date_add(TO_DATE("2020-05-29"),28))

# COMMAND ----------

drop_table("ccu013_covid_trajectory_graph_data_wave1_icu")
create_table("ccu013_covid_trajectory_graph_data_wave1_icu")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 56491308
# MAGIC SELECT count(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_wave1_icu

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Wave 2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_trajectory_graph_data_wave2_icu AS
# MAGIC SELECT * FROM 
# MAGIC (SELECT a.person_id_deid, a.date, a.covid_severity, a.trajectory_phenotype, a.phenotype_order, a.days_passed 
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_icu as a
# MAGIC INNER JOIN (SELECT j.person_id_deid FROM 
# MAGIC (SELECT DISTINCT NHS_NUMBER_DEID as person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as j
# MAGIC LEFT ANTI JOIN (SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort WHERE date < "2020-09-30") as t
# MAGIC ON j.person_id_deid = t.person_id_deid
# MAGIC LEFT JOIN (SELECT person_id_deid, min(death_date) as death_date FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_deaths group by person_id_deid) as c
# MAGIC ON j.person_id_deid = c.person_id_deid
# MAGIC WHERE death_date > "2020-09-30" OR death_date is null ) as b
# MAGIC ON a.person_id_deid == b.person_id_deid
# MAGIC WHERE date <= date_add(TO_DATE("2021-02-12"),28))

# COMMAND ----------

drop_table("ccu013_covid_trajectory_graph_data_wave2_icu")
create_table("ccu013_covid_trajectory_graph_data_wave2_icu")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 55774208
# MAGIC SELECT count(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_wave2_icu

# COMMAND ----------

# MAGIC %md
# MAGIC # 4 Reinfections (not currently used!)
# MAGIC - Identify all individuals who have had a reinfection with COVID-19
# MAGIC - __NB Not included in paper__ due to issues with non overlap on individual and time basis between sgss and pillar2

# COMMAND ----------

import pyspark.sql.functions as funcs
from pyspark.sql.window import Window
reinfec = spark.sql(""" 
SELECT person_id_deid, date FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
WHERE covid_phenotype in ('01_Covid_positive_test')
""")

reinfect_threshold = 90 # SIREN study

# Find days between consecutive positive COVID tests
# Define window to particion by
window = Window.partitionBy('person_id_deid').orderBy('date')
# Calculate difference in days per ID 
reinfec = reinfec.withColumn("days_passed", funcs.datediff(reinfec.date, 
                                  funcs.lag(reinfec.date, 1).over(window)))
# Save to table
reinfec.createOrReplaceGlobalTempView("ccu013_covid_reinfection_days_between_positive_tests")
drop_table("ccu013_covid_reinfection_days_between_positive_tests")
create_table("ccu013_covid_reinfection_days_between_positive_tests")

# Get the maximum difference in days between positive tests per individual
w = Window.partitionBy('person_id_deid')
reinfec_max_days = reinfec.withColumn('max_days_passed', f.max('days_passed').over(w))\
    .where(f.col('days_passed') == f.col('max_days_passed'))\
    .drop('max_days_passed')

## Find reinfected using reinfect_threshold
reinfec_max_days = reinfec_max_days.withColumn('reinfected', f.when((f.col('days_passed') >= reinfect_threshold),1).otherwise(0))
reinfec_max_days = reinfec_max_days.where(f.col('reinfected') == 1)

# Save to table
reinfec_max_days.createOrReplaceGlobalTempView("ccu013_covid_reinfected_after_90_days")
drop_table("ccu013_covid_reinfected_after_90_days")
create_table("ccu013_covid_reinfected_after_90_days")

# COMMAND ----------


