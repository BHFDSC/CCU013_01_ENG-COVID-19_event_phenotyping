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
# MAGIC **Date last updated** 2021-06-14
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** 2021-06-14
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
# MAGIC ## 1 Subset Covid Phenotype data to the cohort population of interest

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
# MAGIC ### 1.0 Old approach

# COMMAND ----------

# MAGIC %sql 
# MAGIC --- Number of Individuals in the TRE
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_cohort_definition_old as 
# MAGIC SELECT Distinct NHS_NUMBER_DEID as person_id_deid
# MAGIC FROM 
# MAGIC   dars_nic_391419_j3w9t_collab.ccu003_direfcts_dataprep_1_gdppr_frzon28may_mm_210528 tab1
# MAGIC LEFT JOIN 
# MAGIC   dars_nic_391419_j3w9t_collab.ccu003_direfcts_dataprep_1_deaths_frzon28may_mm_210528 tab2 
# MAGIC ON 
# MAGIC   tab1.NHS_NUMBER_DEID = tab2.DEC_CONF_NHS_NUMBER_CLEAN_DEID
# MAGIC WHERE ((LEFT(tab1.LSOA,1) = "E") AND (tab1.DATE <= "2020-01-23" ) AND ((tab2.REG_DATE_OF_DEATH is null) 
# MAGIC             OR (TO_DATE(tab2.REG_DATE_OF_DEATH, "yyyyMMdd") >= "2020-01-23")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 New approach (current) using the DP definition

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT NHS_NUMBER_DEID) FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020

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
# MAGIC WHERE date >= "2020-01-23" AND date <= "2021-03-31"

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
# MAGIC WHERE date >= "2020-01-23" AND date <= "2021-03-31"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Value @ 150621 3567617
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
# MAGIC SELECT count (DISTINCT person_id_deid) from dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count (*) from dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT covid_phenotype, count (DISTINCT person_id_deid) as unique_ids, count (person_id_deid) as observations
# MAGIC from dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
# MAGIC group by covid_phenotype
# MAGIC order by covid_phenotype

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.1.3 Subset severity table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count (DISTINCT person_id_deid) from dars_nic_391419_j3w9t_collab.ccu013_covid_severity

# COMMAND ----------

# MAGIC %sql 
# MAGIC --- Subset severity table to cohort population and cohort timeline
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_severity_paper_cohort AS
# MAGIC SELECT DISTINCT tab1.* FROM  
# MAGIC   dars_nic_391419_j3w9t_collab.ccu013_covid_severity tab1
# MAGIC INNER JOIN 
# MAGIC   dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort tab2 
# MAGIC ON 
# MAGIC tab1.person_id_deid = tab2.person_id_deid

# COMMAND ----------

drop_table("ccu013_covid_severity_paper_cohort")
create_table("ccu013_covid_severity_paper_cohort")

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH dars_nic_391419_j3w9t_collab.ccu013_covid_severity_paper_cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC -- value @ 150621 = 3454653
# MAGIC SELECT count(DISTINCT person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_severity_paper_cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_severity_paper_cohort

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 Create input for patient trajectory plots
# MAGIC - Create order and simplified phenotype groups for the plots
# MAGIC - Get the first event date from the new simplified trajectory phenotypes and order by id, date and phenotype order.
# MAGIC - Calculate days between events and write to table for further processing in R 
# MAGIC   - see ccu013 R script ccu013_trajectory_finder.R for next steps

# COMMAND ----------

# MAGIC %sql
# MAGIC --- First create order and simplified phenotype groups for the plots
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_trajectory_plot_data_tmp AS
# MAGIC SELECT DISTINCT tab1.person_id_deid, tab1.date, tab1.covid_phenotype, tab2.covid_severity, 
# MAGIC (case covid_phenotype 
# MAGIC when "01_Covid_positive_test" then 0 
# MAGIC when "01_GP_covid_diagnosis" then 1
# MAGIC when "02_Covid_admission" then 2
# MAGIC when "03_NIV_treatment" then 3
# MAGIC when "03_ICU_admission" then 3
# MAGIC when "03_IMV_treatment" then 3
# MAGIC when "03_ECMO_treatment" then 3
# MAGIC when "04_Fatal_with_covid_diagnosis" then 4
# MAGIC when "04_Fatal_without_covid_diagnosis" then 4
# MAGIC when "04_Covid_inpatient_death" then 4 ELSE 99 end) as phenotype_order,
# MAGIC (case covid_phenotype
# MAGIC when "01_Covid_positive_test" then "Positive test" 
# MAGIC when "01_GP_covid_diagnosis" then "GP Diagnosis"
# MAGIC when "02_Covid_admission" then "Hospitalisation"
# MAGIC when "03_NIV_treatment" then "Critical care"
# MAGIC when "03_ICU_admission" then "Critical care"
# MAGIC when "03_IMV_treatment" then "Critical care"
# MAGIC when "03_ECMO_treatment" then "Critical care"
# MAGIC when "04_Fatal_with_covid_diagnosis" then "Death"
# MAGIC when "04_Fatal_without_covid_diagnosis" then "Death"
# MAGIC when "04_Covid_inpatient_death" then "Death" ELSE 99 end) as trajectory_phenotype
# MAGIC FROM 
# MAGIC dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort as tab1
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu013_covid_severity as tab2 ON tab1.person_id_deid = tab2.person_id_deid
# MAGIC ORDER BY person_id_deid, date, phenotype_order

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from global_temp.ccu013_covid_trajectory_plot_data_tmp
# MAGIC WHERE person_id_deid = '00046L6S0IX8YE1'

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Second this query is used below
# MAGIC --- Get the first event date from the new trajectory phenotypes and order by id, date and phenotype order.
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
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data
# MAGIC WHERE person_id_deid = '00046L6S0IX8YE1'

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 Reinfections
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



# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Demographics
# MAGIC Need to actually re-pivot and rebuild rather than join on a subset because can't do the date subsetting with the existing tables (only report date_first, even if add death last won't be able to cut at precisely the right point)
