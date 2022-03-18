# Databricks notebook source
# MAGIC %md
# MAGIC  # COVID-19 Master Phenotype notebook
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook:
# MAGIC * Identifies all patients with COVID-19 related diagnosis
# MAGIC * Creates a *trajectory* table with all data points for all affected individuals
# MAGIC * Creates a *severity* table where all individuals are assigned a mutually-exclusive COVID-19 severity phenotype (mild, moderate, severe, death) based on the worst event they experience  
# MAGIC   
# MAGIC NB:
# MAGIC * start and stop dates for the phenotyping is defined in notebook `CCU013_01_create_table_aliases`
# MAGIC * Our work at this stage is designed to be maximally inclusive, further subsetting to the cohort for our manuscript occurrs in notebook `CCU013_13_paper_subset_data_to_cohort`
# MAGIC 
# MAGIC **Project(s)** CCU0013
# MAGIC  
# MAGIC **Author(s)** Johan Thygesen, Chris Tomlinson, Spiros Denaxas
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2021-05-31
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** 2021-06-15
# MAGIC  
# MAGIC **Data input**  
# MAGIC All inputs are via notebook `CCU013_01_create_table_aliases`
# MAGIC We also source functions from `CCU013_01_helper_functions`
# MAGIC 
# MAGIC **Data output**
# MAGIC 
# MAGIC Global temp views (intermediate tables):
# MAGIC * `ccu013_chess`
# MAGIC * `ccu013_pillar2_covid`
# MAGIC * `ccu013_sgss_covid`
# MAGIC * `ccu013_gdppr_covid`
# MAGIC * `ccu013_op_covid`
# MAGIC * `ccu013_apc_covid`
# MAGIC * `ccu013_cc_covid`
# MAGIC * `ccu013_niv_covid`
# MAGIC * `ccu013_imv_covid`
# MAGIC 
# MAGIC Saved tables:
# MAGIC * `ccu013_trajectories`
# MAGIC * `ccu013_covid_severity`
# MAGIC 
# MAGIC **Software and versions** SQL, python
# MAGIC  
# MAGIC **Packages and versions** See cell below:

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load functions and input data

# COMMAND ----------

from pyspark.sql.functions import lit, col, udf
from functools import reduce
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.types import DateType

# COMMAND ----------

# Get the production date from the GDPPR table - set in notebook _01
# [production_date] is destructuring, expecting spark.sql to return a list of exactly one value similar to result = spark.sql...; result[0].value
[production_date] = spark.sql("SELECT DISTINCT ProductionDate as value from dars_nic_391419_j3w9t_collab.ccu013_tmp_gdppr  LIMIT 1").collect()

# COMMAND ----------

# NB no longer global temp views but 'proper tables' therefore better to run separately as takes ~40 mins
# To reload the creation of the global temp tables run this line
# dbutils.widgets.removeAll()
# dbutils.notebook.run("./CCU013_01_create_table_aliases", 30000) # timeout_seconds

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU013/COVID-19-SEVERITY-PHENOTYPING/CCU013_00_helper_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Identify all patients with COVID19
# MAGIC 
# MAGIC Create a single table (**`ccu013_covid_trajectories`**) that contains all COVID-19 events from all input tables (SGSS, GDPPR, HES_APC.. etc) with the following format.
# MAGIC 
# MAGIC |Column | Content |
# MAGIC |----------------|--------------------|
# MAGIC |patient_id_deid| Patient NHS Number |
# MAGIC |date | Date of event: date in GDPPR, speciment date SGSS, epistart HES |
# MAGIC |covid_phenotype | Cateogrical: Positive PCR test; Confirmed_COVID19; Suspected_COVID19; Lab confirmed incidence; Lab confirmed historic; Lab confirmed unclear; Clinically confirmed |
# MAGIC |clinical_code | Reported clinical code |
# MAGIC |description | Description of the clinical code if relevant|
# MAGIC |code | Type of code: ICD10; SNOMED |
# MAGIC |source | Source from which the data was drawn: SGSS; HES APC; Primary care |
# MAGIC |date_is | Original column name of date |

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.1: COVID postive and diagnosis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pillar 2 table
# MAGIC -- Not included currently as there are some data issues! (table has been withdrawn)
# MAGIC -- The following codes are negative: 1322791000000100, 1240591000000102
# MAGIC -- The following codes are unknown:  1321691000000102, 1322821000000105
# MAGIC --- NOTE: The inclusion of only positive tests have already been done in notebook ccu013_create_table_aliase
# MAGIC 
# MAGIC --CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_pillar2_covid
# MAGIC --AS
# MAGIC --SELECT person_id_deid, date, 
# MAGIC -- Decision to group all pillar 2 tests together as distinguishing lateral flow vs PCR not required for severity phenotyping
# MAGIC --"01_Covid_positive_test" as covid_phenotype, 
# MAGIC --TestResult as clinical_code, 
# MAGIC --CASE WHEN TestResult = '1322781000000102' THEN "Severe acute respiratory syndrome coronavirus 2 antigen detection result positive (finding)" 
# MAGIC --WHEN TestResult = '1240581000000104' THEN "Severe acute respiratory syndrome coronavirus 2 ribonucleic acid detected (finding)" else NULL END as description,
# MAGIC --'confirmed' as covid_status,
# MAGIC --"SNOMED" as code,
# MAGIC --source, date_is
# MAGIC --FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_pillar2

# COMMAND ----------

# MAGIC %sql 
# MAGIC --- SGSS table
# MAGIC --- all records are included as every record is a "positive test"
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_sgss_covid
# MAGIC AS
# MAGIC SELECT person_id_deid, date, 
# MAGIC "01_Covid_positive_test" as covid_phenotype, 
# MAGIC "" as clinical_code, 
# MAGIC -- TODO: wranglers please clarify whether LAB ID 840 is still the best means of identifying pillar 1 vs 2
# MAGIC -- CASE WHEN REPORTING_LAB_ID = '840' THEN "pillar_2" ELSE "pillar_1" END as description,
# MAGIC "" as description,
# MAGIC "confirmed" as covid_status,
# MAGIC "" as code,
# MAGIC "SGSS" as source, date_is
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_sgss

# COMMAND ----------

# MAGIC %sql 
# MAGIC --- GDPPR 
# MAGIC --- Only includes individuals with a COVID SNOMED CODE
# MAGIC --- SNOMED CODES are defined in: CCU013_01_create_table_aliases
# MAGIC --- Optimisation =  /*+ BROADCAST(tab1) */ -- forces it to send a copy of the small table to each worker
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_gdppr_covid as
# MAGIC with cte_gdppr as (
# MAGIC SELECT /*+ BROADCAST(tab1) */ -- forces it to send a copy of the small table to each worker
# MAGIC tab2.person_id_deid, tab2.date, tab2.code, tab2.date_is, tab1.clinical_code, tab1.description
# MAGIC FROM  global_temp.ccu013_snomed_codes_covid19 tab1
# MAGIC inner join dars_nic_391419_j3w9t_collab.ccu013_tmp_gdppr tab2 on tab1.clinical_code = tab2.code
# MAGIC )
# MAGIC SELECT person_id_deid, date, 
# MAGIC "01_GP_covid_diagnosis" as covid_phenotype,
# MAGIC clinical_code, description,
# MAGIC "" as covid_status, --- See SNOMED code description
# MAGIC "SNOMED" as code, 
# MAGIC "GDPPR" as source, date_is 
# MAGIC from cte_gdppr

# COMMAND ----------

# MAGIC %sql
# MAGIC --- HES Dropped as source since 080621
# MAGIC --- HES_OP
# MAGIC --- Get all patients hospitalised with a covid diagnosis U07.1 or U07.2
# MAGIC 
# MAGIC --CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_op_covid as
# MAGIC --SELECT person_id_deid, date, 
# MAGIC --"01_GP_covid_diagnosis" as covid_phenotype,
# MAGIC --(case when DIAG_4_CONCAT LIKE "%U071%" THEN 'U07.1'
# MAGIC --when DIAG_4_CONCAT LIKE "%U072%" THEN 'U07.2' Else '0' End) as clinical_code,
# MAGIC --(case when DIAG_4_CONCAT LIKE "%U071%" THEN 'Confirmed_COVID19'
# MAGIC --when DIAG_4_CONCAT LIKE "%U072%" THEN 'Suspected_COVID19' Else '0' End) as description,
# MAGIC --(case when DIAG_4_CONCAT LIKE "%U071%" THEN 'confirmed'
# MAGIC --when DIAG_4_CONCAT LIKE "%U072%" THEN 'suspected' Else '0' End) as covid_status,
# MAGIC --"ICD10" as code,
# MAGIC --"HES OP" as source, 
# MAGIC --date_is
# MAGIC --FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_op
# MAGIC --WHERE DIAG_4_CONCAT LIKE "%U071%"
# MAGIC --   OR DIAG_4_CONCAT LIKE "%U072%"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2: Covid Admission

# COMMAND ----------

# MAGIC %sql
# MAGIC --- SUS - Hospitalisations
# MAGIC --- Get all patients hospitalised with a covid diagnosis U07.1 or U07.2
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_sus_covid as
# MAGIC SELECT person_id_deid, date, 
# MAGIC "02_Covid_admission" as covid_phenotype,
# MAGIC (case when DIAG_CONCAT LIKE "%U071%" THEN 'U07.1'
# MAGIC when DIAG_CONCAT LIKE "%U072%" THEN 'U07.2' Else '0' End) as clinical_code,
# MAGIC (case when DIAG_CONCAT LIKE "%U071%" THEN 'Confirmed_COVID19'
# MAGIC when DIAG_CONCAT LIKE "%U072%" THEN 'Suspected_COVID19' Else '0' End) as description,
# MAGIC (case when DIAG_CONCAT LIKE "%U071%" THEN 'confirmed'
# MAGIC when DIAG_CONCAT LIKE "%U072%" THEN 'suspected' Else '0' End) as covid_status,
# MAGIC "ICD10" as code,
# MAGIC "SUS" as source, 
# MAGIC date_is
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_sus
# MAGIC WHERE DIAG_CONCAT LIKE "%U071%"
# MAGIC    OR DIAG_CONCAT LIKE "%U072%"

# COMMAND ----------

# MAGIC %sql
# MAGIC --- CHESS - Hospitalisations
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_chess_covid_hospital as
# MAGIC SELECT person_id_deid, HospitalAdmissionDate as date,
# MAGIC "02_Covid_admission" as covid_phenotype,
# MAGIC "" as clinical_code, 
# MAGIC "HospitalAdmissionDate IS NOT null" as description,
# MAGIC "" as covid_status,
# MAGIC "CHESS" as source, 
# MAGIC "" as code,
# MAGIC "HospitalAdmissionDate" as date_is
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_chess
# MAGIC WHERE HospitalAdmissionDate IS NOT null

# COMMAND ----------

# MAGIC %sql
# MAGIC --- HES_APC
# MAGIC --- Get all patients hospitalised with a covid diagnosis U07.1 or U07.2
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_apc_covid as
# MAGIC SELECT person_id_deid, date, 
# MAGIC "02_Covid_admission" as covid_phenotype,
# MAGIC (case when DIAG_4_CONCAT LIKE "%U071%" THEN 'U07.1'
# MAGIC when DIAG_4_CONCAT LIKE "%U072%" THEN 'U07.2' Else '0' End) as clinical_code,
# MAGIC (case when DIAG_4_CONCAT LIKE "%U071%" THEN 'Confirmed_COVID19'
# MAGIC when DIAG_4_CONCAT LIKE "%U072%" THEN 'Suspected_COVID19' Else '0' End) as description,
# MAGIC (case when DIAG_4_CONCAT LIKE "%U071%" THEN 'confirmed'
# MAGIC when DIAG_4_CONCAT LIKE "%U072%" THEN 'suspected' Else '0' End) as covid_status,
# MAGIC "HES APC" as source, 
# MAGIC "ICD10" as code, date_is, SUSRECID
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_apc

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3: Critical Care

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.1 ICU admission

# COMMAND ----------

# MAGIC %sql
# MAGIC --- CHESS - ICU
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_chess_covid_icu as
# MAGIC SELECT person_id_deid, DateAdmittedICU as date,
# MAGIC "03_ICU_admission" as covid_phenotype,
# MAGIC "" as clinical_code, 
# MAGIC "DateAdmittedICU IS NOT null" as description,
# MAGIC "" as covid_status,
# MAGIC "CHESS" as source, 
# MAGIC "" as code,
# MAGIC "DateAdmittedICU" as date_is
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_chess
# MAGIC WHERE DateAdmittedICU IS NOT null

# COMMAND ----------

# MAGIC %sql
# MAGIC -- HES_CC
# MAGIC -- ID is in HES_CC AND has U071 or U072 from HES_APC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_cc_covid as
# MAGIC SELECT apc.person_id_deid, cc.date,
# MAGIC '03_ICU_admission' as covid_phenotype,
# MAGIC "" as clinical_code,
# MAGIC "id is in hes_cc table" as description,
# MAGIC "" as covid_status,
# MAGIC "" as code,
# MAGIC 'HES CC' as source, cc.date_is, BRESSUPDAYS, ARESSUPDAYS
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_apc as apc
# MAGIC INNER JOIN dars_nic_391419_j3w9t_collab.ccu013_tmp_cc AS cc
# MAGIC ON cc.SUSRECID = apc.SUSRECID
# MAGIC WHERE cc.BESTMATCH = 1
# MAGIC AND (DIAG_4_CONCAT LIKE '%U071%' OR DIAG_4_CONCAT LIKE '%U072%')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.2 NIV

# COMMAND ----------

# MAGIC %sql
# MAGIC --- CHESS - NIV
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_chess_covid_niv as
# MAGIC SELECT person_id_deid, HospitalAdmissionDate as date,
# MAGIC "03_NIV_treatment" as covid_phenotype,
# MAGIC "" as clinical_code, 
# MAGIC "Highflownasaloxygen OR NoninvasiveMechanicalventilation == Yes" as description,
# MAGIC "" as covid_status,
# MAGIC "CHESS" as source, 
# MAGIC "" as code,
# MAGIC "HospitalAdmissionDate" as date_is -- Can't be any more precise
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_chess
# MAGIC WHERE HospitalAdmissionDate IS NOT null
# MAGIC AND (Highflownasaloxygen == "Yes" OR NoninvasiveMechanicalventilation == "Yes")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- HES CC NIV
# MAGIC -- Admissions where BRESSUPDAYS > 0 (i.e there was some BASIC respiratory support)
# MAGIC -- ID is in HES_CC AND has U071 or U072 from HES_APC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_cc_niv_covid as
# MAGIC SELECT person_id_deid, date,
# MAGIC '03_NIV_treatment' as covid_phenotype,
# MAGIC "" as clinical_code,
# MAGIC "bressupdays > 0" as description,
# MAGIC "" as covid_status,
# MAGIC "" as code,
# MAGIC 'HES CC' as source, date_is, BRESSUPDAYS, ARESSUPDAYS
# MAGIC FROM global_temp.ccu013_cc_covid
# MAGIC WHERE BRESSUPDAYS > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- HES APC NIV
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_apc_niv_covid as
# MAGIC SELECT person_id_deid, date, 
# MAGIC "03_NIV_treatment" as covid_phenotype,
# MAGIC (case when OPERTN_4_CONCAT LIKE "%E852%" THEN 'E85.2'
# MAGIC when OPERTN_4_CONCAT LIKE "%E856%" THEN 'E85.6' Else '0' End) as clinical_code,
# MAGIC (case when OPERTN_4_CONCAT LIKE "%E852%" THEN 'Non-invasive ventilation NEC'
# MAGIC when OPERTN_4_CONCAT LIKE "%E856%" THEN 'Continuous positive airway pressure' Else '0' End) as description,
# MAGIC "" as covid_status,
# MAGIC "HES APC" as source,
# MAGIC "OPCS" as code, date_is, SUSRECID
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_apc
# MAGIC WHERE (DIAG_4_CONCAT LIKE "%U071%"
# MAGIC    OR DIAG_4_CONCAT LIKE "%U072%")
# MAGIC AND (OPERTN_4_CONCAT LIKE '%E852%' 
# MAGIC       OR OPERTN_4_CONCAT LIKE '%E856%')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SUS NIV
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_sus_niv_covid as 
# MAGIC SELECT person_id_deid, PRIMARY_PROCEDURE_DATE as date, PROCEDURE_CONCAT,
# MAGIC "03_NIV_treatment" as covid_phenotype,
# MAGIC (case when PROCEDURE_CONCAT LIKE "%E852%" OR PROCEDURE_CONCAT LIKE "%E85.2%" THEN 'E85.2'
# MAGIC when PROCEDURE_CONCAT LIKE "%E856%" OR PROCEDURE_CONCAT LIKE "%E85.6%" THEN 'E85.6' Else '0' End) as clinical_code,
# MAGIC (case when PROCEDURE_CONCAT LIKE "%E852%" OR PROCEDURE_CONCAT LIKE "%E85.2%" THEN 'Non-invasive ventilation NEC'
# MAGIC when PROCEDURE_CONCAT LIKE "%E856%" OR PROCEDURE_CONCAT LIKE "%E85.6%" THEN 'Continuous positive airway pressure' Else '0' End) as description,
# MAGIC "" as covid_status,
# MAGIC "SUS" as source, 
# MAGIC "OPCS" as code, "PRIMARY_PROCEDURE_DATE" as date_is
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_sus
# MAGIC WHERE (DIAG_CONCAT LIKE "%U071%"
# MAGIC    OR DIAG_CONCAT LIKE "%U072%") AND
# MAGIC    (PROCEDURE_CONCAT LIKE "%E852%" OR PROCEDURE_CONCAT LIKE "%E85.2%" OR PROCEDURE_CONCAT LIKE "%E856%" OR PROCEDURE_CONCAT LIKE "%E85.6%") AND
# MAGIC    PRIMARY_PROCEDURE_DATE IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.3 IMV

# COMMAND ----------

# MAGIC %sql
# MAGIC -- HES CC IMV
# MAGIC -- Admissions where ARESSUPDAYS > 0 (i.e there was some ADVANCED respiratory support)
# MAGIC -- ID is in HES_CC AND has U071 or U072 from HES_APC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_cc_imv_covid as
# MAGIC SELECT person_id_deid, date,
# MAGIC '03_IMV_treatment' as covid_phenotype,
# MAGIC "" as clinical_code,
# MAGIC "ARESSUPDAYS > 0" as description,
# MAGIC "" as covid_status,
# MAGIC "" as code,
# MAGIC 'HES CC' as source, date_is, BRESSUPDAYS, ARESSUPDAYS
# MAGIC FROM global_temp.ccu013_cc_covid
# MAGIC WHERE ARESSUPDAYS > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC --- CHESS - IMV
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_chess_covid_imv as
# MAGIC SELECT person_id_deid, DateAdmittedICU as date,
# MAGIC "03_IMV_treatment" as covid_phenotype,
# MAGIC "" as clinical_code, 
# MAGIC "Invasivemechanicalventilation == Yes" as description,
# MAGIC "" as covid_status,
# MAGIC "CHESS" as source, 
# MAGIC "" as code,
# MAGIC "DateAdmittedICU" as date_is -- Using ICU date as probably most of the IMV happened there, but may lose some records (250/10k)
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_chess
# MAGIC WHERE DateAdmittedICU IS NOT null
# MAGIC AND Invasivemechanicalventilation == "Yes"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- HES APC IMV
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_apc_imv_covid as
# MAGIC SELECT person_id_deid, date, 
# MAGIC "03_IMV_treatment" as covid_phenotype,
# MAGIC (case when OPERTN_4_CONCAT LIKE "%E851%" THEN 'E85.1'
# MAGIC when OPERTN_4_CONCAT LIKE "%X56%" THEN 'X56' Else '0' End) as clinical_code,
# MAGIC (case when OPERTN_4_CONCAT LIKE "%E851%" THEN 'Invasive ventilation'
# MAGIC when OPERTN_4_CONCAT LIKE "%X56%" THEN 'Intubation of trachea' Else '0' End) as description,
# MAGIC "" as covid_status,
# MAGIC "HES APC" as source, 
# MAGIC "OPCS" as code, date_is, SUSRECID
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_apc
# MAGIC WHERE (OPERTN_4_CONCAT LIKE '%E851%' 
# MAGIC       OR OPERTN_4_CONCAT LIKE '%X56%')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SUS IMV
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_sus_imv_covid as 
# MAGIC SELECT person_id_deid, PRIMARY_PROCEDURE_DATE as date, PROCEDURE_CONCAT,
# MAGIC "03_IMV_treatment" as covid_phenotype,
# MAGIC (case when PROCEDURE_CONCAT LIKE "%E851%" OR PROCEDURE_CONCAT LIKE "%E85.1%" THEN 'E85.1'
# MAGIC when PROCEDURE_CONCAT LIKE "%X56%" THEN 'X56' Else '0' End) as clinical_code,
# MAGIC (case when PROCEDURE_CONCAT LIKE "%E851%" OR PROCEDURE_CONCAT LIKE "%E85.1%" THEN 'Invasive ventilation'
# MAGIC when PROCEDURE_CONCAT LIKE "%X56%" THEN 'Intubation of trachea' Else '0' End) as description,
# MAGIC "" as covid_status,
# MAGIC "SUS" as source, 
# MAGIC "OPCS" as code, "PRIMARY_PROCEDURE_DATE" as date_is
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_sus
# MAGIC WHERE (DIAG_CONCAT LIKE "%U071%"
# MAGIC    OR DIAG_CONCAT LIKE "%U072%") AND
# MAGIC    (PROCEDURE_CONCAT LIKE "%E851%" OR PROCEDURE_CONCAT LIKE "%E85.1%" OR PROCEDURE_CONCAT LIKE "%X56%") AND
# MAGIC    PRIMARY_PROCEDURE_DATE IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.4 EMCO

# COMMAND ----------

# MAGIC %sql
# MAGIC --- CHESS - ECMO
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_chess_covid_ecmo as
# MAGIC SELECT person_id_deid, DateAdmittedICU as date,
# MAGIC "03_ECMO_treatment" as covid_phenotype,
# MAGIC "" as clinical_code, 
# MAGIC "RespiratorySupportECMO == Yes" as description,
# MAGIC "" as covid_status,
# MAGIC "CHESS" as source, 
# MAGIC "" as code,
# MAGIC "DateAdmittedICU" as date_is -- Reasonable
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_chess
# MAGIC WHERE DateAdmittedICU IS NOT null
# MAGIC AND RespiratorySupportECMO == "Yes"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- HES APC ECMO
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_apc_ecmo_covid as
# MAGIC SELECT person_id_deid, date, 
# MAGIC "03_ECMO_treatment" as covid_phenotype,
# MAGIC "X58.1" as clinical_code,
# MAGIC "Extracorporeal membrane oxygenation" as description,
# MAGIC "" as covid_status,
# MAGIC "HES APC" as source, 
# MAGIC "OPCS" as code, date_is, SUSRECID
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_apc
# MAGIC WHERE (DIAG_4_CONCAT LIKE "%U071%"
# MAGIC    OR DIAG_4_CONCAT LIKE "%U072%")
# MAGIC AND OPERTN_4_CONCAT LIKE '%X581%'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SUS ECMO
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_sus_ecmo_covid as 
# MAGIC SELECT person_id_deid, PRIMARY_PROCEDURE_DATE as date,
# MAGIC "03_ECMO_treatment" as covid_phenotype,
# MAGIC "X58.1" as clinical_code,
# MAGIC "Extracorporeal membrane oxygenation" as description,
# MAGIC "" as covid_status,
# MAGIC "SUS" as source, 
# MAGIC "OPCS" as code, "PRIMARY_PROCEDURE_DATE" as date_is
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_sus
# MAGIC WHERE (DIAG_CONCAT LIKE "%U071%"
# MAGIC    OR DIAG_CONCAT LIKE "%U072%") AND
# MAGIC    (PROCEDURE_CONCAT LIKE "%X58.1%" OR PROCEDURE_CONCAT LIKE "%X581%") AND
# MAGIC    PRIMARY_PROCEDURE_DATE IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4: Death from COVID

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Identify all individuals with a covid diagnosis as death cause
# MAGIC --- OBS: 280421 - before we got a max date of death by grouping the query on person_id_deid. I cannot get this working after adding the case bit to determine confrimed/suspected
# MAGIC ---               so multiple deaths per person might present in table if they exsist in the raw input!
# MAGIC --- CT: Re ^ this is fine as we're presenting a view of the data as it stands, including multiple events etc. further filtering will occurr downstream
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_severe_death as
# MAGIC SELECT person_id_deid, death_date as date,
# MAGIC "04_Fatal_with_covid_diagnosis" as covid_phenotype,
# MAGIC '04_fatal' as covid_severity,
# MAGIC (CASE WHEN S_UNDERLYING_COD_ICD10 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_1 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_2 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_3 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_4 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_5 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_6 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_7 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_8 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_9 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_10 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_11 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_12 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_13 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_14 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_15 LIKE "%U071%" THEN 'U071' Else 'U072' End) as clinical_code, 
# MAGIC '' as description, 
# MAGIC 'ICD10' as code, 
# MAGIC 'deaths' as source, 'REG_DATE_OF_DEATH' as date_is,
# MAGIC (CASE WHEN S_UNDERLYING_COD_ICD10 LIKE "%U071%" THEN 'confirmed'
# MAGIC  WHEN S_COD_CODE_1 LIKE "%U071%" THEN 'confirmed'
# MAGIC  WHEN S_COD_CODE_2 LIKE "%U071%" THEN 'confirmed'
# MAGIC  WHEN S_COD_CODE_3 LIKE "%U071%" THEN 'confirmed'
# MAGIC  WHEN S_COD_CODE_4 LIKE "%U071%" THEN 'confirmed'
# MAGIC  WHEN S_COD_CODE_5 LIKE "%U071%" THEN 'confirmed'
# MAGIC  WHEN S_COD_CODE_6 LIKE "%U071%" THEN 'confirmed'
# MAGIC  WHEN S_COD_CODE_7 LIKE "%U071%" THEN 'confirmed'
# MAGIC  WHEN S_COD_CODE_8 LIKE "%U071%" THEN 'confirmed'
# MAGIC  WHEN S_COD_CODE_9 LIKE "%U071%" THEN 'confirmed'
# MAGIC  WHEN S_COD_CODE_10 LIKE "%U071%" THEN 'confirmed'
# MAGIC  WHEN S_COD_CODE_11 LIKE "%U071%" THEN 'confirmed'
# MAGIC  WHEN S_COD_CODE_12 LIKE "%U071%" THEN 'confirmed'
# MAGIC  WHEN S_COD_CODE_13 LIKE "%U071%" THEN 'confirmed'
# MAGIC  WHEN S_COD_CODE_14 LIKE "%U071%" THEN 'confirmed'
# MAGIC  WHEN S_COD_CODE_15 LIKE "%U071%" THEN 'confirmed' Else 'suspected' End) as covid_status
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_deaths
# MAGIC WHERE ((S_UNDERLYING_COD_ICD10 LIKE "%U071%") OR (S_UNDERLYING_COD_ICD10 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_1 LIKE "%U071%") OR (S_COD_CODE_1 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_2 LIKE "%U071%") OR (S_COD_CODE_2 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_3 LIKE "%U071%") OR (S_COD_CODE_3 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_4 LIKE "%U071%") OR (S_COD_CODE_4 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_5 LIKE "%U071%") OR (S_COD_CODE_5 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_6 LIKE "%U071%") OR (S_COD_CODE_6 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_7 LIKE "%U071%") OR (S_COD_CODE_7 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_8 LIKE "%U071%") OR (S_COD_CODE_8 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_9 LIKE "%U071%") OR (S_COD_CODE_9 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_10 LIKE "%U071%") OR (S_COD_CODE_10 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_11 LIKE "%U071%") OR (S_COD_CODE_11 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_12 LIKE "%U071%") OR (S_COD_CODE_12 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_13 LIKE "%U071%") OR (S_COD_CODE_13 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_14 LIKE "%U071%") OR (S_COD_CODE_14 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_15 LIKE "%U071%") OR (S_COD_CODE_15 LIKE "%U072%"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4.2 Deaths during COVID admission
# MAGIC Based on Sam Hollings comment:
# MAGIC 
# MAGIC > Something I've been thinking about, is whether the HES column discharge method: "DISMETH" = 4 (the code for "died") should also be used to identify dead patients.
# MAGIC Similarly, a DISDEST = 79 (discharge destination not applicable, died or stillborn)  
# MAGIC   
# MAGIC This appears to add ~ 4k deaths that aren't counted as within 28 days of a positive test (presumably most likely at >= day 29) or with COVID-19 on the death certificate.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- APC inpatient deaths during COVID-19 admission
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_apc_covid_deaths as
# MAGIC SELECT 
# MAGIC   person_id_deid,
# MAGIC   DISDATE as date,
# MAGIC   "04_Covid_inpatient_death" as covid_phenotype,
# MAGIC   "" as clinical_code,
# MAGIC   (case when 
# MAGIC       DISMETH = 4 THEN 'DISMETH = 4 (Died)'
# MAGIC   when 
# MAGIC       DISDEST = 79 THEN 'DISDEST = 79 (Not applicable - PATIENT died or still birth)' 
# MAGIC   Else '0' End) as description,
# MAGIC   (case when 
# MAGIC       DIAG_4_CONCAT LIKE "%U071%" THEN 'confirmed'
# MAGIC   when 
# MAGIC       DIAG_4_CONCAT LIKE "%U072%" THEN 'suspected' 
# MAGIC   Else '0' End) as covid_status,
# MAGIC   "" as code,
# MAGIC   "HES APC" as source, 
# MAGIC   "DISDATE" as date_is
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t_collab.ccu013_tmp_apc
# MAGIC WHERE 
# MAGIC   (DIAG_4_CONCAT LIKE "%U071%" OR DIAG_4_CONCAT LIKE "%U072%")
# MAGIC AND (DISMETH = 4 -- died
# MAGIC       OR 
# MAGIC     DISDEST = 79) -- discharge destination not applicable, died or stillborn
# MAGIC     -- WARNING hard-coded study-start date
# MAGIC AND (DISDATE >= TO_DATE("20200123", "yyyyMMdd")) -- death after study start

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SUS inpatient deaths during COVID-19 admission
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_sus_covid_deaths as
# MAGIC SELECT 
# MAGIC   person_id_deid,
# MAGIC   END_DATE_HOSPITAL_PROVIDER_SPELL as date,
# MAGIC   "04_Covid_inpatient_death" as covid_phenotype,
# MAGIC   "" as clinical_code,
# MAGIC   (case when 
# MAGIC       DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL = 4 THEN 'DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL = 4 (Died)'
# MAGIC   when 
# MAGIC       DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL = 79 THEN 'DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL = 79 (Not applicable - PATIENT died or still birth)' 
# MAGIC   Else '0' End) as description,
# MAGIC   (case when 
# MAGIC       DIAG_CONCAT LIKE "%U071%" THEN 'confirmed'
# MAGIC   when 
# MAGIC       DIAG_CONCAT LIKE "%U072%" THEN 'suspected' 
# MAGIC   Else '0' End) as covid_status,
# MAGIC   "" as code,
# MAGIC   "SUS" as source, 
# MAGIC   "END_DATE_HOSPITAL_PROVIDER_SPELL" as date_is
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t_collab.ccu013_tmp_sus
# MAGIC WHERE 
# MAGIC   (DIAG_CONCAT LIKE "%U071%" OR DIAG_CONCAT LIKE "%U072%")
# MAGIC AND (DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL = 4 -- died
# MAGIC       OR 
# MAGIC     DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL = 79) -- discharge destination not applicable, died or stillborn
# MAGIC AND (END_DATE_HOSPITAL_PROVIDER_SPELL IS NOT NULL)

# COMMAND ----------

drop_table("ccu013_covid_severe_death")
create_table("ccu013_covid_severe_death") 

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_covid_severe_death ZORDER BY person_id_deid

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Trajectory table
# MAGIC Build the trajectory table from all the individual tables created in step 2. This will give a table of IDs, dates and phenotypes which are *not* exclusive. This way we can plot distributions of time intervals between events (test-admission-icu etc.) and take a **data-driven** approach to choosing thresholds to define *exclusive* severity phenotypes.

# COMMAND ----------

drop_table("tmp_ccu013_covid_trajectory_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Initiate a temporary trajecotry table with the SGSS data
# MAGIC CREATE TABLE dars_nic_391419_j3w9t_collab.tmp_ccu013_covid_trajectory_delta USING DELTA
# MAGIC as 
# MAGIC SELECT DISTINCT * FROM global_temp.ccu013_sgss_covid

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dars_nic_391419_j3w9t_collab.tmp_ccu013_covid_trajectory_delta OWNER TO dars_nic_391419_j3w9t_collab;

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE dars_nic_391419_j3w9t_collab.tmp_ccu013_covid_trajectory_delta

# COMMAND ----------

# Append each of the covid related events tables to the temporary trajectory table
for table in ['ccu013_gdppr_covid','ccu013_apc_covid','ccu013_sus_covid','ccu013_chess_covid_hospital','ccu013_chess_covid_icu',
              'ccu013_chess_covid_niv','ccu013_chess_covid_imv','ccu013_chess_covid_ecmo','ccu013_cc_covid','ccu013_cc_niv_covid','ccu013_cc_imv_covid','ccu013_apc_niv_covid',
              'ccu013_apc_imv_covid','ccu013_apc_ecmo_covid', 'ccu013_sus_niv_covid', 'ccu013_sus_imv_covid', 'ccu013_sus_ecmo_covid',
             'ccu013_apc_covid_deaths', 'ccu013_sus_covid_deaths']:
  spark.sql(f"""REFRESH TABLE global_temp.{table}""")
  (spark.table(f'global_temp.{table}')
   .select("person_id_deid", "date", "covid_phenotype", "clinical_code", "description", "covid_status", "code", "source", "date_is")
   .distinct()
   .write.format("delta").mode('append')
   .saveAsTable('dars_nic_391419_j3w9t_collab.tmp_ccu013_covid_trajectory_delta'))

# COMMAND ----------

## Add on the fatal covid cases with diagnosis
(spark.table("dars_nic_391419_j3w9t_collab.ccu013_covid_severe_death").select("person_id_deid", "date", "covid_phenotype", "clinical_code", "description", "covid_status", "code", "source", "date_is")
    .distinct()
    .write.format("delta").mode('append')
    .saveAsTable('dars_nic_391419_j3w9t_collab.tmp_ccu013_covid_trajectory_delta'))

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.tmp_ccu013_covid_trajectory_delta ZORDER BY person_id_deid

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Identifying deaths within 28 days

# COMMAND ----------

# Identifying fatal events happing within 28 days of first covid diagnosis/event. 
from pyspark.sql.functions import *

# Get all deaths
all_fatal = spark.sql("""
SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_deaths
""")

# Identify earliest non fatal event from trajectory table
first_non_fatal = spark.sql("""
WITH list_patients_to_omit AS (SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.tmp_ccu013_covid_trajectory_delta WHERE covid_phenotype = '04_Fatal_with_covid_diagnosis')
SELECT /*+ BROADCAST(list_patients_to_omit) */
t.person_id_deid, MIN(t.date) AS first_covid_event
FROM dars_nic_391419_j3w9t_collab.tmp_ccu013_covid_trajectory_delta as t
LEFT ANTI JOIN list_patients_to_omit ON t.PERSON_ID_DEID = list_patients_to_omit.PERSON_ID_DEID
GROUP BY t.person_id_deid
""")

# Join with death data - at this stage, not filtering by death cause
# since events with covid as the cause are already in the 
# trajectories table and are excluded in the step above.
first_non_fatal = first_non_fatal.join(all_fatal, ['person_id_deid'], how='left')
first_non_fatal = first_non_fatal.select(['person_id_deid', 'first_covid_event', 'death_date']) 

# Calculate elapsed number of days between earliest event and death date
first_non_fatal = first_non_fatal.withColumn('days_to_death', \
  when(~first_non_fatal['death_date'].isNull(), \
       datediff(first_non_fatal["death_date"], first_non_fatal['first_covid_event'])).otherwise(-1))
 
# Mark deaths within 28 days
first_non_fatal = first_non_fatal.withColumn('28d_death', \
  when((first_non_fatal['days_to_death'] >= 0) & (first_non_fatal['days_to_death'] <= 28), 1).otherwise(0))

# Merge data into main trajectory table (flag as suspected not confirmed!)
first_non_fatal.createOrReplaceGlobalTempView('first_non_fatal')
  

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Write events to trajectory table
# MAGIC --- person_id_deid, date, covid_phenotype, clinical_code, description, covid_status, code, source, date_is
# MAGIC create or replace global temp view ccu013_covid_trajectory_final_temp AS
# MAGIC select * from dars_nic_391419_j3w9t_collab.tmp_ccu013_covid_trajectory_delta UNION ALL
# MAGIC select distinct
# MAGIC   person_id_deid, 
# MAGIC   death_date as date,
# MAGIC   '04_Fatal_without_covid_diagnosis' as covid_phenotype,
# MAGIC   '' AS clinical_code,
# MAGIC   'ONS death within 28 days' AS description,
# MAGIC   'suspected' AS covid_status,
# MAGIC   '' AS code,
# MAGIC   'deaths' AS source,
# MAGIC   'death_date' AS date_is
# MAGIC FROM
# MAGIC   global_temp.first_non_fatal
# MAGIC WHERE
# MAGIC   28d_death = 1;
# MAGIC   

# COMMAND ----------

## Add in production Date and save as final trajectory table
traject = spark.sql('''SELECT * FROM global_temp.ccu013_covid_trajectory_final_temp''')
traject = traject.withColumn('ProductionDate', lit(str(production_date.value)))
traject.createOrReplaceGlobalTempView('ccu013_covid_trajectory')
drop_table("ccu013_covid_trajectory")
create_table("ccu013_covid_trajectory")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory ZORDER BY person_id_deid

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Check counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT covid_phenotype, source, count(DISTINCT person_id_deid) as count
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
# MAGIC group by covid_phenotype, source
# MAGIC order by covid_phenotype

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT covid_phenotype, count(DISTINCT person_id_deid) as count
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
# MAGIC group by covid_phenotype
# MAGIC order by covid_phenotype

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OLD value @ 29.04.21 = 4244049
# MAGIC -- OLD value @ 29.04.21 v2 post Spiros changes = 4244049
# MAGIC -- OLD value @ 29.04.21 v3 post Johan - pillar2 null not included = 3630983
# MAGIC -- OLD value @ 30.04.21 = 3929787 # New data frezes from 280421
# MAGIC -- OLD value @ 02.06.21 = 4346810 # After including SUS
# MAGIC -- OLD value after enforcing everyone has to be alive in gdppr 
# MAGIC -- OLD value after enforcing everyone has to be alive and in gdppr at study start and minimal followup 28 days
# MAGIC -- OLD value @ 150621 3992872 after going back to no cohort subset and excluding pillar2-antigen and hes_op as sources
# MAGIC -- Current value @220621 after accidental table deletion is still 3992872 - no damage done!
# MAGIC -- OLD value @170821 - 5044357
# MAGIC 
# MAGIC SELECT count(DISTINCT person_id_deid)
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Severity table  
# MAGIC Classify all participants according to their most severe COVID-19 event into a severity phenotype (mild, moderate, severe, death), i.e. these are mutually exclusive and patients can only have one severity classification.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1: Mild COVID
# MAGIC * No hosptitalisation
# MAGIC * No death within 4 weeks of first diagnosis
# MAGIC * No death ever with COVID diagnosis

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_mild as
# MAGIC WITH list_patients_to_omit AS (SELECT person_id_deid from dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory WHERE covid_phenotype IN ('02_Covid_admission', '03_ECMO_treatment', '03_IMV_treatment', '03_NIV_treatment', '03_ICU_treatment', '04_Fatal_with_covid_diagnosis','04_Fatal_without_covid_diagnosis', '04_Covid_inpatient_death'))
# MAGIC SELECT /*+ BROADCAST(list_patients_to_omit) */ person_id_deid, min(date) as date, '01_not_hospitalised' as covid_severity 
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory as t
# MAGIC LEFT ANTI JOIN list_patients_to_omit ON t.person_id_deid = list_patients_to_omit.person_id_deid
# MAGIC group by person_id_deid

# COMMAND ----------

drop_table("ccu013_covid_mild")
create_table("ccu013_covid_mild") 

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_covid_mild ZORDER BY person_id_deid

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2: Moderate COVID - Hospitalised
# MAGIC - Hospital admission
# MAGIC - No critical care within that admission
# MAGIC - No death within 4 weeks
# MAGIC - No death from COVID diagnosis ever

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Moderate COVID
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_moderate as
# MAGIC WITH list_patients_to_omit AS (SELECT person_id_deid from dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory WHERE covid_phenotype IN ('03_ECMO_treatment', '03_IMV_treatment', '03_NIV_treatment', '03_ICU_treatment','04_Fatal_with_covid_diagnosis','04_Fatal_without_covid_diagnosis', '04_Covid_inpatient_death'))
# MAGIC SELECT person_id_deid, min(date) as date, '02_hospitalised' as covid_severity 
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory as t
# MAGIC LEFT ANTI JOIN list_patients_to_omit ON t.person_id_deid = list_patients_to_omit.person_id_deid
# MAGIC WHERE (covid_phenotype IN ('02_Covid_admission'))
# MAGIC group by person_id_deid

# COMMAND ----------

drop_table("ccu013_covid_moderate")
create_table("ccu013_covid_moderate")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_covid_moderate ZORDER BY person_id_deid

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 4.3: Severe COVID - Hospitalised with critical care
# MAGIC Hospitalised with one of the following treatments
# MAGIC - ICU treatment 
# MAGIC - NIV and/or IMV treatment
# MAGIC - ECMO treatment

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Severe COVID
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_severe as
# MAGIC WITH list_patients_to_omit AS (SELECT person_id_deid from dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
# MAGIC WHERE covid_phenotype IN ('04_Fatal_with_covid_diagnosis','04_Fatal_without_covid_diagnosis', '04_Covid_inpatient_death'))
# MAGIC SELECT person_id_deid, min(date) as date, '03_hospitalised_ventilatory_support' as covid_severity 
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory AS t
# MAGIC LEFT ANTI JOIN list_patients_to_omit ON t.person_id_deid = list_patients_to_omit.person_id_deid
# MAGIC WHERE covid_phenotype IN ('03_ECMO_treatment', '03_IMV_treatment', '03_NIV_treatment', '03_ICU_treatment')
# MAGIC group by person_id_deid

# COMMAND ----------

drop_table("ccu013_covid_severe")
create_table("ccu013_covid_severe") 

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_covid_severe ZORDER BY person_id_deid

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4: Fatal COVID
# MAGIC - Fatal Covid with confirmed or supsected Covid on death register
# MAGIC - Death within 28 days without Covid on death register

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Fatal COVID
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_fatal as
# MAGIC SELECT person_id_deid, min(date) as date, '04_fatal' as covid_severity 
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
# MAGIC WHERE (covid_phenotype IN ('04_Fatal_with_covid_diagnosis','04_Fatal_without_covid_diagnosis', '04_Covid_inpatient_death'))
# MAGIC group by person_id_deid

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5: Final combined severity table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_severity_temp
# MAGIC as SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_mild
# MAGIC UNION ALL
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_moderate
# MAGIC UNION ALL
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_severe
# MAGIC UNION ALL
# MAGIC SELECT * FROM global_temp.ccu013_covid_fatal

# COMMAND ----------

severe = spark.sql('''SELECT * FROM global_temp.ccu013_covid_severity_temp''')
severe = severe.withColumn('ProductionDate', lit(str(production_date.value)))
severe.createOrReplaceGlobalTempView('ccu013_covid_severity')
drop_table("ccu013_covid_severity")
create_table("ccu013_covid_severity")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_covid_severity

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_severity

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 Check counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT covid_severity, count(DISTINCT person_id_deid)
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_severity
# MAGIC group by covid_severity
# MAGIC order by covid_severity

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OLD value @ 30.04.21 = 3929787
# MAGIC -- OLD value @ 04.05.21 = 3933813
# MAGIC -- OLD value @ 28.05.21 = 4287595
# MAGIC -- OLD value @ 02.06.21 = 4346793
# MAGIC -- OLD value @ 04.06.21 = 3977185
# MAGIC -- OLD value @ 15.06.21 = 3772432
# MAGIC -- OLD value @ 15.06.21 = 3992872
# MAGIC -- OLD value @ 17.08.21 = 5044357
# MAGIC SELECT count(DISTINCT person_id_deid)
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_severity

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 5: Tidy up drop tmp tables

# COMMAND ----------

# Uncomment to tidy
# drop_table("ccu013_covid_mild")
# drop_table("ccu013_covid_moderate")
# drop_table("ccu013_covid_severe")
# drop_table("ccu013_covid_severe_death")
# drop_table("ccu013_covid_not_mild")
# drop_table("ccu013_covid_not_mild_or_severe")
# drop_table("ccu013_covid_severe_deaths")
# drop_table("ccu013_tmp_apc")
# drop_table("ccu013_tmp_cc")
# drop_table("ccu013_tmp_op")
# drop_table("ccu013_tmp_chess")
# drop_table("ccu013_tmp_sgss")
# drop_table("ccu013_tmp_deaths")
# drop_table("ccu013_tmp_gdppr")
# drop_table("ccu013_tmp_sus")
# drop_table("ccu013_tmp_pillar2")
# drop_table("tmp_ccu013_covid_trajectory_delta")
# drop_table("tmp_ccu013_covid_trajectory")

# COMMAND ----------

# drop_table("ccu013_severity_paper_cohort_tmp")
# drop_table("ccu013_jht_death_checks_tmp")
