-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Explore Primary diagnoses when COVID is Secondary
-- MAGIC 
-- MAGIC Reviwer comments:
-- MAGIC > How are COVID-19 hospitalizations defined? What are the diagnosis codes used? Was it **only defined by the primary diagnosis, or were secondary diagnoses considered?** What were the **primary diagnoses corresponding to the secondary diagnosis of COVID-19** as some may be associated with COVID-19 (such as respiratory or coagulopathy), but others may not be (such as a motor vehicle accident)? Again, these are important considerations in evaluating phenotypes and teaching about how to best approach these large-scale studies.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Derive primary/secondary admission Dx from trajectory table
-- MAGIC Outline:
-- MAGIC 1. Get `MIN(date)` from `trajectory` table `WHERE covid_phenotype == "02_Covid_admission" AND source == "HES APC" `
-- MAGIC   * This date is `EPISTART` see [Cell 22 in `CCU013_01_create_table_aliases`](https://db.core.data.digital.nhs.uk/#notebook/1753732/command/1783084)
-- MAGIC 2. Get the `ADMIDATE` for these episodes from `HES APC`
-- MAGIC 3. Find first episodes occuring on the first day of admission, i.e. where `EPISTART = ADMIDATE` and `EPIORDER = 1`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Get ids from trajectory table  
-- MAGIC * Distinct id as can have multiple in trajectory table
-- MAGIC * `Min(date)` as can have multiple in trajectory table
-- MAGIC * `date` in trajectory table for HES APC admissions is `EPISTART`

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_01_hosp_apc_ids as
SELECT
  distinct person_id_deid as PERSON_ID_DEID,
  MIN(date) as EPISTART
FROM 
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
WHERE
  covid_phenotype == "02_Covid_admission"
AND
  source == "HES APC"
GROUP BY
  person_id_deid

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Retrieve version of HES APC matching our study
-- MAGIC * NB these params are not exactly the same as those in the non-`paper_cohort` table which is more all encompassing
-- MAGIC * Instead see manuscript
-- MAGIC * Pre-print:
-- MAGIC     * `production_date` = `2021-07-29 13:39:04.161949`
-- MAGIC     * `start_date` = `2020-01-23`
-- MAGIC     * `end_date` = `2021-05-31`  
-- MAGIC * Revision 1:  
-- MAGIC     * `production_date` = `2022-01-20 14:58:52.353312`
-- MAGIC     * `start_date` = `2020-01-23`
-- MAGIC     * `end_date` = `2021-11-30` 

-- COMMAND ----------

-- Reproduce parameters from pre-print
CREATE WIDGET TEXT start_date DEFAULT "2020-01-23";
CREATE WIDGET TEXT end_date DEFAULT "2021-11-30";
-- Notebook: CCU013_11_paper_cohort_dp_skinny_record_unassembled
-- Cell: 6
CREATE WIDGET TEXT production_date DEFAULT "2022-01-20 14:58:52.353312" 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC NB this is those patient's matching our study criteria, but all records not minimum. Hence we use the `DIAG_4_CONCAT` like COVID filter.

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_01_hes_apc as
SELECT
  PERSON_ID_DEID,
  ADMIDATE,
  EPISTART,
  DIAG_4_01 as primary_Dx,
  DIAG_4_CONCAT
FROM
  dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive
WHERE
  ProductionDate == "$production_date"
AND 
  (DIAG_4_CONCAT LIKE "%U071%" OR DIAG_4_CONCAT LIKE "%U072%")
AND
  (EPISTART >= "$start_date" AND EPISTART <= "$end_date")
AND
  PERSON_ID_DEID is not null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Get `ADMIDATE`

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_01_hosp_apc_admissions as
SELECT
  distinct trajectory.PERSON_ID_DEID as PERSON_ID_DEID,
  MIN(ADMIDATE) as ADMIDATE
FROM
global_temp.ccu013_01_hosp_apc_ids as trajectory
INNER JOIN
(
SELECT
  PERSON_ID_DEID,
  ADMIDATE,
  EPISTART
FROM
  global_temp.ccu013_01_hes_apc
) as apc
ON
  trajectory.PERSON_ID_DEID = apc.PERSON_ID_DEID
AND
  trajectory.EPISTART = apc.EPISTART
GROUP BY
  trajectory.PERSON_ID_DEID

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Find first EPISODE in ADMISSION
-- MAGIC * First = `EPIORDER = 1`
-- MAGIC * We use the 'full' HES APC table to join onto here, rather than our temp view which is COVID-subsetted, as COVID may not feature in this first episode

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_01_hosp_apc_first_primary_dx as
SELECT
  apc_full.PERSON_ID_DEID,
  apc_full.ADMIDATE,
  apc_full.EPISTART,
  apc_full.EPIORDER,
  substring(apc_full.DIAG_4_01, 1, 1) as first_primary_chapter,
  apc_full.DIAG_4_01 as first_primary_icd10,
  -- Create covid primary Dx flag
  CASE WHEN apc_full.DIAG_4_01 == "U071" OR apc_full.DIAG_4_01 == "U072" then 1 else 0 end as first_primary_covid
FROM
  global_temp.ccu013_01_hosp_apc_admissions as admission
INNER JOIN
(
SELECT
  PERSON_ID_DEID,
  ADMIDATE,
  EPISTART,
  EPIORDER,
  DIAG_4_01
FROM
  dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive
WHERE
  ProductionDate == "$production_date"
AND
  EPIORDER = 1
) as apc_full
ON
  admission.PERSON_ID_DEID = apc_full.PERSON_ID_DEID
AND
-- Episodes on the day of admission
  admission.ADMIDATE = apc_full.EPISTART

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Counts

-- COMMAND ----------

SELECT
  COUNT(*),
  COUNT(distinct PERSON_ID_DEID)
FROm
  global_temp.ccu013_01_hosp_apc_first_primary_dx

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explore those with >1 'first' episode

-- COMMAND ----------

SELECT
  PERSON_ID_DEID,
  COUNT(*) as n
FROm
  global_temp.ccu013_01_hosp_apc_first_primary_dx
GROUP BY
  PERSON_ID_DEID
HAVING n > 1

-- COMMAND ----------

SELECT
  *
FROm
  global_temp.ccu013_01_hosp_apc_first_primary_dx as apc
INNER JOIN
  (SELECT
    PERSON_ID_DEID,
    COUNT(*) as n
  FROm
    global_temp.ccu013_01_hosp_apc_first_primary_dx
  GROUP BY
    PERSON_ID_DEID
  HAVING n > 1) as ids
ON
  apc.PERSON_ID_DEID = ids.PERSON_ID_DEID
ORDER BY
  n Desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## How many pts had >1 episode on first day?

-- COMMAND ----------

SELECT
  COUNT(*) as individuals,
  SUM(CASE WHEN episodes = 1 then 1 else 0 end) as n_1epi,
  ROUND(SUM(CASE WHEN episodes = 1 then 1 else 0 end) / COUNT(*) * 100, 2) as percent_1epi,
  SUM(CASE WHEN episodes > 1 then 1 else 0 end) as n_2more_epi,
  ROUND(SUM(CASE WHEN episodes > 1 then 1 else 0 end) / COUNT(*) * 100, 2) as percent_2more_epi, 
  MIN(episodes),
  MAX(episodes)
FROM
  (
  SELECT 
  distinct PERSON_ID_DEID,
  COUNT(*) as episodes
FROM
  global_temp.ccu013_01_hosp_apc_first_primary_dx
GROUP BY
  PERSON_ID_DEID
ORDER BY
  episodes Desc
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Counts

-- COMMAND ----------

SELECT
  COUNT(*) as episodes,
  COUNT(distinct PERSON_ID_DEID) as individuals,
  ROUND(COUNT(distinct PERSON_ID_DEID) / COUNT(*) * 100, 2) as percentage,
  MAX(EPIORDER),
  MIN(ADMIDATE),
  MAX(ADMIDATE),
  MIN(EPISTART),
  MAX(EPISTART)
FROM
  global_temp.ccu013_01_hosp_apc_first_primary_dx

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * We're looking for first episodes in admission, so it is plausible that both of these extend before study_start
-- MAGIC * 2012 does seem very early, e.g. pt in hospital for 10 years

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Boil down to 1 row per patient, single presence of COVID takes precedence over other codes

-- COMMAND ----------

SELECT
  COUNT(*) as records,
  -- should be 1 row per pt, check
  COUNT(distinct PERSON_ID_DEID) as individuals,
  SUM(first_primary_covid) as n_first_primary_covid,
  ROUND(SUM(first_primary_covid)/COUNT(*)*100, 2) as percent_first_primary_covid,
  COUNT(distinct PERSON_ID_DEID) - SUM(first_primary_covid) as n_NO_first_primary_covid
FROM
(
SELECT 
  distinct PERSON_ID_DEID,
  CASE WHEN SUM(first_primary_covid) >= 1 then 1 else 0 end as first_primary_covid
FROM
  global_temp.ccu013_01_hosp_apc_first_primary_dx
GROUP BY
  PERSON_ID_DEID
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The numbers are very similar to other attempts using different methods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Primary diagnoses when COVID is secondary

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## By ICD-10 Chapter

-- COMMAND ----------

-- Build ICD-10 chapter dictionary
CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_01_lkp_icd10_chapter as
SELECT
  DISTINCT substring(CODE, 1, 1) as chapter,
  ICD10_CHAPTER_DESCRIPTION as description
FROM
  dss_corporate.icd10_group_chapter_v01

-- COMMAND ----------

SELECT
  first_primary_chapter,
  FIRST(description) as description,
  COUNT(*) as records,
  COUNT(distinct PERSON_ID_DEID) as individuals,
  ROUND(COUNT(distinct PERSON_ID_DEID) / (SELECT COUNT(distinct PERSON_ID_DEID) FROM global_temp.ccu013_01_hosp_apc_first_primary_dx) * 100, 2) as percentage_individuals
FROM
global_temp.ccu013_01_hosp_apc_first_primary_dx as apc
-- Don't group by ID for this as multiple episodes relevant
LEFT JOIN
  (SELECT chapter, description FROM global_temp.ccu013_01_lkp_icd10_chapter) as icd
ON
  apc.first_primary_chapter = icd.chapter
-- WHERE
--   first_primary_covid = 0
GROUP BY first_primary_chapter
ORDER BY individuals Desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Full ICD-10 codes
-- MAGIC * Includes % of individuals to show the % which receive a primary COVID-19 Dx within this table

-- COMMAND ----------

SELECT
  first_primary_icd10,
  FIRST(description) as description,
  COUNT(*) as records,
  COUNT(distinct PERSON_ID_DEID) as individuals,
  ROUND(COUNT(distinct PERSON_ID_DEID) / (SELECT COUNT(distinct PERSON_ID_DEID) FROM global_temp.ccu013_01_hosp_apc_first_primary_dx) * 100, 2) as percentage_individuals
FROM
  global_temp.ccu013_01_hosp_apc_first_primary_dx as apc
LEFT JOIN
  (SELECT ALT_CODE as code, ICD10_DESCRIPTION as description FROM dss_corporate.icd10_group_chapter_v01) as icd
ON
  apc.first_primary_icd10 = icd.code
-- WHERE
--   first_primary_covid = 0
GROUP BY first_primary_icd10
-- Protect disclosures (nb expect 1000 rows won't get close to counts of <5 anyway)
HAVING individuals > 5
ORDER BY individuals Desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # N. unique ICD-10 codes in primary position

-- COMMAND ----------

SELECT
  COUNT(distinct first_primary_icd10) - 2 -- Subtract 2 COVID codes
FROM
  global_temp.ccu013_01_hosp_apc_first_primary_dx
