-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## ST5: Primary diagnosis on death certificate for deceased patients 
-- MAGIC > 142,224  primary diagnosis was found for the 139,818 individuals with COVID-19 on the death certificate, and 15,526 primary diagnosis for the 15,486 dying without COVID-19 on the death certificate within 28 days of a COVID-19 event.

-- COMMAND ----------

SELECT
  covid_phenotype,
  COUNT(*) as records,
  COUNT(distinct person_id_deid) as individuals
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
WHERE
  covid_phenotype = '04_Fatal_with_covid_diagnosis' OR covid_phenotype = '04_Fatal_without_covid_diagnosis'
GROUP BY
  covid_phenotype

-- COMMAND ----------

-- Reproduce parameters from study
CREATE WIDGET TEXT start_date DEFAULT "2020-01-23";
CREATE WIDGET TEXT end_date DEFAULT "2021-11-30";
CREATE WIDGET TEXT production_date DEFAULT "2022-01-20 14:58:52.353312" 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 04_Fatal_with_covid_diagnosis

-- COMMAND ----------

SELECT
  COUNT(*) as records,
  COUNT(distinct person_id_deid) as individuals
FROM
(
SELECT
-- Remove duplicates
  distinct deaths.*
FROM
  (SELECT  person_id_deid
    FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
    WHERE covid_phenotype = '04_Fatal_with_covid_diagnosis' ) as trajectory
INNER JOIN
  (
  SELECT
    DEC_CONF_NHS_NUMBER_CLEAN_DEID as person_id_deid,
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') as date,
    S_UNDERLYING_COD_ICD10 as Dx
  FROM dars_nic_391419_j3w9t_collab.deaths_dars_nic_391419_j3w9t_archive
--   Reproduce study criteria in CCU013_01_create_table_alias -> ccu013_tmp_deaths
  WHERE 
    ProductionDate == "$production_date"
  AND 
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') >= "$start_date"
  AND
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') <= "$end_date"
    -- Remove missing Dx
  AND
    S_UNDERLYING_COD_ICD10 is not null
  ) as deaths
ON
  trajectory.person_id_deid = deaths.person_id_deid
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## How many had >1 record

-- COMMAND ----------

SELECT
  COUNT(*) as individuals,
  SUM(CASE WHEN records = 1 then 1 else 0 end) as n_1record,
  ROUND(SUM(CASE WHEN records = 1 then 1 else 0 end) / COUNT(*) * 100, 2) as percent_1record,
  SUM(CASE WHEN records > 1 then 1 else 0 end) as n_2more_records,
  ROUND(SUM(CASE WHEN records > 1 then 1 else 0 end) / COUNT(*) * 100, 2) as percent_2more_records, 
  MIN(records),
  MAX(records)
FROM
(
SELECT
  person_id_deid,
  COUNT(*) as records
FROM
(
SELECT
-- Remove duplicates
  distinct deaths.*
FROM
  (SELECT person_id_deid
    FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
    WHERE covid_phenotype = '04_Fatal_with_covid_diagnosis' ) as trajectory
INNER JOIN
  (
  SELECT
    DEC_CONF_NHS_NUMBER_CLEAN_DEID as person_id_deid,
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') as date,
    S_UNDERLYING_COD_ICD10 as Dx
  FROM dars_nic_391419_j3w9t_collab.deaths_dars_nic_391419_j3w9t_archive
--   Reproduce study criteria in CCU013_01_create_table_alias -> ccu013_tmp_deaths
  WHERE 
    ProductionDate == "$production_date"
  AND 
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') >= "$start_date"
  AND
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') <= "$end_date"
    -- Remove missing Dx
  AND
    S_UNDERLYING_COD_ICD10 is not null
  ) as deaths
ON
  trajectory.person_id_deid = deaths.person_id_deid
  )
GROUP BY
  person_id_deid
ORDER BY
  records Desc
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Primary Cause(s) of Death

-- COMMAND ----------

SELECT
  Dx,
  FIRST(description) as description,
  COUNT(*) as registrations,
  COUNT(distinct person_id_deid) as individuals,
  ROUND(COUNT(distinct person_id_deid) / 
    (SELECT COUNT(distinct person_id_deid) 
    FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
    WHERE covid_phenotype = '04_Fatal_with_covid_diagnosis' ) * 100, 2) as percentage_individuals
FROM
(
SELECT
-- Remove duplicates, defined as same id/date/dx
  distinct deaths.*,
  icd.code,
  icd.description
FROM
  (SELECT person_id_deid
    FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
    WHERE covid_phenotype = '04_Fatal_with_covid_diagnosis' ) as trajectory
--     OR covid_phenotype = '04_Fatal_without_covid_diagnosis'
INNER JOIN
  (
  SELECT
    DEC_CONF_NHS_NUMBER_CLEAN_DEID as person_id_deid,
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') as date,
    S_UNDERLYING_COD_ICD10 as Dx
  FROM dars_nic_391419_j3w9t_collab.deaths_dars_nic_391419_j3w9t_archive
--   Reproduce study criteria in CCU013_01_create_table_alias -> ccu013_tmp_deaths
  WHERE 
    ProductionDate == "$production_date"
  AND 
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') >= "$start_date"
  AND
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') <= "$end_date"
    -- Remove missing Dx
  AND
    S_UNDERLYING_COD_ICD10 is not null
  ) as deaths
ON
  trajectory.person_id_deid = deaths.person_id_deid
LEFT JOIN
  (SELECT ALT_CODE as code, ICD10_DESCRIPTION as description FROM dss_corporate.icd10_group_chapter_v01) as icd
ON
  deaths.Dx = icd.code
)
GROUP BY Dx
-- Protect disclosures (nb expect 1000 rows won't get close to counts of <5 anyway)
HAVING individuals > 5
ORDER BY individuals Desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 04_Fatal_without_covid_diagnosis
-- MAGIC * NB to get correct number of individuals have to comment out the `AND S_UNDERLYING_COD_ICD10 is not null` filter. Because this included any deaths within 28d of first COVID event, where COVID wasn't the diagnosis, and so diagnosis could have been null

-- COMMAND ----------

SELECT
  COUNT(*) as records,
  COUNT(distinct person_id_deid) as individuals
FROM
(
SELECT
-- Remove duplicates
  distinct deaths.*
FROM
  (SELECT person_id_deid
    FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
    WHERE covid_phenotype = '04_Fatal_without_covid_diagnosis' ) as trajectory
INNER JOIN
  (
  SELECT
    DEC_CONF_NHS_NUMBER_CLEAN_DEID as person_id_deid,
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') as date,
    S_UNDERLYING_COD_ICD10 as Dx
  FROM dars_nic_391419_j3w9t_collab.deaths_dars_nic_391419_j3w9t_archive
--   Reproduce study criteria in CCU013_01_create_table_alias -> ccu013_tmp_deaths
  WHERE 
    ProductionDate == "$production_date"
  AND 
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') >= "$start_date"
  AND
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') <= "$end_date"
--     Remove missing Dx
--   AND
--     S_UNDERLYING_COD_ICD10 is not null
  ) as deaths
ON
  trajectory.person_id_deid = deaths.person_id_deid
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **However to get the right number of diagnoses (records) should exclude null because null isn't a diagnosis**

-- COMMAND ----------

SELECT
  COUNT(*) as records,
  COUNT(distinct person_id_deid) as individuals
FROM
(
SELECT
-- Remove duplicates
  distinct deaths.*
FROM
  (SELECT person_id_deid
    FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
    WHERE covid_phenotype = '04_Fatal_without_covid_diagnosis' ) as trajectory
INNER JOIN
  (
  SELECT
    DEC_CONF_NHS_NUMBER_CLEAN_DEID as person_id_deid,
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') as date,
    S_UNDERLYING_COD_ICD10 as Dx
  FROM dars_nic_391419_j3w9t_collab.deaths_dars_nic_391419_j3w9t_archive
--   Reproduce study criteria in CCU013_01_create_table_alias -> ccu013_tmp_deaths
  WHERE 
    ProductionDate == "$production_date"
  AND 
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') >= "$start_date"
  AND
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') <= "$end_date"
--     Remove missing Dx
  AND
    S_UNDERLYING_COD_ICD10 is not null
  ) as deaths
ON
  trajectory.person_id_deid = deaths.person_id_deid
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## How many had >1 record?

-- COMMAND ----------

SELECT
  COUNT(*) as individuals,
  SUM(CASE WHEN records = 1 then 1 else 0 end) as n_1record,
  ROUND(SUM(CASE WHEN records = 1 then 1 else 0 end) / COUNT(*) * 100, 2) as percent_1record,
  SUM(CASE WHEN records > 1 then 1 else 0 end) as n_2more_records,
  ROUND(SUM(CASE WHEN records > 1 then 1 else 0 end) / COUNT(*) * 100, 2) as percent_2more_records, 
  MIN(records),
  MAX(records)
FROM
(
SELECT
  person_id_deid,
  COUNT(*) as records
FROM
(
SELECT
-- Remove duplicates
  distinct deaths.*
FROM
  (SELECT person_id_deid
    FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
    WHERE covid_phenotype = '04_Fatal_without_covid_diagnosis' ) as trajectory
INNER JOIN
  (
  SELECT
    DEC_CONF_NHS_NUMBER_CLEAN_DEID as person_id_deid,
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') as date,
    S_UNDERLYING_COD_ICD10 as Dx
  FROM dars_nic_391419_j3w9t_collab.deaths_dars_nic_391419_j3w9t_archive
--   Reproduce study criteria in CCU013_01_create_table_alias -> ccu013_tmp_deaths
  WHERE 
    ProductionDate == "$production_date"
  AND 
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') >= "$start_date"
  AND
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') <= "$end_date"
  ) as deaths
ON
  trajectory.person_id_deid = deaths.person_id_deid
  )
GROUP BY
  person_id_deid
ORDER BY
  records Desc
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Primary causes(s) of death

-- COMMAND ----------

SELECT
  Dx,
  FIRST(description) as description,
  COUNT(*) as registrations,
  COUNT(distinct person_id_deid) as individuals,
  ROUND(COUNT(distinct person_id_deid) / 
    (SELECT COUNT(distinct person_id_deid) 
    FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
    WHERE covid_phenotype = '04_Fatal_without_covid_diagnosis' ) * 100, 2) as percentage_individuals
FROM
(
SELECT
-- Remove duplicates, defined as same id/date/dx
  distinct deaths.*,
  icd.code,
  icd.description
FROM
  (SELECT person_id_deid
    FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
    WHERE covid_phenotype = '04_Fatal_without_covid_diagnosis' ) as trajectory
INNER JOIN
  (
  SELECT
    DEC_CONF_NHS_NUMBER_CLEAN_DEID as person_id_deid,
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') as date,
    S_UNDERLYING_COD_ICD10 as Dx
  FROM dars_nic_391419_j3w9t_collab.deaths_dars_nic_391419_j3w9t_archive
--   Reproduce study criteria in CCU013_01_create_table_alias -> ccu013_tmp_deaths
  WHERE 
    ProductionDate == "$production_date"
  AND 
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') >= "$start_date"
  AND
    to_date(REG_DATE_OF_DEATH,'yyyyMMdd') <= "$end_date"
    -- Remove missing Dx
--   AND
--     S_UNDERLYING_COD_ICD10 is not null
  ) as deaths
ON
  trajectory.person_id_deid = deaths.person_id_deid
LEFT JOIN
  (SELECT ALT_CODE as code, ICD10_DESCRIPTION as description FROM dss_corporate.icd10_group_chapter_v01) as icd
ON
  deaths.Dx = icd.code
)
GROUP BY Dx
HAVING individuals > 5
ORDER BY individuals Desc
