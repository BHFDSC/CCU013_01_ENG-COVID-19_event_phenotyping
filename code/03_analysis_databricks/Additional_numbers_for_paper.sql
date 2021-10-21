-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Chris's numbers
-- MAGIC I'm going to try and lay this out in a structure mirroring the paper

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Abstract

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Methods

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from pyspark import version
-- MAGIC # Spark SQL version
-- MAGIC spark.version

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Denominator
-- MAGIC > Among 56,609,049 individuals registered with a general practitioner in England and alive on 23rd January 2020,

-- COMMAND ----------

SELECT 
  count(DISTINCT NHS_NUMBER_DEID) as total_population 
FROM 
  dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Individuals & Events + IR
-- MAGIC >  we identified 8,825,738 COVID-19 events in 3,469,528 individuals, representing an infection rate of 6.1%,

-- COMMAND ----------

SELECT 
  COUNT(*) as n_events, 
  COUNT(distinct person_id_deid) as n_individuals, 
  ROUND(COUNT(distinct person_id_deid) / (SELECT count(DISTINCT NHS_NUMBER_DEID) FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) * 100, 2) as infection_rate
FROM 
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### N. per Event

-- COMMAND ----------

SELECT
  SUM(01_Covid_positive_test),
  SUM(01_GP_covid_diagnosis),
  SUM(02_Covid_admission),
  SUM(03_NIV_treatment),
  SUM(03_IMV_treatment),
  SUM(03_ICU_admission),
  SUM(03_ECMO_treatment),
  SUM(04_Covid_inpatient_death),
  SUM(04_Fatal_with_covid_diagnosis),
  SUM(04_Fatal_without_covid_diagnosis)
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Aggregated Events

-- COMMAND ----------

SELECT
  SUM(CASE WHEN (03_NIV_treatment = 1 OR 03_IMV_treatment = 1 OR 03_ICU_admission = 1 OR 03_ECMO_treatment = 1) then 1 else 0 end) as n_critical_care,
  SUM(CASE WHEN (04_Covid_inpatient_death = 1 OR 04_Fatal_with_covid_diagnosis = 1 OR 04_Fatal_without_covid_diagnosis = 1) then 1 else 0 end) as n_deaths,
  SUM(death)
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Mutex Severities
-- MAGIC These are mutually exclusive phenotypes representing the **worst/most severe** COVID-19 events experienced by each individual patient  
-- MAGIC   
-- MAGIC > Most individuals with COVID-19 event(s), (89%, n = 3,056,363) avoided hospitalisation or death related to COVID-19. 

-- COMMAND ----------

WITH t1 AS 
 (SELECT covid_severity, COUNT(*) AS n 
  FROM dars_nic_391419_j3w9t_collab.ccu013_covid_severity_paper_cohort
  GROUP BY covid_severity)
SELECT covid_severity, n, 
       n/(SELECT COUNT(*) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_severity_paper_cohort) * 100 as percentage
FROM t1
ORDER BY
  covid_severity

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Ventilation
-- MAGIC > Of those admitted to hospital, 52,672 (15%) received NIV, 37,620 (11%) were admitted to an ICU, 20,720 (6%) received IMV, 17,108 (4.8%) patients received both NIV and IMV and 534 received ECMO.

-- COMMAND ----------

SELECT
  SUM(02_Covid_admission) as n_hospitalised,
  
  SUM(03_NIV_treatment) as niv_n,
  ROUND(SUM(03_NIV_treatment) / SUM(02_Covid_admission) * 100, 2) as niv_percent,
  
  SUM(03_ICU_admission) as icu_n,
  ROUND(SUM(03_ICU_admission) / SUM(02_Covid_admission) * 100, 2) as icu_percent,
  
  SUM(03_IMV_treatment) as imv_n,
  ROUND(SUM(03_IMV_treatment) / SUM(02_Covid_admission) * 100, 2) as imv_percent,
    
  SUM(03_ECMO_treatment) as ecmo_n,
  ROUND(SUM(03_ECMO_treatment) / SUM(02_Covid_admission) * 100, 2) as ecmo_percent,
  
  SUM(CASE WHEN 03_NIV_treatment = 1 AND 03_IMV_treatment = 1 then 1 else 0 end) as both_n,
  ROUND(SUM(CASE WHEN 03_NIV_treatment = 1 AND 03_IMV_treatment = 1 then 1 else 0 end) / SUM(02_Covid_admission) * 100, 2) as both_percent

FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

SELECT
  SUM(CASE WHEN 03_NIV_treatment = 1 then 1 else 0 end) as NIV_n,
  SUM(CASE WHEN 03_IMV_treatment = 1 then 1 else 0 end) as IMV_n,
  SUM(CASE WHEN 03_NIV_treatment = 1 AND 03_IMV_treatment = 1 then 1 else 0 end) as both_n
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Ventilation OUTSIDE ICU

-- COMMAND ----------

-- Outside ICU
SELECT
  SUM(CASE WHEN 03_NIV_treatment = 1 then 1 else 0 end) as NIV_n,
  SUM(CASE WHEN 03_IMV_treatment = 1 then 1 else 0 end) as IMV_n
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
WHERE
  03_ICU_admission = 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Deaths: pathways to COVID-19 mortality
-- MAGIC > Of the 138,762 individuals with a COVID-19 related death, 39,510 (28%) died without having ever been admitted to hospital and 13,083 (9%) died within 28-days of a COVID-19 event without a confirmed or suspected COVID-19 diagnosis listed on the death certificate.

-- COMMAND ----------

SELECT
  SUM(death) as deaths_total,
  SUM(CASE WHEN 02_Covid_admission = 1 AND death = 1 then 1 else 0 end) as deaths_hospital_contact,
  ROUND(SUM(CASE WHEN 02_Covid_admission = 1 AND death = 1 then 1 else 0 end)/SUM(death)*100,2) as deaths_hospital_contact_percent,
  SUM(CASE WHEN 02_Covid_admission = 0 AND death = 1 then 1 else 0 end) as deaths_NO_hospital_contact,
  ROUND(SUM(CASE WHEN 02_Covid_admission = 0 AND death = 1 then 1 else 0 end)/SUM(death)*100,2) as deaths_NO_hospital_contact_percent
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

SELECT
  SUM(death) as deaths_total,
  
  SUM(04_Covid_inpatient_death) as inpatient_n,
  ROUND(SUM(04_Covid_inpatient_death)/SUM(death)*100,2) as inpatient_percent,
  
  SUM(04_Fatal_with_covid_diagnosis) as with_dx_n,
  ROUND(SUM(04_Fatal_with_covid_diagnosis)/SUM(death)*100,2) as with_dx_percent,
  
  SUM(04_Fatal_without_covid_diagnosis) as without_dx_n,
  ROUND(SUM(04_Fatal_without_covid_diagnosis)/SUM(death)*100,2) as without_dx_percent
  
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

SELECT
  SUM(CASE WHEN (03_NIV_treatment = 1 OR 03_IMV_treatment = 1 OR 03_ICU_admission = 1 OR 03_ECMO_treatment = 1) AND death = 1 then 1 else 0 end) as deaths_critical_care,
  SUM(CASE WHEN 03_ICU_admission = 1 AND death = 1 then 1 else 0 end) as deaths_ICU
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Patient characteristics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### High Risk patients
-- MAGIC > Amongst the 4,014,314 individuals classified as ?high risk?, 377,630 (9.4%) experienced COVID-19 and 40,286 died, a mortality rate of 11%.
-- MAGIC 
-- MAGIC Now..  
-- MAGIC > Amongst the 4,071,794 individuals classified as ?high risk?, 381,497 (9.4%) experienced COVID-19 and 41,446 died, a mortality rate of 11%. 

-- COMMAND ----------

SELECT
  SUM(CASE WHEN high_risk = 1 then 1 else 0 end) as infected,
  SUM(CASE WHEN high_risk = 1 AND death = 1 then 1 else 0 end) as died
FROM 
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

SELECT
  SUM(CASE WHEN high_risk = 1 then 1 else 0 end) as total_high_risk,
  SUM(CASE WHEN high_risk = 1 AND severity != 'no_covid' then 1 else 0 end) as infected,
  SUM(CASE WHEN high_risk = 1 AND severity != 'no_covid' AND death_covid = 1 then 1 else 0 end) as died
FROM 
  dars_nic_391419_j3w9t_collab.ccu013_paper_table_one_56million_denominator

-- COMMAND ----------

SELECT COUNT(DISTINCT gdppr.NHS_NUMBER_DEID) as total_high_risk
FROM
(SELECT NHS_NUMBER_DEID FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive
  WHERE CODE = 1300561000000107 AND ProductionDate == "2021-07-29 13:39:04.161949"
    ) as gdppr
INNER JOIN
  (SELECT NHS_NUMBER_DEID FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as denominator
ON gdppr.NHS_NUMBER_DEID = denominator.NHS_NUMBER_DEID

-- COMMAND ----------

--- TOTAL N of individuals tagged with 'High RIsk'
SELECT
  ( --- TOTAL N of COVID-event individuals who were high-risk
  SELECT
    COUNT(distinct person_id_deid)
  FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
  WHERE high_risk = 1
  ) as got_covid,
    
  ( --- DIED
  SELECT
    COUNT(distinct person_id_deid)
  FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
  WHERE high_risk = 1
  AND (04_Covid_inpatient_death = 1 OR 04_Fatal_with_covid_diagnosis = 1 OR 04_Fatal_without_covid_diagnosis = 1)
  ) as died,
  
  ( --- Calculate percentage DIED of THOSE WHO GOT COVID = mortality rate
    ( --- DIED
    SELECT
      COUNT(distinct person_id_deid)
    FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
    WHERE high_risk = 1
    AND (04_Covid_inpatient_death = 1 OR 04_Fatal_with_covid_diagnosis = 1 OR 04_Fatal_without_covid_diagnosis = 1)
    )
      /
    (
    SELECT
    COUNT(distinct person_id_deid)
  FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
  WHERE high_risk = 1
    )
    * 100
  )  as mortality_rate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Death (composite) in hospitalised & ICU
-- MAGIC > 
-- MAGIC The composite of COVID-19 mortality (including deaths with a recorded diagnosis of COVID-19, deaths within 28 days of a positive test and deaths during a COVID-19 hospital admission) occurred in 28% (99,041 deaths) of hospitalised patients and 41% (15,369 deaths) for those admitted to intensive care. 

-- COMMAND ----------

SELECT
  SUM(CASE WHEN (death = 1 and 02_Covid_admission = 1) then 1 else 0 end) as hosp_deaths,
  ROUND(SUM(CASE WHEN (death = 1 and 02_Covid_admission = 1) then 1 else 0 end)/SUM(02_Covid_admission)*100,2) as hosp_mortality,
  
  SUM(CASE WHEN (death = 1 and 03_ICU_admission = 1) then 1 else 0 end) as icu_deaths,
  ROUND(SUM(CASE WHEN (death = 1 and 03_ICU_admission = 1) then 1 else 0 end)/SUM(03_ICU_admission)*100,2) as icu_mortality
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Discussion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Insights from linkage
-- MAGIC > we identified 21,558 individuals who received NIV outside of ICU, 40% of all patients treated with NIV,

-- COMMAND ----------

SELECT
  SUM(CASE WHEN 03_NIV_treatment = 1 then 1 else 0 end) as NIV_total,
  SUM(CASE WHEN 03_NIV_treatment = 1 AND 03_ICU_admission = 0 then 1 else 0 end) as NIV_outside_ICU,
  ROUND(SUM(CASE WHEN 03_NIV_treatment = 1 AND 03_ICU_admission = 0 then 1 else 0 end)/SUM(CASE WHEN 03_NIV_treatment = 1 then 1 else 0 end)*100,2) as NIV_percent_outside_ICU
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # FINISHED

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Wave numbers

-- COMMAND ----------

-- wave_1_start = "2020-03-20"
-- wave_1_end = "2020-05-29"
-- wave_2_start = "2020-09-30"
-- wave_2_end = "2021-02-12"

-- cohort %<>% 
--   mutate(wave = if_else(date_first >= wave_1_start & 
--                           date_first <= wave_1_end, 
--                         1, 1.5) # name inter-wave period as 1.5
--          ) %>%
--   mutate(wave = if_else(date_first >= wave_2_start & 
--                           date_first <= wave_2_end, 
--                         2, wave)
--          ) %>% 
--   select(-date_first)

-- COMMAND ----------

-- wave_1_start = "2020-03-20"
-- wave_1_end = "2020-05-29"
-- wave_2_start = "2020-09-30"
-- wave_2_end = 
SELECT
  SUM(CASE WHEN date_first >= "2020-03-20" AND date_first <= "2020-05-29" then 1 else 0 end) as wave1_all,
  SUM(CASE WHEN date_first >= "2020-03-20" AND date_first <= "2020-05-29" AND 02_Covid_admission = 1 then 1 else 0 end) as wave1_hosp,
  SUM(CASE WHEN date_first >= "2020-09-30" AND date_first <= "2021-02-12" then 1 else 0 end) as wave2_all,
  SUM(CASE WHEN date_first >= "2020-09-30" AND date_first <= "2021-02-12" AND 02_Covid_admission = 1 then 1 else 0 end) as wave2_hosp
FROM 
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

SELECT
  SUM(CASE WHEN date_first >= "2020-03-20" AND date_first <= "2020-05-29" AND 01_Covid_positive_test = 1 then 1 else 0 end) as wave1_test
FROM 
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 190750/56600000*100

-- COMMAND ----------

SELECT
  SUM(CASE WHEN date_first >= "2020-09-30" AND date_first <= "2021-02-12" then 1 else 0 end) as wave2_all,
  SUM(CASE WHEN date_first >= "2020-09-30" AND date_first <= "2021-02-12" AND 01_Covid_positive_test = 1 then 1 else 0 end) as wave2_test,
  (SELECT COUNT(distinct NHS_NUMBER_DEID) FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) as population,
  ROUND(SUM(CASE WHEN date_first >= "2020-09-30" AND date_first <= "2021-02-12" AND 01_Covid_positive_test = 1 then 1 else 0 end)
    /
    (SELECT COUNT(distinct NHS_NUMBER_DEID) FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020)
    *100,2) as unaffected_to_test
FROM 
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
