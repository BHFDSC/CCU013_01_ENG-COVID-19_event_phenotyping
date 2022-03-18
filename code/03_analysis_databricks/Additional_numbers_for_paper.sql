-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Numbers for manuscript
-- MAGIC  
-- MAGIC **Description** 
-- MAGIC 
-- MAGIC This notebook runs a list of `SQL` queries to extract the numbers (%) of each text entry in the `CCU013: COVID-19 Event Phenotypes` manuscript *"Understanding COVID-19 trajectories from a nationwide linked electronic health record cohort of 56 million people: phenotypes, severity, waves & vaccination"*.  
-- MAGIC <br>
-- MAGIC The layout will follow that of the paper.  
-- MAGIC 
-- MAGIC 
-- MAGIC **Project(s)** CCU013
-- MAGIC  
-- MAGIC **Author(s)** Chris Tomlinson
-- MAGIC  
-- MAGIC **Reviewer(s)** 
-- MAGIC  
-- MAGIC **Date last updated** 2022-01-24
-- MAGIC  
-- MAGIC **Date last reviewed** *NA*
-- MAGIC  
-- MAGIC **Date last run** `1/23/2022, 7:41:23 PM`
-- MAGIC 
-- MAGIC ** Last export requested ** `1/23/2022`
-- MAGIC  
-- MAGIC **Data input**  
-- MAGIC * `ccu013_covid_trajectory_paper_cohort`
-- MAGIC * `ccu013_covid_events_paper_cohort`
-- MAGIC * `ccu013_paper_table_one_56million_denominator`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Abstract

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > We identified X infected individuals (%)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC   COUNT(distinct person_id_deid) as population,
-- MAGIC   SUM(CASE WHEN severity != 'no_covid' then 1 else 0 end) as covid,
-- MAGIC   round(SUM(CASE WHEN severity != 'no_covid' then 1 else 0 end) / COUNT(distinct person_id_deid) * 100, 2) as percent_covid
-- MAGIC FROM
-- MAGIC   dars_nic_391419_j3w9t_collab.ccu013_paper_table_one_56million_denominator

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > with X recorded COVID-19 phenotypes

-- COMMAND ----------

SELECT COUNT(*) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > Of these, X (%) were hospitalised and Y (%) died. 

-- COMMAND ----------

SELECT 
  SUM(02_Covid_admission) as hospitalised,
  round(SUM(02_Covid_admission)/COUNT(*)*100,2) as hospitalised_percent,
  SUM(death) as died,
  round(SUM(death)/COUNT(*)*100,2) as died_percent
FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_paper_cohort 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > Of those hospitalised, X (%) were admitted to intensive care (ICU), Y (%) received non-invasive ventilation and Z (%) invasive ventilation. 

-- COMMAND ----------

SELECT 
  SUM(03_ICU_admission) as ICU,
  round(SUM(03_ICU_admission)/(SELECT COUNT(*) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_paper_cohort WHERE 02_Covid_admission = 1)*100,2) as ICU_percent,
  SUM(03_NIV_treatment) as NIV,
  round(SUM(03_NIV_treatment)/(SELECT COUNT(*) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_paper_cohort WHERE 02_Covid_admission = 1)*100,2) as NIV_percent,
  SUM(03_IMV_treatment) as IMV,
  round(SUM(03_IMV_treatment)/(SELECT COUNT(*) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_paper_cohort WHERE 02_Covid_admission = 1)*100,2) as IMV_percent
FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_paper_cohort 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > X (%) COVID-19 related deaths occurred without diagnoses on the death certificate, but within 30 days of a positive test while Y (%) of cases were identified from mortality data alone with no prior phenotypes recorded.

-- COMMAND ----------

SELECT 
  COUNT(*) as deaths,
  SUM(04_Fatal_without_covid_diagnosis) as fatal_noDX,
  round(SUM(04_Fatal_without_covid_diagnosis)/COUNT(*)*100,2) as fatal_noDX_percent
FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_paper_cohort 
WHERE death = 1

-- COMMAND ----------

SELECT
  SUM(CASE WHEN severity != 'no_covid' then 1 else 0 end) as covid,
  SUM(death_covid) as death_covid,
  SUM(CASE WHEN severity = '4_death_only' then 1 else 0 end) as death_only,
  round(SUM(CASE WHEN severity = '4_death_only' then 1 else 0 end) /SUM(death_covid)*100,2) as death_only_percent_deaths,
  round(SUM(CASE WHEN severity = '4_death_only' then 1 else 0 end) /SUM(CASE WHEN severity != 'no_covid' then 1 else 0 end)*100,2) as death_only_percent_cases
FROM
  dars_nic_391419_j3w9t_collab.ccu013_paper_table_one_56million_denominator

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Methods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > Data cleaning, exploratory analysis, phenotype creation and cohort assembly was performed using Python (3.7) and Spark SQL (X) on Databricks Runtime 6.4 for Machine Learning. 

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from pyspark import version
-- MAGIC # Spark SQL version
-- MAGIC spark.version

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > We assessed X previously described comorbidities, across 16 clinical specialities / organ systems, using validated CALIBER phenotypes

-- COMMAND ----------

SELECT 
  COUNT(distinct phenotype)
  FROM 
    dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort as base
  FULL JOIN
    (SELECT 
      person_id_deid, 
      date, 
      phenotype, 
      1 as value 
    FROM 
      dars_nic_391419_j3w9t_collab.ccu013_caliber_skinny) 
      as phenos
  ON 
    base.person_id_deid = phenos.person_id_deid
  WHERE 
    phenos.date < '2020-01-01'

-- COMMAND ----------

SELECT
  *
FROM
(SELECT 
  distinct phenotype
  FROM 
    dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort as base
  FULL JOIN
    (SELECT 
      person_id_deid, 
      date, 
      phenotype, 
      1 as value 
    FROM 
      dars_nic_391419_j3w9t_collab.ccu013_caliber_skinny) 
      as phenos
  ON 
    base.person_id_deid = phenos.person_id_deid
  WHERE 
    phenos.date < '2020-01-01'
) as phenos
INNER JOIN
  (SELECT phenotype, category FROM dars_nic_391419_j3w9t_collab.ccu013_caliber_category_mapping) as mapping
ON phenos.phenotype = mapping.phenotype

-- COMMAND ----------

SELECT
  category,
  COUNT(distinct mapping.phenotype)
FROM
(SELECT 
  distinct phenotype
  FROM 
    dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort as base
  FULL JOIN
    (SELECT 
      person_id_deid, 
      date, 
      phenotype, 
      1 as value 
    FROM 
      dars_nic_391419_j3w9t_collab.ccu013_caliber_skinny) 
      as phenos
  ON 
    base.person_id_deid = phenos.person_id_deid
  WHERE 
    phenos.date < '2020-01-01'
) as phenos
INNER JOIN
  (SELECT phenotype, category FROM dars_nic_391419_j3w9t_collab.ccu013_caliber_category_mapping) as mapping
ON 
  phenos.phenotype = mapping.phenotype
GROUP BY
  category

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Denominator
-- MAGIC > Among X individuals registered with a general practitioner in England and alive on 23rd January 2020,

-- COMMAND ----------

SELECT 
  count(DISTINCT NHS_NUMBER_DEID) as total_population 
FROM 
  dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Individuals & Events + IR
-- MAGIC >  we identified X events in Y individuals, representing an infection rate of Z%,

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
  "01_Covid_positive_test" as phenotype,
  SUM(01_Covid_positive_test) as n,
  round(SUM(01_Covid_positive_test)/COUNT(*)*100,2) as percentage
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
UNION ALL
SELECT
  "01_GP_covid_diagnosis" as phenotype,
  SUM(01_GP_covid_diagnosis) as n,
  round(SUM(01_GP_covid_diagnosis)/COUNT(*)*100,2) as percentage
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
UNION ALL
SELECT
  "02_Covid_admission" as phenotype,
  SUM(02_Covid_admission) as n,
  round(SUM(02_Covid_admission)/COUNT(*)*100,2) as percentage
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
UNION ALL
SELECT
  "03_NIV_treatment" as phenotype,
  SUM(03_NIV_treatment) as n,
  round(SUM(03_NIV_treatment)/COUNT(*)*100,2) as percentage
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
UNION ALL
SELECT
  "03_IMV_treatment" as phenotype,
  SUM(03_IMV_treatment) as n,
  round(SUM(03_IMV_treatment)/COUNT(*)*100,2) as percentage
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
UNION ALL
SELECT
  "03_ICU_admission" as phenotype,
  SUM(03_ICU_admission) as n,
  round(SUM(03_ICU_admission)/COUNT(*)*100,2) as percentage
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
UNION ALL
SELECT
  "03_ECMO_treatment" as phenotype,
  SUM(03_ECMO_treatment) as n,
  round(SUM(03_ECMO_treatment)/COUNT(*)*100,2) as percentage
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
UNION ALL
SELECT
  "04_Covid_inpatient_death" as phenotype,
  SUM(04_Covid_inpatient_death) as n,
  round(SUM(04_Covid_inpatient_death)/COUNT(*)*100,2) as percentage
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
UNION ALL
SELECT
  "04_Fatal_with_covid_diagnosis" as phenotype,
  SUM(04_Fatal_with_covid_diagnosis) as n,
  round(SUM(04_Fatal_with_covid_diagnosis)/COUNT(*)*100,2) as percentage
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
UNION ALL
SELECT
  "04_Fatal_without_covid_diagnosis" as phenotype,
  SUM(04_Fatal_without_covid_diagnosis) as n,
  round(SUM(04_Fatal_without_covid_diagnosis)/COUNT(*)*100,2) as percentage
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Aggregated Events

-- COMMAND ----------

SELECT
  SUM(CASE WHEN (03_NIV_treatment = 1 OR 03_IMV_treatment = 1 OR 03_ICU_admission = 1 OR 03_ECMO_treatment = 1) then 1 else 0 end) as n_ventilatory_support,
  SUM(CASE WHEN (04_Covid_inpatient_death = 1 OR 04_Fatal_with_covid_diagnosis = 1 OR 04_Fatal_without_covid_diagnosis = 1) then 1 else 0 end) as n_deaths,
  SUM(death)
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Mutex Severities
-- MAGIC These are mutually exclusive phenotypes representing the **worst/most severe** COVID-19 events experienced by each individual patient  
-- MAGIC   
-- MAGIC > Most individuals with COVID-19 event(s), (n=X, %) avoided hospitalisation or death related to COVID-19. 

-- COMMAND ----------

WITH t1 AS 
 (SELECT severity, COUNT(*) AS n 
  FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
  GROUP BY severity)
SELECT severity, n, 
       n/(SELECT COUNT(*) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort) * 100 as percentage
FROM t1
ORDER BY
  severity

-- COMMAND ----------

WITH t1 AS 
 (SELECT 'not_hospitalised' as severity, COUNT(*) AS n 
  FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
  WHERE severity = '0_positive' OR severity = '1_gp'
 )
SELECT severity, n, 
       ROUND(n/(SELECT COUNT(*) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort) * 100,2) as percentage
FROM t1
ORDER BY
  severity

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Ventilation
-- MAGIC > Of those admitted to hospital, X (%) received NIV, Y (%) were admitted to an ICU, Z (%) received IMV, A (%) patients received both NIV and IMV and B received ECMO.

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
  SUM(03_NIV_treatment) as NIV_n,
  SUM(03_IMV_treatment) as IMV_n,
  SUM(CASE WHEN 03_NIV_treatment = 1 AND 03_IMV_treatment = 1 then 1 else 0 end) as both_n
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Ventilation OUTSIDE ICU

-- COMMAND ----------

-- Outside ICU
SELECT
  SUM(03_NIV_treatment) as NIV_n,
  ROUND(SUM(03_NIV_treatment) / 
    (SELECT SUM(03_NIV_treatment) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort) * 100, 2) as NIV_percent_outICU,
  SUM(CASE WHEN 03_IMV_treatment = 1 then 1 else 0 end) as IMV_n,
  ROUND(SUM(03_IMV_treatment) / 
    (SELECT SUM(03_IMV_treatment) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort) * 100, 2) as IMV_percent_outICU
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
WHERE
  03_ICU_admission = 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Mortality
-- MAGIC > X individuals died, representing a mortality rate of Y%

-- COMMAND ----------

SELECT
  SUM(death) as deaths_total,
  round(SUM(death)/COUNT(*)*100,2) as mortality
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > Of these deaths, the majority occurred in patients who were hospitalised (%, n), however n (%) died without having ever been admitted to hospital.

-- COMMAND ----------

SELECT
  COUNT(*) as deaths_total,
  SUM(02_Covid_admission) as deaths_hospital,
  ROUND( SUM(02_Covid_admission)/COUNT(*)*100,2) as deaths_hospital_percent,
  SUM(CASE WHEN 02_Covid_admission = 0 then 1 else 0 end) as deaths_NO_hospital_contact,
  ROUND(SUM(CASE WHEN 02_Covid_admission = 0 then 1 else 0 end)/COUNT(*)*100,2) as deaths_NO_hospital_contact_percent
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
WHERE
  death = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Unheralded deaths
-- MAGIC > Notably we identified X unheralded COVID-19 deaths - individuals who died with COVID-19 as a recorded cause, but for whom no other COVID-19 phenotypes, such as positive tests or primary care diagnosis, were identified.

-- COMMAND ----------

-- See Table 1 main manusript
SELECT
  COUNT(*),
  round(SUM(CASE WHEN age > 70 then 1 else 0 end)/COUNT(*)*100,2) as over70,
  round(SUM(CASE WHEN ethnic_group == "White" then 1 else 0 end)/COUNT(*)*100,2) as white,
  mean(multimorbidity)
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
WHERE
  04_Fatal_with_covid_diagnosis = 1
AND
  01_Covid_positive_test = 0
AND
  01_GP_covid_diagnosis = 0
AND
  02_Covid_admission = 0
AND
  ventilatory_support = 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > X individuals died within 28-days of a COVID-19 event without a confirmed or suspected COVID-19 diagnosis listed on the death certificate.

-- COMMAND ----------

SELECT
  COUNT(*)
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
WHERE
  04_Fatal_without_covid_diagnosis = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > Of the X individuals with a COVID-19 related death, Y (%) died without having ever been admitted to hospital and Z (%) died within 28-days of a COVID-19 event without a confirmed or suspected COVID-19 diagnosis listed on the death certificate.

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
  SUM(CASE WHEN (03_NIV_treatment = 1 OR 03_IMV_treatment = 1 OR 03_ICU_admission = 1 OR 03_ECMO_treatment = 1) AND death = 1 then 1 else 0 end) as deaths_ventilatory_support,
  SUM(CASE WHEN 03_ICU_admission = 1 AND death = 1 then 1 else 0 end) as deaths_ICU
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### High Risk patients
-- MAGIC > Amongst the X individuals classified as ?high risk?, X (%) experienced COVID-19 and Y died, a mortality rate of Y%.  
-- MAGIC   
-- MAGIC * Note significant change in results since preprint (ending May) due to vaccination/transmission

-- COMMAND ----------

SELECT
  SUM(CASE WHEN high_risk = 1 then 1 else 0 end) as infected,
  SUM(CASE WHEN high_risk = 1 AND death = 1 then 1 else 0 end) as died
FROM 
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

SELECT
  COUNT(*) as total_high_risk,
  SUM(CASE WHEN severity != 'no_covid' then 1 else 0 end) as infected,
  round(SUM(CASE WHEN severity != 'no_covid' then 1 else 0 end) / COUNT(*) *100, 2) as infection_rate,
  SUM(CASE WHEN severity != 'no_covid' AND death_covid = 1 then 1 else 0 end) as died,
  round(SUM(CASE WHEN severity != 'no_covid' AND death_covid = 1 then 1 else 0 end) / SUM(CASE WHEN severity != 'no_covid' then 1 else 0 end) *100, 2) as mortality
FROM 
  dars_nic_391419_j3w9t_collab.ccu013_paper_table_one_56million_denominator
WHERE
  high_risk = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Mortality in hospitalised patients
-- MAGIC > 
-- MAGIC The composite of COVID-19 mortality (including deaths with a recorded diagnosis of COVID-19, deaths within 28 days of a positive test and deaths during a COVID-19 hospital admission) occurred in X% (n deaths) of hospitalised patients and Y% (n) deaths) for those admitted to intensive care. 

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
-- MAGIC ## CC-outside-ICU mortality
-- MAGIC > Kaplan-Meier survival analysis (Figure 3) corroborates these results whilst providing additional insight into the temporality of survival. Stratifying critical care treatment by admission to ICU reveals that **mortality is highest for those patients receiving critical care outside of ICU**.

-- COMMAND ----------

SELECT
  
  SUM(CASE WHEN (death = 1 and 02_Covid_admission = 1) then 1 else 0 end) as hosp_deaths,
  ROUND(SUM(CASE WHEN (death = 1 and 02_Covid_admission = 1) then 1 else 0 end)/SUM(02_Covid_admission)*100,2) as hosp_mortality,
  
  SUM(CASE WHEN (death = 1 and 02_Covid_admission = 1 and ventilatory_support = 0) then 1 else 0 end) as hosp_noCC_deaths,
  ROUND(SUM(CASE WHEN (death = 1 and 02_Covid_admission = 1 and ventilatory_support = 0) then 1 else 0 end)/SUM(CASE WHEN (02_Covid_admission = 1 and ventilatory_support = 0) then 1 else 0 end) *100,2) as hosp_noCC_mortality, 

  SUM(CASE WHEN (death = 1 and ventilatory_support = 1) then 1 else 0 end) as cc_deaths,
  ROUND(SUM(CASE WHEN (death = 1 and ventilatory_support = 1) then 1 else 0 end)/SUM(ventilatory_support)*100,2) as cc_mortality,
  
  SUM(CASE WHEN (death = 1 and 03_ICU_admission = 1) then 1 else 0 end) as icu_deaths,
  ROUND(SUM(CASE WHEN (death = 1 and 03_ICU_admission = 1) then 1 else 0 end)/SUM(03_ICU_admission)*100,2) as icu_mortality,
  
  SUM(CASE WHEN (death = 1 and ventilatory_support = 1 and 03_ICU_admission = 0) then 1 else 0 end) as cc_outofICU_deaths,
  ROUND(SUM(CASE WHEN (death = 1 and ventilatory_support = 1 and 03_ICU_admission = 0) then 1 else 0 end)/SUM(CASE WHEN (ventilatory_support = 1 and 03_ICU_admission = 0) then 1 else 0 end) *100,2) as cc_outofICU_mortality  
  
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CC-outside-ICU mortality: Wave 1

-- COMMAND ----------

SELECT
  SUM(CASE WHEN (death = 1 and 02_Covid_admission = 1) then 1 else 0 end) as hosp_deaths,
  ROUND(SUM(CASE WHEN (death = 1 and 02_Covid_admission = 1) then 1 else 0 end)/SUM(02_Covid_admission)*100,2) as hosp_mortality,
  
  SUM(CASE WHEN (death = 1 and 02_Covid_admission = 1 and ventilatory_support = 0) then 1 else 0 end) as hosp_noCC_deaths,
  ROUND(SUM(CASE WHEN (death = 1 and 02_Covid_admission = 1 and ventilatory_support = 0) then 1 else 0 end)/SUM(CASE WHEN (02_Covid_admission = 1 and ventilatory_support = 0) then 1 else 0 end) *100,2) as hosp_noCC_mortality, 

  SUM(CASE WHEN (death = 1 and ventilatory_support = 1) then 1 else 0 end) as cc_deaths,
  ROUND(SUM(CASE WHEN (death = 1 and ventilatory_support = 1) then 1 else 0 end)/SUM(ventilatory_support)*100,2) as cc_mortality,
  
  SUM(CASE WHEN (death = 1 and 03_ICU_admission = 1) then 1 else 0 end) as icu_deaths,
  ROUND(SUM(CASE WHEN (death = 1 and 03_ICU_admission = 1) then 1 else 0 end)/SUM(03_ICU_admission)*100,2) as icu_mortality,
  
  SUM(CASE WHEN (death = 1 and ventilatory_support = 1 and 03_ICU_admission = 0) then 1 else 0 end) as cc_outofICU_deaths,
  ROUND(SUM(CASE WHEN (death = 1 and ventilatory_support = 1 and 03_ICU_admission = 0) then 1 else 0 end)/SUM(CASE WHEN (ventilatory_support = 1 and 03_ICU_admission = 0) then 1 else 0 end) *100,2) as cc_outofICU_mortality  
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
WHERE 
  date_first >= "2020-03-20" AND date_first <= "2020-05-29" 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CC-outside-ICU mortality: Wave 2

-- COMMAND ----------

SELECT
  SUM(CASE WHEN (death = 1 and 02_Covid_admission = 1) then 1 else 0 end) as hosp_deaths,
  ROUND(SUM(CASE WHEN (death = 1 and 02_Covid_admission = 1) then 1 else 0 end)/SUM(02_Covid_admission)*100,2) as hosp_mortality,
  
  SUM(CASE WHEN (death = 1 and 02_Covid_admission = 1 and ventilatory_support = 0) then 1 else 0 end) as hosp_noCC_deaths,
  ROUND(SUM(CASE WHEN (death = 1 and 02_Covid_admission = 1 and ventilatory_support = 0) then 1 else 0 end)/SUM(CASE WHEN (02_Covid_admission = 1 and ventilatory_support = 0) then 1 else 0 end) *100,2) as hosp_noCC_mortality, 

  SUM(CASE WHEN (death = 1 and ventilatory_support = 1) then 1 else 0 end) as cc_deaths,
  ROUND(SUM(CASE WHEN (death = 1 and ventilatory_support = 1) then 1 else 0 end)/SUM(ventilatory_support)*100,2) as cc_mortality,
  
  SUM(CASE WHEN (death = 1 and 03_ICU_admission = 1) then 1 else 0 end) as icu_deaths,
  ROUND(SUM(CASE WHEN (death = 1 and 03_ICU_admission = 1) then 1 else 0 end)/SUM(03_ICU_admission)*100,2) as icu_mortality,
  
  SUM(CASE WHEN (death = 1 and ventilatory_support = 1 and 03_ICU_admission = 0) then 1 else 0 end) as cc_outofICU_deaths,
  ROUND(SUM(CASE WHEN (death = 1 and ventilatory_support = 1 and 03_ICU_admission = 0) then 1 else 0 end)/SUM(CASE WHEN (ventilatory_support = 1 and 03_ICU_admission = 0) then 1 else 0 end) *100,2) as cc_outofICU_mortality  
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
WHERE 
  date_first >= "2020-09-30" AND date_first <= "2021-02-12"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Recording patterns across sources
-- MAGIC > Approximately X% of individuals with a positive test also received a primary care diagnosis, while N (%) had a positive test but no other record. 

-- COMMAND ----------

SELECT
  SUM(01_Covid_positive_test) as positive_tests,
  
  SUM(CASE WHEN 01_Covid_positive_test = 1 and 01_GP_covid_diagnosis = 1 then 1 else 0 end) as positive_and_GP_Dx_n,
  round(SUM(CASE WHEN 01_Covid_positive_test = 1 and 01_GP_covid_diagnosis = 1 then 1 else 0 end) / SUM(01_Covid_positive_test)*100,2) as positive_and_GP_Dx_percent,
  
  SUM(CASE WHEN 01_Covid_positive_test = 1 and 
    01_GP_covid_diagnosis = 0 and
    02_Covid_admission = 0 and
    03_ICU_admission = 0 and
    03_NIV_treatment = 0 and
    03_IMV_treatment = 0 and
    03_ECMO_treatment = 0 and
    04_Fatal_with_covid_diagnosis = 0 and
    04_Fatal_without_covid_diagnosis = 0 and
    04_Covid_inpatient_death = 0 then 1 else 0 end) as positive_only_n,
  round(SUM(CASE WHEN 01_Covid_positive_test = 1 and 
    01_GP_covid_diagnosis = 0 and
    02_Covid_admission = 0 and
    ventilatory_support = 0 and
    death = 0 then 1 else 0 end) / SUM(01_Covid_positive_test)*100,2) as positive_only_percent
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_paper_cohort

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > X% with a primary care record had no other evidence of COVID-19, as did X% with a secondary care record, and X COVID-19 cases were identified exclusively from mortality data with no prior COVID-19 events (Figure 2). A small number of individuals were identified only from PHE hospital surveillance data (CHESS, X individuals, X% of all hospitalisations). See Supplementary Figure 2, for further details on data source overlap.

-- COMMAND ----------

SELECT
  SUM(01_GP_covid_diagnosis) as GP_Dx_n,
  
  SUM(CASE WHEN 01_GP_covid_diagnosis = 1 and 
    01_Covid_positive_test = 0 and
    02_Covid_admission = 0 and
    ventilatory_support = 0 and
    death = 0 then 1 else 0 end) as GP_only_n,
  round(SUM(CASE WHEN 01_GP_covid_diagnosis = 1 and 
    01_Covid_positive_test = 0 and
    02_Covid_admission = 0 and
    ventilatory_support = 0 and
    death = 0 then 1 else 0 end) / SUM(01_Covid_positive_test)*100,2) as GP_only_percent,
    
    SUM(02_Covid_admission) as Hosp_n,
    
    SUM(CASE WHEN 02_Covid_admission = 1 and 
    01_Covid_positive_test = 0 and
    01_GP_covid_diagnosis = 0 and
    -- Remove ventilatory support as that's a secondary care record
    death = 0 then 1 else 0 end) as Hosp_only_n,
  round(SUM(CASE WHEN 02_Covid_admission = 1 and 
    01_Covid_positive_test = 0 and
    01_GP_covid_diagnosis = 0 and
    death = 0 then 1 else 0 end) / SUM(01_Covid_positive_test)*100,2) as Hosp_only_percent
    
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_paper_cohort

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Alternative approach ** NB requires running notebook `upset_datagen` to update table `ccu013_01_paper_upset` first **  
-- MAGIC 
-- MAGIC **this takes a while!

-- COMMAND ----------

-- MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU013/COVID-19-SEVERITY-PHENOTYPING/03_analysis_databricks/upset_datagen

-- COMMAND ----------

-- Alternative approach using upset table (run that first)
SELECT
  COUNT(distinct person_id_deid)
FROM
  dars_nic_391419_j3w9t_collab.ccu013_01_paper_upset
WHERE
  SGSS = 1
AND CHESS = 0 AND HES_APC = 0 AND HES_CC = 0 and GDPPR = 0 and SUS = 0 and deaths = 0

-- COMMAND ----------

-- Alternative approach using upset table (run that first)
SELECT
  COUNT(distinct person_id_deid)
FROM
  dars_nic_391419_j3w9t_collab.ccu013_01_paper_upset
WHERE
  GDPPR = 1
AND CHESS = 0 AND HES_APC = 0 AND HES_CC = 0 and SGSS = 0 and SUS = 0 and deaths = 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > A small number of individuals were identified only from PHE hospital surveillance data (CHESS, X individuals, X% of all hospitalisations)

-- COMMAND ----------

SELECT
  COUNT(distinct person_id_deid) as individuals,
  round(COUNT(*) / 
    (SELECT COUNT(*) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort WHERE 02_Covid_admission = 1)*100,2) as percentage_hospitalisations
FROM
  dars_nic_391419_j3w9t_collab.ccu013_01_paper_upset
WHERE
  CHESS = 1
AND GDPPR = 0 AND HES_APC = 0 AND HES_CC = 0 and SGSS = 0 and SUS = 0 and deaths = 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Discussion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Insights from linkage
-- MAGIC > we identified X individuals who received NIV outside of ICU, X% of all patients treated with NIV,

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
