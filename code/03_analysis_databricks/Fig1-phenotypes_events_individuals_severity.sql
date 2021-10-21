-- Databricks notebook source
-- MAGIC %md
-- MAGIC Runs a series of queries to produce the numbers for:
-- MAGIC > **Figure 1**: Flowchart of phenotyping COVID-19 severity phenotypes using seven linked data sources in 56.6 million people. Information derived from the following data sources: COVID-19 testing from SGSS (Second Generation Surveillance System) Pillars 1 & 2, including test from NHS hospitals for those with a clinical need and healthcare workers (Pillar 1) and swab testing from the wider population (Pillar 2). Primary care EHR diagnosis from GDPPR. Secondary care events from hospitalisation EHR from HES Admitted Patient Care (APC) and Critical Care (CC), SUS (Secondary Uses Service) and CHESS (COVID-19 Hospitalisations in England Surveillance System). Fatal COVID-19 events from national death registrations from the ONS, HES APC and SUS. Sources used to identify each step are indicated with data buckets on the left and COVID-19 events in rectangles on the right. Ventilation support is defined either as Non-Invasive Ventilation (NIV), Invasive Mechanical Ventilation (IMV) or Extracorporeal Membrane Oxygenation (ECMO). HES CC does not give info on ECMO treatments. Fatal COVID-19 events are defined as inpatient deaths registered from HES APC or SUS, or deaths any point in time with COVID-19 recorded as the cause of death (at any position on the death certificate) or within 28-days of the earliest COVID-19 ascertainment event irrespective of the cause of death recorded on the death certificate. In all sources, ontology terms for both suspected and confirmed diagnosis were used. (%) indicate the percentage of individuals with a given COVID-19 event phenotype out of all individuals with any event phenotype.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Phenotype | Events | Individuals

-- COMMAND ----------

SELECT
  covid_phenotype,
  COUNT(*) as events,
  COUNT(distinct person_id_deid) as individuals
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
GROUP BY
  covid_phenotype
ORDER BY
  covid_phenotype

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Severity | n | %

-- COMMAND ----------

SELECT
  severity,
  COUNT(distinct person_id_deid) as individuals,
  ROUND(COUNT(distinct person_id_deid) / (SELECT COUNT(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort) * 100, 2) as percentage
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort
GROUP BY
  severity
ORDER BY
  severity
