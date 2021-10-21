-- Databricks notebook source
-- MAGIC %md
-- MAGIC # COVID-19 Event Phenotypes: codelists
-- MAGIC  
-- MAGIC **Description** 
-- MAGIC 
-- MAGIC This notebook runs a list of `SQL` queries to extract the different codelists used in the `CCU013: COVID-19 event phenotypes` work.
-- MAGIC 
-- MAGIC The codelists generated from these queries will be made available at [`https://github.com/BHFDSC/CCU013_01_ENG-COVID-19_event_phenotyping`](https://github.com/BHFDSC/CCU013_01_ENG-COVID-19_event_phenotyping) for use by other researchers
-- MAGIC 
-- MAGIC **Project(s)** CCU013
-- MAGIC  
-- MAGIC **Author(s)** Chris Tomlinson
-- MAGIC  
-- MAGIC **Reviewer(s)** 
-- MAGIC  
-- MAGIC **Date last updated** 2021-06-23
-- MAGIC  
-- MAGIC **Date last reviewed** *NA*
-- MAGIC  
-- MAGIC **Date last run** 2021-10-21
-- MAGIC  
-- MAGIC **Data input**  
-- MAGIC * `ccu013_covid_trajectory`
-- MAGIC 
-- MAGIC **Data output**  
-- MAGIC Export of this notebook
-- MAGIC 
-- MAGIC **Software and versions** `SQL`
-- MAGIC  
-- MAGIC **Packages and versions** Nil

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # All COVID-19 event phenotypes and their codelists

-- COMMAND ----------

SELECT
  DISTINCT covid_phenotype, 
  clinical_code, 
  code as terminology, 
  description, 
  source
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
GROUP BY
  covid_phenotype, clinical_code, code, description, source
ORDER BY
  covid_phenotype, source, clinical_code
