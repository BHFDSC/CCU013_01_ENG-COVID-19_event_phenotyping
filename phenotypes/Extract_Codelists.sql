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
-- MAGIC **Date last run** 2021-06-23
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
  covid_status,
  source
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
GROUP BY
  covid_phenotype, clinical_code, code, description, covid_status, source
ORDER BY
  covid_phenotype, source, clinical_code

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Individual event phenotypes and their codelists

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## `01_GP_covid_diagnosis`

-- COMMAND ----------

SELECT
  covid_phenotype, 
  clinical_code, 
  code as terminology, 
  description, 
  covid_status,
  source
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
WHERE
  covid_phenotype = '01_GP_covid_diagnosis'
GROUP BY
  covid_phenotype, clinical_code, code, description, covid_status, source
ORDER BY
  source, clinical_code

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## `02_Covid_admission`

-- COMMAND ----------

SELECT
  covid_phenotype, 
  clinical_code, 
  code as terminology, 
  description, 
  covid_status,
  source
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
WHERE
  covid_phenotype = '02_Covid_admission'
GROUP BY
  covid_phenotype, clinical_code, code, description, covid_status, source
ORDER BY
  source, clinical_code

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## `03_NIV_treatment`

-- COMMAND ----------

SELECT
  covid_phenotype, 
  clinical_code, 
  code as terminology, 
  description, 
  covid_status,
  source
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
WHERE
  covid_phenotype = '03_NIV_treatment'
GROUP BY
  covid_phenotype, clinical_code, code, description, covid_status, source
ORDER BY
  source, clinical_code

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## `03_IMV_treatment`

-- COMMAND ----------

SELECT
  covid_phenotype, 
  clinical_code, 
  code as terminology, 
  description, 
  covid_status,
  source
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
WHERE
  covid_phenotype = '03_IMV_treatment'
GROUP BY
  covid_phenotype, clinical_code, code, description, covid_status, source
ORDER BY
  source, clinical_code

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## `03_ICU_admission`

-- COMMAND ----------

SELECT
  covid_phenotype, 
  clinical_code, 
  code as terminology, 
  description, 
  covid_status,
  source
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
WHERE
  covid_phenotype = '03_ICU_admission'
GROUP BY
  covid_phenotype, clinical_code, code, description, covid_status, source
ORDER BY
  source, clinical_code

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## `03_ECMO_treatment`

-- COMMAND ----------

SELECT
  covid_phenotype, 
  clinical_code, 
  code as terminology, 
  description, 
  covid_status,
  source
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
WHERE
  covid_phenotype = '03_ECMO_treatment'
GROUP BY
  covid_phenotype, clinical_code, code, description, covid_status, source
ORDER BY
  source, clinical_code

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## `04_Fatal_with_covid_diagnosis`

-- COMMAND ----------

SELECT
  covid_phenotype, 
  clinical_code, 
  code as terminology, 
  description, 
  covid_status,
  source
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
WHERE
  covid_phenotype = '04_Fatal_with_covid_diagnosis'
GROUP BY
  covid_phenotype, clinical_code, code, description, covid_status, source
ORDER BY
  source, clinical_code

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## `04_Fatal_without_covid_diagnosis`

-- COMMAND ----------

SELECT
  covid_phenotype, 
  clinical_code, 
  code as terminology, 
  description, 
  covid_status,
  source
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
WHERE
  covid_phenotype = '04_Fatal_without_covid_diagnosis'
GROUP BY
  covid_phenotype, clinical_code, code, description, covid_status, source
ORDER BY
  source, clinical_code

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## `04_Covid_inpatient_death`

-- COMMAND ----------

SELECT
  covid_phenotype, 
  clinical_code, 
  code as terminology, 
  description, 
  covid_status,
  source
FROM
  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
WHERE
  covid_phenotype = '04_Covid_inpatient_death'
GROUP BY
  covid_phenotype, clinical_code, code, description, covid_status, source
ORDER BY
  source, clinical_code
