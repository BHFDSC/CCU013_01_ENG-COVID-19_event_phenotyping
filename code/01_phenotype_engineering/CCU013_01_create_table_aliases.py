# Databricks notebook source
# MAGIC %md
# MAGIC  # Creating table aliases
# MAGIC  
# MAGIC **Description** 
# MAGIC   
# MAGIC This notebook creates input tables for the main datasets required for the COVID phenotyping work. After discussion with Sam this was deemed to be the best approach to parameterising the datasets.    
# MAGIC It can be sourced for another notebook using:  
# MAGIC   
# MAGIC   `dbutils.notebook.run("./ccu013_01_create_table_aliases", 30000) # timeout_seconds`  
# MAGIC This is advantageous to using `%run ./ccu013_01_create_table_aliases` because it doesn't include the markdown component (although you can just click "hide result" on the cell with `%run`) 
# MAGIC 
# MAGIC **Project(s)** CCU013
# MAGIC  
# MAGIC **Author(s)** Chris Tomlinson, Johan Thygesen (inspired by Sam Hollings!)
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2022-01-22
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** 2022-01-22
# MAGIC  
# MAGIC **Data input** 
# MAGIC This notebook uses the archive tables made by the data wranglers - selecting the latest data by `productionDate`. The `productionDate` variabel is carried forward to master_phenotype in the `ccu13_tmp_gdppr` table, and will be saved in the main output tables; trajectory, severity and events, to ensure the data for the produced phenotypes is back tracable to source, for reproducability.
# MAGIC 
# MAGIC **Data output**
# MAGIC * `ccu013_tmp_sgss`
# MAGIC * `ccu013_tmp_gdppr`
# MAGIC * `ccu013_tmp_deaths`
# MAGIC * `ccu013_tmp_sus`
# MAGIC * `ccu013_tmp_apc`
# MAGIC * `ccu013_tmp_cc`
# MAGIC * `ccu013_snomed_codes_covid19` 
# MAGIC * `ccu013_tmp_chess`
# MAGIC * `ccu013_vaccine_status`
# MAGIC   
# MAGIC The table `ccu013_snomed_codes_covid19` is a hardcoded list of SNOMEDct COVID-19 codes, descriptions and categories.  
# MAGIC   
# MAGIC *Previously* these were `GLOBAL TEMP VIEW`, and therefore called in SQL using `FROM global_temp.ccu013_X`, however due to lazy evaluation this results in a massive amount of computation at the final point of joining, to distribute this we instead make 'proper' tables named `ccu013_tmp_X` and then can delete them after the joining process has occurred. Saving as a proper table also allows us to run optimisation to improve speed of future queries & joins.
# MAGIC 
# MAGIC **Software and versions** `sql`, `python`
# MAGIC  
# MAGIC **Packages and versions** `pyspark`

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU013/COVID-19-SEVERITY-PHENOTYPING/CCU013_00_helper_functions

# COMMAND ----------

LatestProductionDate = spark.sql("SELECT MAX(ProductionDate) FROM dars_nic_391419_j3w9t_collab.wrang002b_data_version_batchids").first()[0]
LatestAPC = spark.sql("SELECT MAX(ADMIDATE) FROM dars_nic_391419_j3w9t_collab.hes_apc_all_years").first()[0]
print(f"Most recent Production Date: {LatestProductionDate} \n Maximum date in HES APC is {LatestAPC} which represents a common cut-off across all datasets")

# COMMAND ----------

from pyspark.sql.functions import lit, to_date, col, udf, substring, regexp_replace, max
from pyspark.sql import functions as f
from datetime import datetime
from pyspark.sql.types import DateType

start_date = '2020-01-01'
# end_date = '2021-09-01' # The maximal date covered by all sources.
end_date = '2021-11-30'
# NB common cut-off data across all data sources is implemented in CCU013_13_paper_subset_data_to_cohort

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.0 Subseting all source tables by dates

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find latest production date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct * FROM
# MAGIC dars_nic_391419_j3w9t_collab.wrang002b_data_version_batchids
# MAGIC order by ProductionDate DESC

# COMMAND ----------

# production_date = "2021-08-18 14:47:00.887883"
production_date = "2022-01-20 14:58:52.353312"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 SGSS

# COMMAND ----------

# SGSS
sgss = spark.sql(f'''SELECT person_id_deid, REPORTING_LAB_ID, specimen_date FROM 
                    dars_nic_391419_j3w9t_collab.sgss_dars_nic_391419_j3w9t_archive
                    WHERE ProductionDate == "{production_date}"''')
sgss = sgss.withColumnRenamed('specimen_date', 'date')
sgss = sgss.withColumn('date_is', lit('specimen_date'))
sgss = sgss.filter((sgss['date'] >= start_date) & (sgss['date'] <= end_date))
sgss = sgss.filter(sgss['person_id_deid'].isNotNull())
sgss.createOrReplaceGlobalTempView('ccu013_tmp_sgss')
drop_table("ccu013_tmp_sgss")
create_table("ccu013_tmp_sgss")
#sgss = sgss.orderBy('specimen_date', ascending = False)
#display(sgss)
#print(sgss.count(), len(sgss.columns))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(date), max(date) FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_sgss

# COMMAND ----------

# MAGIC %sql -- create_table makes all tables as deltatables by default optimize
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_tmp_sgss

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 GDPPR

# COMMAND ----------

# GDPPR
gdppr = spark.sql(f'''SELECT DISTINCT NHS_NUMBER_DEID, DATE, CODE, ProductionDate FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive 
                     WHERE ProductionDate == "{production_date}"''')
gdppr = gdppr.withColumnRenamed('DATE', 'date').withColumnRenamed('NHS_NUMBER_DEID', 'person_id_deid').withColumnRenamed('CODE', 'code')
gdppr = gdppr.withColumn('date_is', lit('DATE'))
gdppr = gdppr.filter((gdppr['date'] >= start_date) & (gdppr['date'] <= end_date))
gdppr = gdppr.filter(gdppr['person_id_deid'].isNotNull())
gdppr.createOrReplaceGlobalTempView('ccu013_tmp_gdppr')
#display(gdppr)
drop_table("ccu013_tmp_gdppr")
create_table("ccu013_tmp_gdppr") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(date), max(date) FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_gdppr

# COMMAND ----------

# MAGIC %sql -- create_table makes all tables as deltatables by default optimize
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_tmp_gdppr

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Deaths

# COMMAND ----------

# Deaths
death = spark.sql(f'''SELECT * FROM dars_nic_391419_j3w9t_collab.deaths_dars_nic_391419_j3w9t_archive
                      WHERE ProductionDate == "{production_date}"''')
death = death.withColumn("death_date", to_date(death['REG_DATE_OF_DEATH'], "yyyyMMdd"))
death = death.withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'person_id_deid')
death = death.withColumn('date_is', lit('REG_DATE_OF_DEATH'))
death = death.filter((death['death_date'] >= start_date) & (death['death_date'] <= end_date))
death = death.filter(death['person_id_deid'].isNotNull())
death.createOrReplaceGlobalTempView('ccu013_tmp_deaths')

drop_table("ccu013_tmp_deaths")
create_table("ccu013_tmp_deaths")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(date), max(death_date) FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_deaths

# COMMAND ----------

# MAGIC %sql -- create_table makes all tables as deltatables by default optimize
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_tmp_deaths

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 HES APC

# COMMAND ----------

# HES APC with suspected or confirmed COVID-19
apc = spark.sql(f'''SELECT PERSON_ID_DEID, EPISTART, DIAG_4_CONCAT, OPERTN_4_CONCAT, DISMETH, DISDEST, DISDATE, SUSRECID FROM
                    dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive
                    WHERE ProductionDate == "{production_date}" AND
                    DIAG_4_CONCAT LIKE "%U071%" OR DIAG_4_CONCAT LIKE "%U072%"''')
apc = apc.withColumnRenamed('PERSON_ID_DEID', 'person_id_deid').withColumnRenamed('EPISTART', 'date')
apc = apc.withColumn('date_is', lit('EPISTART'))
apc = apc.filter((apc['date'] >= start_date) & (apc['date'] <= end_date))
apc = apc.filter(apc['person_id_deid'].isNotNull())
apc.createOrReplaceGlobalTempView('ccu013_tmp_apc')
#display(apc)
drop_table("ccu013_tmp_apc")
create_table("ccu013_tmp_apc")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(date), max(date) FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_apc

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_tmp_apc

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 HES CC

# COMMAND ----------

# HES CC
cc = spark.sql(f'''SELECT * FROM dars_nic_391419_j3w9t_collab.hes_cc_all_years_archive
              WHERE ProductionDate == "{production_date}"''')
cc = cc.withColumnRenamed('CCSTARTDATE', 'date').withColumnRenamed('PERSON_ID_DEID', 'person_id_deid')
cc = cc.withColumn('date_is', lit('CCSTARTDATE'))
# reformat dates for hes_cc as currently strings
asDate = udf(lambda x: datetime.strptime(x, '%Y%m%d'), DateType())
cc = cc.filter(cc['person_id_deid'].isNotNull())
cc = cc.withColumn('date', asDate(col('date')))
cc = cc.filter((cc['date'] >= start_date) & (cc['date'] <= end_date))
cc.createOrReplaceGlobalTempView('ccu013_tmp_cc')
drop_table("ccu013_tmp_cc")
create_table("ccu013_tmp_cc")
#display(cc)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(date), max(date) FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_cc

# COMMAND ----------

# MAGIC %sql -- create_table makes all tables as deltatables by default optimize
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_tmp_cc

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 Pillar 2 Antigen testing **Not currently in use**
# MAGIC Briefly this dataset should be entirely encapsulated within SGSS (which includes Pilars 1 & 2), however this was not the case. We were additionally detecting multiple tests per indidivdual, in contrast to the dataset specification. This dataset has currently been recalled by NHS-Digital.

# COMMAND ----------

# # pillar2 only include positve tests for now!
# pillar2 = spark.sql("""
# SELECT Person_ID_DEID as person_id_deid, 
# AppointmentDate as date, 
# CountryCode,
# TestResult,
# ResultInfo,
# 'AppointmentDate' as date_is,
# 'Pillar 2' as source,
# TestType, TestLocation, AdministrationMethod -- these seem the interesting 3 variables to me
# FROM dars_nic_391419_j3w9t_collab.ccu003_direfcts_dataprep_1_pillar2_frzon28may_mm_210528
# WHERE TestResult IN ('SCT:1322781000000102','SCT:1240581000000104')""")
# # reformat date as currently string
# pillar2 = pillar2.withColumn('date', substring('date', 0, 10)) # NB pillar2 dates in 2019-01-01T00:00:0000. format, therefore subset first
# pillar2 = pillar2.withColumn('TestResult', regexp_replace('TestResult', 'SCT:', ''))
# pillar2 = pillar2.filter(pillar2['person_id_deid'].isNotNull())
# pillar2 = pillar2.filter(pillar2['date'].isNotNull())
# asDate = udf(lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())
# pillar2 = pillar2.withColumn('date', asDate(col('date')))
# # Trim dates
# pillar2 = pillar2.filter((pillar2['date'] >= start_date) & (pillar2['date'] <= end_date))
# pillar2.createOrReplaceGlobalTempView('ccu013_tmp_pillar2')

# COMMAND ----------

# drop_table("ccu013_tmp_pillar2")
# create_table("ccu013_tmp_pillar2")

# COMMAND ----------

# %sql
# SELECT max(date) FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_pillar2

# COMMAND ----------

# %sql -- create_table makes all tables as deltatables by default optimize
# OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_tmp_pillar2

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.7 CHESS
# MAGIC * Previously we weren't using the `_archive` table as it wasn't updated/didn't exist

# COMMAND ----------

# chess = spark.sql('''SELECT PERSON_ID_DEID as person_id_deid, Typeofspecimen, Covid19, AdmittedToICU, Highflownasaloxygen, NoninvasiveMechanicalventilation, Invasivemechanicalventilation,
#                       RespiratorySupportECMO, DateAdmittedICU, HospitalAdmissionDate, InfectionSwabDate as date, 'InfectionSwabDate' as date_is 
#                       FROM dars_nic_391419_j3w9t.chess_dars_nic_391419_j3w9t''')    
chess = spark.sql(f'''SELECT PERSON_ID_DEID as person_id_deid, Typeofspecimen, Covid19, AdmittedToICU, Highflownasaloxygen, NoninvasiveMechanicalventilation, Invasivemechanicalventilation,
                     RespiratorySupportECMO, DateAdmittedICU, HospitalAdmissionDate, InfectionSwabDate as date, 'InfectionSwabDate' as date_is FROM dars_nic_391419_j3w9t_collab.chess_dars_nic_391419_j3w9t_archive WHERE ProductionDate == "{production_date}"''')
chess = chess.filter(chess['Covid19'] == 'Yes')
chess = chess.filter(chess['person_id_deid'].isNotNull())
#chess = chess.filter((chess['date'] >= start_date) & (chess['date'] <= end_date))
chess = chess.filter(((chess['date'] >= start_date) | (chess['date'].isNull())) & ((chess['date'] <= end_date) | (chess['date'].isNull())))
chess = chess.filter(((chess['HospitalAdmissionDate'] >= start_date) | (chess['HospitalAdmissionDate'].isNull())) & ((chess['HospitalAdmissionDate'] <= end_date) |(chess['HospitalAdmissionDate'].isNull())))
chess = chess.filter(((chess['DateAdmittedICU'] >= start_date) | (chess['DateAdmittedICU'].isNull())) & ((chess['DateAdmittedICU'] <= end_date) | (chess['DateAdmittedICU'].isNull())))
chess.createOrReplaceGlobalTempView('ccu013_tmp_chess')
drop_table("ccu013_tmp_chess")
create_table("ccu013_tmp_chess")
display(chess)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(date), max(date) FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_chess

# COMMAND ----------

# MAGIC %sql -- create_table makes all tables as deltatables by default optimize
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_tmp_chess

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.8 SUS
# MAGIC NB this is a large dataset and the coalescing of diagnosis & procedure fields into a format compatible with our HES queries takes some time (~40 mins)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT ProductionDate FROM dars_nic_391419_j3w9t_collab.sus_dars_nic_391419_j3w9t_archive 

# COMMAND ----------

## SUS
# ! Takes ~ 40 mins 
sus = spark.sql('''SELECT NHS_NUMBER_DEID, EPISODE_START_DATE, 
CONCAT (COALESCE(PRIMARY_DIAGNOSIS_CODE, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_1, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_2, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_3, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_4, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_5, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_6, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_7, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_8, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_9, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_10, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_11, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_12, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_13, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_14, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_15, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_16, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_17, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_18, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_19, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_20, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_21, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_22, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_23, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_24, '')) as DIAG_CONCAT,
CONCAT (COALESCE(PRIMARY_PROCEDURE_CODE, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_1, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_2, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_1, ''),  ',',
COALESCE(SECONDARY_PROCEDURE_CODE_3, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_4, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_5, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_6, ''), ',',
COALESCE(SECONDARY_PROCEDURE_CODE_7, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_8, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_9, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_10, ''), ',',
COALESCE(SECONDARY_PROCEDURE_CODE_11, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_12, '')) as PROCEDURE_CONCAT, PRIMARY_PROCEDURE_DATE, SECONDARY_PROCEDURE_DATE_1,
DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL, DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL, END_DATE_HOSPITAL_PROVIDER_SPELL FROM dars_nic_391419_j3w9t.sus_dars_nic_391419_j3w9t''')
#sus = spark.sql(f'''SELECT NHS_NUMBER_DEID, EPISODE_START_DATE, 
#CONCAT (COALESCE(PRIMARY_DIAGNOSIS_CODE, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_1, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_2, ''), ',',
#COALESCE(SECONDARY_DIAGNOSIS_CODE_3, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_4, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_5, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_6, ''), ',',
#COALESCE(SECONDARY_DIAGNOSIS_CODE_7, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_8, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_9, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_10, ''), ',',
#COALESCE(SECONDARY_DIAGNOSIS_CODE_11, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_12, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_13, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_14, ''), ',',
#COALESCE(SECONDARY_DIAGNOSIS_CODE_15, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_16, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_17, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_18, ''), ',',
#COALESCE(SECONDARY_DIAGNOSIS_CODE_19, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_20, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_21, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_22, ''), ',',
#COALESCE(SECONDARY_DIAGNOSIS_CODE_23, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_24, '')) as DIAG_CONCAT,
#CONCAT (COALESCE(PRIMARY_PROCEDURE_CODE, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_1, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_2, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_1, ''),  ',',
#COALESCE(SECONDARY_PROCEDURE_CODE_3, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_4, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_5, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_6, ''), ',',
#COALESCE(SECONDARY_PROCEDURE_CODE_7, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_8, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_9, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_10, ''), ',',
#COALESCE(SECONDARY_PROCEDURE_CODE_11, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_12, '')) as PROCEDURE_CONCAT, PRIMARY_PROCEDURE_DATE, SECONDARY_PROCEDURE_DATE_1,
#DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL, DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL, END_DATE_HOSPITAL_PROVIDER_SPELL
#FROM dars_nic_391419_j3w9t_collab.sus_dars_nic_391419_j3w9t_archive 
#WHERE ProductionDate == "{production_date}"
#''')
## Other potential interstersting columns: END_DATE_HOSPITAL_PROVIDER_SPELL, EPISODE_START_DATE, EPISODE_END_DATE, PRIMARY_PROCEDURE_CODE, PRIMARY_PROCEDURE_DATE, SECONDARY_PROCEDURE_CODE1 - 12
sus = sus.withColumnRenamed('NHS_NUMBER_DEID', 'person_id_deid').withColumnRenamed('EPISODE_START_DATE', 'date')
sus = sus.withColumn('date_is', lit('EPISODE_START_DATE'))
sus = sus.filter((sus['date'] >= start_date) & (sus['date'] <= end_date))
sus = sus.filter(((sus['END_DATE_HOSPITAL_PROVIDER_SPELL'] >= start_date) | (sus['END_DATE_HOSPITAL_PROVIDER_SPELL'].isNull())) & ((sus['END_DATE_HOSPITAL_PROVIDER_SPELL'] <= end_date) | (sus['END_DATE_HOSPITAL_PROVIDER_SPELL'].isNull())))
sus = sus.filter(sus['person_id_deid'].isNotNull()) # Loads of rows with missing IDs
sus = sus.filter(sus['date'].isNotNull())
sus.createOrReplaceGlobalTempView('ccu013_tmp_sus')
drop_table("ccu013_tmp_sus")
create_table("ccu013_tmp_sus")
display(sus)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(END_DATE_HOSPITAL_PROVIDER_SPELL) FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_sus

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(date) FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_sus

# COMMAND ----------

# MAGIC %sql -- create_table makes all tables as deltatables by default optimize
# MAGIC -- ! Takes ~ 24 mins
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_tmp_sus

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.9 Vaccination status

# COMMAND ----------

vaccine = spark.sql(f'''SELECT PERSON_ID_DEID, DOSE_SEQUENCE, DATE_AND_TIME, ProductionDate FROM dars_nic_391419_j3w9t_collab.vaccine_status_dars_nic_391419_j3w9t_archive 
                        WHERE ProductionDate == "{production_date}"''')
vaccine = vaccine.withColumnRenamed('PERSON_ID_DEID', 'person_id_deid')
# pillar2 = pillar2.withColumn('date', substring('date', 0, 10))
vaccine = vaccine.withColumn('date',substring('DATE_AND_TIME', 0,8))
vaccine = vaccine.withColumn('date', to_date(vaccine['date'], "yyyyMMdd"))
vaccine = vaccine.filter((vaccine['date'] >= start_date) & (vaccine['date'] <= end_date))
vaccine = vaccine.withColumn('date_is', lit('DATE_AND_TIME'))
vaccine = vaccine.select('person_id_deid', 'date', 'DOSE_SEQUENCE', 'ProductionDate')
vaccine.createOrReplaceGlobalTempView('ccu013_vaccine_status')
drop_table("ccu013_vaccine_status")
create_table("ccu013_vaccine_status")
display(vaccine)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.0 Hardcode SNOMED-CT codes for COVID-19

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SNOMED COVID-19 codes
# MAGIC -- Create Temporary View with of COVID-19 codes and their grouping - to be used whilst waiting for them to be uploaded onto the TR
# MAGIC -- Covid-1 Status groups:
# MAGIC -- - Lab confirmed incidence
# MAGIC -- - Lab confirmed historic
# MAGIC -- - Clinically confirmed
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_snomed_codes_covid19 AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ("1008541000000105","Coronavirus ribonucleic acid detection assay (observable entity)","0","1","Lab confirmed incidence"),
# MAGIC ("1029481000000103","Coronavirus nucleic acid detection assay (observable entity)","0","1","Lab confirmed incidence"),
# MAGIC ("120814005","Coronavirus antibody (substance)","0","1","Lab confirmed historic"),
# MAGIC ("121973000","Measurement of coronavirus antibody (procedure)","0","1","Lab confirmed historic"),
# MAGIC ("1240381000000105","Severe acute respiratory syndrome coronavirus 2 (organism)","0","1","Clinically confirmed"),
# MAGIC ("1240391000000107","Antigen of severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed incidence"),
# MAGIC ("1240401000000105","Antibody to severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed historic"),
# MAGIC ("1240411000000107","Ribonucleic acid of severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed incidence"),
# MAGIC ("1240421000000101","Serotype severe acute respiratory syndrome coronavirus 2 (qualifier value)","0","1","Lab confirmed historic"),
# MAGIC ("1240511000000106","Detection of severe acute respiratory syndrome coronavirus 2 using polymerase chain reaction technique (procedure)","0","1","Lab confirmed incidence"),
# MAGIC ("1240521000000100","Otitis media caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240531000000103","Myocarditis caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240541000000107","Infection of upper respiratory tract caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240551000000105","Pneumonia caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240561000000108","Encephalopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240571000000101","Gastroenteritis caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240581000000104","Severe acute respiratory syndrome coronavirus 2 ribonucleic acid detected (finding)","0","1","Lab confirmed incidence"),
# MAGIC ("1240741000000103","Severe acute respiratory syndrome coronavirus 2 serology (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1240751000000100","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1300631000000101","Coronavirus disease 19 severity score (observable entity)","0","1","Clinically confirmed"),
# MAGIC ("1300671000000104","Coronavirus disease 19 severity scale (assessment scale)","0","1","Clinically confirmed"),
# MAGIC ("1300681000000102","Assessment using coronavirus disease 19 severity scale (procedure)","0","1","Clinically confirmed"),
# MAGIC ("1300721000000109","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed by laboratory test (situation)","0","1","Lab confirmed historic"),
# MAGIC ("1300731000000106","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed using clinical diagnostic criteria (situation)","0","1","Clinically confirmed"),
# MAGIC ("1321181000000108","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 record extraction simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321191000000105","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 procedures simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321201000000107","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 health issues simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321211000000109","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 presenting complaints simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321241000000105","Cardiomyopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1321301000000101","Severe acute respiratory syndrome coronavirus 2 ribonucleic acid qualitative existence in specimen (observable entity)","0","1","Lab confirmed incidence"),
# MAGIC ("1321311000000104","Severe acute respiratory syndrome coronavirus 2 immunoglobulin M qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321321000000105","Severe acute respiratory syndrome coronavirus 2 immunoglobulin G qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321331000000107","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 total immunoglobulin in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321341000000103","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin G in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321351000000100","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin M in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321541000000108","Severe acute respiratory syndrome coronavirus 2 immunoglobulin G detected (finding)","0","1","Lab confirmed historic"),
# MAGIC ("1321551000000106","Severe acute respiratory syndrome coronavirus 2 immunoglobulin M detected (finding)","0","1","Lab confirmed historic"),
# MAGIC ("1321761000000103","Severe acute respiratory syndrome coronavirus 2 immunoglobulin A detected (finding)","0","1","Lab confirmed historic"),
# MAGIC ("1321801000000108","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin A in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321811000000105","Severe acute respiratory syndrome coronavirus 2 immunoglobulin A qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1322781000000102","Severe acute respiratory syndrome coronavirus 2 antigen detection result positive (finding)","0","1","Lab confirmed incidence"),
# MAGIC ("1322871000000109","Severe acute respiratory syndrome coronavirus 2 antibody detection result positive (finding)","0","1","Lab confirmed historic"),
# MAGIC ("186747009","Coronavirus infection (disorder)","0","1","Clinically confirmed")
# MAGIC 
# MAGIC AS tab(clinical_code, description, sensitive_status, include_binary, covid_phenotype);
