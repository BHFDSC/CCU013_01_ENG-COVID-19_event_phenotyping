# Databricks notebook source
# MAGIC %md
# MAGIC Builds a binary feature matrix of CALIBER phenotypes prior to 01/01/2020

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU013/COVID-19-SEVERITY-PHENOTYPING/CCU013_00_helper_functions

# COMMAND ----------

import databricks.koalas as ks

from functools import reduce
from operator import add
from pyspark.sql.functions import lit, col

def row_sum_across(*cols):
    return reduce(add, cols, lit(0))

# COMMAND ----------

# Table names
demographics_table = "dars_nic_391419_j3w9t_collab.ccu013_master_demographics"
skinny_table = "dars_nic_391419_j3w9t_collab.ccu013_caliber_skinny"

# without dars_nic_391419_j3w9t_collab. prefix
output_table = "ccu013_caliber_comorbidities_pre2020"

# COMMAND ----------

comorbidities = spark.sql(f"""
  SELECT 
    base.person_id_deid,
    phenotype,
    value
  FROM 
    {demographics_table} as base
  FULL JOIN
    (SELECT 
      person_id_deid, 
      date, 
      phenotype, 
      1 as value 
    FROM 
      {skinny_table}) 
      as phenos
  ON 
    base.person_id_deid = phenos.person_id_deid
  WHERE 
    phenos.date < '2020-01-01'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pivot
# MAGIC Use `koalas` to use `pandas` like pivot syntax but parallelisable for large dataframes

# COMMAND ----------

comorbidities = comorbidities \
  .to_koalas() \
  .pivot(index='person_id_deid', 
         columns='phenotype', 
         values='value') \
  .fillna(0) \
  .reset_index() \
  .to_spark()
# Reset index to breakout ids to separate col

# COMMAND ----------

display(comorbidities)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add multimorbidity variable

# COMMAND ----------

phenos = comorbidities.schema.names[1:]

vars = [col(x) for x in phenos]

comorbidities = comorbidities \
  .fillna(0) \
  .withColumn('multimorbidity', row_sum_across(*vars))

# COMMAND ----------

display(comorbidities)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Commit

# COMMAND ----------

comorbidities.createOrReplaceGlobalTempView(output_table)
drop_table(output_table)
create_table(output_table)

# COMMAND ----------

spark.sql(f"OPTIMIZE dars_nic_391419_j3w9t_collab.{output_table} ZORDER BY person_id_deid")
