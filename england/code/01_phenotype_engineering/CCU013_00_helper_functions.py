# Databricks notebook source
# MAGIC %md
# MAGIC  # Helper Functions
# MAGIC  
# MAGIC **Description** 
# MAGIC   
# MAGIC This notebook contains a couple of `pyspark` functions used frequently within the `CCU013` project. 
# MAGIC   
# MAGIC It can be sourced for another notebook using:  
# MAGIC   
# MAGIC   `dbutils.notebook.run("./CCU013_00_helper_functions", 30000) # timeout_seconds`  
# MAGIC   
# MAGIC This is advantageous to using `%run ./CCU013_00_helper_functions` because it doesn't include the markdown component (although you can just click "hide result" on the cell with `%run`) 
# MAGIC 
# MAGIC **Project(s)** CCU013
# MAGIC  
# MAGIC **Author(s)** Sam Hollings, Chris Tomlinson
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2021-06-21
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** NA
# MAGIC  
# MAGIC **Data input** <NA>
# MAGIC   
# MAGIC **Data output**  <NA>
# MAGIC   
# MAGIC **Software and versions** `python`
# MAGIC  
# MAGIC **Packages and versions** `pyspark`

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Sam Hollings
# MAGIC * Functions to create and drop (separate) a table from a pyspark data frame
# MAGIC * Builds delta tables to allow for optimisation
# MAGIC * Modifies owner to allow tables to be dropped

# COMMAND ----------

def create_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', select_sql_script:str=None, if_not_exists=True) -> None:
  """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
  Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifcied database."""
  
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
  
  if select_sql_script is None:
    select_sql_script = f"SELECT * FROM global_temp.{table_name}"
  
  if if_not_exists is True:
    if_not_exists_script=' IF NOT EXISTS'
  else:
    if_not_exists_script=''
  
  spark.sql(f"""CREATE TABLE {if_not_exists_script} {database_name}.{table_name} USING DELTA AS
                {select_sql_script}
             """)
  spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}")
  
def drop_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', if_exists=True):
  if if_exists:
    IF_EXISTS = 'IF EXISTS'
  else: 
    IF_EXISTS = ''
  spark.sql(f"DROP TABLE {IF_EXISTS} {database_name}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Chris Tomlinson
# MAGIC * Couple of useful functions for common tasks that I haven't found direct pyspark equivalents of
# MAGIC * Likely more efficient ways exist for those familiar with pyspark
# MAGIC * Code is likley to be heavily inspired by solutions seen on StackOverflow, unfortunately reference links not kept

# COMMAND ----------

from pyspark.sql.functions import lit, col, udf
from functools import reduce
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.types import DateType

asDate = udf(lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())

# Pyspark has no .shape() function therefore define own
import pyspark

def spark_shape(self):
    return (self.count(), len(self.columns))
pyspark.sql.dataframe.DataFrame.shape = spark_shape

# Checking for duplicates
def checkOneRowPerPt(table, id):
  n = table.select(id).count()
  n_distinct = table.select(id).dropDuplicates([id]).count()
  print("N. of rows:", n)
  print("N. distinct ids:", n_distinct)
  print("Duplicates = ", n - n_distinct)
  if n > n_distinct:
    raise ValueError('Data has duplicates!')
