# Databricks notebook source
### 1. Sam Hollings
# Functions to create and drop (separate) a table from a pyspark data frame
# Builds delta tables to allow for optimisation
# Modifies owner to allow tables to be dropped
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

## 2. Chris Tomlinson
# Couple of useful functions for common tasks that I haven't found direct pyspark equivalents of
# Likely more efficient ways exist for those familiar with pyspark
# Code is likley to be heavily inspired by solutions seen on StackOverflow, unfortunately reference links not kept

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

# COMMAND ----------

def optimise_table(table_name:str):
    spark.sql("""OPTIMIZE dars_nic_391419_j3w9t_collab.{}""".format(table_name))
