# Databricks notebook source
df = spark.table('.ccu013_covid_events')

if df.count() > df.dropDuplicates(['person_id_deid']).count():
    raise ValueError('Data has duplicates')
    df.exceptAll(df.dropDuplicates(['person_id_deid'])).show()
else:
    print("No duplicate ids detected")

# COMMAND ----------

display(
  df.exceptAll(df.dropDuplicates(['person_id_deid']))
)
