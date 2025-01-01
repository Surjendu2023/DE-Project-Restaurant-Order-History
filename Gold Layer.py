# Databricks notebook source
# MAGIC %run "../My Workspace/config"

# COMMAND ----------

df=spark.read.format('parquet').option('header',True).load("abfss://silver@shouryadatalake.dfs.core.windows.net/master")

# COMMAND ----------

# MAGIC %md
# MAGIC **Business KPI**
# MAGIC - Total Sales Month Wise
# MAGIC - Total Sales Region Wise
# MAGIC - Top 10 Purchased Product
# MAGIC - Customer who purchased item more than 3 times

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Total Sales Month Wise

# COMMAND ----------

from pyspark.sql.functions import *

df.groupBy("Month").agg(count('order_id').alias('Total Order'),sum('Iteam Price').alias('Total Sales')).display()

Month_sales=df.groupBy("Month").agg(count('order_id').alias('Total Order'),sum('Iteam Price').alias('Total Sales'))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Total Sales Region Wise

# COMMAND ----------

df.groupBy('Region').agg(sum('Iteam Price').alias('Total Sales'))\
  .sort(col('Total Sales').desc())\
  .display()

region_sales=df.groupBy('Region').agg(sum('Iteam Price').alias('Total Sales'))\
  .sort(col('Total Sales').desc())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 Purchased Product

# COMMAND ----------

df.groupBy('Iteam Name').agg(count('order_id').alias('Total Order'))\
  .sort('Total Order',ascending=False)\
  .limit(10).display()

TOP_Product=df.groupBy('Iteam Name').agg(count('order_id').alias('Total Order'))\
  .sort('Total Order',ascending=False)\
  .limit(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer who purchased item more than 3 times

# COMMAND ----------

df.groupBy('customer_id').agg(count('order_id').alias('total order')).filter(col('total order')>3).display()

TOP_Customer=df.groupBy('customer_id').agg(count('order_id').alias('total order')).filter(col('total order')>3)

# COMMAND ----------

## save all the agg files to adls gold layer

TOP_Customer.repartition(1).write.mode('overwrite').option('header',True).format('csv').save("abfss://gold@shouryadatalake.dfs.core.windows.net/Top_customer")

TOP_Product.repartition(1).write.mode('overwrite').format('csv').option('header',True).save("abfss://gold@shouryadatalake.dfs.core.windows.net/Top_Product")

region_sales.repartition(1).write.mode('overwrite').format('csv').option('header',True).save("abfss://gold@shouryadatalake.dfs.core.windows.net/Region_sales")

Month_sales.repartition(1).write.mode('overwrite').format('csv').option('header',True).save("abfss://gold@shouryadatalake.dfs.core.windows.net/Month_Sales")
