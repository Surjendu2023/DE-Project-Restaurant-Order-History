# Databricks notebook source
# MAGIC %run "../My Workspace/config"

# COMMAND ----------

customer_df=spark.read\
    .option('header',True)\
    .format('parquet')\
    .load("abfss://bronze@shouryadatalake.dfs.core.windows.net/customerdataset")


order_df=spark.read\
              .option('header',True)\
              .format('parquet')\
              .load("abfss://bronze@shouryadatalake.dfs.core.windows.net/orderdataset")

# COMMAND ----------

#basic transformation
from pyspark.sql.functions import col,concat,lit,initcap,date_format,collect_list

customer_df=customer_df.withColumn('Full Name',initcap(concat(col('F_Name'),lit(' '),col('L_Name'))))\
                       .withColumnRenamed('cust_id','customer_id')



order_df=order_df.withColumn('Month',date_format('order_date','MMM'))

customer_df.limit(3).display()

order_df.limit(3).display()

# COMMAND ----------

food_df=spark.read.format('csv')\
          .option('header',True)\
          .load('abfss://bronze@shouryadatalake.dfs.core.windows.net/Food List.csv',inFerschema=True)

# COMMAND ----------

food_df.count()

# COMMAND ----------

## Master dataset single Table

master=customer_df.join(order_df,customer_df['customer_id']==order_df['cust_id'],'inner')

master=master.join(food_df,master['order_id']==food_df['Iteam Number'],'left')


# COMMAND ----------

## Saving Parquet File
master.select('customer_id', 'Full Name','Phone_Number','Region','Address','order_id','order_date','Month','Iteam Name','Iteam Price').write.mode('overwrite').format('parquet').option('header',True)\
    .save("abfss://silver@shouryadatalake.dfs.core.windows.net/master")