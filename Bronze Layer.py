# Databricks notebook source
import random
import datetime 
from datetime import datetime,timedelta 

def dategen():
    rand_days=random.randint(1,100)
    startdate=datetime(2024,1,1)
    new_date=startdate+timedelta(days=rand_days)
    return new_date.date()

order_data=[]
for i in range(1,101):
    order_date=dategen()
    order_id=random.randint(100,999)
    cust_id=random.randint(1,100)
    order_element={'order_date':order_date,
                   'order_id':order_id,
                   'cust_id':cust_id}
    order_data.append(order_element)
    
print('Data is ready')

# COMMAND ----------

from pyspark.sql.types import *


order_schema=StructType([StructField('order_date',DateType(),False),
                         StructField('order_id',IntegerType(),False),
                         StructField('cust_id',IntegerType(),False)
                         ])

order_df=spark.createDataFrame(order_data,schema=order_schema)


# COMMAND ----------

order_df.limit(5).display()

# COMMAND ----------

# MAGIC %run "../My Workspace/config"

# COMMAND ----------

## data saving to ADLS

order_df.repartition(1).write.mode('append').format('parquet').option('header',True).save("abfss://bronze@shouryadatalake.dfs.core.windows.net/orderdataset")