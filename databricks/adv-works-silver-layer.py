# Databricks notebook source
# MAGIC %md
# MAGIC ## SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data access using APP

# COMMAND ----------

# service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.adworksprojectstorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adworksprojectstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adworksprojectstorage.dfs.core.windows.net", "<client-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.adworksprojectstorage.dfs.core.windows.net", "<secret-value>")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adworksprojectstorage.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Loading

# COMMAND ----------

# Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading data

# COMMAND ----------

df_cal = spark.read.format("csv")\
    .options(header='true',inferSchema='true')\
    .load("abfss://bronze@adworksprojectstorage.dfs.core.windows.net/calender/calender.csv")

# COMMAND ----------

df_cust = spark.read.format("csv")\
    .options(header='true',inferSchema='true')\
    .load("abfss://bronze@adworksprojectstorage.dfs.core.windows.net/customers/customers.csv")

# COMMAND ----------

df_products = spark.read.format("csv")\
    .options(header='true',inferSchema='true')\
    .load("abfss://bronze@adworksprojectstorage.dfs.core.windows.net/products/products.csv")
df_product_cats = spark.read.format("csv")\
    .options(header='true',inferSchema='true')\
    .load("abfss://bronze@adworksprojectstorage.dfs.core.windows.net/products/product-categories.csv")
df_product_subcats = spark.read.format("csv")\
    .options(header='true',inferSchema='true')\
    .load("abfss://bronze@adworksprojectstorage.dfs.core.windows.net/products/product-subcategories.csv")

# COMMAND ----------

df_returns = spark.read.format("csv")\
    .options(header='true',inferSchema='true')\
    .load("abfss://bronze@adworksprojectstorage.dfs.core.windows.net/returns/returns.csv")

# COMMAND ----------

df_sales = spark.read.format("csv")\
    .options(header='true',inferSchema='true')\
    .load("abfss://bronze@adworksprojectstorage.dfs.core.windows.net/sales/*.csv")

# COMMAND ----------

df_territories = spark.read.format("csv")\
    .options(header='true',inferSchema='true')\
    .load("abfss://bronze@adworksprojectstorage.dfs.core.windows.net/territories/territories.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tranform Data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calender

# COMMAND ----------

# display(df_cal)

df_cal = df_cal.withColumn('Month',month('Date'))\
      .withColumn('Year',year('Date'))\
      .withColumn('Day',dayofmonth('Date'))

# COMMAND ----------

df_cal.write.format('parquet')\
    .mode('overwrite')\
    .save("abfss://silver@adworksprojectstorage.dfs.core.windows.net/AdventureWorks_Calender")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customer

# COMMAND ----------

df_cust.display()

# COMMAND ----------

df_cust = df_cust.withColumn('FullName',concat_ws(' ',df_cust.Prefix,df_cust.FirstName,df_cust.LastName))
df_cust.display()

# COMMAND ----------

df_cust.write.format('parquet')\
    .mode('overwrite')\
    .save("abfss://silver@adworksprojectstorage.dfs.core.windows.net/AdventureWorks_Customer")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Product Sub categories

# COMMAND ----------

df_product_subcats.display()

# COMMAND ----------

df_product_subcats.write.format('parquet')\
    .mode('append')\
    .save("abfss://silver@adworksprojectstorage.dfs.core.windows.net/AdventureWorks_Subcategories")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Products

# COMMAND ----------

df_products.display()

# COMMAND ----------

df_products = df_products.withColumn('ProductSKU',split(df_products.ProductSKU,'-')[0])\
    .withColumn('ProductName',split(df_products.ProductName,' ')[0])

# COMMAND ----------

df_products.write.format('parquet')\
    .mode('overwrite')\
    .save("abfss://silver@adworksprojectstorage.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Returns
# MAGIC

# COMMAND ----------

df_returns.display()

# COMMAND ----------

df_returns.write.format('parquet')\
    .mode('append')\
    .save("abfss://silver@adworksprojectstorage.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Territories

# COMMAND ----------

df_territories.display()

# COMMAND ----------

df_territories.write.format('parquet')\
    .mode('append')\
    .save("abfss://silver@adworksprojectstorage.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Product categories

# COMMAND ----------

df_product_cats.display()

# COMMAND ----------

df_product_cats.write.format('parquet')\
    .mode('append')\
    .save("abfss://silver@adworksprojectstorage.dfs.core.windows.net/AdventureWorks_Categories")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate',to_timestamp(col('StockDate')))

# COMMAND ----------

df_sales = df_sales.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_sales = df_sales.withColumn('multiply',col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.write.format('parquet')\
    .mode('append')\
    .save("abfss://silver@adworksprojectstorage.dfs.core.windows.net/AdventureWorks_Sales")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales Analysis

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count(col('OrderNumber')).alias('TotalOrder')).display()

# COMMAND ----------

# df_product_cats.display()
# df_products.distinct().display()
# df_product_subcats.display()

df_products.join(df_product_subcats,on='ProductSubcategoryKey')\
    .join(df_product_cats,on='ProductCategoryKey')\
    .groupBy('CategoryName')\
    .agg(count('ProductKey').alias('TotalProducts'))\
    .display()