-- CREATE MASTER KEY ENCRYPTION BY PASSWORD ='password'
CREATE DATABASE SCOPED CREDENTIAL cred_shiv
WITH IDENTITY = 'Managed Identity'

--------------------------------
-----CREATE DATA SOURCES FOR----
----READ(SILVER) & WRITE(GOLD)-
--------------------------------

CREATE EXTERNAL DATA SOURCE silver_source
WITH(
    LOCATION = 'https://adworksprojectstorage.blob.core.windows.net/silver',
    CREDENTIAL = cred_shiv
)

CREATE EXTERNAL DATA SOURCE gold_source
WITH(
    LOCATION = 'https://adworksprojectstorage.blob.core.windows.net/gold',
    CREDENTIAL = cred_shiv
)

--------------------------------
-----CREATE FILE FORMAT---------
--------------------------------
CREATE EXTERNAL FILE FORMAT parquet_format
WITH (
    FORMAT_TYPE=PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

--------------------------------
-----CREATE EXTERNAL TABLES-----
--------------------------------
CREATE EXTERNAL TABLE gold.extSales
WITH
(
	LOCATION = '/extsales',
	DATA_SOURCE = gold_source,
	FILE_FORMAT = parquet_format
)
AS SELECT * FROM gold.sales

CREATE EXTERNAL TABLE gold.extCalender
WITH
(
	LOCATION = '/extcalender',
	DATA_SOURCE = gold_source,
	FILE_FORMAT = parquet_format
)
AS SELECT * FROM gold.calender

CREATE EXTERNAL TABLE gold.extCategories
WITH
(
	LOCATION = '/extCategories',
	DATA_SOURCE = gold_source,
	FILE_FORMAT = parquet_format
)
AS SELECT * FROM gold.Categories

CREATE EXTERNAL TABLE gold.extCustomers
WITH
(
	LOCATION = '/extCustomers',
	DATA_SOURCE = gold_source,
	FILE_FORMAT = parquet_format
)
AS SELECT * FROM gold.customer

CREATE EXTERNAL TABLE gold.extProducts
WITH
(
	LOCATION = '/extProducts',
	DATA_SOURCE = gold_source,
	FILE_FORMAT = parquet_format
)
AS SELECT * FROM gold.products

CREATE EXTERNAL TABLE gold.extReturns
WITH
(
	LOCATION = '/extReturns',
	DATA_SOURCE = gold_source,
	FILE_FORMAT = parquet_format
)
AS SELECT * FROM gold.returns

CREATE EXTERNAL TABLE gold.extSubcategories
WITH
(
	LOCATION = '/extSubcategories',
	DATA_SOURCE = gold_source,
	FILE_FORMAT = parquet_format
)
AS SELECT * FROM gold.subcategories

CREATE EXTERNAL TABLE gold.extTerritories
WITH
(
	LOCATION = '/extTerritories',
	DATA_SOURCE = gold_source,
	FILE_FORMAT = parquet_format
)
AS SELECT * FROM gold.territories
