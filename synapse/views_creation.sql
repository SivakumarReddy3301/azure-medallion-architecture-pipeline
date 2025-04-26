CREATE SCHEMA gold

-----------------------
-- create view calender
-----------------------
CREATE VIEW gold.calender
AS
SELECT * FROM OPENROWSET(
    BULK 'https://adworksprojectstorage.blob.core.windows.net/silver/AdventureWorks_Calender/',
    FORMAT='PARQUET'
) as q1;

-----------------------
-- create view customer
-----------------------
CREATE VIEW gold.customer
AS
SELECT * FROM OPENROWSET(
    BULK 'https://adworksprojectstorage.blob.core.windows.net/silver/AdventureWorks_Customer/',
    FORMAT='Parquet'
) as q2;

-----------------------
-- create view products
-----------------------
CREATE VIEW gold.products
AS
SELECT * FROM OPENROWSET(
    BULK 'https://adworksprojectstorage.blob.core.windows.net/silver/AdventureWorks_Products/',
    FORMAT='Parquet'
) as q3;

-----------------------
-- create view Categories
-----------------------
CREATE VIEW gold.Categories
AS
SELECT * FROM OPENROWSET(
    BULK 'https://adworksprojectstorage.blob.core.windows.net/silver/AdventureWorks_Categories/',
    FORMAT='Parquet'
) as q4;

-----------------------
-- create view returns
-----------------------
CREATE VIEW gold.returns
AS
SELECT * FROM OPENROWSET(
    BULK 'https://adworksprojectstorage.blob.core.windows.net/silver/AdventureWorks_Returns/',
    FORMAT='Parquet'
) as q5;

-----------------------
-- create view Sales
-----------------------
CREATE VIEW gold.sales
AS
SELECT * FROM OPENROWSET(
    BULK 'https://adworksprojectstorage.blob.core.windows.net/silver/AdventureWorks_Sales/',
    FORMAT='Parquet'
) as q6;

-----------------------
-- create view Subcategories
-----------------------
CREATE VIEW gold.subcategories
AS
SELECT * FROM OPENROWSET(
    BULK 'https://adworksprojectstorage.blob.core.windows.net/silver/AdventureWorks_Subcategories/',
    FORMAT='Parquet'
) as q7;

-----------------------
-- create view territoties
-----------------------
CREATE VIEW gold.territories
AS
SELECT * FROM OPENROWSET(
    BULK 'https://adworksprojectstorage.blob.core.windows.net/silver/AdventureWorks_Territories/',
    FORMAT='Parquet'
) as q8;
