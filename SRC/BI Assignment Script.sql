/*
	Business integerelligence & Reporting
	Home Assignment II

*/

USE [iict6011a02];
GO

-- Preliminary Analysis to check for duplicates
SELECT		productId
			, orderId
			, COUNT(*) AS 'Count'
FROM		[oltp].[orderItem]
GROUP BY	productId, orderId
HAVING COUNT (*) > 1 

-- CTE To Delete the duplicates found in the previous statement
WITH	CTE
AS		(SELECT	productId, orderId, ROW_NUMBER() OVER (PARTITION BY orderId ORDER BY orderId) AS rowNumber
		FROM	[oltp].[orderItem])
DELETE	FROM CTE
WHERE	rowNumber > 1;

-- DROP Procedure
DROP PROCEDURE [ProductFact].[sp_registerClient];
GO

-- Drop Tables
DROP TABLE [ProductFact].[OrderFact];
DROP TABLE [ProductFact].[Product];
DROP TABLE [ProductFact].[Store];
DROP TABLE [ProductFact].[Time];
DROP TABLE [ProductFact].[Client];
GO

-- Drop Schema
DROP SCHEMA [ProductFact];
GO

-- Creation of Schema
CREATE SCHEMA [ProductFact];
GO

-- Creation of Tables
CREATE TABLE [ProductFact].[Client](
	clientKey UNIQUEIDENTIFIER CONSTRAINT client_dim_key PRIMARY KEY DEFAULT newid()
	, country NVARCHAR(256) NOT NULL
	, region NVARCHAR (256) NOT NULL
	, city NVARCHAR (256) NOT NULL
	, gender CHAR NOT NULL
	, age NUMERIC (3) NOT NULL
	, fromDate DATE NOT NULL
	, toDate DATE
	, clientDescription NVARCHAR(MAX) NOT NULL
	, clientId UNIQUEIDENTIFIER NOT NULL
	, CONSTRAINT client_OLTP_ID UNIQUE (clientId, fromDate)
);
GO

CREATE TABLE [ProductFact].[time](
	DATEKey UNIQUEIDENTIFIER CONSTRAINT DATE_key PRIMARY KEY DEFAULT newid()
	, year integer NOT NULL
	, quarter integer NOT NULL
	, month integer NOT NULL
	, dayOfTheMonth integer NOT NULL
	, dayOfTheWeek integer NOT NULL
	, DateValue DATE NOT NULL
	, CONSTRAINT timeValue UNIQUE(DateValue)
);
GO

CREATE TABLE [ProductFact].[Store](
	storeKey UNIQUEIDENTIFIER CONSTRAINT store_key PRIMARY KEY DEFAULT newid()
	, country NVARCHAR(256) NOT NULL
	, region NVARCHAR(256) NOT NULL
	, city NVARCHAR(256) NOT NULL
	, storeType NVARCHAR(256)
	, storeDescription NVARCHAR(256)
	, storeID UNIQUEIDENTIFIER
	, CONSTRAINT order_OLTP_ID UNIQUE (storeId)
);
GO



CREATE TABLE [ProductFact].[Product](
	productKey UNIQUEIDENTIFIER CONSTRAINT product_Key PRIMARY KEY DEFAULT newid()
	, FamilyName NVARCHAR(256) NOT NULL
	, departmentName NVARCHAR(256) NOT NULL
	, categoryName NVARCHAR(256) NOT NULL
	, subcatogoryName NVARCHAR(256) NOT NULL
	, brandName NVARCHAR(256) NOT NULL
	, productDescription NVARCHAR(256) NOT NULL
	, productID UNIQUEIDENTIFIER NOT NULL
	, SKU NUMERIC(11) NOT NULL
	, CONSTRAINT productId UNIQUE (productId)
	, CONSTRAINT productNatKey UNIQUE (SKU)
);
GO

CREATE TABLE [ProductFact].[OrderFact](
	orderFactKey UNIQUEIDENTIFIER CONSTRAINT order_key PRIMARY KEY DEFAULT newid()
	, storeCost FLOAT NOT NULL
	, storePrice FLOAT NOT NULL
	, unitsSold FLOAT NOT NULL
	, orderId UNIQUEIDENTIFIER NOT NULL
	, ProductID UNIQUEIDENTIFIER NOT NULL
	, storeKey UNIQUEIDENTIFIER NOT NULL REFERENCES [ProductFact].[Store](storeKey)
	, dateKey UNIQUEIDENTIFIER NOT NULL REFERENCES [ProductFact].[Time](DATEKey)
	, productKey UNIQUEIDENTIFIER NOT NULL REFERENCES [ProductFact].[Product](productKey)
	, clientKey UNIQUEIDENTIFIER NOT NULL REFERENCES [ProductFact].[Client](clientKey)
	, CONSTRAINT Order_OLTPID UNIQUE (orderId, ProductID)
);
GO

-- Procedure
CREATE PROCEDURE [ProductFact].[sp_registerClient](@country nvarchar(256), @region nvarchar(256), @city nvarchar(256), @age numeric(3), @gender char(1), @orderDate DATE, @clientId UNIQUEIDENTIFIER)
AS
BEGIN
	SET NOCOUNT ON;

	IF(NOT EXISTS
		(SELECT clientKey FROM [ProductFact].[Client]
		WHERE country = @country and region=@region and city=@city
		and  age = @age and gender = @gender and clientId = @clientId))
	BEGIN
		UPDATE [ProductFact].[Client]
			SET toDate=dateadd(day, -1, @orderDate)
			WHERE clientId = @clientId and toDate IS NULL;

		INSERT [ProductFact].[Client] (country, region, city, age, gender, fromDate, toDate, clientDescription, clientId)
		VALUES (@country, @region, @city, @age, @gender, @orderDate, NULL, concat(@country, ',', @region, ',', @city, ',', @age, ', ' , @gender, ',', @orderDate), @clientId)
	END;
END;
GO

-- ETL
BEGIN
	SET NOCOUNT ON;

	-- Time 
	INSERT INTO [ProductFact].[time]
		(year, quarter, month, dayOftheMonth, dayOfTheWeek,dateValue)
		(SELECT	DISTINCT datepart(year, orderDate), datepart(quarter, orderDate)
				, datepart(month, orderDate), datepart(day, orderDate)
				, datepart(weekday, orderDate), cast(orderDate AS DATE)
		FROM	[oltp].[order]);

	-- Store
		INSERT INTO [ProductFact].[store]
		(country, region, city, storeType, storeDescription, storeId)
		(SELECT	DISTINCT cnt.countryName
						, reg.regionName
						, cty.cityName
						, sto.storeName
						, concat(cnt.countryName, ',', reg.regionName, ',', cty.cityName,',', sto.storeName)
						, sto.storeId
		FROM	[oltp].[store] sto 
				JOIN 
				[oltp].[city] cty 
				ON (sto.cityId=cty.cityId)
				JOIN 
				[oltp].[region] reg
				ON (cty.regionId=reg.regionId)
				JOIN 
				[oltp].[country] cnt
				ON (reg.countryId=cnt.countryId));

	-- Product
	INSERT INTO [ProductFact].[Product]
				(FamilyName, departmentName, categoryName, subcatogoryName, brandName, SKU, productID, productDescription)
				(SELECT DISTINCT prdFam.familyName
								, prdDep.departmentName
								, prdCat.categoryName
								, prdSub.subcategoryName
								, brn.brandName
								, prd.sku
								, prd.productId
								, prdFam.familyName + ', ' + prdDep.departmentName + ', ' + prdCat.categoryName + ', ' + prdSub.subcategoryName + ', ' + brn.brandName

				FROM	[oltp].[product] prd
						JOIN
						[oltp].[productSubcategory] prdSub
						ON (prd.subcategoryId = prdSub.subcategoryId)
						JOIN
						[oltp].[productCategory] prdCat
						ON (prdSub.categoryId = prdCat.categoryId)
						JOIN
						[oltp].[productDepartment] prdDep
						ON (prdCat.departmentId = prdDep.departmentId)
						JOIN
						[oltp].[brand] brn
						ON (prd.brandId = brn.brandId)
						JOIN
						[oltp].[productFamily] prdFam
						ON (prdDep.familyId = prdFam.familyId))
	
	-- Client
	declare db_cursor cursor for
		SELECT	cty.countryName
				, reg.regionName
				, city.cityName
				, DATEDIFF(YEAR, client.dateOfBirth, ord.orderDate)
				, client.gender
				, CAST(ord.orderDate AS date)
				, client.clientId

		FROM	[oltp].[client] client
				JOIN
				[oltp].[city] city
				ON (client.cityId = city.cityId)
				JOIN
				[oltp].[region] reg
				ON (city.regionId = reg.regionId)
				JOIN
				[oltp].[country] cty
				ON (reg.countryId = cty.countryId)
				JOIN
				[oltp].[order] ord
				ON (client.clientId = ord.clientId)
		ORDER BY ord.orderDate;

	declare @countryName nvarchar(256);
	declare @regionName nvarchar(256);
	declare @cityName nvarchar(256);
	declare @age numeric(3);
	declare @gender char(1);
	declare @orderDate date;
	declare @clientId UNIQUEIDENTIFIER;

	open db_cursor
		fetch next from db_cursor INTO @countryName, @regionName, @cityName, @age, @gender, @orderDate, @clientId;

	while @@fetch_status=0
	begin
		exec [ProductFact].[sp_registerClient] @countryName, @regionName, @cityName, @age, @gender, @orderDate, @clientId;

		fetch next from db_cursor INTO @countryName, @regionName, @cityName, @age, @gender, @orderDate, @clientId;
	end;

	close db_cursor;
	deallocate db_cursor;

	-- Orders Fact
		INSERT INTO [ProductFact].[OrderFact]
				(storeCost, storePrice, unitsSold, orderId, ProductID, storeKey, dateKey, productKey, clientKey)
				(SELECT	OrdItem.storeCost
						, ordItem.storePrice
						, ordItem.unitsSold
						, ord.orderId
						, prd.productId
						, (SELECT storeKey FROM [ProductFact].[Store] WHERE storeID = ord.storeId)
						, (SELECT dateKey FROM [ProductFact].[time] WHERE dateValue=cast(ord.orderDate as date))
						, (SELECT productKey FROM [ProductFact].[Product] WHERE productId = ordItem.productId)
						, (SELECT clientKey FROM [ProductFact].[Client] WHERE clientId = ord.clientId	AND ((toDate IS NULL AND ord.orderDate >= fromDate) 
																									OR (toDate IS NOT NULL AND ord.orderDate between fromDate AND toDate)))
								
				FROM	[oltp].[order] ord
						JOIN
						[oltp].[orderItem] orditem
						ON (orditem.orderId = ord.orderId)
						JOIN
						[oltp].[product] prd
						ON (orditem.productId = prd.productId))

				END;
				GO

-- SELECT STATEMENTS TO CHECK INSERTS FROM ETL
SELECT * FROM [ProductFact].[Product];
SELECT * FROM [ProductFact].[Store];
SELECT * FROM [ProductFact].[time];
SELECT * FROM [ProductFact].[Client];
SELECT * FROM [ProductFact].[OrderFact];
GO

-- Query to count total amount of all products for analysis in excel.
SELECT	clt.age
		, clt.gender
		, clt.country
		, tim.year
		, prd.categoryName
		
FROM	[ProductFact].[Client] clt
		JOIN
		[ProductFact].[OrderFact] OrdFac
		ON (OrdFac.clientKey = clt.clientKey)
		JOIN
		[ProductFact].[time] tim
		ON (OrdFac.dateKey = tim.DATEKey)
		JOIN
		[ProductFact].[Product] prd
		ON (OrdFac.productKey = prd.productKey)
WHERE	prd.categoryName = 'Candles'

SELECT	TOP 15 prd.FamilyName
		, prd.departmentName
		, prd.categoryName
		, prd.subcatogoryName
		, prd.productDescription
		, prd.brandName
FROM	[ProductFact].[OrderFact] OrdFac
		JOIN
		[ProductFact].[Product] Prd
		ON (OrdFac.productKey = prd.productKey)