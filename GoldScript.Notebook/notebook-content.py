# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

-- =========================================================================
-- Gold Star Schema Build Script v6
-- Optimized for Microsoft Fabric / Synapse Data Warehouse
-- =============================================================================
-- ---- DimCustomers ----
DROP TABLE IF EXISTS dbo.DimCustomers;
CREATE TABLE dbo.DimCustomers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CustomerID) AS CustomerKey,
    CustomerID, 
    CompanyName, 
    ContactName, 
    ContactTitle,
    Address, 
    City, 
    Region, 
    PostalCode, 
    Country, 
    Phone, 
    Fax
FROM Silver_LH.dbo.silver_customers
WHERE is_current = 1;
-- ---- DimEmployees ----
DROP TABLE IF EXISTS dbo.DimEmployees;
CREATE TABLE dbo.DimEmployees AS
SELECT
    ROW_NUMBER() OVER (ORDER BY EmployeeID) AS EmployeeKey,
    EmployeeID,
    LastName,
    FirstName,
    Title,
    TitleOfCourtesy,
    BirthDate,
    HireDate,
    Address,
    City,
    Region,
    PostalCode,
    Country,
    HomePhone,
    Extension,
    Photo,
    Notes,
    ReportsTo,
    PhotoPath
FROM Silver_LH.dbo.silver_employees;
-- ---- DimCategories ----
DROP TABLE IF EXISTS dbo.DimCategories;
DECLARE @MinValidCategoryID INT = 1;
DECLARE @MaxValidCategoryID INT = 8;
CREATE TABLE dbo.DimCategories AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CategoryID) AS CategoryKey,
    CategoryID,
    CategoryName,
    Description,
    Picture
FROM Silver_LH.dbo.silver_categories
WHERE CategoryID >= @MinValidCategoryID
  AND CategoryID <= @MaxValidCategoryID;
DECLARE @OthersCategoryKey INT = (SELECT ISNULL(MAX(CategoryKey), 0) + 1 FROM dbo.DimCategories);
DECLARE @OthersCategoryID INT = -1;
INSERT INTO dbo.DimCategories (CategoryKey, CategoryID, CategoryName)
VALUES (@OthersCategoryKey, @OthersCategoryID, 'Others');
-- ---- DimSuppliers ----
DROP TABLE IF EXISTS dbo.DimSuppliers;
CREATE TABLE dbo.DimSuppliers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY SupplierID) AS SupplierKey,
    SupplierID,
    CompanyName,
    ContactName,
    ContactTitle,
    Address,
    City,
    Region,
    PostalCode,
    Country,
    Phone,
    Fax,
    HomePage
FROM Silver_LH.dbo.silver_suppliers;
-- ---- DimShippers ----
DROP TABLE IF EXISTS dbo.DimShippers;
CREATE TABLE dbo.DimShippers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ShipperID) AS ShipperKey,
    ShipperID,
    CompanyName,
    Phone
FROM Silver_LH.dbo.silver_shippers;
-- ---- DimRegions ----
DROP TABLE IF EXISTS dbo.DimRegions;
CREATE TABLE dbo.DimRegions AS
SELECT
    ROW_NUMBER() OVER (ORDER BY RegionID) AS RegionKey,
    RegionID,
    RegionDescription
FROM Silver_LH.dbo.silver_regions;
-- ---- DimTerritories ----
DROP TABLE IF EXISTS dbo.DimTerritories;
CREATE TABLE dbo.DimTerritories AS
SELECT
    ROW_NUMBER() OVER (ORDER BY TerritoryID) AS TerritoryKey,
    TerritoryID,
    TerritoryDescription,
    RegionID
FROM Silver_LH.dbo.silver_territories;
-- ---- DimProducts ----
DROP TABLE IF EXISTS dbo.DimProducts;
CREATE TABLE dbo.DimProducts AS
SELECT
    ROW_NUMBER() OVER (ORDER BY p.ProductID) AS ProductKey,
    p.ProductID,
    p.ProductName,
    p.SupplierID,
    p.CategoryID,
    p.QuantityPerUnit,
    p.UnitPrice,
    p.UnitsInStock,
    p.UnitsOnOrder,
    p.ReorderLevel,
    p.Discontinued,
    CASE
        WHEN p.CategoryID IS NULL THEN @OthersCategoryKey
        WHEN p.CategoryID < @MinValidCategoryID THEN @OthersCategoryKey
        WHEN p.CategoryID > @MaxValidCategoryID THEN @OthersCategoryKey
        ELSE c.CategoryKey
    END AS CategoryKey,
    s.SupplierKey
FROM Silver_LH.dbo.silver_products AS p
LEFT JOIN dbo.DimCategories AS c
    ON p.CategoryID = c.CategoryID
    AND p.CategoryID BETWEEN @MinValidCategoryID AND @MaxValidCategoryID
LEFT JOIN dbo.DimSuppliers AS s 
    ON p.SupplierID = s.SupplierID
WHERE p.is_current = 1;
-- ---- DimDate (covers OrderDate, ShippedDate, and RequiredDate) ----
DROP TABLE IF EXISTS dbo.DimDate;
DECLARE @MinDate DATE,
        @MaxDate DATE;
SELECT
    @MinDate =
        DATEADD(
            DAY,
            -7,
            MIN(MinDate)
        ),
    @MaxDate =
        DATEADD(
            DAY,
            7,
            MAX(MaxDate)
        )
FROM
(
    SELECT
        MIN(TRY_CONVERT(DATE, OrderDate, 120)) AS MinDate,
        MAX(TRY_CONVERT(DATE, OrderDate, 120)) AS MaxDate
    FROM Silver_LH.dbo.silver_orders
    WHERE is_current = 1
    UNION ALL
    SELECT
        MIN(TRY_CONVERT(DATE, ShippedDate, 120)) AS MinDate,
        MAX(TRY_CONVERT(DATE, ShippedDate, 120)) AS MaxDate
    FROM Silver_LH.dbo.silver_orders
    WHERE is_current = 1
    UNION ALL
    SELECT
        MIN(TRY_CONVERT(DATE, RequiredDate, 120)) AS MinDate,
        MAX(TRY_CONVERT(DATE, RequiredDate, 120)) AS MaxDate
    FROM Silver_LH.dbo.silver_orders
    WHERE is_current = 1
) AS DateRanges;
CREATE TABLE dbo.DimDate AS
WITH E1(N) AS (SELECT 1 FROM (VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1)) AS X(N)),
     E2(N) AS (SELECT 1 FROM E1 a CROSS JOIN E1 b),
     E4(N) AS (SELECT 1 FROM E2 a CROSS JOIN E2 b),
     E6(N) AS (SELECT 1 FROM E4 a CROSS JOIN E2 b),
     Tally(N) AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM E6)
SELECT TOP (DATEDIFF(day, @MinDate, @MaxDate) + 1)
    CAST(DATEADD(day, N - 1, @MinDate) AS DATE) AS FullDate,
    CAST(FORMAT(DATEADD(day, N - 1, @MinDate), 'yyyyMMdd') AS INT) AS DateKey,
    YEAR(DATEADD(day, N - 1, @MinDate)) AS [Year],
    DATEPART(quarter, DATEADD(day, N - 1, @MinDate)) AS [Quarter],
    MONTH(DATEADD(day, N - 1, @MinDate)) AS [Month],
    CAST(DATENAME(month, DATEADD(day, N - 1, @MinDate)) AS VARCHAR(20)) AS MonthName,
    DAY(DATEADD(day, N - 1, @MinDate)) AS [Day],
    CAST(DATENAME(weekday, DATEADD(day, N - 1, @MinDate)) AS VARCHAR(20)) AS DayName,
    DATEPART(week, DATEADD(day, N - 1, @MinDate)) AS WeekOfYear
FROM Tally;
-- ============================================================================
-- SALES FACT TABLE
-- Grain: One row per Order Detail
-- 
-- Date roles:
--   OrderDateKey    -> DimDate (ACTIVE relationship)
--   ShippedDateKey  -> DimDate (INACTIVE relationship)
--   RequiredDateKey -> DimDate (INACTIVE relationship)
-- ============================================================================
DROP TABLE IF EXISTS dbo.Sales;
CREATE TABLE dbo.Sales AS
SELECT
    -- ============================================================
    -- Business Keys
    -- ============================================================
    od.OrderID,
    od.ProductID,
    -- ============================================================
    -- Dimension Surrogate Keys
    -- ============================================================
    cu.CustomerKey,
    em.EmployeeKey,
    pr.ProductKey,
    sh.ShipperKey,
    -- ============================================================
    -- DATE KEYS (Join to DimDate)
    -- ============================================================
    dtOrder.DateKey AS OrderDateKey,
    dtShip.DateKey AS ShippedDateKey,
    dtRequired.DateKey AS RequiredDateKey,
    -- ============================================================
    -- ORIGINAL DATES (For debugging and calculations)
    -- ============================================================
    TRY_CONVERT(DATE, o.OrderDate, 120) AS OrderDate,
    TRY_CONVERT(DATE, o.ShippedDate, 120) AS ShippedDate,
    TRY_CONVERT(DATE, o.RequiredDate, 120) AS RequiredDate,
    -- ============================================================
    -- ORDER DETAIL MEASURES
    -- ============================================================
    od.Quantity,
    od.UnitPrice,
    od.Discount,
    CAST(od.Quantity * od.UnitPrice * (1 - od.Discount) AS DECIMAL(18,2)) AS LineTotal,
    -- ============================================================
    -- SHIPPING INFORMATION
    -- ============================================================
    o.Freight,
    o.ShipCountry,
    o.ShipRegion,
    -- ============================================================
    -- OPERATIONAL METRICS
    -- ============================================================
    
    -- Delivery Days (Order to Shipment)
    CASE
        WHEN TRY_CONVERT(DATE, o.OrderDate, 120) IS NOT NULL
         AND TRY_CONVERT(DATE, o.ShippedDate, 120) IS NOT NULL
        THEN DATEDIFF(
            DAY,
            TRY_CONVERT(DATE, o.OrderDate, 120),
            TRY_CONVERT(DATE, o.ShippedDate, 120)
        )
        ELSE NULL
    END AS DeliveryDays,
    -- On-Time Flag: 1 = On Time, 0 = Late, NULL = Unshipped/Missing Date
    CASE
        WHEN TRY_CONVERT(DATE, o.ShippedDate, 120) IS NULL
          OR TRY_CONVERT(DATE, o.RequiredDate, 120) IS NULL
        THEN NULL
        WHEN TRY_CONVERT(DATE, o.ShippedDate, 120) <= TRY_CONVERT(DATE, o.RequiredDate, 120)
        THEN 1
        ELSE 0
    END AS OnTimeFlag
FROM Silver_LH.dbo.silver_order_details AS od
-- ============================================================
-- JOIN TO ORDERS
-- ============================================================
LEFT JOIN Silver_LH.dbo.silver_orders AS o
    ON od.OrderID = o.OrderID
    AND o.is_current = 1
-- ============================================================
-- JOIN TO DIMENSIONS
-- ============================================================
LEFT JOIN dbo.DimCustomers AS cu
    ON o.CustomerID = cu.CustomerID
LEFT JOIN dbo.DimEmployees AS em
    ON o.EmployeeID = em.EmployeeID
LEFT JOIN dbo.DimProducts AS pr
    ON od.ProductID = pr.ProductID
LEFT JOIN dbo.DimShippers AS sh
    ON o.ShipVia = sh.ShipperID
-- ============================================================
-- JOIN TO DATE DIMENSION (3 separate joins for 3 date roles)
-- ============================================================
LEFT JOIN dbo.DimDate AS dtOrder
    ON TRY_CONVERT(DATE, o.OrderDate, 120) = dtOrder.FullDate
LEFT JOIN dbo.DimDate AS dtShip
    ON TRY_CONVERT(DATE, o.ShippedDate, 120) = dtShip.FullDate
LEFT JOIN dbo.DimDate AS dtRequired
    ON TRY_CONVERT(DATE, o.RequiredDate, 120) = dtRequired.FullDate
WHERE od.is_current = 1;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
