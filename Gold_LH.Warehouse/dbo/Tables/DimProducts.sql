CREATE TABLE [dbo].[DimProducts] (
    [ProductKey]      BIGINT         NULL,
    [ProductID]       BIGINT         NULL,
    [ProductName]     VARCHAR (8000) NULL,
    [SupplierID]      BIGINT         NULL,
    [CategoryID]      BIGINT         NULL,
    [QuantityPerUnit] VARCHAR (8000) NULL,
    [UnitPrice]       FLOAT (53)     NULL,
    [UnitsInStock]    BIGINT         NULL,
    [UnitsOnOrder]    BIGINT         NULL,
    [ReorderLevel]    BIGINT         NULL,
    [Discontinued]    BIT            NULL,
    [CategoryKey]     BIGINT         NULL,
    [SupplierKey]     BIGINT         NULL
);


GO