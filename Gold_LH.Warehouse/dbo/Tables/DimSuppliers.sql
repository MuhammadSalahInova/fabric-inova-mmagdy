CREATE TABLE [dbo].[DimSuppliers] (
    [SupplierKey]  BIGINT         NULL,
    [SupplierID]   BIGINT         NULL,
    [CompanyName]  VARCHAR (8000) NULL,
    [ContactName]  VARCHAR (8000) NULL,
    [ContactTitle] VARCHAR (8000) NULL,
    [Address]      VARCHAR (8000) NULL,
    [City]         VARCHAR (8000) NULL,
    [Region]       VARCHAR (8000) NULL,
    [PostalCode]   VARCHAR (8000) NULL,
    [Country]      VARCHAR (8000) NULL,
    [Phone]        VARCHAR (8000) NULL,
    [Fax]          VARCHAR (8000) NULL,
    [HomePage]     VARCHAR (8000) NULL
);


GO