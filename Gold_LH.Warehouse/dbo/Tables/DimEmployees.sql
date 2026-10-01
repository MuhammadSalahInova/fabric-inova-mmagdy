CREATE TABLE [dbo].[DimEmployees] (
    [EmployeeKey]     BIGINT         NULL,
    [EmployeeID]      BIGINT         NULL,
    [LastName]        VARCHAR (8000) NULL,
    [FirstName]       VARCHAR (8000) NULL,
    [Title]           VARCHAR (8000) NULL,
    [TitleOfCourtesy] VARCHAR (8000) NULL,
    [BirthDate]       DATETIME2 (6)  NULL,
    [HireDate]        DATETIME2 (6)  NULL,
    [Address]         VARCHAR (8000) NULL,
    [City]            VARCHAR (8000) NULL,
    [Region]          VARCHAR (8000) NULL,
    [PostalCode]      VARCHAR (8000) NULL,
    [Country]         VARCHAR (8000) NULL,
    [HomePhone]       VARCHAR (8000) NULL,
    [Extension]       VARCHAR (8000) NULL,
    [Photo]           VARCHAR (8000) NULL,
    [Notes]           VARCHAR (8000) NULL,
    [ReportsTo]       BIGINT         NULL,
    [PhotoPath]       VARCHAR (8000) NULL,
    [FullName]        VARCHAR (200)  NULL
);


GO