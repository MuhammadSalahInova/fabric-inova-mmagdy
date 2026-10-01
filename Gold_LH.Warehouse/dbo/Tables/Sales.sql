CREATE TABLE [dbo].[Sales] (
    [OrderID]         BIGINT          NULL,
    [ProductID]       BIGINT          NULL,
    [CustomerKey]     BIGINT          NULL,
    [EmployeeKey]     BIGINT          NULL,
    [ProductKey]      BIGINT          NULL,
    [ShipperKey]      BIGINT          NULL,
    [OrderDateKey]    INT             NULL,
    [ShippedDateKey]  INT             NULL,
    [RequiredDateKey] INT             NULL,
    [OrderDate]       DATE            NULL,
    [ShippedDate]     DATE            NULL,
    [RequiredDate]    DATE            NULL,
    [Quantity]        BIGINT          NULL,
    [UnitPrice]       FLOAT (53)      NULL,
    [Discount]        FLOAT (53)      NULL,
    [LineTotal]       DECIMAL (18, 2) NULL,
    [Freight]         FLOAT (53)      NULL,
    [ShipCountry]     VARCHAR (8000)  NULL,
    [ShipRegion]      VARCHAR (8000)  NULL,
    [DeliveryDays]    INT             NULL,
    [OnTimeFlag]      INT             NULL
);


GO