CREATE TABLE [dbo].[DimDate] (
    [FullDate]   DATE         NULL,
    [DateKey]    INT          NULL,
    [Year]       INT          NULL,
    [Quarter]    INT          NULL,
    [Month]      INT          NULL,
    [MonthName]  VARCHAR (20) NULL,
    [Day]        INT          NULL,
    [DayName]    VARCHAR (20) NULL,
    [WeekOfYear] INT          NULL,
    [Era]        VARCHAR (30) NULL
);


GO