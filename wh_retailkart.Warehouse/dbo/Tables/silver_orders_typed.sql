CREATE TABLE [dbo].[silver_orders_typed] (
    [order_id]    INT             NULL,
    [customer_id] INT             NULL,
    [city]        VARCHAR (8000)  NULL,
    [amount]      DECIMAL (12, 2) NULL,
    [order_date]  DATE            NULL,
    [category]    VARCHAR (8000)  NULL
);


GO

CREATE STATISTICS [st_city]
    ON [dbo].[silver_orders_typed]([city]);


GO