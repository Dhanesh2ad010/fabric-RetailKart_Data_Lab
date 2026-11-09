CREATE TABLE [dbo].[silver_orders_clustered] (
    [order_id]    INT             NULL,
    [customer_id] INT             NULL,
    [city]        VARCHAR (8000)  NULL,
    [amount]      DECIMAL (12, 2) NULL,
    [order_date]  DATE            NULL,
    [category]    VARCHAR (8000)  NULL
)
WITH (CLUSTER BY ([city]));


GO