CREATE TABLE [dbo].[fact_sales] (
    [order_id]   BIGINT                                                           NULL,
    [customer]   VARCHAR (8000) MASKED WITH (FUNCTION = 'partial(1, "XXXXX", 0)') NULL,
    [email]      VARCHAR (8000) MASKED WITH (FUNCTION = 'email()')                NULL,
    [city]       VARCHAR (8000)                                                   NULL,
    [amount]     FLOAT (53)                                                       NULL,
    [order_date] DATE                                                             NULL
);


GO