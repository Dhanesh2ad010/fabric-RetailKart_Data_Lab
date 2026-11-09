CREATE SECURITY POLICY [sec].[city_policy]
    ADD FILTER PREDICATE [sec].[fn_city_filter]([city]) ON [dbo].[fact_sales]
    WITH (STATE = OFF);


GO