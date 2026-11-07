CREATE VIEW dbo.vw_city_count AS
SELECT 
    [city_name], 
    COUNT(*) AS total_count
FROM 
    [dbo].[dim_city]
GROUP BY 
    [city_name];

GO