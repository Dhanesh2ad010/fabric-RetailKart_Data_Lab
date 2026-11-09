CREATE FUNCTION sec.fn_city_filter(@city AS VARCHAR(50))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS allowed
    FROM dbo.city_manager AS m
    WHERE m.manager_email = USER_NAME()
      AND m.city = @city;

GO