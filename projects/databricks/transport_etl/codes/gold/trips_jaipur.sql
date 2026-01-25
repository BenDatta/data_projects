CREATE OR REPLACE VIEW data_projects.gold.fact_trips_jaipur
AS (
SELECT *
FROM data_projects.gold.fact_trips
WHERE city_id = 'RJ01'
);