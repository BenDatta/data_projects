CREATE OR REPLACE VIEW data_projects.gold.fact_trips_lucknow
AS (
SELECT *
FROM data_projects.gold.fact_trips
WHERE city_id = 'UP01'
);