CREATE OR REPLACE VIEW data_projects.gold.fact_trips_kochi
AS (
SELECT *
FROM data_projects.gold.fact_trips
WHERE city_id = 'KL01'
);