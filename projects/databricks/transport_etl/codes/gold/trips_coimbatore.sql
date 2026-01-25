CREATE OR REPLACE VIEW data_projects.gold.fact_trips_coimbatore
AS (
SELECT *
FROM data_projects.gold.fact_trips
WHERE city_id = 'TN01'
);