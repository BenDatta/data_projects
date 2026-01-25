CREATE OR REPLACE VIEW data_projects.gold.fact_trips_surat
AS (
SELECT *
FROM data_projects.gold.fact_trips
WHERE city_id = 'GJ01'
);