INSERT INTO pings_universe.polling_locations_2022
SELECT *
FROM pings_universe.polling_locations_staging AS staging
WHERE NOT EXISTS (
    SELECT 1
    FROM pings_universe.polling_locations_2022 AS target
    WHERE target.spatially_distinct_geohash_key = staging.spatially_distinct_geohash_key
        AND target.location_type = staging.location_type
);