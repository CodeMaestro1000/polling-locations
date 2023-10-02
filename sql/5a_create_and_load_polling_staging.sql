CREATE TABLE pings_universe.polling_locations_staging as 
with polling_locations_staging_v1 as (
    with polling_locations_temp as (
        select
            polls.SOURCE_FILE_ELECTION_DATE AS ELECTION_DATE,
            polls.STATE,
            polls.COUNTY as COUNTY_NAME,
            polls.GOOGLE_ADDRESS as ADDRESS,
            polls.PRECINCT_NAME,
            polls.LOCATION_TYPE,
            polls.NAME,
            polls.SOURCE as SOURCE,
            ST_MAKEPOINT(LONGITUDE, polls.LATITUDE)::GEOGRAPHY as GEOG,
            ST_GEOHASH(GEOG::GEOMETRY) as GEOG_GEOHASH,
            polls.SOURCE as CND_POLLING_LOCATION_SOURCE,
            md5(
                'X' -- coalesce(CPI_TABLE_NAME,'X')
                || coalesce(ELECTION_DATE, '1900-01-01')
                || coalesce(polls.STATE, 'X')
                || coalesce(COUNTY_NAME, 'X')
                || 'X' -- coalesce(JURISDICTION, 'X')
                || coalesce(polls.ADDRESS, 'X')
                || 'X' -- coalesce(JURISDICTION_TYPE, 'X')
                || coalesce(polls.PRECINCT_NAME, 'X')
                || 'X' -- coalesce(PRECINCT_ID, 'X')
                || 'X' -- || coalesce(POLLING_PLACE_ID, 'X')
                || coalesce(polls.LOCATION_TYPE, 'X')
                || coalesce(polls.NAME, 'X')
                || 'X' -- || coalesce(NOTES, 'X')
                || coalesce(SOURCE, 'X')
                || '1900-01-01' -- || coalesce(SOURCE_DATE, '1900-01-01')
                || 'X' -- || coalesce(SOURCE_NOTES, 'X')
                || coalesce(st_x(GEOG::GEOMETRY), 0)
                || coalesce(st_y(GEOG::GEOMETRY), 0)
                || 0 -- coalesce(st_x(GOOGLE_GEOG), 0)
                || 0 -- coalesce(st_y(GOOGLE_GEOG), 0)
                || 0 -- coalesce(st_x(HERE_GEOG), 0)
                || 0 -- coalesce(st_y(HERE_GEOG), 0)
                || 0 -- coalesce(st_x(MAPQUEST_GEOG), 0)
                || 0 -- coalesce(st_y(MAPQUEST_GEOG), 0)
                || 0 -- coalesce(st_x(AZUREMAPS_GEOG), 0)
                || 0 -- coalesce(st_y(AZUREMAPS_GEOG), 0)
                || 'X' -- coalesce(GOOGLE_GEOG_GEOHASH, 'X')
                || coalesce(GEOG_GEOHASH, 'X')
                || coalesce(polls.SOURCE, 'X')
            ) as CND_POLL_UUID,
            substring(geog_geohash, 1, 20) || (CASE WHEN location_type = 'early_vote' THEN '_early' ELSE '_general' END) as spatially_distinct_geohash_key,
            polls.LONGITUDE as ANY_LONGITUDE,
            polls.LATITUDE as ANY_LATITUDE,
            polls.SOURCE_FILE
        from pings_universe.polling_locations_temp as polls
    )
     
    select
        polling_locations_temp.*,
        timezones.tzid as TIME_ZONE_TZ,
        fips.STATE_FIPS as STATE_ID,
        fips.fips_code as COUNTY_FIPS
    from polling_locations_temp 
    left join fips.CARTOGRAPHY2020 as fips 
        on ST_CONTAINS(
            fips.polygon::GEOMETRY,
            polling_locations_temp.GEOG::GEOMETRY
        )
    left join fips.TIMEZONE_POLYGONS as timezones 
        on ST_CONTAINS(
            timezones.POLYGON::GEOMETRY,
            polling_locations_temp.GEOG::GEOMETRY
        )
)
SELECT * FROM polling_locations_staging_v1