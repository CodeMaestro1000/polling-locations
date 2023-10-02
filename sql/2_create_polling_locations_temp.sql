create table if not exists pings_universe.polling_locations_temp (
    COUNTY varchar,
    NAME varchar,
    ADDRESS varchar,
    CITY varchar,
    STATE varchar,
    GOOGLE_ADDRESS varchar,
    LATITUDE float4,
    LONGITUDE float4,
    SOURCE varchar,
    PRECINCT_NAME varchar(65535),
    LOCATION_TYPE varchar,
    SOURCE_FILE VARCHAR,
    SOURCE_FILE_ELECTION_DATE DATE
);

TRUNCATE TABLE pings_universe.polling_locations_temp;
-- precint name should hold 65535 chars to prevent any issues with loading or any data truncation
