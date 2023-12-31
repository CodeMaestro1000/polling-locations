create table IF NOT EXISTS {POLLING_LOCATIONS_TABLE} (
	ELECTION_DATE TIMESTAMP,
	STATE VARCHAR,
	COUNTY_NAME VARCHAR,
	ADDRESS VARCHAR,
	PRECINCT_NAME VARCHAR(65535),
	LOCATION_TYPE VARCHAR,
	NAME VARCHAR,
	SOURCE VARCHAR,
	GEOG GEOGRAPHY,
	GEOG_GEOHASH VARCHAR,
	CND_POLLING_LOCATION_SOURCE VARCHAR(14),
	CND_POLL_UUID VARCHAR(32),
	SPATIALLY_DISTINCT_GEOHASH_KEY VARCHAR,
	ANY_LONGITUDE FLOAT4,
	ANY_LATITUDE FLOAT4,
    SOURCE_FILE VARCHAR,
    TIME_ZONE_TZ VARCHAR,
    STATE_ID VARCHAR,
	COUNTY_FIPS VARCHAR,
	BUILDING_SHAPE GEOMETRY
)