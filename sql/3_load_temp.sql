insert into {POLLING_LOCATIONS_TEMP_TABLE} (
    COUNTY,
    NAME,
    ADDRESS,
    CITY,
    STATE,
    GOOGLE_ADDRESS,
    LATITUDE,
    LONGITUDE,
    SOURCE,
    PRECINCT_NAME,
    LOCATION_TYPE,
    SOURCE_FILE,
    SOURCE_FILE_ELECTION_DATE
) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
