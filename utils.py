""" A simple place to put simple utility functions """
import sys
import os
import argparse
import re
import pygeohash as pgh


from geocode_constants import (
    ADDRESS_COMPONENTS,
    BOUNDS,
    CITY_COMPONENT,
    COUNTY_COMPONENT,
    FORMATTED_ADDRESS,
    GEOMETRY,
    LAT,
    LNG,
    LOCATION,
    LONG_NAME,
    NORTHEAST,
    SHORT_NAME,
    SOUTHWEST,
    SPATIALLY_DISTINCT_GEOHASH_GEOID_TRUNCATE_LEN,
    STATE_COMPONENT,
    TYPES,
)


from dw_constants import (
    ADDRESS_CITY,
    ADDRESS_FULL,
    ADDRESS_LINE,
    ADDRESS_STATE,
    ADDRESS_ZIP,
    CITY,
    COUNTY,
    GOOGLE_ADDRESS,
    IS_DROP_OFF,
    IS_EARLY_VOTING,
    LATITUDE,
    LONGITUDE,
    NE_LATITUDE,
    NE_LONGITUDE,
    STATE,
    STATE_LONG,
    SW_LATITUDE,
    SW_LONGITUDE,
)


def dir_path(path):
    if not os.path.isdir(path):
        raise argparse.ArgumentTypeError(f"readable_dir:{path} is not a valid path")
    return path


def is_blank(my_string):
    return not (my_string and my_string.strip())


def fail(msg: str, error_code: int = 1):
    """Display a message and quit with an error code"""
    print(msg)
    sys.exit(error_code)

def parse_state(address: str) -> str:
    if not address:
        return None

    matches = re.search(r"(?i)(\w\w),? *\d{5}(-\d{4})? *(,? *USA)? *$", address)
    if not matches:
        return None

    return matches.group(1)


def is_valid_address_columns(row: dict) -> bool:
    address_line: str = row[ADDRESS_LINE]
    address_city: str = row[ADDRESS_CITY]
    address_state: str = row[ADDRESS_STATE]
    address_zip: str = row[ADDRESS_ZIP]

    if is_blank(address_line) or is_blank(address_city) or is_blank(address_state) or is_blank(address_zip):
        return False

    if not re.search(r"\w{3}", address_line):
        return False

    if not re.search(r"\w{3}", address_city):
        return False

    if not re.match(r"^\w{2}", address_state):
        return False

    if not re.match(r"^\d{5}", address_zip):
        return False

    return True


def get_geocode_address(row: dict) -> str:
    if not is_valid_address_columns(row):
        return row[ADDRESS_FULL]

    address_line: str = row[ADDRESS_LINE].strip()
    address_city: str = row[ADDRESS_CITY].strip()
    address_state: str = row[ADDRESS_STATE].strip()
    address_zip: str = row[ADDRESS_ZIP].strip()

    return f'{address_line}, {address_city} {address_state} {address_zip}'


## Google geocode related parsing

def get_county(address_components: list) -> str:
    county_item = next(
        item for item in address_components if COUNTY_COMPONENT in item[TYPES]
    )
    county = county_item[LONG_NAME]
    return county.upper().removesuffix(" COUNTY")


def get_city(address_components: list) -> str:
    county_item = next(
        item for item in address_components if CITY_COMPONENT in item[TYPES]
    )
    county = county_item[LONG_NAME]
    return county.upper()

def get_state(address_components: list) -> str:
    county_item = next(
        item for item in address_components if STATE_COMPONENT in item[TYPES]
    )
    county = county_item[SHORT_NAME]
    return county.upper()

def get_state_long(address_components: list) -> str:
    county_item = next(
        item for item in address_components if STATE_COMPONENT in item[TYPES]
    )
    county = county_item[LONG_NAME]
    return county.upper()

def get_address_components(geocode_result_0: dict) -> dict:
    return geocode_result_0[ADDRESS_COMPONENTS]


def parse_geocode(geocode_result: list) -> dict:
    geocode_result_0 = geocode_result[0]
    # print(json.dumps(geocode_result_0))

    geometry = geocode_result_0[GEOMETRY]
    location = geometry[LOCATION]
    lat = location[LAT]
    lng = location[LNG]

    # Google does not always return bounds. Running multiple queries on the same
    # address will return diffrent results
    bounds = geometry.get(BOUNDS)
    if bounds:
        bounds = geometry[BOUNDS]
        ne_bounds = bounds[NORTHEAST]
        ne_lat = ne_bounds[LAT]
        ne_lng = ne_bounds[LNG]

        sw_bounds = bounds[SOUTHWEST]
        sw_lat = sw_bounds[LAT]
        sw_lng = sw_bounds[LNG]
    else:
        ne_lat = ne_lng = sw_lat = sw_lng = None

    address_components = get_address_components(geocode_result_0)

    try:
        county = get_county(address_components)
    except StopIteration:
        return {"error": f"Missing county from Google {geocode_result}"}

    try:
        city = get_city(address_components)
    except StopIteration:
        return {"error": f"Missing city from Google {geocode_result}"}

    try:
        state = get_state(address_components)
    except StopIteration:
        return {"error": f"Missing state from Google {geocode_result}"}

    try:
        state_long = get_state_long(address_components)
    except StopIteration:
        return {"error": f"Missing state long from Google {geocode_result}"}


    google_address = geocode_result_0[FORMATTED_ADDRESS]

    return {
        COUNTY: county,
        CITY: city,
        STATE_LONG: state_long,
        STATE: state,
        GOOGLE_ADDRESS: google_address,
        LATITUDE: lat,
        LONGITUDE: lng,
        NE_LATITUDE: ne_lat,
        NE_LONGITUDE: ne_lng,
        SW_LATITUDE: sw_lat,
        SW_LONGITUDE: sw_lng,
    }


def create_spatially_distinct_geohash_key(
    latitude: float,
    longitude: float,
    is_early_vote: bool,
) -> str:
    '''
        Create a spatially_distinct_geohash_key.  This is needed to flag duplicates in the csv
        before it is loaded in the sql.  Truncation was removed.

        This was formerly the following sql:
            substr(geog_geohash, 1, 7) || iff(
                location_type = 'early_vote',
                '_early',
                '_general'
            ) as spatially_distinct_geohash_key,

    '''
    geoid = pgh.encode(
        longitude=longitude,
        latitude=latitude,
        precision=SPATIALLY_DISTINCT_GEOHASH_GEOID_TRUNCATE_LEN,
    )

    if is_early_vote:
        return f'{geoid}_early'
    return f'{geoid}_general'


def parse_is_early_vote(row: dict) -> bool:
    return str(row[IS_EARLY_VOTING]).lower() == "true"


def parse_is_drop_off(row: dict) -> bool:
    return str(row[IS_DROP_OFF]).lower() == "true"
