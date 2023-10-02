''' Constants related to google's geocoding api '''

LAT = 'lat'
LNG = 'lng'
NE_LAT = 'ne_lat'
NE_LNG = 'ne_lng'
SW_LAT = 'sw_lat'
SW_LNG = 'sw_lng'

CITY = 'City'

NORTHEAST = 'northeast'
SOUTHWEST = 'southwest'
LOCATION = 'location'
GEOMETRY = 'geometry'
BOUNDS = 'bounds'
ADDRESS_COMPONENTS = 'address_components'
CITY_COMPONENT = 'locality'
STATE_COMPONENT = 'administrative_area_level_1'
COUNTY_COMPONENT = 'administrative_area_level_2'

SHORT_NAME = 'short_name'
LONG_NAME = 'long_name'
FORMATTED_ADDRESS = 'formatted_address'

TYPES = 'types'

# This was updated to 20 from the original 7 so there would be no
# overlap on polling locations
SPATIALLY_DISTINCT_GEOHASH_GEOID_TRUNCATE_LEN = 20
