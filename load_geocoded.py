# %%
from collections import OrderedDict, defaultdict
import csv
from datetime import date
import json
from typing import Any
import psycopg2, os
from dw_constants import (
    ADDRESS_FULL,
    COUNTY,
    LOCATION_NAME,
    LATITUDE,
    LONGITUDE,
    MANUAL_COORD,
    MANUAL_ADDRESS,
    GOOGLE_ADDRESS,
    CITY,
    DROP_BOX,
    EARLY_VOTE,
    ELECTION_DAY,
    PRECINCT,
    STATE,
    IMPORT_SOURCE
)
from utils import parse_is_drop_off, parse_is_early_vote

# %%
DB_HOST = "chen-lite-cluster.c7a5adrls65x.us-west-2.redshift.amazonaws.com"
DB_PORT = "5439"
DATABASE_NAME = "dev"
DB_USER = "chen-admin"
DB_PASSWORD = "Ochuokpa1998"
ENCODING = "UTF-8" 
SCHEMA_NAME = "PINGS_UNIVERSE"
POLLING_LOCATIONS_TABLE = f"{SCHEMA_NAME}.POLLING_LOCATIONS_2022"
POLLING_LOCATIONS_TEMP_TABLE = f"{SCHEMA_NAME}.POLLING_LOCATIONS_TEMP"

# %%
def get_warehouse_config():
    return {
        'host': DB_HOST,
        'port': DB_PORT,
        'database': DATABASE_NAME,
        'user': DB_USER,
        'password': DB_PASSWORD
    }

# %%
def read_sql_template(filename: str) -> str:
    # sql_path = os.path.dirname(os.path.abspath(__file__)) + f"/sql/{filename}"
    sql_path = os.getcwd() + f"/sql/{filename}"
    with open(sql_path, encoding=ENCODING) as file:
        return file.read().format(
            POLLING_LOCATIONS_TABLE=POLLING_LOCATIONS_TABLE,
            POLLING_LOCATIONS_TEMP_TABLE=POLLING_LOCATIONS_TEMP_TABLE,
        )

# %%
def build_load_temp_rows(source_file: str, rows: csv.DictReader) -> tuple[list, list]:
    """
    Take a csv row from DW and convert it to the expected temp tables columns
    as needed by 04_load_temp.sql

    Note: this function will dedup items on GOOGLE_ADDRESS, and return a list
    of found duplicates.
    """

    duplicates = defaultdict(list)

    results = OrderedDict()
    source_file_election_date = source_file.split('_')[-2][:10]
    for line_num, row in enumerate(rows):
        is_drop_off = parse_is_drop_off(row)
        is_early_vote = parse_is_early_vote(row)
        if is_drop_off:
            location_type = DROP_BOX
        elif is_early_vote:
            location_type = EARLY_VOTE
        else:
            location_type = ELECTION_DAY

        county = row[COUNTY]
        name = row[LOCATION_NAME]
        address = row[ADDRESS_FULL]
        city = row[CITY]
        #For _manual.csv, there is a manually address
        #column that does not exists
        #in the aumatically geocoded version.
        #Check if these columns exist and preferentially use these values
        if MANUAL_ADDRESS in row and row[MANUAL_ADDRESS] != '':
            google_address = row[MANUAL_ADDRESS]
        else:
            google_address = row[GOOGLE_ADDRESS]
        state = row[STATE]
        try:
            #For _manual.csv, there is a manually entered lat / long
            #column  that does not exists
            #in the aumatically geocoded version.
            #Check if these columns exist and preferentially use these values
            if MANUAL_COORD in row and row[MANUAL_COORD] != '':
                latitude = float(row[MANUAL_COORD].split(', ')[0])
                longitude = float(row[MANUAL_COORD].split(', ')[1])
            else:
                latitude = float(row[LATITUDE])
                longitude = float(row[LONGITUDE])
        except ValueError as err:
            print("Error", err, json.dumps(row))
            continue

        source = IMPORT_SOURCE
        precint_name = row[PRECINCT]

        sql_row = (
            county,
            name,
            address,
            city,
            state,
            google_address,
            latitude,
            longitude,
            source,
            precint_name,
            location_type,
            source_file,
            source_file_election_date
        )

        # print(json.dumps(sql_row))

        duplicate_key = f'{google_address}\t{location_type}'
        if duplicate_key in results:
            dup_address = { ADDRESS_FULL: address, GOOGLE_ADDRESS: google_address, 'location_type': location_type}
            duplicates[duplicate_key].append( (line_num, json.dumps(dup_address)) )

        results[duplicate_key] = sql_row

    results_list = list(results.values())

    return (results_list, duplicates)

# %%
def print_duplicates(source_file: str, duplicates: list):
    duplicate_list = []
    for duplicate in duplicates.values():
        if len(duplicate) < 2:
            continue
        for line_num, address in duplicate:
            duplicate_list.append(f'  Line {line_num}: {address}')

    if len(duplicate_list) > 0:
        print('\n\n-----------------------------------------------')
        print(f'WARNING: duplicates were found in {source_file}:')
        for duplicate in duplicate_list:
            print(duplicate)

# %%
def create_polling_locations_table(conn):
    create_polling_location_sql = read_sql_template('1_create_polling_locations.sql')
    try:
        conn.cursor().execute(create_polling_location_sql)
        conn.commit()
    except Exception as err:
        print(f' Error.  Rolling back db commits and exiting.\n{err}')
        conn.rollback()
        raise err

# %%
def create_polling_locations_temp_table(conn):
    create_polling_location_sql = read_sql_template('2_create_polling_locations_temp.sql')
    try:
        conn.cursor().execute(create_polling_location_sql)
        conn.commit()
    except Exception as err:
        print(f' Error.  Rolling back db commits and exiting.\n{err}')
        conn.rollback()
        raise err

# %%
def load_polling_locations_temp(conn, source_path):
    load_temp_sql = read_sql_template("3_load_temp.sql")
    delete_by_file_source = read_sql_template("4_delete_by_source.sql")

    # print(load_temp_sql)
    source_file = os.path.basename(source_path)

    with open(source_path, encoding=ENCODING) as csvfile:
        # Force the rows to an array
        reader = csv.DictReader(csvfile)

        rows, duplicates = build_load_temp_rows(source_file, reader)

        print_duplicates(source_file, duplicates)

        num_rows = len(rows)
        try:
            # create_polling_locations_temp_table(conn)
            print(f' loading {num_rows} polling locations into temp table {POLLING_LOCATIONS_TEMP_TABLE} ')
            conn.cursor().executemany(load_temp_sql, rows)

            if "_manual" in source_file:
                base_name = source_file.split('_manual')[0]
            elif "_geocode" in source_file:
                base_name = source_file.split('_geocode')[0]
            else:
                raise Exception("source file name must end in _manual.csv or _geocode.csv")
                        
            valid_suffix = ['_geocode.csv', '_manual.csv']
            possible_name_list  = [base_name + suffix for suffix in valid_suffix]
            for file_name in possible_name_list:
                print(f'  deleting all {file_name} source file entries in {POLLING_LOCATIONS_TABLE}')
                conn.cursor().execute(
                    delete_by_file_source,
                    [file_name]
                )
            conn.commit()
            print(f'  Loaded from {source_file}')
        except Exception as err:
            print(f' Error.  Rolling back db commits and exiting.\n{err}')
            conn.rollback()
            raise err

# %%
config_details = get_warehouse_config()

# %%
conn = psycopg2.connect(**config_details)

# %%
create_polling_locations_temp_table(conn)

# %%
create_polling_locations_table(conn)

# %%
files = [f for f in os.listdir('geocoded/') if f.endswith('.csv')]
files[:10]

# %%
start = 20
end = len(files)
files_slice = files[start:end]
for i, filename in enumerate(files_slice, start=1):
    full_path = f"geocoded/{filename}"
    print(f"{i}/{len(files_slice)} ==> {filename}")
    load_polling_locations_temp(conn, full_path)
    print(f'======================== Done ============================')

# %%
def load_polling_locations(conn):
    create_and_load_polling_staging = read_sql_template("5a_create_and_load_polling_staging.sql")
    load_final_polling_location = read_sql_template("5b_load_polling_locations.sql")
    delete_polling_staging = read_sql_template("5c_drop_polling_staging.sql")
    delete_polling_locations_temp = read_sql_template("5d_drop_polling_locations_temp.sql")
    try:
        print(f'============= creating and loading staging table for polling location ==============')
        conn.cursor().execute(create_and_load_polling_staging)
        print(f'==================================== Done ==========================================')
        print(f'============ loading polling locations table {POLLING_LOCATIONS_TABLE} =============')
        conn.cursor().execute(load_final_polling_location)
        print(f'==================================== Done ==========================================')
        print(f'==================== deleting polling locations staging table ======================')
        conn.cursor().execute(delete_polling_staging)
        print(f'==================================== Done ==========================================')
        print(f'====================== deleting polling locations temp table =======================')
        conn.cursor().execute(delete_polling_locations_temp)
        print(f'==================================== Done ==========================================')
        conn.commit()
        print("All operations done")
    except Exception as err:
        print(f'*************** Error.  Rolling back db commits and exiting.\n{err} ****************')
        conn.rollback()


# %%
load_polling_locations(conn)

# %%



