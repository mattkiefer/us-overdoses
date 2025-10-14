"""
C17002
Ratio of Income to Poverty Level in the Past 12 Months
Table universe: Population for whom poverty status is determined
https://censusreporter.org/tables/C17002/
"""

import os, csv
from models import County, State, db
from settings import data_dir

### START CONFIG ###
#infile_path = data_dir + 'acs2023_5yr_B27010_05000US17113/acs2023_5yr_B27010_05000US17113.csv'
state_c17002_path = data_dir + 'acs/acs2023_1yr_C17002_04000US29/acs2023_1yr_C17002_04000US29.csv'
county_c17002_path = data_dir + 'acs/acs2023_5yr_C17002_05000US47063/acs2023_5yr_C17002_05000US47063.csv'
### END CONFIG ###



def load():
    """
    get csv
    convert field names
    load into peewee
    """

    # print filename to screen
    print(f"""
    =======
    loading state data {state_c17002_path}
    =======
    """)

    # via https://censusreporter.org/data/table/?table=C17002&geo_ids=040|01000US&primary_geo_id=01000US
    states_data = [x for x in csv.DictReader(open(state_c17002_path))]

    # via data/acs/acs2023_1yr_C17002_04000US29/metadata.json

    # keep list for bulk_update
    states = []

    # roll through each state
    for state_row in states_data:
        # get state name
        state_name = state_row['name']
        # derive state fips from last 2 chars of geoid
        state_fips = state_row['geoid'][-2:]
        print(state_name)
        try:
            # lookup by fips
            state = State.get(fips=state_fips)
            # dictreader row is json
            state.poverty_c17002 = state_row
        except Exception as e:
            # fips lookup failures get printed
            print(e)
            import ipdb; ipdb.set_trace()
        # add this state for bulk update
        states.append(state)
    
    # in transaction in case of failure
    with db.atomic():
        # use batches to avoid sqlite query char limits
        State.bulk_update(states,
                          fields=['poverty_c17002'],
                          batch_size=50)    


    # print filename to screen
    print(f"""
    =======
    loading county data {county_c17002_path}
    =======
    """)

    # keep list for bulk_update
    counties = []

    # load counties data
    counties_data = csv.DictReader(open(county_c17002_path))

    # iterate through c17002 counties
    for county_row in counties_data:
        # e.g. Autauga County, AL
        county_name = county_row['name']
        # 5-char fips ends geoid i.e. 2 char state + 3 char county
        county_fips = county_row['geoid'][-5:]
        try:
            # attempt lookup via fips
            county = County.get(fips=county_fips)
            # csv dictreader rows are json
            county.poverty_c17002 = county_row
        except Exception as e:
            print('fips lookup failed for',county_name,':', county_fips)
            # extraneous rows in download? TODO double check
            continue

        # updated counties added to bulk update list
        counties.append(county)
    
    # in transaction in case of failure
    with db.atomic():
        # use batches to avoid sqlite query char limits
        County.bulk_update(counties, 
                           fields=['poverty_c17002'], 
                           batch_size = 50)
        


