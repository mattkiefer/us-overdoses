import requests, os, csv
from models import State, StateYear, StateMonth, CountyMonth, County, db
from settings import data_dir


### START CONFIGS ###
output_dir = data_dir + 'acs/'
state_acs_years = [x for x in range(2015,2026)] # no acs1y during covid
county_acs_year = 2023 # latest and best 5-year available, 2019-2023
census_key = open('census_key').read().strip() if os.path.exists('census_key') else ''
### END CONFIGS ###


def filename(locale, year, product):
    """
    for reading and writing
    """
    return output_dir + f'{locale}_pop_{year}_{product}.csv' 

def fetch():
    """
    download from internet
    if you don't already have
    ACS 1-year state pop 2015-latest,
    ACS 5-year county pop 2019-2023
    """
    ##########
    # STATES #
    ##########

    # collect pop for each year
    for year in state_acs_years:
        # no acs1 during covid, no 2025 data available yet
        if year in (2020, 2025):
            continue
        # output file path
        outfile_path = filename('state',year,'acs1')

        # skip if exists
        if os.path.exists(outfile_path):
            continue

        try:
            # Build ACS 1-Year API URL for total population (table B01003) for states
            url = f'https://api.census.gov/data/{year}/acs/acs1'
            params = {'get': 'NAME,B01003_001E', 'for': 'state:*', 'key': census_key}

            # Make request to Census API
            response = requests.get(url, params=params)
            response.raise_for_status()

            # Parse JSON response
            data = response.json()

            # Save data to CSV file
            with open(outfile_path, 'w', newline='') as outfile:
                csv.writer(outfile).writerows(data)

            print(f'Saved {year} data to {outfile_path}')

        except Exception as e:
            print(f'Failed for {year}: {e}')
            import ipdb; ipdb.set_trace()

    ############
    # COUNTIES #
    ############

    # output file path
    outfile_path = filename('county',county_acs_year,'acs5')

    # skip if exists
    if not os.path.exists(outfile_path):

        try:
            # Build ACS 5-Year API URL for total population (table B01003) for counties
            url = f'https://api.census.gov/data/{county_acs_year}/acs/acs5'
            params = {'get': 'NAME,B01003_001E', 'for': 'county:*', 'key': census_key}

            # Make request to Census API
            r = requests.get(url, params=params)
            r.raise_for_status()

            # Parse JSON response
            data = r.json()

            # Save data to CSV file
            with open(outfile_path, 'w', newline='') as outfile:
                csv.writer(outfile).writerows(data)

            print(f'Saved {county_acs_year} data to {outfile_path}')

        except Exception as e:
            print(f'Failed for {year}: {e}')



def load():
    """
    fetch if needed,
    read files
    update tables
    """
    ##########
    # STATES #
    ##########

    print('acs_pop')

    # in case we need the data
    fetch()
    # store for bulk update
    stateyears = []
    # loop through years
    for year in state_acs_years:
        # NOTE no ACS 1-years for 2020 (covid), 2025
        if year in (2020,2025):
            # fallback to prior year 
            path = filename('state',year - 1,'acs1') # TODO verify we want to use prior year where ACS1Y doesn't exist
        else:
            path = filename('state',year,'acs1')
        # loop through states
        for state_row in csv.DictReader(open(path)):
            # territories cause exception TODO figure out how best to load them
            try:
                # get matching stateyear
                stateyear = StateYear.get(state_name=state_row['NAME'],year=year)
            except Exception as e:
                print(state_row['NAME'],e)
                continue
            # Estimate!!Total	TOTAL POPULATION
            stateyear.pop = int(state_row['B01003_001E'])
            # hard save because set_od_mort() doesnt stick in bulk update
            # stateyear.save()
            stateyears.append(stateyear)
            stateyear.save()

            # calculate od mortality by state month
            for sm in StateMonth.filter(state_name=state_row['NAME'],year=year):
                sm.set_mort_data()
                sm.save()

    for state in State.filter():
        state.set_mort_100k()
        state.set_pct_change_deaths()
        state.save()

    ############
    # COUNTIES #
    ############
    county_path = filename('county',county_acs_year,'acs5')
    for county_row in csv.DictReader(open(county_path)):
        fips = county_row['state'] + county_row['county']
        try:
            county = County.get(fips=fips)
            county.pop = county_row['B01003_001E']
            county.save()
        except Exception as e:
            print(e, county_row)
        

    """
    with db.atomic():
        StateYear.bulk_update(stateyears,fields=['pop'],batch_size=1000)
    """
