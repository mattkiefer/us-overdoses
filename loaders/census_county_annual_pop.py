import csv
from models import County, CountyMonth, CountyYear, db
from settings import data_dir


### START CONFIGS ###
county_census_file_path = data_dir + 'population/co-est2024-alldata.csv'
county_census_years = range(2020,2025) # 2020-2024
### END CONFIGS ###


def load():
    """
    open census file,
    add annual county pops to CountyYear,
    calculate mortality per 100k
    """
    # idempotent
    CountyYear.delete().execute()
    # read data
    county_census_csv = csv.DictReader(open(county_census_file_path,encoding="latin-1"))
    # each row is a county with annual pop data fields
    for county_row in county_census_csv:
        # fips = county + state
        fips = county_row['STATE'] + county_row['COUNTY']
        # look up county by fips
        try:
            county = County.get(fips=fips)
        except Exception as e:
            print(e, fips, county_row)
            continue
        # loop through years
        for year in county_census_years:
            # find the right field
            pop_year = int(county_row['POPESTIMATE' + str(year)])
            # create the countyyear record
            countyyear = CountyYear.create(
                    county=county,
                    county_name=county.name,
                    state_name=county.state_name,
                    year=year,
                    pop=pop_year
                    )
            countyyear.set_od_mort()
            countyyear.pct_chg_mort()
            countyyear.save()
            print(countyyear.state_name, countyyear.county_name, countyyear.pop, countyyear.od_mort, countyyear.od_mort_100k)
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
