"""
this loader goes first
"""
import os, csv
from models import CountyMonth, County, State, db
from settings import data_dir

### START CONFIG ###
infile_path = data_dir + 'cdc/VSRR_Provisional_County-Level_Drug_Overdose_Death_Counts_20250716.csv'
### END CONFIG ###


# map input csv to db fields
fields = {'Data as of':'data_as_of',
          'Year': 'year',
          'Month': 'month',
          'ST_ABBREV': 'st_abbrev',
          'STATE_NAME': 'state_name',
          'COUNTYNAME': 'county_name',
          'FIPS': 'fips',
          'STATEFIPS': 'state_fips',
          'COUNTYFIPS': 'county_fips',
          'CODE2013': 'code_2013',
          'Provisional Drug Overdose Deaths': 'deaths',
          'Footnote': 'footnote',
          'Percentage Of Records Pending Investigation': 'pct_pending',
          'HistoricalDataCompletenessNote': 'completeness_note',
          'MonthEndingDate': 'month_ending',
          'Start Date': 'month_starting',
          'End Date': 'end_date'}



def cleanup_csv():
    """
    get rid of a few annoying characters
    """
    with open(infile_path) as infile:
        content = infile.read().replace('\ufeff', '') # remove BOM if present
        # reformat as dicts
        return [x for x in csv.DictReader(content.splitlines())]


def update_keys(row):
    """
    for each field
    add new key
    pop old key
    """
    keys = row.keys()
    new_row = {}
    for key in keys:
        if key in fields:
            try:
                new_field = fields[key]
            except Exception as e:
                print(e)
                import ipdb; ipdb.set_trace()
            # only proceed if fields mismatch and we have a key lookup
            if new_field != key and key in fields:
                # copy value from old key to new
                new_row[new_field] = row[key]
                # pop old key
                # row.pop(key)
    return new_row



def load(drop=True):
    """
    get csv
    convert field names
    load into peewee
    """
    
    # print filepath to screen
    print(f"""
    =======
    loading {infile_path}
    =======
    """)
    # idempotent
    if drop:
        CountyMonth.delete().execute()

    # for bulk inserts
    cms = []
    clean_csv = cleanup_csv()

    # iterate through source data
    for row in clean_csv:
        # rename field
        new_row = update_keys(row)

        # get or create state
        state, created = State.get_or_create(name=new_row['state_name'])
        if created:
            print(state.name)
            state.abbrev = row['ST_ABBREV']
            # TODO verify how we handle state fips where leading zero is omitted for states 1-9 (Alabama-Connecticut)
            state.fips = row['FIPS'][0:2] if len(row['FIPS']) == 5 else '0' + row['FIPS'][0]# first two chars
            state.save()
        
        # need fips to create county
        fips = row['FIPS']
        fips = '0' + fips if len(fips) == 4 else fips

        # get or create county
        county, created = County.get_or_create(name=new_row['county_name'],state=state,state_name=state.name,fips=fips)

        if created:
            county.save()
        
        # load into peewee
        try:
            new_row['county_id'] = county.id
            # type conversion
            new_row['deaths'] = int(new_row['deaths'].replace(',','')) if new_row['deaths'] else None
            cm = CountyMonth.create(**new_row)
            # saving each to db is slower but avoids duplication issues referenced below
            cm.save()
            cms.append(cm)
            #print(row)
        except Exception as e:
            print(e)
            import ipdb; ipdb.set_trace()

    # bulk insert
    with db.atomic():
        print('bulk create')
        # this is somehow causing duplication so we're doing cm.save() above instead
        #CountyMonth.bulk_create(cms,batch_size=100)

    # derived fields
    set_values()

def set_values():
    """
    derived fields
    for all counties
    """
    print('setting pct_chg')
    counties = []
    for county in County.filter():
        county.set_pct_change_deaths()
        counties.append(county)

    # bulk update
    with db.atomic():
        print('bulk update county pct change deaths')
        County.bulk_update(counties,fields=['pct_chg_deaths_202308_202412'])

