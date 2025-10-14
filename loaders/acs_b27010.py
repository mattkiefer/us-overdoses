import requests, os, csv
from zipfile import ZipFile
from models import CountyMonth, County, State, db
from settings import data_dir

### START CONFIG ###
# downloaded data goes here
acs_path = data_dir + 'acs/'
# download urls by state, county
urls = {'State':'https://api.censusreporter.org/1.0/data/download/acs2023_1yr?table_ids=B27010&geo_ids=040|01000US&format=csv',
        'County':'https://api.censusreporter.org/1.0/data/download/acs2023_5yr?table_ids=B27010&geo_ids=040|01000US,050|01000US&format=csv'}
### END CONFIG ###


def get_data():
    """
    request fresh data from
    censusreporter.org
    for states, counties
    """
    for locale in urls:
        # get the zipfile
        response = requests.get(urls[locale],allow_redirects=True)
        # zipfile requires .content
        zdata = response.content
        # temp file for unzipping
        zipfile_path =  acs_path + 'b27010_' + locale + '.zip'
        # save it
        with open(zipfile_path,'wb') as zipfile:
            zipfile.write(zdata)
        # unzip 
        with ZipFile(zipfile_path,'r') as unzipper:
            unzipper.extractall(acs_path)


def load(get=False):
    """
    loop through state, county
    b27010 files,
    loading into jsonfield 
    for each locale
    """
    # only if you need it
    if get:
        get_data()
    
    # iterate through state and counties using ObjClass variable
    for ObjClass in [State, County]:
        # check if this is a state or county
        class_name = ObjClass.__name__
        # ... and load data accordingly
        # hack
        if class_name == 'State':
            infile_path = acs_path + 'acs2023_1yr_B27010_04000US29/acs2023_1yr_B27010_04000US29.csv'
        elif class_name == 'County':
            infile_path = acs_path + 'acs2023_5yr_B27010_05000US17113/acs2023_5yr_B27010_05000US17113.csv'
        # print filename to screen
        print(f"""
        =======
        loading {infile_path}
        =======
        """)

        data = [x for x in csv.DictReader(open(infile_path))]

        # keep list for bulk_update
        objs = []

        # iterate through b27010 counties
        for row in data:
            # state fips are last 2 characters, county fips are last 5
            fips = row['geoid'][-2:] if class_name == 'State' else row['geoid'][-5:] 
            try:
                obj = ObjClass.get(fips=fips)
            except Exception as e:
                print(fips, row['name'])
                # extraneous rows in download? TODO double check
                continue
            # set src data
            obj.insurance_b27010 = row    

            # set pct medicaid 19-64
            obj.set_b27010_breakdown()

            # set pop
            obj.set_pop()

            # updated locales added to bulk update list
            objs.append(obj)
        
        # in transaction in case of failure
        with db.atomic():
            # use batches to avoid sqlite query char limits
            ObjClass.bulk_update(objs, 
                               fields=['insurance_b27010','pct_medicaid_19_64','pop'], 
                               batch_size = 50)
            
