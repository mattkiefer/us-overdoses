"""
builds db,
creates tables,
loads data,
gets and sets,
exports reports
+ json for viz
"""
import argparse, os, inspect, datetime, time
from zipfile import ZipFile
from peewee import Model
import models 
from models import County, State, CountyMonth, CountyYear, StateMonth, StateYear, SpecialState, SpecialStateMonth
from loaders import cdc_vsrr_cty_od_mort, cdc_vsrr_state_od_mort, \
        cdc_bup_disp, cdc_nal_disp, \
        acs_b27010, acs_c17002, acs_pop, census_county_annual_pop, \
        kff_aca_exp, \
        nflis_drug_reports_by_state, nflis_stateyear, \
        tiger_geos, \
        mh_det_con, \
        county_mort, mort_by_month 
from settings import db_filename, db_path

# arg handling
parser = argparse.ArgumentParser(description=__doc__) # see docstring 👆
parser.add_argument('--build-db', action='store_true', help="create database, load tables")
parser.add_argument('--shapes', action='store_true', help="not loading shapes unless you ask for them")
parser.add_argument('--shell', action='store_true', help="loads select models in interactive shell")
parser.add_argument('--reports', action='store_true', help="run all reports")
parser.add_argument('--viz', action='store_true', help="export jsons to viz/output/")
parser.add_argument('--no-pandas', action='store_true', help="shell starts up faster without dataframes")


# CONFIGS
db_backup_path = 'db_backups/' + db_filename + '~' + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + '.zip'


# gnarly list comprehension that fetches all the peewee models
peewee_models = [cls for name, cls in inspect.getmembers(models, inspect.isclass)
                 if cls.__module__ == models.__name__ 
                 and issubclass(cls, Model)]
# and their names
model_names = "\t" + "\n\t".join([name for name, obj in inspect.getmembers(models)
               if inspect.isclass(obj) 
               and issubclass(obj, Model) 
               and obj is not Model
               and name != 'BaseModel'])



# define tasks

def backup_db():
    """
    sqlite dbs are just files
    """
    with ZipFile(db_backup_path, mode='w') as zf:
        zf.write(db_path)
        os.remove(db_path)
        

def create_db():
    """
    creates db,
    infers classes from models.py
    and creates corresponding tables
    """
    # create db
    models.db.connect()

    # create tables
    models.db.create_tables(peewee_models)


def load_data(shapes):
    """
    manually adding loaders here for now
    loaders include get/set action before bulk_create
    """
    # cdc county-level od mortality by monthly 12-month-ending, 2020-2024
    cdc_vsrr_cty_od_mort.load()
    # acs b27010 types of insurance
    acs_b27010.load()
    # acs c17002 poverty
    acs_c17002.load()
    # cdc state-level od mortality by monthly 12-month-ending, 2015-2024
    cdc_vsrr_state_od_mort.load()
    # millenium health is not part of this open-source repository
    # mh_det_con.load()
    # kff's 50-state aca expansion table
    kff_aca_exp.load()
    # cdc 50-state buprenorphine dispensing rates by year, 2019-2023
    cdc_bup_disp.load()
    # cdc 50-state naloxone dispensing rates by year, 2019-2023
    cdc_nal_disp.load()
    # nflis drug reports by 50 states, year and drug class
    nflis_drug_reports_by_state.load()
    # break those nflis reports down into stateyears
    nflis_stateyear.load()
    # updates county mortality rates
    county_mort.update()
    # acs stateyear pop, 2015-2024 via B01003
    acs_pop.load()
    # county pop by year
    census_county_annual_pop.load()
    # calculates od mortality per 100k by monthly 12-month-ending period
    mort_by_month.update()
    # conditionally load shapes
    if shapes:
        # census shapes for counties and states, optimized as needed
        tiger_geos.load()
 

def shell():
    """
    load all the variables
    and go interactive
    """
    
    # calling ipython for shell
    from IPython import embed

    # make some pandas vars
    import pandas as pd
    
    ## variables declared here will be printed on console start
    # load peewee querysets
    counties = County.filter()
    states = State.filter()
    countymonths = CountyMonth.filter()
    statemonths = StateMonth.filter()
    stateyears = StateYear.filter()

    # individual peewee records for exploration
    il = State.get(State.name=='Illinois')
    cook = County.get(County.name=='Cook',County.state==il)

    ak = State.get(State.name=='Alaska')
    anchorage = County.get(County.name=='Anchorage',County.state==ak)

    pa = State.get(State.name=='Pennsylvania')
    philly = County.get(County.name=='Philadelphia',County.state==pa)

    va = State.get(State.name=='Virginia')
    bedford = County.get(County.name=='Bedford',County.state==va)
    
    ga = State.get(State.name=='Georgia')
    catoosa = County.get(County.name=='Catoosa',County.state==ga)

    # time/locale examples
    il24 = StateYear.get(StateYear.state==il,StateYear.year==2024) if list(il.years) else None

    # special cases
    ny = State.get(State.name=='New York')
    nys = SpecialState.get(abbrev='NY')
    nyc = SpecialState.get(abbrev='YC')

    # load dfs conditionally
    if not args.no_pandas:
        # load querysets into pandas
        states_df = pd.DataFrame(list(states.dicts()))
        counties_df = pd.DataFrame(list(counties.dicts()))
        county_months = pd.DataFrame(list(countymonths.dicts()))
        state_months_df = pd.DataFrame(list(statemonths.dicts()))
        state_years_df = pd.DataFrame(list(stateyears.dicts()))

        # individual state, county records in pandas
        il_df = states_df[states_df['name']=='Illinois']
        cook_df = counties_df[(counties_df['name']=='Cook') & (counties_df['state_name']=='Illinois')]
        
    local_vars = "\t" + "\n\t".join([x for x in locals() if x != 'embed'])

    

    ## end variables

    header = f"""

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~



    💻 db loaded from 
        {db_path}

    📀 models.py db classes:
{model_names}

    💾 variables loaded:
{local_vars}
       """
    
    # load shell with variables
    embed(header=header,local=locals(),colors='Linux')


def run_reports():
    """
    run all listed reports
    """
    from reports import counties_report, states_report
    counties_report.run()
    states_report.run()

def export_viz():
    """
    export json for viz
    """
    from viz.export import export_json
    export_json()


# arg processing

args = parser.parse_args()

# db build involves backup, create, load 
if args.build_db:
    # timer start
    start = time.perf_counter()
    print(datetime.datetime.now())

    # backup, create, load
    backup_db()
    create_db()
    load_data(shapes=args.shapes)

    # print elapsed time
    print(round(time.perf_counter()-start))


elif args.shell:
    shell()
    
elif args.reports:
    run_reports()

elif args.viz:
    export_viz()
