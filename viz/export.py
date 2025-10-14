"""
export county, state data
into json
for viz
"""
import json
from settings import viz_dir
from models import County, State
from utils.format import round_float, year_month_str

### START CONFIG ###
outfile_name = 'od.json'
outfile_dir = viz_dir + 'output/'
geos = True # include geojson? NOTE simplified vs fullrez
### END CONFIG ###


def export_json():
    """
    export all jsons
    """

    ############
    # COUNTIES #
    ############

    # query counties data
    counties = {county.fips: {'cty_name': county.name, # county name
                              'st_name': county.state_name, # state name
                              'pop': county.insurance_b27010['B27010001'] if county.insurance_b27010 else None, # population derived from b27010 TODO find better pop figure
                              'mort_100k': round_float(county.mort_per_100k_202412), # drug od mortality per 100k
                              'pct_chg_mort': round_float(county.pct_chg_deaths_202308_202412), # pct change in deaths since peak ods
                              'pct_medicaid': round_float(county.pct_medicaid_19_64), # pct of 19-64 year olds enrolled in Medicaid
                              }
                for county in County.filter()}

    # write out counties data
    with open(outfile_dir + 'od_counties.json','w') as outfile:
        json.dump(counties, outfile)

    ##########
    # STATES #
    ##########

    # query states data
    states = {state.fips: {'st_name': state.name,
                           'pop': state.pop, # population derived from b27010 TODO find better pop figure
                            'mort_100k': round_float(state.mort_per_100k_202504), # drug od mortality per 100k
                            'pct_chg': round_float(state.pct_chg_deaths_202308_202504), # pct change in deaths since peak od TODO this should be Sept 2023 not august
                            'pct_medicaid': round_float(state.pct_medicaid_19_64), # pct of 19-64 year olds enrolled in Medicaid
                           }
                for state in State.filter()}

    # write out states data
    with open(outfile_dir + 'od_states.json','w') as outfile:
        json.dump(states, outfile)

    ##############
    # STATEYEARS #
    ##############

    # query stateyears
    stateyears = {
        state.fips: {
            sy.year: {'mort': sy.od_mort, 
                      'mort100k': sy.od_mort_100k, 
                      'fent': sy.nflis_fent_reports,
                      'pct_chg_mort': sy.pct_chg_mort()}
            for sy in state.years
        }
        for state in State.filter()
    }

    # write out stateyear data
    with open(outfile_dir + 'od_stateyears.json','w') as outfile:
        json.dump(stateyears,outfile)
    
    ###############
    # STATEMONTHS #
    ###############

    # query stateyears
    statemonths = {
        state.fips: {
            # YYYY-MM: {'mort':x, ...}
            '-'.join([str(x) for x in (sm.year,sm.month)]): {'mort': sm.od_mort, 'mort_100k': round_float(sm.od_mort_100k)}
            for sm in state.months
        }
        for state in State.filter()
    }

    # write out stateyear data
    with open(outfile_dir + 'od_statemonths.json','w') as outfile:
        json.dump(statemonths,outfile)


        
    ################
    # COUNTYMONTHS #
    ################

    # query stateyears
    countymonths = {
        county.fips: {
            # YYYY-MM: {'mort':x, ...}
            '-'.join([str(x) for x in (cm.year,cm.month)]): {'mort': cm.deaths, 'mort_100k': round_float(cm.mort_per_100k)}
            for cm in county.months
        }
        for county in County.filter()
    }

    # write out stateyear data
    with open(outfile_dir + 'od_countymonths.json','w') as outfile:
        json.dump(countymonths,outfile)
