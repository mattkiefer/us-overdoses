"""
Naloxone Dispensing Rate
via CDC
🔗 https://www.cdc.gov/overdose-prevention/data-research/facts-stats/naloxone-dispensing-rate-maps.html
converting csv -> json
models.State.cdc_naloxone_disp
"""

import csv, json
from models import State, db
from settings import data_dir

### START CONFIG ###
cdc_nal_disp_csv = data_dir + 'cdc/State Naloxone Dispensing Rates.csv'
### END CONFIG ###


def load():
    """
    see module docstring
    """
    print(f"""
    =================================
    loading {cdc_nal_disp_csv} into State.cdc_naloxone_disp
    =================================
    """)
    # for bulk insert ... 
    states_data = dict()
    # no extraneous headers to skip
    infile = open(cdc_nal_disp_csv)
    # read csv to json-ready list/dicts
    jdata = list(csv.DictReader(infile))
    # loop thru states *and years*
    for nal_state_year in jdata:
        # clean unicode char
        nal_state_year = {k.lstrip('\ufeff'): v for k, v in nal_state_year.items()}
        print(nal_state_year)
        # check if state is in dict
        state_name = nal_state_year['STATE_NAME']
        # if not, add it
        if state_name not in states_data:
            states_data[state_name] = []
        # append this year's naloxone disp data to list
        states_data[state_name].append(nal_state_year)

    # NOW that annual data is compiled by state, 
    # update all the states in db
    states = [] # placeholder for bulk update

    # loop thru one more time to assign state data
    for state_name in states_data:
        # get the state to update
        state = State.get(State.name==state_name)
        state.cdc_naloxone_disp = states_data[state_name]
        # extract rate and put on model
        state.set_nal_disp_rate()
        states.append(state)
    
    # avoid transaction errors esp when debugging
    with db.atomic():
        # bulk update
        State.bulk_update(states,fields=['cdc_naloxone_disp','cdc_naloxone_disp_rate'])
