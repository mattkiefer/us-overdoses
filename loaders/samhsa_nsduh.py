"""
National Survey on Drug Use and Health
*Opioid Use Disorder in Past Year Among Individuals Aged 12 or Older, by State
via SAMHSA Interactive NSDUH State Estimates
🔗 https://datatools.samhsa.gov/saes/state
converting csv -> json (multipart time-series, one year-pair per file e.g. 2022-2023)
models.State.samhsa_nsduh
"""

import csv, json
from models import State, db
from settings import data_dir

### START CONFIG ###
# downloaded from samhsa dashboard linked above
filenames = ['nsduh_oud_22-23.csv',
             'nsduh_oud_21-22.csv']
samhsa_nsduh_filepaths = [data_dir + 'samhsa/' + filename for filename in filenames]
### END CONFIG ###


def load():
    # for bulk insert
    states = {}
    for filepath in samhsa_nsduh_filepaths:
        infile = open(filepath)
        # read csv to json-ready list/dicts
        jdata = list(csv.DictReader(infile))
        # loop thru states
        for samhsa_state_row in jdata:
            print(samhsa_state_row)
            # look up db state using nsduh state column
            try:
                state = State.get(State.name==samhsa_state_row['state'])
            except Exception as e:
                print('failed to lookup',samhsa_state_row['state'])
            # check if state is already in dict
            if state.name not in states:
                # if not, add it
                states[state.name] = []
            # append this row to state data
            states[state.name].append(samhsa_state_row)
    # reshape data for bulk update   
    state_objs = []
    # loop thru states to make assignments
    for state in State.filter():
        # look up nsduh data we just colated for this state
        state_nsduh = states[state.name]
        state.samhsa_nsduh = state_nsduh
        state_objs.append(state)
        
    # avoid transaction errors esp when debugging
    with db.atomic():
        # bulk update
        State.bulk_update(state_objs,fields=['samhsa_nsduh'])
