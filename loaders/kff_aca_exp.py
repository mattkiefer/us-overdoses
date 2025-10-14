"""
Status of State Action on the Medicaid Expansion Decision
via KFF
🔗 https://www.kff.org/affordable-care-act/state-indicator/state-activity-around-expanding-medicaid-under-the-affordable-care-act
converting csv -> json
models.State.kff_aca_exp
"""

import csv, json
from models import State, db
from settings import data_dir

### START CONFIG ###
kff_aca_exp_path = data_dir + 'kff/aca_exp_raw_data.csv'
skip_lines = 2 # header is on line 3
### END CONFIG ###


def load():
    print(f"""
    loading {kff_aca_exp_path} into State.kff_aca_exp
    """)
    # for bulk insert
    states = []
    # skip the extraneous lines
    infile = open(kff_aca_exp_path).readlines()[skip_lines:]
    # read csv to json-ready list/dicts
    jdata = list(csv.DictReader(infile))
    # loop thru states
    for kff_state_row in jdata[1:52]: # skip United States line + footnotes we should read
        print(kff_state_row)
        # look up db state using kff Location column
        state = State.get(State.name==kff_state_row['Location'])
        # add the kff_aca_exp data as json
        state.kff_aca_exp = kff_state_row
        # add to bulk update list
        states.append(state)
    # avoid transaction errors esp when debugging
    with db.atomic():
        # bulk update
        State.bulk_update(states,fields=['kff_aca_exp'])
