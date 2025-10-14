"""
Drug reports by state, 2014-2025
via HHG<->NFLIS data request 2025-08-27
, also available via DQS (glitchy - state names are ommitted)
🔗 https://www.nflis.deadiversion.usdoj.gov/login.xhtml;jsessionid=g6JyDUTC2tttDc8W3dBGZClA0gz9B12iIpKncBPs.nflis-pd-jbs-02-rhel7
converting xlsx -> pandas
models.State.nflis_drug_reports
⚠️  these 'state' records didn't load into the db: 
    AS
    GU
    PR
    VI
    XX
    MP
"""

import pandas as pd
from models import State, StateYear, db
from settings import data_dir

### START CONFIG ###
nflis_drug_reports_path = data_dir + 'nflis/Hannah Green Data Request - Total reports.xlsx'
### END CONFIG ###


def load():
    print(f"""
    loading {nflis_drug_reports_path} into State.nflis_drug_reports
    """)
    # for collecting state data by year
    states = {}
    # load xlsx via pandas
    drug_report_sheets = pd.read_excel(nflis_drug_reports_path,sheet_name=None)
    # burn after readme
    print(drug_report_sheets['README'])
    drug_report_sheets.pop('README')
    # loop through tabs (years)
    for year in drug_report_sheets:
        # short name for sheet
        sheet = drug_report_sheets[year]
        # loop through rows (state/drug combos)
        for _, row in sheet.iterrows(): # ignore index _
            # 2-char abbreviation
            state = row['State']
            # check if we've seen this state before
            if state not in states:
                # if not, add to states dict
                states[state] = []
            # make a row data dict, minus State (the key)
            row_data = row.drop(labels=['State']).to_dict()
            # don't forget the year
            row_data['Year'] = year
            # append row data to state list, in states dict
            states[state].append(row_data)

    # save to db
    # for bulk inserts
    state_objs = []
    
    # loop thru reshaped state data and save to db
    for state_abbrev in states:
        try:
            # match nflis to db using state abbrev
            state_obj = State.get(State.abbrev==state_abbrev)
        except Exception as e:
            print(state_abbrev)
        # convert this state dict to json
        state_json = states[state_abbrev]
        # assign nflis data to json field
        state_obj.nflis_drug_reports = state_json
        # append to bulk insert list
        state_objs.append(state_obj)

    # avoid transaction errors esp when debugging
    with db.atomic():
        # bulk update overrides old data every time
        State.bulk_update(state_objs,fields=['nflis_drug_reports'])
