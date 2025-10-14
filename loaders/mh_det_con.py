"""
Millenium Health loader
see https://docs.google.com/document/d/18NVTJkay-Q2XhZefhf4LE376mWogphhxfblu3L1EWY4/edit?tab=t.0
for data documentation
NOTE that this script combines state detection and concentration tabs using state, year and analyte as merge keys
"""
import os, csv
from models import State, StateYear, db
from settings import data_dir
import pandas as pd

### START CONFIG ###
infile_path = data_dir + 'mh/Guardian_dataset_20250827.xlsx'
### END CONFIG ###


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
    # for bulk inserts
    sys = []

    # split xlsx into sheets
    millenium_xlsx = pd.read_excel(infile_path,sheet_name=None)
    # compiling two sets of state data into one here
    state_data = []
    # convert sheets to lists of dicts for easy ref
    state_detection_rates = millenium_xlsx['state_detection_rates'].to_dict(orient='records')
    state_concentration = millenium_xlsx['state_concentration'].to_dict(orient='records')
    # loop thru detections
    for row in state_detection_rates:
        # extract merge keys
        state, year, analyte = row['State'], row['Year'], row['analyte']
        # check concentrations for matching rows
        matches = [x for x in state_concentration if\
                state==x['State'] and year==x['Year'] and analyte==x['analyte']]
        # any matches?
        match = matches[0] if matches else None
        # merge if matched
        try: 
            # merge rows if two exist, else return the original
            merged_row = row | match if match is not None else row
            # TODO handle state/year concentrations that don't show up in detections
        except Exception as e:
            print(e)
        # add to bulk insert
        state_data.append(merged_row)

    # create new StateYear
    # and this is where actual objects go
    state_years = []
    # iterate through states, years, analytes
    for row in state_data:
        # get this state
        state = State.select().where(State.name ** row['State'])[0]
        # get or create the state_year
        try:
            state_year = StateYear.get(state=state,year=int(row['Year']))
        except Exception as e:
            print(e)
            import ipdb; ipdb.set_trace()
        # save new record
        if not state_year.millenium:
            # add initial millenium health data
            state_year.millenium = [row]
        # we have an existing state_year
        else:
            # add the millenium data
            state_year.millenium.append(row)
        # full save to db
        state_year.save()
