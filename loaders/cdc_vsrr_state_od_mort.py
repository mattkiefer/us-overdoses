import os, csv
from models import State, StateMonth, StateYear, SpecialState, SpecialStateMonth, db
from settings import data_dir
from utils.format import month_name_to_number

### START CONFIG ###
infile_path = data_dir + 'cdc/VSRR_Provisional_Drug_Overdose_Death_Counts_20250917.csv'
### END CONFIG ###


def cleanup_csv():
    """
    get rid of a few annoying characters
    """
    with open(infile_path) as infile:
        content = infile.read().replace('\ufeff', '') # remove BOM if present
    # csvify
    input_list = [x for x in csv.DictReader(content.splitlines())]
    clean_list = []
    # clean data and append to list
    for row in input_list:
        # get rid of commas in data values
        row['Data Value'] = row['Data Value'].replace(',','')
        clean_list.append(row)
    return clean_list


def organize_data(rows):
    """
    because indicators are broken out separately, 
    there are multiple rows by statemonth.
    return a dict where {state:{month:[indicator_rows]}}
    """
    # a place to put statemonth:rows
    data = {}
    # iterate through each row
    for row in rows:
        # state-level key
        if row['State'] not in data:
            data[row['State']] = {}
        # month-level key
        year_month = '_'.join([row['Year'],row['Month']])
        if year_month not in data[row['State']]:
            data[row['State']][year_month] = []
        # append data
        data[row['State']][year_month].append(row)
    # send back organized data for processing
    return data


def load(drop=True):
    """
    for each statemonth,
    add indicator-level data as json to vsrr,
    plus a few noteworthy fields
    """

    # print filepath to screen
    print(f"""
    =======
    loading {infile_path}
    =======
    """)
    # idempotent
    if drop:
        StateMonth.delete().execute()
        SpecialStateMonth.delete().execute()
        SpecialState.delete().execute()
    # placeholder special states for NY, YS to be combined later
    ss_nys = SpecialState.create(name="New York State",abbrev="NY")
    ss_nyc = SpecialState.create(name="New York City",abbrev="YC")

    # for bulk inserts
    sms = []
    # clean up useless characters
    clean_csv = cleanup_csv()
    # restructure to {state:{month:[indicator_rows]}}
    organized_data = organize_data(clean_csv)

    # iterate through source data
    for state_abbrev in organized_data:
        # NY is a special case because state and city are broken out separately
        if state_abbrev in ('NY','YC'): 
            StateObj, StateMonthObj = SpecialState, SpecialStateMonth
        # make sure regular states are handled as such
        else:
            StateObj, StateMonthObj = State, StateMonth

        # query for single state in way that doesn't raise exception for territories
        states = StateObj.filter(abbrev=state_abbrev)
        if states:
            state = states[0]
            print(state.name)
        else:
            print("couldn't look up",state_abbrev)
            continue

        # extract year, month
        for year_month in organized_data[state_abbrev]:
            year, month = year_month.split('_')
        
            # then get into indicator-level data
            indicators = organized_data[state_abbrev][year_month]

            ##############
            # STATEMONTH #
            ##############
            # load into db
            sm = StateMonthObj(
                state = state, 
                state_name = state.name,
                year = int(year),
                month = month,
                month_no = month_name_to_number(month),
                vsrr = indicators)

            # set derived fields
            # sm.set_mort_data()
            # add to bulk insert list
            sms.append(sm)
            sm.save()

    ### START NY SPECIAL CASE ###

    # re-assemble NY
    ny_combined = State.get(name="New York")
    ny_city = SpecialState.get(abbrev="YC")
    ny_state = SpecialState.get(abbrev="NY")
    # for each month of separated data, combine them
    for nys_month in ny_state.months:
        # look up corresponding nyc month for each nys month
        nyc_month = ny_city.months.filter(month=nys_month.month,year=nys_month.year)[0]
        # look up overdose counts by nyc, nys
        nyc_deaths = int([x for x in nyc_month.vsrr if x['Indicator'] == 'Number of Drug Overdose Deaths'][0]['Data Value'])
        nys_deaths = int([x for x in nys_month.vsrr if x['Indicator'] == 'Number of Drug Overdose Deaths'][0]['Data Value'])
        # combine VSRR
        # note that we're only tracking 'Number of Drug Overdose Deaths' but could add other indicators
        # ... and keeping it in a list of one for sake of consistent form
        ny_vsrr = [{"State":"NY","Year":nys_month.year,"Month":nys_month.month,"Period":"12 month-ending","Indicator":"Number of Drug Overdose Deaths","Data Value":"","Percent Complete":"","Percent Pending Investigation":"","State Name":"New York","Footnote":"Combined NYS + NYC. See SpecialState","Footnote Symbol":"","Predicted Value":""}]
        # add up NYS + NYC deaths
        ny_vsrr[0]['Data Value'] = nyc_deaths + nys_deaths
        # create monthly record for combined NYC + NYS (just called NY, consistent with other states)
        ny_combined_month = StateMonth(
                state = ny_combined,
                state_name = 'New York',
                year = int(nys_month.year),
                month = nys_month.month,
                month_no = month_name_to_number(month),
                vsrr = ny_vsrr)
        ny_combined_month.save()
        
        # save totals to state and city for auditing
        nys_month.od_mort = nys_deaths
        nyc_month.od_mort = nyc_deaths
        nys_month.save()
        nyc_month.save()
    ### END NY SPECIAL CASE ###


    # bulk insert may require explicitly setting atttributes e.g. set_mort_data() doesn't seem to stick
    """
    with db.atomic():
        print('bulk create')
        StateMonth.bulk_create(sms,batch_size=100)
    """
    # create stateyears, deriving year range from the last state (Wyoming)
    stateyears = set([(sm.state_name,sm.year) for sm in state.months])
    # all the states, all the years (not including SpecialStates)
    stateyears = {(month.state_name, month.year) for state in State.filter() for month in state.months}
    # loop thru each combo 
    for stateyear in stateyears:
        state_name, year = stateyear[0], stateyear[1]
        state = State.get(name=state_name)
        # create the state year
        sy = StateYear.create(state=state,state_name=state_name,year=year)
        # derive mortality stats NOTE we'll do this in acs_pop to get the right time series
        # sy.set_od_mort()
        # hard save
        sy.save()

    # state derived fields
    print('calculating pct chg by state')
    for state in State.filter():
        state.set_pct_change_deaths()
        state.save()


