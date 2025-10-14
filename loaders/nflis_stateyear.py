from models import db, State, StateYear

### START CONFIG ###
years = range(2015,2025) # TODO derive this 
### END CONFIG ###

def load():
    """
    traverse nflis json data
    and sum up all report counts
    where basedescription contains 'fentanyl'
    """
    for state in State.filter():
        for stateyear in state.years:
            nflis_this_year = [x for x in state.nflis_drug_reports if int(x['Year']) == stateyear.year]
            # TODO is this the best method i.e. string-search 'fentanyl'?
            nflis_fent_this_year = [x for x in nflis_this_year if x['BaseDescription'] and  'fentanyl' in x['BaseDescription'].lower()]
            stateyear.nflis_fent_reports = sum([int(x['ReportCount']) for x in nflis_fent_this_year if x['ReportCount']])
            stateyear.save()



