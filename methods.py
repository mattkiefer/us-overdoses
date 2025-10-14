"""
helper methods
for analysis.py
places = states, counties + other things
"""
# CDC #

## mortality

### state

def state_pct_change_deaths(self,start,end,indicator='Number of Drug Overdose Deaths'):
    """
    pass related model StateMonth so analysis.py has access to it.
    given a drug and two dates, calc pct chg in od deaths.
    date overrides must be tuples w/ four-digi year, title-case month 
    e.g.: (2025,'January')
    NOTE indicator defaults to all types of drug od deaths but can be specified e.g. fentanyl
    """
    # need access to statemonths for this analysis
    from models import StateMonth
    # unpack these dates
    start_year, start_month = int(start[0]), start[1]
    end_year, end_month = int(end[0]), end[1]

    # query those statemonths
    
    # exception handling for limited scenarios
    try:
        start_state_month = self.months.where(StateMonth.year==start_year,StateMonth.month==start_month).get()
    except Exception as e:
        import ipdb; ipdb.set_trace()

    end_state_month = self.months.where(StateMonth.year==end_year,
                                        StateMonth.month==end_month).get()
    # extract counts
    start_count = start_state_month.od_mort
    end_count = end_state_month.od_mort

    # return pct chg,
    # handling any quirks
    pct_chg = (int(end_count) - int(start_count)) / float(start_count) if start_count and end_count!= '' else None

    return {'pct_chg':pct_chg,start:start_count,end:end_count}


def state_deaths_by_year(self,year):
    """
    get calendar year by querying state.counties
    where month=December
    since months are 12-month trailing
    """
    pass
    


## county

def county_pct_change_deaths(self,start,end):
    """
    start, end refer to year-months
    and must be passed as tuples of ints
    e.g. (2023,9),(2024,12)
    """
    from models import CountyMonth
    # split start and end dates for querying
    start_year, start_month = start[0], start[1]
    end_year, end_month = end[0], end[1]
    try: 
        countymonth_start = CountyMonth.get(county=self,month=start_month,year=start_year)
        countymonth_end = CountyMonth.get(county=self,month=end_month,year=end_year)
    except Exception as e:
        # TODO better handling of unresolved queries
        print("couldn't find",self.name,start,end)
        # go fish
        return
    pct_chg = float(countymonth_end.deaths-countymonth_start.deaths)/countymonth_start.deaths if countymonth_start.deaths and countymonth_end.deaths else None
    return {'pct_chg':pct_chg,start:countymonth_start.deaths,end:countymonth_end.deaths}
    # get the counts for the start month
    # subtract from the end month
    # divide by start month


# mortality per 100K

def county_mortality_per_100k(self,year_month=(2024,12)):
    """
    get mortality for specified month,
    divide by pop then x100k
    """
    from models import CountyMonth
    year, month = year_month[0], year_month[1]
    cm = CountyMonth.get(county=self, year=year, month=month)
    deaths = cm.deaths
    pop = float(self.insurance_b27010['B27010001']) if self.insurance_b27010 else None # Total TODO handle pops better
    mort_per_100k = deaths/pop*100000 if deaths and pop else None
    return mort_per_100k



def state_mortality_per_100k(self,year_month=(2025,'April')):
    """
    get mortality for specified month,
    divide by pop then x100k
    """
    from models import StateMonth
    sm = StateMonth.filter(state=self,month=year_month[1],year=year_month[0])
    deaths = sm[0].od_mort if sm else None
    mort_per_100k = deaths/float(self.pop)*100000 if deaths and self.pop else None
    return mort_per_100k


# census


## locale (states and counties)


def b27010_breakdown(self):
    """
    derive relevant figures 
    from src data 
    """
    # TODO double-check metadata.json from censusreporter
    # NOTE medicaid means medicaid exlusively (should we include co-enrolled?)
    medicaid_under_19 = self.insurance_b27010['B27010007']
    medicaid_19_34    = self.insurance_b27010['B27010023']
    medicaid_35_64    = self.insurance_b27010['B27010039']
    medicaid_65_plus  = self.insurance_b27010['B27010055']
    no_ins_under_19   = self.insurance_b27010['B27010017']
    no_ins_19_34      = self.insurance_b27010['B27010033']
    no_ins_35_64      = self.insurance_b27010['B27010050']
    no_ins_65_plus    = self.insurance_b27010['B27010066']
    total_under_19    = self.insurance_b27010['B27010002']
    total_19_34       = self.insurance_b27010['B27010018']
    total_35_64       = self.insurance_b27010['B27010034']
    total_65_plus     = self.insurance_b27010['B27010051']
    total             = self.insurance_b27010['B27010001']


    return {
            'pct_medicaid_19_64': sum(int(x) for x in [medicaid_19_34,medicaid_35_64])/
            float(sum(int(x) for x in [total_19_34,total_35_64])),
            'pct_medicaid': sum(int(x) for x in [medicaid_under_19,medicaid_19_34,medicaid_35_64,medicaid_65_plus])/float(total)
            }

