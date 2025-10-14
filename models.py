from peewee import *
from datetime import datetime
from playhouse.sqlite_ext import SqliteExtDatabase, JSONField
from settings import db_path
from methods import state_pct_change_deaths, county_pct_change_deaths, \
        county_mortality_per_100k, state_mortality_per_100k,\
        b27010_breakdown

db = SqliteDatabase(db_path)


class BaseModel(Model):
    class Meta:
        database = db


class Locale(BaseModel):
    """
    abstract base class
    for attributes and methods
    common to counties and states
    """

    ############
    # METADATA #
    ############
    name = CharField()
    fips = CharField(null=True)

    ###############
    # TIGER/LINES #
    ###############

    geometry = JSONField(null=True)

    #######
    # ACS #
    #######

    pop = IntegerField(null=True)
    # TODO double-check pop source, currently b27010
    def get_pop(self):
        return self.insurance_b27010['B27010001'] # Total
    def set_pop(self):
        self.pop = self.get_pop()


    # B27010 #
    # types of health insurance: private, public, uninsured etc. #
    insurance_b27010 = JSONField(null=True)

    # breakdowns

    # how many 19-64 yos on medicaid exclusively?
    pct_medicaid_19_64 = FloatField(null=True)

    def set_b27010_breakdown(self):
        """
        not saving because we can do that in bulk
        NOTE these are ppl exclusively covered by medicaid,
        which is maybe best for state-to-state comparisons
        """
        # NOTE you can store field names for update + return
        # for bulk_update fields
        self.pct_medicaid_19_64 = b27010_breakdown(self)['pct_medicaid_19_64']

    
    # C17002 #
    # ratio of income to poverty level#
    poverty_c17002 = JSONField(null=True)


class State(Locale):
    """
    more stuff here eventually
    """
    ############
    # METADATA #
    ############
    abbrev = CharField(null=True)

    #######
    # KFF #
    #######
    # ACA expansion
    kff_aca_exp = JSONField(null=True)

    #######
    # DEA #
    #######
    nflis_drug_reports = JSONField(null=True)

    #######
    # ACS #
    #######
    # see Locale

    #######
    # CDC #
    #######

    # buprenorphine
    cdc_buprenorphine_disp = JSONField(null=True)

    cdc_buprenorphine_disp_rate = FloatField(null=True)
    def set_bup_disp_rate(self):
        """
        simplifying to just get rate
        """
        most_recent_year = max(self.cdc_buprenorphine_disp, key = lambda x: int(x['YEAR']))
        self.cdc_buprenorphine_disp_rate = most_recent_year['buprenorphine_dispensing_rate']

    # naloxone
    cdc_naloxone_disp = JSONField(null=True)
    cdc_naloxone_disp_rate = FloatField(null=True)
    def set_nal_disp_rate(self):
        """
        simplifying just to get rate
        """
        most_recent_year = max(self.cdc_naloxone_disp, key = lambda x: int(x['YEAR']))
        self.cdc_naloxone_disp_rate = most_recent_year['naloxone_dispensing_rate']

    # mortality - see StateMonth
    pct_chg_deaths_202308_202412 = FloatField(null=True)
    pct_chg_deaths_202308_202504 = FloatField(null=True)

    mort_per_100k_202504 = FloatField(null=True)


    # dynamically get latest mortality data
    def get_latest_month(self):
        max_year = max([x.year for x in self.months])
        max_month = max([x.month_no for x in self.months if x.year == max_year])

        # return latest year, month
        return StateMonth.get(
            (StateMonth.state == self) &
            (StateMonth.year == max_year) &
            (StateMonth.month_no == max_month)
        )


    def get_latest_mort(self):
        """
        drug od mortality for the latest month available
        """
        latest_month = self.get_latest_month()
        return latest_month.od_mort
    
    def get_latest_mort_100k(self):
        """
        latest mortality / pop * 100k
        we can rely on state.pop here because it's the latest we have
        """
        return self.get_latest_mort() / self.pop * 100000

    def set_mort_100k(self):
        # NOTE see https://github.com/mattkiefer/overdoses/issues/11
        self.mort_per_100k_202504 = state_mortality_per_100k(self)

    def set_pct_change_deaths(self):
        """
        takes a (year int, month str) combo 
        e.g. (2023, 'August') 
        defaults peak->latest
        i.e. 2023 August-> 2024 December.
        """
        # explicitly query the dates we're setting
        pct_chg_202308_202412 = state_pct_change_deaths(self, start=(2023,'August'), end=(2024,'December'))
        # NOTE verify we have data (i.e. LA, PA) and set 
        self.pct_chg_deaths_202308_202412 = pct_chg_202308_202412['pct_chg'] if pct_chg_202308_202412 else None
        # we have new data so let's use it
        pct_chg_202308_202504 = state_pct_change_deaths(self,start=(2023,'August'), end=(2025,'April'))
        self.pct_chg_deaths_202308_202504 = pct_chg_202308_202504['pct_chg'] if pct_chg_202308_202504 else None

    ##########
    # SAMHSA #
    ##########
    # National Survey on Drug Use and Health 
    # currently loading opioid use disorder
    # TODO check if this is the right outcome/indicator to use
    samhsa_nsduh = JSONField(null=True)



class StateYear(BaseModel):
    """
    everything we know about a state
    in a given year
    2015-2024
    """
    state = ForeignKeyField(State,backref='years')
    state_name = CharField()
    year = IntegerField()
    millenium = JSONField(null=True)
    pop = IntegerField(null=True) # pop changes over time so we use ACS 1-year B01003_001E
    od_mort = IntegerField(null=True)
    od_mort_100k = FloatField(null=True)
    nflis_fent_reports = IntegerField(null=True) # TODO verify loader

    class Meta:
        indexes = ((('state','year'),True),)

    def set_od_mort(self):
        """
        sets od mort total, per 100k
        but only if we have a full year
        """
        # check if we have year-end data ... otherwise we're skipping
        year_end = self.state.months.select().where(StateMonth.year==self.year,StateMonth.month=='December')
        year_end = year_end[0] if year_end else None
        if year_end:
            # this counts all 12 months of od deaths
            self.od_mort = year_end.od_mort # TODO check if this is set elsewhere
            # if we have mortality and population
            if self.od_mort and self.pop:
                # set mortality rate per 100k
                self.od_mort_100k = round(year_end.od_mort/self.pop*100000,1)

    def pct_chg_mort(self):
        """
        od mortality, 
        this year vs last year.
        decimal notation rounded
        """
        # search for a previous year
        prior_year = self.state.years.select().where(StateYear.year==self.year-1)
        # you either found one or you didn't
        prior_year = prior_year[0] if prior_year else None
        # if you have a prior year with data, and this year has data ...
        if prior_year and prior_year.od_mort and self.od_mort != None: # don't make null = 100% decrease
            # ...  you can return a pct change
            return round((self.od_mort-prior_year.od_mort)/float(prior_year.od_mort),3)




class StateMonth(BaseModel):
    """
    od death counts by State,
    by drug Indicator,
    and by Month (Jan 2015 - present)
    CDC overdose time series
    💾 data/VSRR_Provisional_Drug_Overdose_Death_Counts.csv
    🏗️ loader/cdc_vsrr_state_od_mort.py
    """
    state = ForeignKeyField(State,backref='months')
    state_name = CharField(null=True)
    year = IntegerField(null=True)
    month = CharField(null=True)
    month_no = IntegerField(null=True)
    vsrr = JSONField(null=True) # all indicator-level data from VSRR
    # these fields correspond to Indicator = 'Number of Drug Overdose Deaths'
    od_mort = IntegerField(null=True) # total od deaths, see above
    pct_complete = FloatField(null=True)
    pct_pending = FloatField(null=True)
    footnote = CharField(null=True) 
    footnote_symbol = CharField(null=True)

    # derived using state pop data
    od_mort_100k = FloatField(null=True)

    def set_mort_data(self):
        """
        extracts the vsrr indicator for 
        'Number of Drug Overdose Deaths', i.e. all deaths
        and sets a few values on the statemonth
        to avoid having to sort through nested json in statemonth.vsrr
        """
        all_ods = [x for x in self.vsrr if x['Indicator'] == 'Number of Drug Overdose Deaths'][0]
        self.od_mort = int(all_ods['Data Value'])
        self.pct_complete = all_ods['Percent Complete']
        self.pct_pending = all_ods['Percent Pending Investigation']
        self.footnote = all_ods['Footnote']
        self.footnote_symbol = all_ods['Footnote Symbol']
        self.od_mort_100k = float(all_ods['Data Value']) / self.state.years.filter(StateYear.year==self.year)[0].pop * 100000 if self.state.years.filter(StateYear.year==self.year)[0].pop else None


class SpecialState(State):
    """
    handles situations like NYC+NYS
    and potentially provinces/territories.
    these are functionally identical to canonical states but need to be stored separately
    """
    note = CharField(null=True)


class SpecialStateMonth(StateMonth):
    """
    handles situations like NYC+NYS
    and potentially provinces/territories.
    these are functionally identical to canonical statemonths but need to be stored separately
    """
    state = ForeignKeyField(SpecialState,backref='months')
    note = CharField(null=True)





class County(Locale):
    """
    ~3,144 counties, 
    each with 
    health, demographic, geographic, political data
    """

    ############
    # METADATA #
    ############

    state = ForeignKeyField(State)
    state_name = CharField()


    #######
    # CDC #
    #######

    pct_chg_deaths_202308_202412 = FloatField(null=True)


    def set_pct_change_deaths(self,start=(2023,8),end=(2024,12)):
        """
        start, end take
        (YYYY,MM) format
        e.g. start=(2023,8), end=(2024,12)
        """
        pct_chg = county_pct_change_deaths(self,start,end)
        # quality control - parameters must match attribute
        if pct_chg and (2023,8) in pct_chg.keys() and (2024,12) in pct_chg.keys():
            self.pct_chg_deaths_202308_202412 = pct_chg['pct_chg']

    
    mort_per_100k_202412 = FloatField(null=True)

    def set_mort_100k(self):
        self.mort_per_100k_202412 = county_mortality_per_100k(self)



class CountyMonth(BaseModel):
    """
    normalizes time series of 
    cdc overdose mortality
    """
    county = ForeignKeyField(County,backref='months')
    data_as_of = DateField(null=True)
    year = IntegerField()
    month = IntegerField()
    st_abbrev = CharField()
    state_name = CharField()
    county_name = CharField()
    fips = CharField()
    state_fips = CharField()
    county_fips = CharField()
    code_2013 = IntegerField()
    deaths = IntegerField(null=True)
    footnote = CharField(null=True)
    pct_pending = FloatField()
    completeness_note = CharField(null=True)
    month_ending = DateField()
    month_starting = DateField()
    end_date = DateField()
    
    # derived from pop
    mort_per_100k = FloatField(null=True)
    def set_mort_data(self):
        try:
            this_year_pop = CountyYear.get(county=self.county,year=self.year).pop
            self.mort_per_100k = float(self.deaths)/this_year_pop*100000 if self.deaths and this_year_pop else None
        except Exception as e:
            print(e)


class CountyYear(BaseModel):
    """
    everything we know about a county
    in a given year
    2020-2024
    """
    county = ForeignKeyField(County,backref='years')
    county_name = CharField()
    state_name = CharField()
    year = IntegerField()
    pop = IntegerField(null=True) # pop changes over time so we use Census PEP annual county estimates
    od_mort = IntegerField(null=True)
    od_mort_100k = FloatField(null=True)

    class Meta:
        indexes = ((('county','year'),True),)



    def set_od_mort(self):
        """
        sets od mort total, per 100k
        but only if we have a full year
        """
        # check if we have year-end data ... otherwise we're skipping
        year_end = self.county.months.select().where(CountyMonth.year==self.year,CountyMonth.month==12)
        year_end = year_end[0] if year_end else None
        if year_end:
            # this counts all 12 months of od deaths
            self.od_mort = year_end.deaths # TODO check if this is set elsewhere
            # if we have mortality and population
            if self.od_mort and self.pop:
                # set mortality rate per 100k
                self.od_mort_100k = round(year_end.deaths/self.pop*100000,1)


    def pct_chg_mort(self):
        """
        od mortality, 
        this year vs last year.
        decimal notation rounded
        """
        # search for a previous year
        prior_year = self.county.years.select().where(CountyYear.year==self.year-1)
        # you either found one or you didn't
        prior_year = prior_year[0] if prior_year else None
        # if you have a prior year with data, and this year has data ...
        if prior_year and prior_year.od_mort and self.od_mort != None: # don't make null = 100% decrease
            # ...  you can return a pct change
            return round((self.od_mort-prior_year.od_mort)/float(prior_year.od_mort),3)



