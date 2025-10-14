import csv
from models import County
from utils.format import round_float
from reports.runner import write_upload

### START CONFIG ###
outfile_name = 'counties_report.csv'
### END CONFIG ###

def run():
    """
    run the county report,
    upload to drive,
    """
    # store output data
    rows = []

    # roll thru all counties
    for county in County.select():

        # build row 
        row = {'fips': county.fips,
               'county': county.name,
               'state': county.state.name,
               'pop': county.insurance_b27010['B27010001'] if county.insurance_b27010 else None, # "Total" via metadata.json for 5-year ACS TODO better pop source?
               'mort_per_100k': round_float(county.mort_per_100k_202412),
               'pct_chg_deaths_202308_202412': round_float(county.pct_chg_deaths_202308_202412),
               'pct_medicaid_19-64': round_float(county.pct_medicaid_19_64),
               }
        # will write later
        rows.append(row)

    # sort by mort per 100k desc
    rows.sort(key=lambda row: (row['mort_per_100k'] if row['mort_per_100k'] is not None else float('inf')),reverse=True)

    # write to reports/output, replace gdrive file 
    write_upload(rows,outfile_name) 
