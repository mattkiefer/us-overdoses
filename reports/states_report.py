import csv
from models import State
from utils.format import round_float
from reports.runner import write_upload

### START CONFIG ###
outfile_name = 'states_report.csv'
### END CONFIG ###

def run():
    """
    run the county report,
    upload to drive,
    """
    # store output data
    rows = []

    # roll thru all counties
    for state in State.select():

        # build row 
        try:
            row = {'fips': state.fips,
                   'state': state.name,
                   'pop': state.insurance_b27010['B27010001'] if state.insurance_b27010 else None, # "Total" via metadata.json for 5-year ACS TODO better pop source?
                   'mort_per_100k': round_float(state.get_latest_mort_100k()),
                   'pct_chg_deaths_202308_202412': round_float(state.pct_chg_deaths_202308_202412),
                   'pct_medicaid_19-64': round_float(state.pct_medicaid_19_64),
                   'medicaid_expansion': state.kff_aca_exp['Status of Medicaid Expansion Decision'],
                   'cdc_buprenorphine_disp_rate': state.cdc_buprenorphine_disp_rate,
                   'cdc_naloxone_disp_rate': state.cdc_naloxone_disp_rate,
                   }
        except Exception as e:
            print(e)
            import ipdb; ipdb.set_trace()
        # will write later
        rows.append(row)

    # sort by mort per 100k desc
    rows.sort(key=lambda row: (row['mort_per_100k'] if row['mort_per_100k'] is not None else float('inf')),reverse=True)

    # write to reports/output, replace gdrive file
    write_upload(rows,outfile_name)
