import csv
from models import County
#from reports.gdrive import upload_to_drive

### START CONFIG ###
outfile_dir = 'reports/output/'
start='2023-8'
end='2024-12'
outfile_path = outfile_dir + 'county_deaths_pct_chg_' + start + '_' + end + '.csv'
outfile = open(outfile_path,'w')
### END CONFIG ###

# get all counties
counties = County.select()

# store output data
rows = []

# roll thru
for county in counties:
    # pct change in ods
    county_pct_chg = county.get_pct_change(start,end)
    # build row 
    row = {'county':county.name,
           'state': county.state.name,
           start: county_pct_chg[start],
           end: county_pct_chg[end],
           'pct_chg_od_mortality': county_pct_chg['pct_chg'],
           'pct_medicaid_19-64__b27010': county.pct_medicaid_19_64__b27010,
           }
    # will write later
    rows.append(row)

# setup output file
headers = row.keys()
outcsv = csv.DictWriter(outfile,headers)
outcsv.writeheader()
outcsv.writerows(rows)
outfile.close()

# upload
# upload_to_drive(outfile_path)
