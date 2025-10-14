import csv
from models import County, State
from utils.gdrive import upload_csv_to_sheet
from utils.format import round_float
from settings import reports_output_dir

def write_upload(rows, outfile_name):
    """
    save to {settings.reports_output_dir} + {outfile_name},
    upload to drive,
    """
    # get headers from data
    headers = rows[0].keys()

    # setup output file
    outfile_path = reports_output_dir + outfile_name
    with open(outfile_path,'w') as outfile:
        outcsv = csv.DictWriter(outfile,headers)
        outcsv.writeheader()
        outcsv.writerows(rows)

    # upload to drive
    upload_csv_to_sheet(outfile_path)
