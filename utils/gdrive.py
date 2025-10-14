from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from googleapiclient.discovery import build
import os, csv, webbrowser


def get_number_format_requests(data):
    """
    Analyzes data columns and returns a list of requests to format them as numbers.
    """
    if not data or not data[0]:
        return []

    requests = []
    # Assumes the first row is headers, so we start from the second row
    for col_index in range(len(data[0])):
        is_float = False
        is_number = True
        for row_index in range(1, len(data)):
            try:
                # Check if the cell can be a number
                value = data[row_index][col_index]
                if value and value.strip():
                    if '.' in value:
                        float(value)
                        is_float = True
                    else:
                        int(value)
            except (ValueError, IndexError):
                is_number = False
                break  # Not a number, move to the next column

        if is_number:
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': 0,
                        'startRowIndex': 1,
                        'startColumnIndex': col_index,
                        'endColumnIndex': col_index + 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'NUMBER',
                                'pattern': '#,##0.00' if is_float else '#,##0'
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat'
                }
            })
    return requests

def upload_csv_to_sheet(filepath, folder_id=open('utils/gdrive_folder').read().strip()):
    """
    Finds or creates a Google Sheet and updates its content with CSV data.
    Returns the URL of the Google Sheet.
    """
    print(filepath)
    # set up auth
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile("creds")
    if gauth.credentials is None:
        # No creds, need to authenticate
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        try:
            # Try to refresh using the stored refresh_token
            gauth.Refresh()
        except Exception:
            # If refresh fails (e.g. revoked), do full re-auth
            gauth.LocalWebserverAuth()
    else:
        # Credentials good, proceed as normal
        gauth.Authorize()

    drive = GoogleDrive(gauth)
    
    # check if file exists on drive
    filename = os.path.basename(filepath).replace('.csv','')
    query = f"title='{filename}' and trashed=false and mimeType='application/vnd.google-apps.spreadsheet'"
    if folder_id:
        query += f" and '{folder_id}' in parents"
    
    file_list = drive.ListFile({'q': query}).GetList()

    # replace contents of first matching file found ...
    if file_list:
        sheet_id = file_list[0]['id']
        url = file_list[0]['alternateLink']
    # ... or else create one new
    else:
        new_sheet = drive.CreateFile({
            'title': filename,
            'mimeType': 'application/vnd.google-apps.spreadsheet',
            'parents': [{'id': folder_id}] if folder_id else []
        })
        new_sheet.Upload()
        sheet_id = new_sheet['id']
        url = new_sheet['alternateLink']

    # build the spreadsheet from the csv data
    sheets_service = build('sheets', 'v4', credentials=gauth.credentials)
    with open(filepath, 'r') as f:
        data = list(csv.reader(f))

    """
    # clear old data first
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=sheet_id, range='Sheet1'
    ).execute()
    """

    # then add the new data
    sheets_service.spreadsheets().values().update(
        spreadsheetId=sheet_id, range='Sheet1!A1',
        valueInputOption='USER_ENTERED', body={'values': data}
    ).execute()
  
    # Get the number format requests
    number_format_requests = get_number_format_requests(data)

    # formatting
    updates = [
        # freeze first row
        {
            'updateSheetProperties': {
                'properties': {
                    'sheetId': 0,
                    'gridProperties': {'frozenRowCount': 1}
                },
                'fields': 'gridProperties.frozenRowCount'
            }
        },
    ]

    # Add the new number formatting requests
    updates.extend(number_format_requests)

    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={'requests': updates}
    ).execute()



    # collect your file
    print(url)
    webbrowser.open(url)
    return url
