# <> Imports <> #
# - import variables from config.py - #
import config
# - import built in libraries - #
import os
import sys
from datetime import datetime
import glob
import concurrent.futures
from concurrent.futures import as_completed
# - Third party imports - #
import google.oauth2.service_account
from googleapiclient import discovery
from googleapiclient.errors import HttpError
import polars as pl


#== Logging == #
Log_File = 'Path\\To\\Log\\Log_File.txt'
sys.stdout = open(Log_File, 'a')
sys.stderr = sys.stdout
print(f'Starting Looker Sync at {datetime.now()}\n\n')

# <> Sheets Service class for defining the creds and taking sheets actions <> #
class Sheets_Service:
    def __init__(self):
        self.service = self.Get_Service()

    # <> Updated oauth2 method of creating service account credentials <> #
    def Get_Service(self):
        try:
            credential = google.oauth2.service_account.Credentials.from_service_account_file(
            config.key, scopes=config.scope)
            return discovery.build('sheets', 'v4', credentials=credential)
        except Exception as e:
            raise RuntimeError(f'Could not generate service credentials: {e}')
    
    # <> Function to clear the existing Google Sheet's contents <> #
    def Clear_Spreadsheet(self, file_id, sheet_name):
        print(f"Attempting to clear values for {file_id}")
        try:
            self.service.spreadsheets().values().batchClear(
                spreadsheetId=file_id,
                body={"ranges": f"{sheet_name}!A2:Z100000"}
            ).execute()
            print(f'Successfully cleared data in {file_id}')
        except Exception as e:
            print(f'Error deleting rows:\n {e}')

    # <> Function to update sheets in batches of 5000 rows <> #
    def Update_Sheet(self, file_id, sheet_name, df, chunk_size=5000):
        # - Get full row count to determine range - #
        row_count = len(df)
        run_count = 0
        # - Iterate through each 5000 row batch - #
        for batch in range(0, row_count, chunk_size):
            run_count += 1
            # - offset to preserve headers - #
            start_row = batch + 2
            # - Increment range each batch
            sheets_range = f'{sheet_name}!A{start_row}'
            # - Split each chunk starting at current batch with a length of 5000 - #
            print(f'Processing batch: {run_count} for {file_id[:6]}')
            chunk = df.slice(batch, chunk_size).rows()
            try:
                self.service.spreadsheets().values().update(
                    spreadsheetId=file_id,
                    range=sheets_range,
                    valueInputOption='RAW',
                    body={'values': chunk}
                ).execute()
            except HttpError as e:
                raise Exception(f'Terminating program... {e}')
        # - Completion message - #    
        print(f'Finished Updating {file_id} over {run_count} batches')
    
# - Read in the file with Polars to speed up the process - #
def Get_Content(file):
    print(f'Reading in {file}')
    # <> Account for xlsx files included <> #
    if '.xlsx' in file:
        try:
            contents_df = pl.read_excel(file,infer_schema_length=0)
        except FileNotFoundError as e:
            print(f'Could not open file: {e}')
            return None
        except PermissionError as e:
            print(f'Could not open file: {e}')
            return None
        except Exception as e:
            print(f'Failed to read file: {e}')
            return None
    # <> Account for csv files included <> #
    elif '.csv' in file:
        try:
            contents_df = pl.read_csv(file, encoding='utf8-lossy', truncate_ragged_lines=True, infer_schema_length=0)
        except FileNotFoundError as e:
            print(f'Could not open file: {e}')
            return None
        except PermissionError as e:
            print(f'Could not open file: {e}')
            return None
        except Exception as e:
            print(f'Failed to read file: {e}')
            return None
    # <> Check if nothing was read in <> #
    if contents_df.is_empty():
        raise Exception('Could not read in file - Closing program')   
    contents_df = contents_df.with_columns(pl.lit(datetime.now().strftime('%Y-%m-%d %H:%M')).alias('Last Updated'))
    return contents_df

# <> Process each file and clear spreadsheet and write new contents <> #
def Process_File(file):
    # - Create sheets service Object - #
    sheets_service = Sheets_Service()
    # - Get just the filename because the path has an underscore - #
    filename = file.split('\\')[5]
    # - Lazy delete old files - #
    if '_' in filename:
        print(f'Deleting {filename}')
        os.remove(file)
        return
    # - These two files have different sheet names all others are default - #
    if 'Sem1' in file: sheet_name = config.sem1
    elif 'Sem2' in file: sheet_name = config.sem2
    else: sheet_name = config.default
    
    # - Get FileID from filename, otherwise skip the file - #
    try:
        file_id = config.file_ids[filename]
    except KeyError:
        print(f'{filename} not in file_ids dict. Skipping file..')
        return

    # - Call function clear contents of the current_sheet - #
    sheets_service.Clear_Spreadsheet(file_id, sheet_name)
    contents_df = Get_Content(file)
    # - Confirm contents_df read in correctly - #
    if contents_df is None: return
    # - One weird column in this file - #
    if '6-Missing-Assignments' in file:
        print(f'Dropping "End Date" from 6-missing-assignments')
        contents_df = contents_df.drop('End Date')
    sheets_service.Update_Sheet(file_id, sheet_name, contents_df)

# <> Main function that writes data to Google Sheet files <> #
def Sheets_Builder():
    # - Create list of files - #
    files = [
        file 
        for path in config.file_paths 
        for file_type in ['*.csv', '*.xlsx']
        for file in glob.glob(os.path.join(path,file_type))
        ]
    # - Threadpool executor to concurrently process files - #
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(Process_File, file) for file in files]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f'File failed to process: {e}')

# <> Function calls <> #
if __name__ == '__main__':
    Sheets_Builder()
    print(f'Finishing run at {datetime.now()}\n\n')

