# Sheets_Updater
Script used to concurrently process and upload data from multiple files to Google Sheets via Sheets API. 
### Tech Stack 
Python - Google Sheets API - conccurent.futures.ThreadPoolExecutor - Polars

A group at our district needed files exported from our SIS to be uploaded to Google Sheets to be used in Google Looker Studio to create a dashboard. Files are saved in various locations so created a list of folders to look into and a dict mapping of filenames to Google Sheet SpreadsheetId mappings. Iterates through each folder and file and if it is in the dict it will read it in using Polars and add a column for the last updated time. A Sheets_Service class is used to create the service credentials, clear existing data from the spreadsheet and then write new data.

concurrent.futures.ThreadPoolExecutor is used to process up to 4 files at a time (maxworkers = 4). Because each file is processed and written to Sheets independently it's a perfect way to increase the speed of the script. 
