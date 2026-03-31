# - Dict of FileIds. Key = File Name : Value = Google Sheet FileID - #
file_ids = {
    'File1.csv.example': 'FileID1',
    'File2.csv.example': 'FileID2', #so on and so forth
}
# - Various folders to look through to find files in the file_ids dict - #
file_paths = [
    'List of paths to folders'
]
# - Google API Scope - #
scope = ['https://www.googleapis.com/auth/spreadsheets']
# - API Service Account Key - #
key = 'Path\\To\\Service\\Key.json'

# <> Sheet Names <> #
default= 'IMPORT'
sem1 = 'IMPORT_SEM1'
sem2 = 'IMPORT_SEM2'