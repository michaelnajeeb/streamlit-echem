import pandas as pd
import gspread
from typing import Union, IO
from oauth2client.service_account import ServiceAccountCredentials

# Constant variables
googlesheet_name = "(DO NOT USE) SANDBOX Griffith Group Cell Tracker"
credentials_file = "credentials.json"

# 1. Authorize Google Sheets client
def authorize_google() -> gspread.Client:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
    return gspread.authorize(creds)

# 2. Load metadata row from correct tab using Cell ID and initials
def load_googlesheet(cell_id: str) -> pd.Series:
    initials = cell_id[:3]  # e.g., MEN from MEN0001
    client = authorize_google()
    sheet = client.open(googlesheet_name)
    try:
        worksheet = sheet.worksheet(initials)
    except gspread.exceptions.WorksheetNotFound:
        raise ValueError(f"No worksheet named '{initials}' found in Google Sheet.")

    data = worksheet.get_all_records()
    df = pd.DataFrame(data)

    match = df[df["Cell ID"] == cell_id]
    if match.empty:
        raise ValueError(f"Cell ID '{cell_id}' not found in tab '{initials}'.")
    return match.iloc[0]

# 3. Load and clean .txt file
def load_txt_data(txt_file: Union[str, IO]) -> pd.DataFrame:
    df = pd.read_csv(txt_file, sep='\t', engine="python")
    # Standardize column names
    df.columns = df.columns.str.strip()

    #drop empty rows
    df.dropna(how='all', inplace=True)
    return df
