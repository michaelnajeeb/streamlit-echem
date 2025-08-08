from googleapiclient.discovery import build
from google.oauth2 import service_account
from typing import Dict

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SERVICE_ACCOUNT_FILE = 'credentials.json'
FOLDER_ID = '1TdfI0rCaXMy1UVeob1uuuca_GKsBfgZi'

def authorize_drive():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build('drive', 'v3', credentials=creds)

def get_available_cell_ids_from_drive() -> Dict[str, Dict[str, str]]:
    service = authorize_drive()
    cell_map = {}

    query = f"'{FOLDER_ID}' in parents and mimeType='text/plain' and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])

    for file in items:
        filename = file['name']
        if not filename.endswith('.txt'):
            continue
        parts = filename.split("_")
        if len(parts) > 0:
            cell_id = parts[0]
            if cell_id not in cell_map:
                cell_map[cell_id] = {
                    'file_id': file['id'],
                    'filename': filename
                }

    return cell_map
