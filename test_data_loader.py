from data_loader import load_googlesheet, load_txt_data
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

import io
import os

# define variables
google_drive_folder_id = "1TdfI0rCaXMy1UVeob1uuuca_GKsBfgZi"
credentials_file = "credentials.json"

def download_first_txt_file_from_drive():
    # Authenticate
    creds = service_account.Credentials.from_service_account_file(
        credentials_file, scopes=["https://www.googleapis.com/auth/drive"]
    )
    service = build('drive', 'v3', credentials=creds)

    # Search for .txt files in the specified folder
    query = f"'{google_drive_folder_id}' in parents and mimeType='text/plain'"
    results = service.files().list(q=query, fields="files(id, name)", pageSize=1).execute()
    files = results.get('files', [])
    if not files:
        raise ValueError("No .txt files found in the specified Google Drive folder.")
    
    file = files[0]
    file_id = file['id']
    file_name = file['name']
    print(f"Downloading file: {file_name}")

    # Download the file
    fh = io.BytesIO()
    request = service.files().get_media(fileId=file_id)
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
        print(f"Download progress: {downloader.progress() * 100:.2f}%")


    # Save to local file
    fh.seek(0)
    with open(file_name, 'wb') as f:
        f.write(fh.getbuffer())
    print(f"File downloaded and saved as: {file_name}")
    return file_name

def main():
    # Download the first .txt file from Google Drive
    txt_filename = download_first_txt_file_from_drive()

    # Extract cell ID from filename
    cell_id = txt_filename.split('_')[0]  # e.g., MEN0001
    print(f"Extracted Cell ID: {cell_id}")

    # Load Google Sheet data
    metadata = load_googlesheet(cell_id)
    print("\nMetadata for Cell ID:", cell_id)
    print(metadata.to_frame().T)

    # Load .txt data
    df_txt = load_txt_data(txt_filename)
    print("\nFirst 5 rows of the .txt data:")
    print(df_txt.head())

if __name__ == "__main__":
    main()