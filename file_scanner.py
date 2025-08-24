import re
from typing import Dict, Optional

from google_auth import get_drive_service

# Define Google Driver folder ID where .txt files are stored
FOLDER_ID = "1TdfI0rCaXMy1UVeob1uuuca_GKsBfgZi"

# Pattern: start-of-string, initials (>=2 letters), digits (>=4), and finally underscore
CELL_ID_RE = re.compile(r"^([A-Za-z]{2,}\d{4,})_")

# Extract 'CellID' from .txt filename
def _extract_cell_id(filename: str) -> Optional[str]:
    m = CELL_ID_RE.match(filename)
    return m.group(1) if m else None

# Get available cell IDs from Google Drive folder
# Returns dictionary:
# {
#   cell_id: {
#     "file_id": <Drive file id>,
#     "filename": <file name>,
#     "modifiedTime": <RFC3339 string>,
#     "size": <string of bytes>
#   },
# }     
def get_available_cell_ids_from_drive() -> Dict[str, Dict[str, str]]:
    service = get_drive_service()
    cell_map: Dict[str, Dict[str, str]] = {}

    query = f"'{FOLDER_ID}' in parents and trashed = false"
    fields = "nextPageToken, files(id, name, modifiedTime, size)"
    page_token = None
    # Paginates 1000 results per page 
    while True:
        resp = service.files().list(
            q=query,
            fields=fields,
            orderBy="modifiedTime desc",
            pageSize=1000,
            pageToken=page_token,
        ).execute()

        for file in resp.get("files", []):
            filename = file["name"]
            if not filename.endswith(".txt"):
                continue  # Skip non-txt files

            cell_id = _extract_cell_id(filename)
            if not cell_id:
                print(f"Skipping file with invalid Cell ID format: {filename}")
                continue

            # If cell_id already exists, use most recent file or ignore older duplicates
            if cell_id not in cell_map:
                cell_map[cell_id] = {
                    "file_id": file["id"],
                    "filename": filename,
                    "modifiedTime": file.get("modifiedTime", ""),
                    "size": file.get("size", ""),
                }
            

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return cell_map
