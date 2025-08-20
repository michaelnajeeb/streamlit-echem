import os
import logging
from functools import lru_cache
from typing import List, Optional, Tuple
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
log = logging.getLogger(__name__)

# Credentials file path
SERVICE_ACCOUNT_FILE: str = os.getenv("SERVICE_ACCOUNT_FILE", "credentials.json")

# Google Drive/Sheets API read-only scopes
REQUIRED_SCOPES: List[str] = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

DEFAULT_SPREADSHEET_ID: Optional[str] = "1DnQTSfwymCfGzIjAr87L2rfhqMCC-aLjJcDYAR_XbWw"

def _load_credentials():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(
            f"Service account file not found at '{SERVICE_ACCOUNT_FILE}'."
            "Set SERVICE_ACCOUNT_FILE env variable or place credentials.json in project root."
            )
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=REQUIRED_SCOPES
    )
    # Check if scopes are set
    scopes = set(creds.scopes or [])
    missing = [s for s in REQUIRED_SCOPES if s not in scopes]
    if missing:
        raise PermissionError(
            f"Credentials missing required scopes: {missing}."
            f"Current scopes: {sorted(scopes)}."
        )
    return creds

# Build API service with credentials
def _build_service(api_name: str, api_version: str):
    creds = _load_credentials()
    try:
        return build(api_name, api_version, credentials=creds, cache_discovery=False)
    except Exception as e:
        raise RuntimeError(f"Failed to build {api_name} v{api_version} client: {e}") from e

# Return cached Google Drive Client    
@lru_cache(maxsize=None)
def get_drive_service():
    """Return a cached Google Drive v3 client."""
    return _build_service("drive", "v3")

# Return cached Google Sheets Client
@lru_cache(maxsize=None)
def get_sheets_service():
    """Return a cached Google Sheets v4 client."""
    return _build_service("sheets", "v4")

# Try to validate Google Drive access
def validate_drive_access() -> Tuple[bool, str]:
    try: 
        svc = get_drive_service()
        _ = svc.files().list(pageSize=1, fields="files(id)").execute()
        return True, "Google Drive access validated."
    except Exception as e:
        return False, f"Google Drive access failed: {e}"
    
# Try to validate Google Sheets access
def validate_sheets_access(spreadsheet_id: Optional[str]=None) -> Tuple[bool, str]:
    try:
        svc = get_sheets_service()
    except Exception as e:
        return False, f"Google Sheets client build failed: {e}"
    
    sid = spreadsheet_id or DEFAULT_SPREADSHEET_ID
    try:
        # Fetch Cell A1
        _ = svc.spreadsheets().values().get(spreadsheetId=sid, range="A1").execute()
        return True, f"Google Sheets access validated."
    except Exception as e:
        return False, f"Google Sheets access failed: {e}"
    
# CLI check
if __name__ == "__main__":
    log.info("Running google_auth self-check...")
    log.info(f"Using SERVICE_ACCOUNT_FILE: {SERVICE_ACCOUNT_FILE}")

    ok_drive, msg_drive = validate_drive_access()
    log.info(msg_drive)

    ok_sheets, msg_sheets = validate_sheets_access()
    log.info(msg_sheets)

    if ok_drive and ok_sheets:
        log.info("Google auth validation PASSED.")
    else:
        log.error("Google auth validation FAILED.")
        raise SystemExit(1)