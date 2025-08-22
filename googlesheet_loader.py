import os
import logging
from functools import lru_cache
from typing import Optional, List
import pandas as pd
from google_auth import get_sheets_service  

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# Configure Google Sheet ID
SPREADSHEET_ID: Optional[str] = "1DnQTSfwymCfGzIjAr87L2rfhqMCC-aLjJcDYAR_XbWw"

# Configure required headers
REQUIRED_HEADERS: List[str] = [
    "Cell ID",
    "Working Electrode",
    "WE Active Material Mass (mg)",
]


# Create DataFrame from Sheet headers and normalize 
def _values_to_dataframe(values: list[list[str]]) -> pd.DataFrame:
    if not values:
        return pd.DataFrame()
    headers = [(h or "").replace("\n", " ").strip() for h in values[0]]
    rows = values[1:] if len(values) > 1 else []
    return pd.DataFrame(rows, columns=headers)

# Validate required headers are present
def _validate_headers(df: pd.DataFrame, initials: str) -> None:
    missing = [h for h in REQUIRED_HEADERS if h not in df.columns]
    if missing:
        raise KeyError(
            f"Missing required header(s) in tab '{initials}': {missing}. "
            f"Found columns: {list(df.columns)}"
        )

# Fetch and cache a tab as a DataFrame
@lru_cache(maxsize=64)
def _get_tab_df(initials: str) -> pd.DataFrame:
    if not SPREADSHEET_ID:
        raise ValueError("SPREADSHEET_ID is not set in googlesheet_loader.py")

    sheets = get_sheets_service()
    rng = f"'{initials}'!A:ZZ" 
    result = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=rng,
    ).execute()
    values = result.get("values", [])
    df = _values_to_dataframe(values)
    if df.empty:
        raise ValueError(f"Tab '{initials}' is empty or missing headers.")

    _validate_headers(df, initials)
    return df


# Load a cell's metadata from its initials tab
def load_googlesheet(cell_id: str) -> pd.Series:
    initials = cell_id[:3]
    df = _get_tab_df(initials)

    row = df.loc[df["Cell ID"] == cell_id]
    if row.empty:
        raise ValueError(f"Cell ID '{cell_id}' not found in tab '{initials}'.")

    s = row.iloc[0]
    s.index = s.index.str.strip()

    mass_key = "WE Active Material Mass (mg)"
    if mass_key in s.index:
        try:
            _ = float(str(s[mass_key]).strip())
        except Exception:
            log.warning(
                f"'{mass_key}' for {cell_id} is not numeric: {s[mass_key]!r}. "
                "Normalization will be skipped for this cell."
            )
    return s


# Validates:
    # Spreadsheet is reachable
    # At least one tab is readable and headers are normalized and complete
    # A specific cell row can be loaded
def _self_check() -> int:
    try:
        if not SPREADSHEET_ID:
            raise ValueError("SPREADSHEET_ID is not set.")

        svc = get_sheets_service()

        # Spreadsheet metadata
        meta = svc.spreadsheets().get(
            spreadsheetId=SPREADSHEET_ID,
            fields="sheets(properties(title))"
        ).execute()
        sheets_meta = meta.get("sheets", [])
        if not sheets_meta:
            raise RuntimeError("No tabs found in spreadsheet.")

        tab_titles = [s["properties"]["title"] for s in sheets_meta]
        initials = os.getenv("SELF_TEST_INITIALS") or tab_titles[0]

        log.info("=" * 40)
        log.info("🔍 Spreadsheet check")
        log.info("=" * 40)
        log.info(f"Spreadsheet reachable. Using tab '{initials}' for validation.\n")

        # Headers
        df = _get_tab_df(initials)
        log.info("-" * 40)
        log.info(f"📑 Headers for '{initials}'")
        log.info("-" * 40)
        for col in df.columns:
            log.info(f"  • {col}")
        log.info("")

        # Cell preview
        test_cell = os.getenv("SELF_TEST_CELL_ID")
        if not test_cell and "Cell ID" in df.columns and not df["Cell ID"].empty:
            test_cell = next((cid for cid in df["Cell ID"] if str(cid).strip()), None)

        if test_cell:
            s = load_googlesheet(test_cell)
            preview_keys = [
                "Cell ID",
                "Working Electrode",
                "WE Active Material Mass (mg)",
            ]
            preview = {k: (s[k] if k in s.index else "<missing>") for k in preview_keys}

            log.info("-" * 40)
            log.info(f"📋 Cell preview for '{test_cell}'")
            log.info("-" * 40)
            for k, v in preview.items():
                log.info(f"  {k:<30} {v}")
            log.info("")
        else:
            log.info("No test cell ID found; skipping row-level validation.\n")

        log.info("Googlesheet_loader validation PASSED.")
        return 0

    except Exception as e:
        log.error("Googlesheet_loader validation FAILED.")
        log.error(str(e))
        return 1


if __name__ == "__main__":
    raise SystemExit(_self_check())
