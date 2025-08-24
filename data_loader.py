import io
import logging
from typing import Dict, Any
import pandas as pd
from googleapiclient.http import MediaIoBaseDownload
from google_auth import get_drive_service
from googlesheet_loader import load_googlesheet

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# Normalize headers
def _log_headers(df: pd.DataFrame, title: str) -> None:
    log.info("-" * 40)
    log.info(f"📑 Headers for {title}")
    log.info("-" * 40)
    for col in df.columns:
        log.info(f"  • {col}")
    log.info("")

# Ensure capacity column exists and is numeric
def _validate_capacity_column(df: pd.DataFrame, capacity_col: str) -> None:
    if capacity_col not in df.columns:
        raise KeyError(f"Missing '{capacity_col}' in raw data. Available: {list(df.columns)}")

    pd.to_numeric(df[capacity_col], errors="raise")

# Normalize capacity by WE Active Material Mass (mg)
def add_normalized_capacity(
    raw_df: pd.DataFrame,
    metadata: dict,
    capacity_col: str = "Capacity/mA.h",
    mass_key: str = "WE Active Material Mass (mg)",
) -> pd.DataFrame:

    # Validate capacity column presence and numeric
    _validate_capacity_column(raw_df, capacity_col)

    # Validate mass presence and numeric and > 0
    if mass_key not in metadata:
        raise KeyError(f"Missing '{mass_key}' in metadata for normalization.")
    try:
        mass_mg = float(str(metadata[mass_key]).strip())
    except (TypeError, ValueError) as e:
        raise ValueError(f"Mass value '{metadata[mass_key]}' for '{mass_key}' is not numeric.") from e
    if mass_mg <= 0:
        raise ValueError("Mass must be > 0 mg to normalize capacity.")

    mass_g = mass_mg / 1000.0  # Mass unit conversion

    # Convert capacity strictly
    cap = pd.to_numeric(raw_df[capacity_col], errors="raise")

    # Disallow NaN or negative capacities
    assert cap.notna().all(), "Capacity column contains NaN values after numeric conversion."
    assert (cap >= 0).all(), "Capacity column contains negative values (unexpected)."

    # Compute normalized capacity
    raw_df["Normalized Capacity (mAh/g)"] = cap / mass_g

    # Preview last 3 rows of raw & normalized with row index and WE mass in grams
    tail_df = raw_df[[capacity_col, "Normalized Capacity (mAh/g)"]].tail(3)
    log.info(f"  ↳ Using WE Active Material Mass: {mass_g} g")
    log.info("  ↳ Capacity & Normalized Capacity preview (last 3 rows):")
    for idx, row in tail_df.iterrows():
        log.info(f"     Row {idx}: Capacity={row[capacity_col]} mAh, "
                 f"Normalized={row['Normalized Capacity (mAh/g)']} mAh/g")
    log.info("")

    return raw_df


# Download and parse .txt file
def download_and_parse_txt_file(file_id: str) -> pd.DataFrame:
    service = get_drive_service()
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _status, done = downloader.next_chunk()

    text = fh.getvalue().decode("utf-8", errors="replace")
    df = pd.read_csv(io.StringIO(text), sep="\t", engine="c", low_memory=False)
    df.columns = df.columns.str.strip()
    df.dropna(how="all", inplace=True)
    return df


# Load all cell data from Google Drive and Google Sheets, validate, normalize, and return structured dictionary
def load_all_cell_data(cell_file_map: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}

    for cell_id, file_info in cell_file_map.items():
        file_id = file_info.get("file_id")
        if not file_id:
            raise ValueError(f"{cell_id}: missing file_id in map.")

        # Load raw .txt
        raw_data = download_and_parse_txt_file(file_id)
        _log_headers(raw_data, f"{cell_id} (.txt)")

        # Load metadata row from Google Sheets and convert to dictionary
        metadata_series = load_googlesheet(cell_id)
        metadata = metadata_series.to_dict()

        # Perform mandatory normalization
        add_normalized_capacity(raw_data, metadata)

        # Store results
        result[cell_id] = {"raw_data": raw_data, "metadata": metadata}
        log.info(f"Loaded & normalized: {cell_id}\n")

    return result


# CLI check
if __name__ == "__main__":
    try:
        from file_scanner import get_available_cell_ids_from_drive

        cell_map = get_available_cell_ids_from_drive()
        print(f"🔍 Found {len(cell_map)} .txt files in Drive folder.\n")

        cell_data = load_all_cell_data(cell_map)

        # Summary
        for cell_id, data in cell_data.items():
            meta = data["metadata"]
            df = data["raw_data"]
            we = meta.get("Working Electrode", "N/A")
            print(f"{cell_id}: {we} | {len(df)} rows loaded | columns: {len(df.columns)}")

        print("\n data_loader run PASSED.")

    except Exception as e:
        print(f" data_loader run FAILED: {e}")
        raise SystemExit(1)
