# =============================================================================
# 03_parse_extractions.py
# Parses JSON text files produced by 02_extract.py into four flat CSVs:
#   data/incidents_extract.csv
#   data/plaintiffs_extract.csv
#   data/defendants_extract.csv
#   data/harms_extract.csv
#
# Reads:  data/extracted/<model>/*.txt
#         data/filtered_texts.csv  (for case/document ID look-up)
# =============================================================================

import json
import re
import uuid
import pandas as pd
from pathlib import Path

# ----------------------------- PATHS -----------------------------------------

EXTRACT_DIR   = Path("data/extracted/openai/")   # Change model folder as needed
FILTERED_CSV  = Path("data/filtered_texts.csv")
OUT_DIR       = Path("data/")

# ----------------------------- HELPERS ---------------------------------------

def get_file_id(filename: str) -> str:
    """Extract the 32-char MD5 file_id from the filename prefix."""
    match = re.match(r"([a-f0-9]{32})", Path(filename).name)
    return match.group(1) if match else ""


def parse_extraction(filepath: str) -> list[dict]:
    """Read a text file, strip markdown fences, and return a list of incident dicts."""
    with open(filepath, "r", encoding="utf-8") as f:
        raw = f.read().strip()
    raw  = re.sub(r"^```(?:json)?\s*", "", raw)
    raw  = re.sub(r"\s*```$",          "", raw)
    data = json.loads(raw)
    return [data] if isinstance(data, dict) else data


def extraction_to_tables(filepath: str):
    """Convert one extraction file into four DataFrames."""
    incidents_rows, plaintiffs_rows, defendants_rows, harms_rows = [], [], [], []
    file_id = get_file_id(filepath)

    for inc in parse_extraction(filepath):
        iid = str(uuid.uuid4())

        incidents_rows.append({
            "incident_uuid":   iid,
            "file_id":         file_id,
            "incident_id":     inc.get("incident_id", ""),
            "location_street": inc.get("location_street", ""),
            "location_city":   inc.get("location_city", ""),
            "location_county": inc.get("location_county", ""),
            "location_state":  inc.get("location_state", ""),
            "location_zip":    inc.get("location_zip", ""),
            "location_type":   inc.get("location_type", ""),
        })

        for p in inc.get("plaintiffs", []):
            plaintiffs_rows.append({
                "plaintiff_uuid":      str(uuid.uuid4()),
                "incident_uuid":       iid,
                "file_id":             file_id,
                "plaintiff_id":        p.get("plaintiff_id", ""),
                "name":                p.get("name", ""),
                "race":                p.get("race", ""),
                "gender":              p.get("gender", ""),
                "disability_status":   p.get("disability_status", ""),
                "immigration_status":  p.get("immigration_status", ""),
                "plaintiff_compliance": p.get("plaintiff_compliance", ""),
            })

        for d in inc.get("defendants", []):
            defendants_rows.append({
                "defendant_uuid":   str(uuid.uuid4()),
                "incident_uuid":    iid,
                "file_id":          file_id,
                "defendant_id":     d.get("defendant_id", ""),
                "name":             d.get("name", ""),
                "race":             d.get("race", ""),
                "gender":           d.get("gender", ""),
                "doe_status":       d.get("doe_status", ""),
                "entity_type":      d.get("entity_type", ""),
                "agency":           d.get("agency", ""),
                "agency_type":      d.get("agency_type", ""),
                "role_in_incident": d.get("role_in_incident", ""),
            })

        for h in inc.get("harms", []):
            for harm_type in h.get("type", "").split(";"):
                if harm_type.strip():
                    harms_rows.append({
                        "harm_uuid":                str(uuid.uuid4()),
                        "incident_uuid":            iid,
                        "file_id":                  file_id,
                        "harm_description":         h.get("harm_description", ""),
                        "harm_type":                harm_type.strip(),
                        "associated_plaintiff_ids": h.get("associated_plaintiff_ids", ""),
                        "associated_defendant_ids": h.get("associated_defendant_ids", ""),
                    })

    return (
        pd.DataFrame(incidents_rows),
        pd.DataFrame(plaintiffs_rows),
        pd.DataFrame(defendants_rows),
        pd.DataFrame(harms_rows),
    )

# ----------------------------- LOAD FOLDER -----------------------------------

def load_folder(folder: Path):
    all_incidents, all_plaintiffs, all_defendants, all_harms, failed = [], [], [], [], []

    for txt_file in sorted(folder.glob("*.txt")):
        try:
            inc, pla, defs, har = extraction_to_tables(str(txt_file))
            for df in [inc, pla, defs, har]:
                df.insert(0, "source_file", txt_file.name)
            all_incidents.append(inc)
            all_plaintiffs.append(pla)
            all_defendants.append(defs)
            all_harms.append(har)
        except Exception as e:
            failed.append({"file": txt_file.name, "error": str(e)})

    incidents  = pd.concat(all_incidents,  ignore_index=True)
    plaintiffs = pd.concat(all_plaintiffs, ignore_index=True)
    defendants = pd.concat(all_defendants, ignore_index=True)
    harms      = pd.concat(all_harms,      ignore_index=True)
    failed_df  = pd.DataFrame(failed)

    print(f"Loaded {len(incidents)} incidents from {len(all_incidents)} files  |  {len(failed_df)} failed")
    return incidents, plaintiffs, defendants, harms, failed_df

# ----------------------------- ID ATTACHMENT ---------------------------------

def attach_ids(df: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    """Join document_id and case_id onto any table that has file_id."""
    return df.merge(lookup, on="file_id", how="left")

# ----------------------------- MAIN ------------------------------------------

if __name__ == "__main__":
    incidents, plaintiffs, defendants, harms, failed = load_folder(EXTRACT_DIR)

    lookup = (
        pd.read_csv(FILTERED_CSV, dtype=str)[["file_id", "document_id", "case_id"]]
        .drop_duplicates()
    )

    incidents  = attach_ids(incidents,  lookup)
    plaintiffs = attach_ids(plaintiffs, lookup)
    defendants = attach_ids(defendants, lookup)
    harms      = attach_ids(harms,      lookup)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    incidents .to_csv(OUT_DIR / "incidents_extract.csv",  index=False)
    plaintiffs.to_csv(OUT_DIR / "plaintiffs_extract.csv", index=False)
    defendants.to_csv(OUT_DIR / "defendants_extract.csv", index=False)
    harms     .to_csv(OUT_DIR / "harms_extract.csv",      index=False)

    print(
        f"Saved: {len(incidents)} incidents | {len(plaintiffs)} plaintiffs | "
        f"{len(defendants)} defendants | {len(harms)} harms"
    )

    if len(failed) > 0:
        failed.to_csv(OUT_DIR / "failed_extractions.csv", index=False)
        print(f"{len(failed)} failed files → data/failed_extractions.csv")
