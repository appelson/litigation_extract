# =============================================================================
# 03_parse_extractions.py
# =============================================================================

import os
import json
import re
import uuid
import pandas as pd
from pathlib import Path

# ----------------------------- CONFIG ----------------------------------------

with open("config.json") as f:
    cfg = json.load(f)

DATA_DIR      = cfg["paths"]["data_dir"]
EXTRACT_DIR   = Path(cfg["paths"]["extract_dir"]) / cfg["parameters"]["extract_model"]
FILTERED_CSV  = os.path.join(DATA_DIR, "filtered_texts.csv")

# ----------------------------- HELPERS ---------------------------------------

def get_file_id(filename):
    match = re.match(r"([a-f0-9]{32})", Path(filename).name)
    return match.group(1) if match else ""

def parse_extraction(filepath):
    with open(filepath, encoding="utf-8") as f:
        raw = f.read().strip()
    raw  = re.sub(r"^```(?:json)?\s*", "", raw)
    raw  = re.sub(r"\s*```$",          "", raw)
    data = json.loads(raw)
    return [data] if isinstance(data, dict) else data

def extraction_to_tables(filepath):
    incidents_rows, plaintiffs_rows, defendants_rows, harms_rows = [], [], [], []
    file_id = get_file_id(filepath)
    for inc in parse_extraction(filepath):
        iid = str(uuid.uuid4())
        incidents_rows.append({
            "incident_uuid": iid, "file_id": file_id,
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
                "plaintiff_uuid": str(uuid.uuid4()), "incident_uuid": iid, "file_id": file_id,
                "plaintiff_id": p.get("plaintiff_id", ""), "name": p.get("name", ""),
                "race": p.get("race", ""), "gender": p.get("gender", ""),
                "disability_status":    p.get("disability_status", ""),
                "immigration_status":   p.get("immigration_status", ""),
                "plaintiff_compliance": p.get("plaintiff_compliance", ""),
            })
        for d in inc.get("defendants", []):
            defendants_rows.append({
                "defendant_uuid": str(uuid.uuid4()), "incident_uuid": iid, "file_id": file_id,
                "defendant_id": d.get("defendant_id", ""), "name": d.get("name", ""),
                "race": d.get("race", ""), "gender": d.get("gender", ""),
                "doe_status": d.get("doe_status", ""), "entity_type": d.get("entity_type", ""),
                "agency": d.get("agency", ""), "agency_type": d.get("agency_type", ""),
                "role_in_incident": d.get("role_in_incident", ""),
            })
        for h in inc.get("harms", []):
            for harm_type in h.get("type", "").split(";"):
                if harm_type.strip():
                    harms_rows.append({
                        "harm_uuid": str(uuid.uuid4()), "incident_uuid": iid, "file_id": file_id,
                        "harm_description":         h.get("harm_description", ""),
                        "harm_type":                harm_type.strip(),
                        "associated_plaintiff_ids": h.get("associated_plaintiff_ids", ""),
                        "associated_defendant_ids": h.get("associated_defendant_ids", ""),
                    })
    return (pd.DataFrame(incidents_rows), pd.DataFrame(plaintiffs_rows),
            pd.DataFrame(defendants_rows), pd.DataFrame(harms_rows))

# ----------------------------- MAIN ------------------------------------------

if __name__ == "__main__":
    all_i, all_p, all_d, all_h, failed = [], [], [], [], []

    for txt in sorted(EXTRACT_DIR.glob("*.txt")):
        try:
            i, p, d, h = extraction_to_tables(str(txt))
            for df in [i, p, d, h]:
                df.insert(0, "source_file", txt.name)
            all_i.append(i); all_p.append(p); all_d.append(d); all_h.append(h)
        except Exception as e:
            failed.append({"file": txt.name, "error": str(e)})

    incidents  = pd.concat(all_i, ignore_index=True)
    plaintiffs = pd.concat(all_p, ignore_index=True)
    defendants = pd.concat(all_d, ignore_index=True)
    harms      = pd.concat(all_h, ignore_index=True)

    lookup = pd.read_csv(FILTERED_CSV, dtype=str)[["file_id", "document_id", "case_id"]].drop_duplicates()
    incidents  = incidents .merge(lookup, on="file_id", how="left")
    plaintiffs = plaintiffs.merge(lookup, on="file_id", how="left")
    defendants = defendants.merge(lookup, on="file_id", how="left")
    harms      = harms     .merge(lookup, on="file_id", how="left")

    os.makedirs(DATA_DIR, exist_ok=True)
    incidents .to_csv(os.path.join(DATA_DIR, "incidents_extract.csv"),  index=False)
    plaintiffs.to_csv(os.path.join(DATA_DIR, "plaintiffs_extract.csv"), index=False)
    defendants.to_csv(os.path.join(DATA_DIR, "defendants_extract.csv"), index=False)
    harms     .to_csv(os.path.join(DATA_DIR, "harms_extract.csv"),      index=False)

    print(f"Saved: {len(incidents)} incidents | {len(plaintiffs)} plaintiffs | "
          f"{len(defendants)} defendants | {len(harms)} harms | {len(failed)} failed")

    if failed:
        pd.DataFrame(failed).to_csv(os.path.join(DATA_DIR, "failed_extractions.csv"), index=False)
