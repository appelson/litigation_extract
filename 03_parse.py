import json
import re
import uuid
import pandas as pd
from pathlib import Path

def get_file_id(filename):
    match = re.match(r"([a-f0-9]{32})", Path(filename).name)
    return match.group(1) if match else ""

def parse_extraction(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        raw = f.read().strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    data = json.loads(raw)
    return [data] if isinstance(data, dict) else data

def extraction_to_tables(filepath):
    incidents_rows, plaintiffs_rows, defendants_rows, harms_rows = [], [], [], []
    file_id = get_file_id(filepath)
    for inc in parse_extraction(filepath):
        iid = str(uuid.uuid4())
        incidents_rows.append({
            "incident_uuid": iid,
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
                "plaintiff_uuid":  str(uuid.uuid4()),
                "incident_uuid":   iid,
                "file_id":         file_id,
                "plaintiff_id":    p.get("plaintiff_id", ""),
                "name":            p.get("name", ""),
                "race":            p.get("race", ""),
                "gender":          p.get("gender", ""),
                "disability_status":   p.get("disability_status", ""),
                "immigration_status":  p.get("immigration_status", ""),
            })
        for d in inc.get("defendants", []):
            defendants_rows.append({
                "defendant_uuid":  str(uuid.uuid4()),
                "incident_uuid":   iid,
                "file_id":         file_id,
                "defendant_id":    d.get("defendant_id", ""),
                "name":            d.get("name", ""),
                "race":            d.get("race", ""),
                "gender":          d.get("gender", ""),
                "doe_status":      d.get("doe_status", ""),
                "entity_type":     d.get("entity_type", ""),
                "agency":          d.get("agency", ""),
                "agency_type":     d.get("agency_type", ""),
                "role_in_incident": d.get("role_in_incident", ""),
            })
        for h in inc.get("harms", []):
            for harm_type in h.get("type", "").split(";"):
                if harm_type.strip():
                    harms_rows.append({
                        "harm_uuid":               str(uuid.uuid4()),
                        "incident_uuid":           iid,
                        "file_id":                 file_id,
                        "harm_type":               harm_type.strip(),
                        "associated_plaintiff_ids": h.get("associated_plaintiff_ids", ""),
                        "associated_defendant_ids": h.get("associated_defendant_ids", ""),
                    })
    return pd.DataFrame(incidents_rows), pd.DataFrame(plaintiffs_rows), pd.DataFrame(defendants_rows), pd.DataFrame(harms_rows)
  
def load_folder(folder_path):
    all_incidents, all_plaintiffs, all_defendants, all_harms, failed = [], [], [], [], []
    for txt_file in Path(folder_path).glob("*.txt"):
        try:
            inc, pla, defs, har = extraction_to_tables(str(txt_file))
            for df in [inc, pla, defs, har]:
                df.insert(0, "source_file", txt_file.name)
            all_incidents.append(inc); all_plaintiffs.append(pla)
            all_defendants.append(defs); all_harms.append(har)
        except Exception as e:
            failed.append({"file": txt_file.name, "error": str(e)})
    incidents  = pd.concat(all_incidents,  ignore_index=True)
    plaintiffs = pd.concat(all_plaintiffs, ignore_index=True)
    defendants = pd.concat(all_defendants, ignore_index=True)
    harms      = pd.concat(all_harms,      ignore_index=True)
    failed     = pd.DataFrame(failed)
    print(f"{len(failed)} failed, {len(incidents)} incidents loaded.")
    return incidents, plaintiffs, defendants, harms, failed

# Load extraction outputs
incidents, plaintiffs, defendants, harms, failed = load_folder("data/openai_extracted_text/")

# Load filtered_texts — join on file_id to get both document_id and case_id
filtered = pd.read_csv("data/filtered_texts.csv", dtype=str)[["file_id", "document_id", "case_id"]].drop_duplicates()
filtered

# Join document_id and case_id onto all four tables via file_id
def attach_ids(df, filtered):
    return df.merge(filtered, on="file_id", how="left")

incidents  = attach_ids(incidents,  filtered)
plaintiffs = attach_ids(plaintiffs, filtered)
defendants = attach_ids(defendants, filtered)
harms      = attach_ids(harms,      filtered)


harms_exploded_p = harms.copy()
harms_exploded_p["plaintiff_id"] = harms_exploded_p["associated_plaintiff_ids"].str.split(";")
harms_exploded_p = harms_exploded_p.explode("plaintiff_id")
harms_exploded_p["plaintiff_id"] = harms_exploded_p["plaintiff_id"].str.strip()

harms_with_plaintiffs = harms_exploded_p.merge(
    plaintiffs[["incident_uuid", "plaintiff_id", "plaintiff_uuid", "name", "race", "gender", "disability_status", "immigration_status"]],
    on=["incident_uuid", "plaintiff_id"],
    how="left"
).rename(columns={
    "name": "plaintiff_name",
    "race": "plaintiff_race",
    "gender": "plaintiff_gender",
    "disability_status": "plaintiff_disability_status",
    "immigration_status": "plaintiff_immigration_status",
})

# Merge harms with defendant details
harms_exploded_d = harms.copy()
harms_exploded_d["defendant_id"] = harms_exploded_d["associated_defendant_ids"].str.split(";")
harms_exploded_d = harms_exploded_d.explode("defendant_id")
harms_exploded_d["defendant_id"] = harms_exploded_d["defendant_id"].str.strip()

harms_with_defendants = harms_exploded_d.merge(
    defendants[["incident_uuid", "defendant_id", "defendant_uuid", "name", "race", "gender", "doe_status", "entity_type", "agency", "agency_type", "role_in_incident"]],
    on=["incident_uuid", "defendant_id"],
    how="left"
).rename(columns={
    "name": "defendant_name",
    "race": "defendant_race",
    "gender": "defendant_gender",
})

harms_with_plaintiffs.to_csv("data/harms_plaintiffs.csv", index=False)
harms_with_defendants.to_csv("data/harms_defendants.csv", index=False)
incidents.to_csv("data/incidents_extract.csv", index=False)
plaintiffs.to_csv("data/plaintiffs_extract.csv", index=False)
defendants.to_csv("data/defendants_extract.csv", index=False)
harms.to_csv("data/harms_extract.csv", index=False)
