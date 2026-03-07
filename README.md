# Legal Complaint Extraction Pipeline

A pipeline for extracting structured data from federal civil rights complaints using LLMs. Complaint texts sourced from Lex Machina are fed through a structured prompt, and the JSON outputs are parsed into relational tables for analysis.

> **Note:** The included dataset is built on a sample of 500 extracted complaints.

---

## Project Structure

```
project/
├── data/
│   ├── filtered_cases.csv              # Cleaned case metadata from Lex Machina
│   ├── filtered_texts.csv              # Complaint texts matched to cases
│   ├── text_documents.csv              # All text file metadata
│   ├── pdf_documents.csv               # All PDF file metadata
│   ├── extracted/
│   │   └── {model}_extracted_text/     # Raw LLM JSON outputs (.txt), one per complaint
│   ├── incidents_extract.csv           # Parsed incidents
│   ├── plaintiffs_extract.csv          # Parsed plaintiffs
│   ├── defendants_extract.csv          # Parsed defendants
│   ├── harms_extract.csv               # Parsed harms (raw)
│   ├── incidents_joined.csv            # Long-form join of all tables
│   └── incidents_summary.csv           # One row per incident with collapsed fields
├── 01_data_prep.R                      # Loads and cleans Lex Machina case + document data
├── 02_extraction.py                    # Runs LLM extraction across all complaint texts
├── 03_parse.py                         # Parses JSON outputs into relational tables
├── 04_cleaning.R                       # Joins and summarises extracted tables
├── 05_analysis.R                       # Geocodes incidents and runs descriptive analysis
├── config.json                         # Paths, model settings, and run parameters
└── prompt.txt                          # Extraction prompt template
```

---

## Pipeline Overview

```
Lex Machina downloads (.xls + complaint files)
         │
         ▼
  01_data_prep.R
  ─────────────────────────────────────────────────────────
  • Loads and deduplicates case metadata from three Lex Machina export batches
  • Extracts state abbreviation from court name
  • Generates stable case_id and document_id MD5 hashes
  • Reads and decompresses complaint text (.gz)
  • Filters to cases filed and terminated before DATE_CUTOFF
  • Outputs: filtered_cases.csv, filtered_texts.csv
         │
         ▼
  02_extraction.py
  ─────────────────────────────────────────────────────────
  • Reads filtered_texts.csv
  • Fills complaint text into prompt.txt template
  • Sends prompts to enabled LLMs asynchronously (up to BATCH_SIZE concurrent)
  • Skips already-processed file_ids automatically
  • Saves one .txt JSON output per complaint per model
  • Outputs: data/extracted/{model}_extracted_text/*.txt + summary JSONs
         │
         ▼
  03_parse.py
  ─────────────────────────────────────────────────────────
  • Parses each .txt file from JSON into DataFrames
  • Assigns UUID primary keys to incidents, plaintiffs, defendants, and harms
  • Joins document_id and case_id via file_id lookup
  • Outputs: incidents_extract.csv, plaintiffs_extract.csv,
             defendants_extract.csv, harms_extract.csv
         │
         ▼
  04_cleaning.R
  ─────────────────────────────────────────────────────────
  • Joins all four tables into a long-form harm-plaintiff-defendant table
  • Explodes semicolon-separated ID strings into individual rows
  • Produces a one-row-per-incident summary with collapsed multi-value fields
  • Outputs: incidents_joined.csv, incidents_summary.csv
         │
         ▼
  05_analysis.R
  ─────────────────────────────────────────────────────────
  • Builds full addresses from extracted location fields
  • Geocodes addresses via Google Maps API (with local cache)
  • Renders an interactive Leaflet map colored by location type
  • Prints frequency tables for all key categorical variables
  • Produces a harm type co-occurrence heatmap (top 20 harm types)
```

---

## Configuration (`config.json`)

All paths and run parameters are set in `config.json`. No hardcoded paths appear in any script.

### Paths

| Key | Description |
|---|---|
| `raw_data_dir` | Directory containing Lex Machina `.xls` export files |
| `raw_complaints_dir` | Directory containing downloaded complaint files (`.pdf`, `.txt.gz`) |
| `data_dir` | Output directory for all processed CSVs |
| `extract_dir` | Output directory for raw LLM JSON outputs |
| `geocode_dir` | Cache directory for geocoded addresses |
| `prompt_file` | Path to the extraction prompt template |

### Parameters

| Key | Default | Description |
|---|---|---|
| `date_cutoff` | `"2025-01-01"` | Excludes cases filed or terminated on or after this date |
| `sample_size` | `500` | Number of complaints to sample; set to `null` to run all |
| `batch_size` | `15` | Max concurrent API requests per model |
| `batch_delay` | `0.1` | Seconds between batches |
| `max_tokens` | `16384` | Max output tokens per LLM request |
| `extract_model` | `"openai"` | Which model's outputs `03_parse.py` reads |

### Models

Enable or disable models in the `models` block. All enabled models run simultaneously in `02_extraction.py`.

| Key | Model | Provider |
|---|---|---|
| `openai` | `gpt-4o-mini` | OpenAI |
| `claude` | `claude-3-5-sonnet-20241022` | Anthropic |
| `gemini` | `gemini-2.5-flash-lite` | Google |
| `llama` | `Llama-3.3-70B-Instruct` | HuggingFace |
| `deepseek` | `DeepSeek-V3.2` | HuggingFace |

---

## Setup

### Environment variables

Create a `.env` file in the project root:

```
OPENAI_API_KEY=...
ANTHROPIC_API_KEY=...
GOOGLE_API_KEY=...
HUGGINGFACE_API_KEY=...
GOOGLE_GEOCODE_API_KEY=...
```

### R dependencies

```r
install.packages(c("tidyverse", "digest", "janitor", "readxl", "readr",
                   "jsonlite", "tidygeocoder", "ggmap", "leaflet"))
```

### Python dependencies

```bash
pip install pandas python-dotenv openai anthropic google-generativeai
```

---

## Step-by-Step

### Step 1 — Data Preparation (`01_data_prep.R`)

Loads three batches of federal district court cases from Lex Machina exports, cleans and deduplicates them, and links them to downloaded complaint files.

**ID generation:** `case_id` and `document_id` are MD5 hashes of `court + civil_action_number` (plus document numbers), producing stable, reproducible IDs across runs. `file_id` is an MD5 hash of the raw filename.

**Key outputs:**
- `filtered_cases.csv` — one row per case with `case_id`, court, filing/termination dates, and case length
- `filtered_texts.csv` — one row per complaint with `file_id`, `document_id`, `case_id`, and decompressed text content

---

### Step 2 — LLM Extraction (`02_extraction.py`)

Sends each complaint through `prompt.txt` and saves the raw JSON response as a `.txt` file.

Output files are named `{file_id}_{model_name}_{timestamp}.txt` and saved to `data/extracted/{model}/`. A `summary_{timestamp}.json` per model and a `combined_summary_{timestamp}.json` are also written with runtime, token usage, and success/error counts.

The script skips any `file_id` that already has a corresponding output file, so reruns are safe and incremental.

---

### Step 3 — Parsing (`03_parse.py`)

Parses raw JSON outputs into four relational tables. Each incident, plaintiff, defendant, and harm row is assigned a UUID primary key at parse time. `document_id` and `case_id` are joined in from `filtered_texts.csv` via `file_id`.

Failed parses are written to `failed_extractions.csv` for inspection.

---

### Step 4 — Joining & Summarising (`04_cleaning.R`)

Joins all four tables into two output formats:

- `incidents_joined.csv` — long-form, one row per harm-plaintiff-defendant combination
- `incidents_summary.csv` — one row per incident, with multi-value fields (e.g. harm types, plaintiff names) collapsed into semicolon-separated strings

---

### Step 5 — Geocoding & Analysis (`05_analysis.R`)

Builds the best available address from extracted location fields (`street > zip > city > county > state`) and geocodes via the Google Maps API. Results are cached locally so subsequent runs don't re-query already geocoded addresses.

Outputs an interactive Leaflet map and prints frequency tables for all key categorical variables across incidents, plaintiffs, defendants, and harms. Also renders a co-occurrence heatmap for the top 20 harm types.

---

## Prompt Design (`prompt.txt`)

The prompt instructs the model to extract structured incident data from complaint text. Key design decisions:

**Incident definition:** A single discrete event at one location and time. Continuous encounters are kept as one incident; clearly separate events become separate incidents.

**Strict extraction:** No inference or assumption. Any field not explicitly stated in the text is returned as an empty string `""`.

**ID assignment:** `plaintiff_id` and `defendant_id` are globally unique integers that increment across all incidents within a complaint, allowing harms to reference specific people by ID across incidents.

**Harms structure:** One harms object per distinct plaintiff-defendant pairing. Multiple harm types within a pairing are semicolon-separated. `associated_plaintiff_ids` and `associated_defendant_ids` store the relevant integer IDs.

**Pre-output sketch:** The model is instructed to enumerate all parties and assign their IDs before writing any JSON, which reduces ID assignment errors in complex multi-party complaints.

---

## Data Model

### Tables and Relationships

```
                    ┌──────────────────────┐
                    │    filtered_texts    │
                    │──────────────────────│
                    │ file_id              │
                    │ document_id          │
                    │ case_id, ...         │
                    └──────────┬───────────┘
                               │
                             file_id
                               │
                               ▼
   ┌────────────────────────────────────────────────────────┐
   │                   incidents_extract                    │
   │────────────────────────────────────────────────────────│
   │ incident_uuid                                          │
   │ file_id                                                │
   │ incident_id, location_street, city, county, state, ... │
   │ document_id, case_id                                   │
   └──────────┬─────────────────┬──────────────────┬────────┘
              │                 │                  │
        incident_uuid     incident_uuid      incident_uuid
              │                 │                  │
              ▼                 ▼                  ▼
┌──────────────────┐  ┌─────────────────────┐  ┌──────────────────┐
│   plaintiffs     │  │   harms_extract     │  │   defendants     │
│──────────────────│  │─────────────────────│  │──────────────────│
│ plaintiff_uuid   │  │ harm_uuid           │  │ defendant_uuid   │
│ incident_uuid    │  │ incident_uuid       │  │ incident_uuid    │
│ plaintiff_id     │  │ harm_type           │  │ defendant_id     │
│ name, race,      │  │ associated_         │  │ name, agency,    │
│ gender, ...      │  │   plaintiff_ids     │  │ role, ...        │
└──────────────────┘  │ associated_         │  └──────────────────┘
                      │   defendant_ids     │
                      └─────────────────────┘
                                │
                    IDs exploded + joined in 04_cleaning.R
                                │
               ┌────────────────┴─────────────────┐
               ▼                                  ▼
  incidents_joined.csv                 incidents_summary.csv
  (long-form, one row per             (one row per incident,
   harm × plaintiff × defendant)       fields collapsed)
```

---

### Output Tables

#### `incidents_extract.csv`
One row per incident extracted from a complaint.

| Column | Description |
|---|---|
| `source_file` | Raw extraction filename |
| `incident_uuid` | Globally unique incident identifier, generated at parse time |
| `file_id` | Links to `filtered_texts` |
| `incident_id` | Integer assigned by the model within this complaint (1, 2, 3…) |
| `location_street` / `city` / `county` / `state` / `zip` | Where the incident occurred |
| `location_type` | Categorical location (Street, Home, Jail, etc.) |
| `document_id` | Joined from `filtered_texts` |
| `case_id` | Joined from `filtered_texts` |

#### `plaintiffs_extract.csv`
One row per plaintiff per incident.

| Column | Description |
|---|---|
| `plaintiff_uuid` | Globally unique plaintiff row identifier |
| `incident_uuid` | Links to `incidents_extract` |
| `plaintiff_id` | Integer assigned by model; unique within a complaint |
| `name` | Full name verbatim from complaint |
| `race` / `gender` / `disability_status` / `immigration_status` / `plaintiff_compliance` | Extracted demographics and behavior |

#### `defendants_extract.csv`
One row per defendant per incident.

| Column | Description |
|---|---|
| `defendant_uuid` | Globally unique defendant row identifier |
| `incident_uuid` | Links to `incidents_extract` |
| `defendant_id` | Integer assigned by model; unique within a complaint |
| `name` | Full name or organization name verbatim from complaint |
| `doe_status` | `Not Doe` (named) or `Doe` (placeholder) |
| `entity_type` | `Individual` or `Organization` |
| `agency` / `agency_type` | Agency name and categorical type |
| `role_in_incident` | Primary Actor, Authority, Secondary Involvement, etc. |

#### `harms_extract.csv`
One row per harm type. Raw table before junction expansion; retains original semicolon-separated ID strings.

| Column | Description |
|---|---|
| `harm_uuid` | Globally unique harm row identifier |
| `incident_uuid` | Links to `incidents_extract` |
| `harm_type` | Single harm category |
| `harm_description` | Verbatim description from complaint |
| `associated_plaintiff_ids` | Semicolon-separated `plaintiff_id` values |
| `associated_defendant_ids` | Semicolon-separated `defendant_id` values |

#### `incidents_joined.csv`
Long-form join of all tables. One row per harm-plaintiff-defendant combination. Use for granular analysis.

#### `incidents_summary.csv`
One row per incident. All multi-value fields (harm types, plaintiff names, agency names, etc.) are collapsed into semicolon-separated strings. Use for incident-level counts and filtering.
