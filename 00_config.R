# =============================================================================
# 00_config.R
# Master configuration file
# =============================================================================

# ----------------------------- PATHS -----------------------------------------

# Raw data inputs
RAW_DATA_DIR      <- "/Users/eljahappelson/Desktop/lex_data/"
RAW_COMPLAINTS_DIR <- "/Users/eljahappelson/Desktop/lex_complaints/"

# Project output directories
DATA_DIR          <- "data/"
EXTRACT_DIR       <- file.path(DATA_DIR, "extracted/")   # LLM output text files
GEOCODE_DIR       <- file.path(DATA_DIR, "geocoded/")    # Geocoded results cache

# Derived file paths
FILTERED_CASES_CSV   <- file.path(DATA_DIR, "filtered_cases.csv")
TEXT_DOCUMENTS_CSV   <- file.path(DATA_DIR, "text_documents.csv")
PDF_DOCUMENTS_CSV    <- file.path(DATA_DIR, "pdf_documents.csv")
FILTERED_TEXTS_CSV   <- file.path(DATA_DIR, "filtered_texts.csv")

INCIDENTS_CSV    <- file.path(DATA_DIR, "incidents_extract.csv")
PLAINTIFFS_CSV   <- file.path(DATA_DIR, "plaintiffs_extract.csv")
DEFENDANTS_CSV   <- file.path(DATA_DIR, "defendants_extract.csv")
HARMS_CSV        <- file.path(DATA_DIR, "harms_extract.csv")

INCIDENTS_JOINED_CSV <- file.path(DATA_DIR, "incidents_joined.csv")
INCIDENTS_SUMMARY_CSV <- file.path(DATA_DIR, "incidents_summary.csv")

# Python pipeline files
PROMPT_FILE      <- "prompt.txt"
PYTHON_EXTRACT   <- "02_extract.py"

# ----------------------------- PARAMETERS ------------------------------------

# Date cutoff for case filtering
DATE_CUTOFF <- as.Date("2025-01-01")

# API keys (set as environment variables — never hardcode)
GOOGLE_GEOCODE_KEY <- Sys.getenv("GOOGLE_GEOCODE_API_KEY")

# ----------------------------- HELPERS ---------------------------------------

# Ensure all output directories exist
create_dirs <- function() {
  dirs <- c(DATA_DIR, EXTRACT_DIR, GEOCODE_DIR)
  invisible(lapply(dirs, dir.create, showWarnings = FALSE, recursive = TRUE))
}

# Counting function for nested/newline-separated variables
count_nested <- function(df, variable) {
  df %>%
    mutate(findings_list = str_split({{ variable }}, "\n")) %>%
    unnest(findings_list) %>%
    mutate(findings_list = str_trim(findings_list)) %>%
    filter(findings_list != "", !is.na(findings_list)) %>%
    count(findings_list, name = "n_cases") %>%
    arrange(desc(n_cases))
}

message("Config loaded. Call create_dirs() to initialise output folders.")
