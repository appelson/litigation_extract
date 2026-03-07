# =============================================================================
# 04_join_summarise.R
# =============================================================================

library(tidyverse)
library(janitor)
library(jsonlite)

cfg <- fromJSON("config.json")
DATA_DIR <- cfg$paths$data_dir

INCIDENTS_CSV         <- file.path(DATA_DIR, "incidents_extract.csv")
PLAINTIFFS_CSV        <- file.path(DATA_DIR, "plaintiffs_extract.csv")
DEFENDANTS_CSV        <- file.path(DATA_DIR, "defendants_extract.csv")
HARMS_CSV             <- file.path(DATA_DIR, "harms_extract.csv")
INCIDENTS_JOINED_CSV  <- file.path(DATA_DIR, "incidents_joined.csv")
INCIDENTS_SUMMARY_CSV <- file.path(DATA_DIR, "incidents_summary.csv")

# ----------------------------- LOAD -----------------------------------------

incidents  <- read_csv(INCIDENTS_CSV,  show_col_types = FALSE)
plaintiffs <- read_csv(PLAINTIFFS_CSV, show_col_types = FALSE)
defendants <- read_csv(DEFENDANTS_CSV, show_col_types = FALSE)
harms      <- read_csv(HARMS_CSV,      show_col_types = FALSE)

# ----------------------------- JOIN ------------------------------------------

joined <- incidents %>%
  select(-c(document_id, case_id)) %>%
  left_join(harms %>% select(-c(document_id, case_id)),
            by = c("source_file", "incident_uuid", "file_id")) %>%
  separate_rows(associated_plaintiff_ids, sep = ";") %>%
  separate_rows(associated_defendant_ids, sep = ";") %>%
  mutate(across(c(associated_plaintiff_ids, associated_defendant_ids), as.integer)) %>%
  left_join(
    plaintiffs %>% select(-c(document_id, case_id)) %>%
      rename(plaintiff_name = name, plaintiff_race = race, plaintiff_gender = gender,
             plaintiff_disability = disability_status, plaintiff_immigration = immigration_status),
    by = c("source_file", "incident_uuid", "file_id", "associated_plaintiff_ids" = "plaintiff_id")
  ) %>%
  left_join(
    defendants %>% select(-c(document_id, case_id)) %>%
      rename(defendant_name = name, defendant_race = race, defendant_gender = gender),
    by = c("source_file", "incident_uuid", "file_id", "associated_defendant_ids" = "defendant_id")
  ) %>%
  select(source_file, incident_uuid, harm_uuid, plaintiff_uuid, defendant_uuid,
         file_id, incident_id,
         location_street, location_city, location_county, location_state, location_zip, location_type,
         harm_description, harm_type, associated_plaintiff_ids, associated_defendant_ids,
         plaintiff_name, plaintiff_race, plaintiff_gender, plaintiff_disability,
         plaintiff_immigration, plaintiff_compliance,
         defendant_name, defendant_race, defendant_gender,
         doe_status, entity_type, agency, agency_type, role_in_incident)

# ----------------------------- SUMMARISE ------------------------------------

collapse <- function(...) na_if(paste(unique(na.omit(c(...))), collapse = ";"), "")

joined_summary <- joined %>%
  group_by(source_file, incident_uuid, file_id, incident_id,
           location_street, location_city, location_county,
           location_state, location_zip, location_type) %>%
  summarise(
    harm_uuids             = collapse(harm_uuid),
    harm_types             = collapse(harm_type),
    harm_descriptions      = collapse(harm_description),
    plaintiff_uuids        = collapse(plaintiff_uuid),
    plaintiff_names        = collapse(plaintiff_name),
    plaintiff_races        = collapse(plaintiff_race),
    plaintiff_genders      = collapse(plaintiff_gender),
    plaintiff_disabilities = collapse(plaintiff_disability),
    plaintiff_immigrations = collapse(plaintiff_immigration),
    plaintiff_compliances  = collapse(plaintiff_compliance),
    n_plaintiffs           = n_distinct(plaintiff_uuid, na.rm = TRUE),
    defendant_uuids        = collapse(defendant_uuid),
    defendant_names        = collapse(defendant_name),
    defendant_races        = collapse(defendant_race),
    defendant_genders      = collapse(defendant_gender),
    doe_statuses           = collapse(doe_status),
    entity_types           = collapse(entity_type),
    agencies               = collapse(agency),
    agency_types           = collapse(agency_type),
    roles                  = collapse(role_in_incident),
    n_defendants           = n_distinct(defendant_uuid, na.rm = TRUE),
    .groups = "drop"
  )

# ----------------------------- EXPORT ----------------------------------------

write_csv(joined,         INCIDENTS_JOINED_CSV)
write_csv(joined_summary, INCIDENTS_SUMMARY_CSV)

message(sprintf("Saved: %d joined rows | %d incident summaries",
                nrow(joined), nrow(joined_summary)))
