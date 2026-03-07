# =============================================================================
# 04_join_summarise.R
# Joins incidents, harms, plaintiffs, and defendants into a wide flat table,
# then produces an incident-level summary.
#
# Reads:  data/incidents_extract.csv
#         data/plaintiffs_extract.csv
#         data/defendants_extract.csv
#         data/harms_extract.csv
# Writes: data/incidents_joined.csv
#         data/incidents_summary.csv
# =============================================================================

library(tidyverse)
library(janitor)

source("00_config.R")

# ----------------------------- LOAD -----------------------------------------

incidents  <- read_csv(INCIDENTS_CSV,  show_col_types = FALSE)
plaintiffs <- read_csv(PLAINTIFFS_CSV, show_col_types = FALSE)
defendants <- read_csv(DEFENDANTS_CSV, show_col_types = FALSE)
harms      <- read_csv(HARMS_CSV,      show_col_types = FALSE)

# ----------------------------- JOIN ------------------------------------------
# Produces one row per unique
# (incident × harm_type × plaintiff × defendant) combination.

joined <- incidents %>%
  select(-c(document_id, case_id)) %>%
  left_join(
    harms %>% select(-c(document_id, case_id)),
    by = c("source_file", "incident_uuid", "file_id")
  ) %>%
  separate_rows(associated_plaintiff_ids, sep = ";") %>%
  separate_rows(associated_defendant_ids, sep = ";") %>%
  mutate(
    associated_plaintiff_ids = as.integer(associated_plaintiff_ids),
    associated_defendant_ids = as.integer(associated_defendant_ids)
  ) %>%
  left_join(
    plaintiffs %>%
      select(-c(document_id, case_id)) %>%
      rename(
        plaintiff_name        = name,
        plaintiff_race        = race,
        plaintiff_gender      = gender,
        plaintiff_disability  = disability_status,
        plaintiff_immigration = immigration_status
      ),
    by = c("source_file", "incident_uuid", "file_id",
           "associated_plaintiff_ids" = "plaintiff_id")
  ) %>%
  left_join(
    defendants %>%
      select(-c(document_id, case_id)) %>%
      rename(
        defendant_name   = name,
        defendant_race   = race,
        defendant_gender = gender
      ),
    by = c("source_file", "incident_uuid", "file_id",
           "associated_defendant_ids" = "defendant_id")
  ) %>%
  select(
    source_file, incident_uuid, harm_uuid, plaintiff_uuid, defendant_uuid,
    file_id, incident_id,
    location_street, location_city, location_county,
    location_state, location_zip, location_type,
    harm_description, harm_type,
    associated_plaintiff_ids, associated_defendant_ids,
    plaintiff_name, plaintiff_race, plaintiff_gender,
    plaintiff_disability, plaintiff_immigration, plaintiff_compliance,
    defendant_name, defendant_race, defendant_gender,
    doe_status, entity_type, agency, agency_type, role_in_incident
  )

# ----------------------------- SUMMARISE ------------------------------------
# Collapses to one row per incident, concatenating multi-value fields with ";".

collapse <- function(...) na_if(paste(unique(na.omit(c(...))), collapse = ";"), "")

joined_summary <- joined %>%
  group_by(
    source_file, incident_uuid, file_id, incident_id,
    location_street, location_city, location_county,
    location_state, location_zip, location_type
  ) %>%
  summarise(
    # Harms
    harm_uuids        = collapse(harm_uuid),
    harm_types        = collapse(harm_type),
    harm_descriptions = collapse(harm_description),

    # Plaintiffs
    plaintiff_uuids       = collapse(plaintiff_uuid),
    plaintiff_names       = collapse(plaintiff_name),
    plaintiff_races       = collapse(plaintiff_race),
    plaintiff_genders     = collapse(plaintiff_gender),
    plaintiff_disabilities = collapse(plaintiff_disability),
    plaintiff_immigrations = collapse(plaintiff_immigration),
    plaintiff_compliances  = collapse(plaintiff_compliance),
    n_plaintiffs          = n_distinct(plaintiff_uuid, na.rm = TRUE),

    # Defendants
    defendant_uuids  = collapse(defendant_uuid),
    defendant_names  = collapse(defendant_name),
    defendant_races  = collapse(defendant_race),
    defendant_genders = collapse(defendant_gender),
    doe_statuses     = collapse(doe_status),
    entity_types     = collapse(entity_type),
    agencies         = collapse(agency),
    agency_types     = collapse(agency_type),
    roles            = collapse(role_in_incident),
    n_defendants     = n_distinct(defendant_uuid, na.rm = TRUE),

    .groups = "drop"
  )

# ----------------------------- EXPORT ----------------------------------------

write_csv(joined,         INCIDENTS_JOINED_CSV)
write_csv(joined_summary, INCIDENTS_SUMMARY_CSV)

message(glue::glue(
  "Saved: {nrow(joined)} joined rows  |  {nrow(joined_summary)} incident summaries"
))
