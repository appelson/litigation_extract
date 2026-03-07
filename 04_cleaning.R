# Loading libraries
library(tidyverse)
library(janitor)

# Loading data
incidents <- read_csv("data/incidents_extract.csv")
plaintiffs <- read_csv("data/plaintiffs_extract.csv")
defendants <- read_csv("data/defendants_extract.csv")
harms <- read_csv("data/harms_extract.csv")

# Joined incidents
joined <- incidents %>%
  select(-c("document_id", "case_id")) %>%
  left_join(harms %>% select(-c("document_id", "case_id")),
            by = c("source_file", "incident_uuid", "file_id")) %>%
  separate_rows(associated_plaintiff_ids, sep = ";") %>%
  separate_rows(associated_defendant_ids, sep = ";") %>%
  mutate(
    associated_plaintiff_ids = as.integer(associated_plaintiff_ids),
    associated_defendant_ids = as.integer(associated_defendant_ids)
  ) %>%
  left_join(
    plaintiffs %>% 
      select(-c("document_id", "case_id")) %>% 
      rename(plaintiff_name = name, plaintiff_race = race,
             plaintiff_gender = gender, plaintiff_disability = disability_status,
             plaintiff_immigration = immigration_status),
    by = c("source_file", "incident_uuid", "file_id",
           "associated_plaintiff_ids" = "plaintiff_id")
  ) %>%
  left_join(
    defendants %>% 
      select(-c("document_id", "case_id")) %>%
      rename(defendant_name = name, defendant_race = race, defendant_gender = gender),
    by = c("source_file", "incident_uuid", "file_id",
           "associated_defendant_ids" = "defendant_id")
  ) %>%
  select(source_file, incident_uuid, harm_uuid, plaintiff_uuid, defendant_uuid,
         file_id, incident_id,
         location_street, location_city, location_county, location_state, location_zip, location_type,
         harm_description, harm_type, associated_plaintiff_ids, associated_defendant_ids,
         plaintiff_name, plaintiff_race, plaintiff_gender, plaintiff_disability, plaintiff_immigration, plaintiff_compliance,
         defendant_name, defendant_race, defendant_gender, doe_status, entity_type, agency, agency_type, role_in_incident)

# Incident level joined incidents
joined_summary <- joined %>%
  group_by(
    source_file, incident_uuid, file_id, incident_id,
    location_street, location_city, location_county, 
    location_state, location_zip, location_type
  ) %>%
  summarise(
    # Harms
    harm_uuids = na_if(paste(unique(na.omit(harm_uuid)), collapse = ";"), ""),
    harm_types = na_if(paste(unique(na.omit(harm_type)), collapse = ";"), ""),
    harm_descriptions = na_if(paste(unique(na.omit(harm_description)), collapse = ";"), ""),
    
    # Plaintiffs
    plaintiff_uuids = na_if(paste(unique(na.omit(plaintiff_uuid)), collapse = ";"), ""),
    plaintiff_names = na_if(paste(unique(na.omit(plaintiff_name)), collapse = ";"), ""),
    plaintiff_races = na_if(paste(unique(na.omit(plaintiff_race)), collapse = ";"), ""),
    plaintiff_genders = na_if(paste(unique(na.omit(plaintiff_gender)), collapse = ";"), ""),
    plaintiff_disabilities = na_if(paste(unique(na.omit(plaintiff_disability)), collapse = ";"), ""),
    plaintiff_immigrations = na_if(paste(unique(na.omit(plaintiff_immigration)), collapse = ";"), ""),
    plaintiff_compliances = na_if(paste(unique(na.omit(plaintiff_compliance)), collapse = ";"), ""),
    n_plaintiffs = n_distinct(plaintiff_uuid, na.rm = TRUE),
    
    # Defendants
    defendant_uuids = na_if(paste(unique(na.omit(defendant_uuid)), collapse = ";"), ""),
    defendant_names = na_if(paste(unique(na.omit(defendant_name)), collapse = ";"), ""),
    defendant_races = na_if(paste(unique(na.omit(defendant_race)), collapse = ";"), ""),
    defendant_genders = na_if(paste(unique(na.omit(defendant_gender)), collapse = ";"), ""),
    doe_statuses = na_if(paste(unique(na.omit(doe_status)), collapse = ";"), ""),
    entity_types = na_if(paste(unique(na.omit(entity_type)), collapse = ";"), ""),
    agencies = na_if(paste(unique(na.omit(agency)), collapse = ";"), ""),
    agency_types = na_if(paste(unique(na.omit(agency_type)), collapse = ";"), ""),
    roles = na_if(paste(unique(na.omit(role_in_incident)), collapse = ";"), ""),
    n_defendants = n_distinct(defendant_uuid, na.rm = TRUE),
    .groups = "drop"
  )
