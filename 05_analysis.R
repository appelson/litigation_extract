# =============================================================================
# 05_geocode_analyse.R
# Geocodes incident addresses, maps them, and produces descriptive tables
# for incidents, defendants, plaintiffs, and harms.
#
# Reads:  data/incidents_extract.csv
#         data/defendants_extract.csv
#         data/plaintiffs_extract.csv
#         data/harms_extract.csv
# Writes: data/geocoded/geocoded_addresses.csv  (cached look-up table)
# =============================================================================

library(tidyverse)
library(janitor)
library(tidygeocoder)
library(ggmap)
library(leaflet)

source("00_config.R")
create_dirs()

# Register Google Geocoding API
register_google(key = GOOGLE_GEOCODE_KEY)

# ============================= INCIDENTS / GEOCODING =========================

incidents_raw <- read_csv(INCIDENTS_CSV, show_col_types = FALSE) %>%
  select(incident_uuid, starts_with("location_"))

incidents <- incidents_raw %>%
  mutate(
    # Build the most specific address string available
    full_address = case_when(
      !is.na(location_street) & !is.na(location_city) & !is.na(location_state) ~
        paste0(location_street, ", ", location_city, ", ", location_state,
               ifelse(!is.na(location_zip), paste0(" ", location_zip), "")),
      !is.na(location_street) & !is.na(location_state) ~
        paste0(location_street, ", ", location_state,
               ifelse(!is.na(location_zip), paste0(" ", location_zip), "")),
      !is.na(location_city) & !is.na(location_state) ~
        paste0(location_city, ", ", location_state,
               ifelse(!is.na(location_zip), paste0(" ", location_zip), "")),
      !is.na(location_county) & !is.na(location_state) ~
        paste0(location_county, ", ", location_state),
      !is.na(location_zip)   ~ location_zip,
      !is.na(location_state) ~ location_state,
      TRUE ~ NA_character_
    ),
    location_granularity = ordered(
      case_when(
        !is.na(location_street) ~ "street",
        !is.na(location_zip)    ~ "zip",
        !is.na(location_city)   ~ "city",
        !is.na(location_county) ~ "county",
        !is.na(location_state)  ~ "state",
        TRUE                    ~ "none"
      ),
      levels = c("street", "zip", "city", "county", "state", "none")
    )
  )

# ---- Geocode (cache results so re-runs are instant) -------------------------

geocache_path <- file.path(GEOCODE_DIR, "geocoded_addresses.csv")

if (file.exists(geocache_path)) {
  geocoded <- read_csv(geocache_path, show_col_types = FALSE)
  message("Loaded geocode cache: ", nrow(geocoded), " addresses")
} else {
  geocoded <- incidents %>%
    filter(!is.na(full_address)) %>%
    distinct(full_address) %>%
    mutate_geocode(full_address)
  write_csv(geocoded, geocache_path)
  message("Geocoded and cached: ", nrow(geocoded), " addresses")
}

incidents <- incidents %>%
  left_join(geocoded, by = "full_address")

# ---- Map --------------------------------------------------------------------

pal <- colorFactor("Set1", incidents$location_type)

leaflet(incidents %>% filter(!is.na(lat) & !is.na(lon))) %>%
  addTiles() %>%
  addCircleMarkers(
    lng         = ~lon, lat = ~lat,
    radius      = 5,
    color       = ~pal(location_type),
    fillOpacity = 0.7,
    stroke      = FALSE,
    popup       = ~paste0("<b>", location_type, "</b><br>", full_address, "<br>", incident_uuid)
  ) %>%
  addLegend(position = "bottomright", pal = pal, values = ~location_type, title = "Location Type")

# ---- Frequency tables -------------------------------------------------------

cat("\n--- Location type ---\n");   print(tabyl(incidents, location_type)  %>% arrange(-n))
cat("\n--- Granularity ---\n");     print(tabyl(incidents, location_granularity) %>% arrange(-n))
cat("\n--- State ---\n");           print(count(incidents, location_state) %>% arrange(-n))

# ============================= DEFENDANTS ====================================

defendants <- read_csv(DEFENDANTS_CSV, show_col_types = FALSE) %>%
  select(incident_uuid, defendant_id, name, race, gender,
         doe_status, entity_type, agency, agency_type, role_in_incident) %>%
  mutate(name = str_to_title(name), agency = str_to_title(agency))

cat("\n--- Defendant race ---\n");        print(tabyl(defendants, race))
cat("\n--- Defendant gender ---\n");      print(tabyl(defendants, gender))
cat("\n--- Doe status ---\n");            print(tabyl(defendants, doe_status))
cat("\n--- Entity type ---\n");           print(tabyl(defendants, entity_type))
cat("\n--- Agency (top 20) ---\n");       print(tabyl(defendants, agency) %>% arrange(-n) %>% head(20))
cat("\n--- Agency type ---\n");           print(tabyl(defendants, agency_type))
cat("\n--- Role in incident ---\n");      print(tabyl(defendants, role_in_incident))

# ============================= PLAINTIFFS ====================================

plaintiffs <- read_csv(PLAINTIFFS_CSV, show_col_types = FALSE) %>%
  select(incident_uuid, plaintiff_id, name, race, gender,
         disability_status, immigration_status) %>%
  mutate(name = str_to_title(name))

cat("\n--- Plaintiff race ---\n");        print(tabyl(plaintiffs, race))
cat("\n--- Plaintiff gender ---\n");      print(tabyl(plaintiffs, gender))
cat("\n--- Disability status ---\n");     print(tabyl(plaintiffs, disability_status))
cat("\n--- Immigration status ---\n");    print(tabyl(plaintiffs, immigration_status))

# ============================= HARMS =========================================

harms <- read_csv(HARMS_CSV, show_col_types = FALSE) %>%
  select(incident_uuid, harm_type, associated_plaintiff_ids, associated_defendant_ids)

cat("\n--- Harm type (all) ---\n");  print(tabyl(harms, harm_type) %>% arrange(-n))

# ---- Co-occurrence heatmap (top 20 harm types) ------------------------------

top_harms <- harms %>%
  count(harm_type) %>%
  slice_max(n, n = 20) %>%
  pull(harm_type)

co_matrix <- harms %>%
  distinct(incident_uuid, harm_type) %>%
  mutate(present = 1L) %>%
  pivot_wider(id_cols = incident_uuid, names_from = harm_type,
              values_from = present, values_fill = 0L) %>%
  select(-incident_uuid) %>%
  { t(as.matrix(.)) %*% as.matrix(.) }

co_matrix %>%
  as.data.frame() %>%
  rownames_to_column("harm1") %>%
  pivot_longer(-harm1, names_to = "harm2", values_to = "n") %>%
  filter(harm1 %in% top_harms, harm2 %in% top_harms,
         as.integer(factor(harm1)) < as.integer(factor(harm2))) %>%
  mutate(across(c(harm1, harm2), ~ str_trunc(., 20))) %>%
  ggplot(aes(x = harm1, y = harm2, fill = n)) +
  geom_tile() +
  geom_text(aes(label = n), size = 3) +
  scale_fill_gradient(low = "white", high = "steelblue") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  labs(title = "Harm Type Co-occurrence (Top 20)", x = NULL, y = NULL, fill = "Count")
