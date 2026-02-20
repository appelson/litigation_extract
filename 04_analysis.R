# Loading libraries
library(tidyverse)
library(janitor)
library(tidygeocoder)
library(ggmap)
library(leaflet)

# Defining Google Geolocation API Key
register_google(key = "API_KEY")

# Creating incidents dataframe
incidents <- read_csv("data/incidents_extract.csv") %>%
  select(
    incident_uuid,
    location_street,
    location_city,
    location_county,
    location_state,
    location_zip,
    location_type
  ) %>%
  mutate(
    full_address = case_when(
      !is.na(location_street) & !is.na(location_city) & !is.na(location_state) ~
        paste0(paste(location_street, location_city, location_state, sep = ", "),
               ifelse(!is.na(location_zip), paste0(" ", location_zip), "")),
      !is.na(location_street) & !is.na(location_state) ~          # street + no city
        paste0(paste(location_street, location_state, sep = ", "),
               ifelse(!is.na(location_zip), paste0(" ", location_zip), "")),
      !is.na(location_city) & !is.na(location_state) ~
        paste0(paste(location_city, location_state, sep = ", "),
               ifelse(!is.na(location_zip), paste0(" ", location_zip), "")),
      !is.na(location_county) & !is.na(location_state) ~
        paste(location_county, location_state, sep = ", "),
      !is.na(location_zip) ~ location_zip,
      !is.na(location_state) ~ location_state,
      TRUE ~ NA
    ),
    location_granularity = case_when(
      !is.na(location_street) ~ "street",
      !is.na(location_zip)    ~ "zip",
      !is.na(location_city)   ~ "city",
      !is.na(location_county) ~ "county",
      !is.na(location_state)  ~ "state",
      TRUE                    ~ "none"
    ) %>% ordered(levels = c("street", "zip", "city", "county", "state", "none"))
  )

# Geolocating addresses
unique_addresses <- incidents %>%
  filter(!is.na(full_address)) %>%
  distinct(full_address) %>%
  mutate_geocode(full_address)

# Joining incidents with geolocation
incidents <- incidents %>%
  left_join(unique_addresses, by = "full_address")
  
# Creating a plot of addresses
leaflet(incidents %>% filter(!is.na(lat) & !is.na(lon))) %>%
  addTiles() %>%
  addCircleMarkers(
    lng = ~lon,
    lat = ~lat,
    radius = 5,
    color = ~colorFactor("Set1", location_type)(location_type),
    fillOpacity = 0.7,
    stroke = FALSE,
    popup = ~paste0(
      "<b>", location_type, "</b><br>",
      full_address, "<br>",
      incident_uuid
    )
  ) %>%
  addLegend(
    position = "bottomright",
    pal = colorFactor("Set1", incidents$location_type),
    values = ~location_type,
    title = "Location Type"
  )

# Incident type count
incidents %>%
  tabyl(location_type) %>%
  arrange(-n)

# Location granularity count
incidents %>%
  tabyl(location_granularity) %>%
  arrange(-n)

# State count
incidents %>%
  count(location_state) %>%
  arrange(-n)

# ------------------------------------------------------------------------------

# Loading defendant data
defendants <- read_csv("data/defendants_extract.csv") %>%
  select(
    incident_uuid,
    defendant_id,
    name,
    race,
    gender,
    doe_status,
    entity_type,
    agency,
    agency_type,
    role_in_incident
  ) %>%
  mutate(
    name = str_to_title(name),
    agency = str_to_title(agency)
  )

# Race count
defendants %>%
  tabyl(race)

# Gender count
defendants %>%
  tabyl(gender)

# Doe Status count
defendants %>%
  tabyl(doe_status)

# Entity type count
defendants %>%
  tabyl(entity_type)

# Agency count
defendants %>%
  tabyl(agency) %>%
  arrange(-n)

# Agency type count
defendants %>%
  tabyl(agency_type)

# Role count
defendants %>%
  tabyl(role_in_incident)

# ------------------------------------------------------------------------------

# Loading plaintiff data
plaintiffs <- read_csv("data/plaintiffs_extract.csv") %>%
  select(
    incident_uuid,
    plaintiff_id,
    name,
    race,
    gender,
    disability_status,
    immigration_status
  ) %>%
  mutate(
    name = str_to_title(name)
  )

# Race count
plaintiffs %>%
  tabyl(race)

# Gender count
plaintiffs %>%
  tabyl(gender)

# Disability count
plaintiffs %>%
  tabyl(disability_status)

# Immigration count
plaintiffs %>%
  tabyl(immigration_status)

# ------------------------------------------------------------------------------

# Loading harms data
harms <- read_csv("data/harms_extract.csv") %>%
  select(
    incident_uuid,
    harm_type,
    associated_plaintiff_ids,
    associated_defendant_ids
  )

# Harm count
harms %>%
  tabyl(harm_type) %>%
  arrange(-n)

# Harm co-occurrence
co_occurrence <- harms %>%
  distinct(incident_uuid, harm_type) %>%
  mutate(present = 1) %>%
  pivot_wider(
    id_cols = incident_uuid,
    names_from = harm_type,
    values_from = present,
    values_fill = 0
  ) %>%
  select(-incident_uuid) %>%
  { t(as.matrix(.)) %*% as.matrix(.) }

# Harm co-occurence plot
top_20_harms <- harms %>%
  count(harm_type) %>%
  slice_max(n, n = 20) %>%
  pull(harm_type)

co_occurrence %>%
  as.data.frame() %>%
  rownames_to_column("harm1") %>%
  pivot_longer(-harm1, names_to = "harm2", values_to = "n") %>%
  filter(harm1 %in% top_20_harms, harm2 %in% top_20_harms) %>%
  filter(as.integer(factor(harm1)) < as.integer(factor(harm2))) %>%
  mutate(across(c(harm1, harm2), ~str_trunc(., 20))) %>%
  ggplot(aes(x = harm1, y = harm2, fill = n)) +
  geom_tile() +
  geom_text(aes(label = n), size = 3) +
  scale_fill_gradient(low = "white", high = "steelblue") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  labs(title = "Harm Type Co-occurrence (Top 20)", x = NULL, y = NULL, fill = "Count")