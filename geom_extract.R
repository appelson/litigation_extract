library(tidyverse)

api_key <- "API_KEY"

df <- read_csv("data/incidents_extract.csv", show_col_types = FALSE) %>%
  unite("full_address", location_street, location_city, location_state, location_zip,
        sep = ", ", na.rm = TRUE, remove = FALSE) %>%
  mutate(full_address = na_if(full_address, ""))

unique_addresses <- df %>%
  filter(!is.na(full_address)) %>%
  distinct(full_address)

n <- nrow(unique_addresses)

results <- imap(unique_addresses$full_address, ~ {
  message(sprintf("[%d/%d] %s", .y, n, .x))
  google_geocode(.x, key = api_key)
})

names(results) <- unique_addresses$full_address

saveRDS(results, "geocode_raw_results.rds")
