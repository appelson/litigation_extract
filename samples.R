# Loading library
library(tidyverse)

# Defining create sample function
create_sample <- function(sample_folder, seed, sample_size = 50) {
  set.seed(seed)
  dir.create(sample_folder, showWarnings = FALSE)
  
  # Sampling extracts
  sample_extracts <- list.files("data/extracted2/openai") %>% sample(sample_size)
  
  # Defining file paths
  all_files <- read_csv("data/pdf_documents.csv") %>%
    left_join(read_csv("data/text_documents.csv"), by = "document_id") %>%
    select(file_id = file_id.y, file_name = file_names.x) %>%
    mutate(extract_name = sample_extracts[match(file_id, str_replace(sample_extracts, "_gpt-4o-mini_20260310.txt", ""))]) %>%
    filter(!is.na(extract_name)) %>%
    mutate(
      dest_folder  = file.path(sample_folder, file_id),
      file_path    = paste0("/Users/eljahappelson/Desktop/lex_complaints/", file_name),
      extract_path = file.path("data/extracted2/openai", extract_name)
    )
  
  # Creating new folders within samples
  walk(all_files$dest_folder, dir.create, showWarnings = FALSE)
  walk2(all_files$file_path, all_files$dest_folder, file.copy)
  walk2(all_files$extract_path, all_files$dest_folder, file.copy)
}

# Creating Elijah and Zooey samples
create_sample("data/sample_elijah", seed = 1)
create_sample("data/sample_zooey", seed = 2)
