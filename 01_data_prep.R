# =============================================================================
# 01_load_clean.R
# =============================================================================

library(tidyverse)
library(digest)
library(janitor)
library(readxl)
library(readr)
library(jsonlite)

# ----------------------------- CONFIG ----------------------------------------

cfg <- fromJSON("config.json")

RAW_DATA_DIR       <- cfg$paths$raw_data_dir
RAW_COMPLAINTS_DIR <- cfg$paths$raw_complaints_dir
DATA_DIR           <- cfg$paths$data_dir
DATE_CUTOFF        <- as.Date(cfg$parameters$date_cutoff)

# Derived paths
FILTERED_CASES_CSV <- file.path(DATA_DIR, "filtered_cases.csv")
TEXT_DOCUMENTS_CSV <- file.path(DATA_DIR, "text_documents.csv")
PDF_DOCUMENTS_CSV  <- file.path(DATA_DIR, "pdf_documents.csv")
FILTERED_TEXTS_CSV <- file.path(DATA_DIR, "filtered_texts.csv")

dir.create(DATA_DIR, showWarnings = FALSE, recursive = TRUE)

# ----------------------------- CASES -----------------------------------------

df1 <- read_excel(paste0(RAW_DATA_DIR, "3,305 Federal District Court Cases (downloaded on 2025-10-31).xls"))
df2 <- read_excel(paste0(RAW_DATA_DIR, "3,385 Federal District Court Cases (downloaded on 2025-10-31).xls"))
df3 <- read_excel(paste0(RAW_DATA_DIR, "3,640 Federal District Court Cases (downloaded on 2025-10-31).xls"))

filtered_cases <- rbind(df1, df2, df3) %>%
  clean_names() %>%
  mutate(
    filed_on        = ymd(filed_on),
    terminated      = ymd(terminated),
    year_filed      = str_sub(filed_on, 1, 4),
    year_terminated = str_sub(terminated, 1, 4),
    length          = terminated - filed_on,
    state = case_when(
      str_detect(court, "Cal")    ~ "CA",
      str_detect(court, "Nev")    ~ "NV",
      str_detect(court, "Wash")   ~ "WA",
      str_detect(court, "Ariz")   ~ "AZ",
      str_detect(court, "Or")     ~ "OR",
      str_detect(court, "Mo")     ~ "MT",
      str_detect(court, "Idaho")  ~ "ID",
      str_detect(court, "Haw")    ~ "HI",
      str_detect(court, "Alaska") ~ "AK",
      str_detect(court, "Mar")    ~ "MP",
      str_detect(court, "Guam")   ~ "GU"
    ),
    case_id = sapply(paste0(court, civil_action_number), digest, algo = "md5")
  ) %>%
  filter(!is.na(terminated), filed_on < DATE_CUTOFF, terminated < DATE_CUTOFF) %>%
  select(case_id, everything())

# ----------------------------- DOWNLOADS -------------------------------------

file_names <- list.files(RAW_COMPLAINTS_DIR)

downloads <- as.data.frame(file_names) %>%
  filter(!(str_detect(file_names, "\\.txt") & !str_detect(file_names, "\\.gz"))) %>%
  mutate(
    court = str_sub(file_names, 1, 3),
    court = case_when(
      court == "akd" ~ "D.Alaska",   court == "azd" ~ "D.Ariz.",
      court == "cac" ~ "C.D.Cal.",   court == "cae" ~ "E.D.Cal.",
      court == "can" ~ "N.D.Cal.",   court == "cas" ~ "S.D.Cal.",
      court == "gud" ~ "D.Guam",     court == "hid" ~ "D.Haw.",
      court == "idd" ~ "D.Idaho",    court == "moe" ~ "E.D.Mo.",
      court == "mow" ~ "W.D.Mo.",    court == "nmi" ~ "D.N.Mar.I.",
      court == "nvd" ~ "D.Nev.",     court == "ord" ~ "D.Or.",
      court == "wae" ~ "E.D.Wash.",  court == "waw" ~ "W.D.Wash.",
      TRUE ~ NA_character_
    ),
    civil_action_number = str_extract(file_names, "\\d+-\\d+-(cv|mc|cr|mj)-\\d+"),
    civil_action_number = sub("-", ":", civil_action_number),
    doc_number1 = as.integer(str_extract(file_names, "(?<= - )\\d+(?= - [^-]+(?:\\.[^.]+)+$)")),
    doc_number2 = as.integer(str_extract(file_names, "(?<= - )\\d+(?=(?:\\.[^.]+)+$)")),
    file_type   = case_when(
      str_detect(file_names, "\\.pdf$")      ~ "PDF",
      str_detect(file_names, "\\.txt|\\.gz") ~ "Text",
      TRUE ~ NA_character_
    ),
    primary     = str_detect(file_names, "Primary"),
    file_id     = sapply(file_names, digest, algo = "md5"),
    document_id = sapply(paste0(court, civil_action_number, doc_number1, doc_number2), digest, algo = "md5"),
    case_id     = sapply(paste0(court, civil_action_number), digest, algo = "md5")
  )

pdf_order <- downloads %>%
  filter(file_type == "PDF") %>%
  arrange(court, civil_action_number, doc_number1, doc_number2) %>%
  group_by(court, civil_action_number) %>%
  mutate(order = row_number(), total_documents = n()) %>%
  ungroup() %>%
  select(document_id, order, total_documents)

downloads <- downloads %>%
  left_join(pdf_order, by = "document_id") %>%
  select(file_id, document_id, case_id, file_names, file_type, order, total_documents)

pdf_documents  <- downloads %>% filter(file_type == "PDF")  %>% select(-file_type)
text_documents <- downloads %>% filter(file_type != "PDF")  %>% select(-file_type)

# ----------------------------- TEXTS -----------------------------------------

filtered_texts <- text_documents %>%
  mutate(
    file_path    = file.path(RAW_COMPLAINTS_DIR, file_names),
    text_content = map_chr(
      file_path,
      ~ tryCatch(read_file(gzfile(.x)), error = function(e) NA_character_)
    )
  ) %>%
  select(case_id, document_id, file_id, file_names, text_content, order, total_documents) %>%
  filter(case_id %in% filtered_cases$case_id)

# ----------------------------- EXPORT ----------------------------------------

write_csv(filtered_cases,  FILTERED_CASES_CSV)
write_csv(text_documents,  TEXT_DOCUMENTS_CSV)
write_csv(pdf_documents,   PDF_DOCUMENTS_CSV)
write_csv(filtered_texts,  FILTERED_TEXTS_CSV)

message(sprintf("Saved: %d cases | %d texts | %d PDFs",
                nrow(filtered_cases), nrow(filtered_texts), nrow(pdf_documents)))
