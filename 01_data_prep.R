# Loading libraries
library(tidyverse)
library(digest)
library(janitor)
library(readxl)
library(readr)
library(jsonlite)

# ------------------- LOADING + CLEANING HIGH LEVEL DATA -----------------------

# Defining a counting function for nested variables
count_nested <- function(df, variable){
  df %>%
    mutate(findings_list = str_split({{ variable }}, "\n")) %>%
    unnest(findings_list) %>%
    mutate(findings_list = str_trim(findings_list)) %>%
    filter(findings_list != "", !is.na(findings_list)) %>%
    count(findings_list, name = "n_cases") %>%
    arrange(desc(n_cases))
}

# Defining data file path
file_path <- "/Users/eljahappelson/Desktop/lex_data/"

# Loading data
df1 <- read_excel(paste0(file_path, "3,305 Federal District Court Cases (downloaded on 2025-10-31).xls"))
df2 <- read_excel(paste0(file_path, "3,385 Federal District Court Cases (downloaded on 2025-10-31).xls"))
df3 <- read_excel(paste0(file_path, "3,640 Federal District Court Cases (downloaded on 2025-10-31).xls"))

all_cases <- rbind(df1,df2,df3)

filtered_cases <- all_cases %>%
  clean_names() %>%
  mutate(
    filed_on = ymd(filed_on),
    terminated = ymd(terminated),
    year_filed = str_sub(filed_on,1,4),
    year_terminated = str_sub(terminated,1,4),
    length = terminated - filed_on,
    state = case_when(
      str_detect(court,"Cal") ~ "CA",
      str_detect(court,"Nev") ~ "NV",
      str_detect(court,"Wash") ~ "WA",
      str_detect(court,"Ariz") ~ "AZ",
      str_detect(court,"Or") ~ "OR",
      str_detect(court,"Mo") ~ "MT",
      str_detect(court,"Idaho") ~ "ID",
      str_detect(court,"Haw") ~ "HI",
      str_detect(court,"Alaska") ~ "AK",
      str_detect(court,"Mar") ~ "MP",
      str_detect(court,"Guam") ~ "GU",
    ),
    case_id = sapply(
      paste0(court, civil_action_number),
      digest,
      algo = "md5"
    )
  ) %>%
  filter(!is.na(terminated),
         filed_on < "2025-01-01",
         terminated < "2025-01-01") %>%
  select(case_id, everything())

# ------------------- LOADING + CLEANING DOWNLOAD DATA -------------------------

# Reading all download files
file_names <- list.files("/Users/eljahappelson/Desktop/lex_complaints/")

# Defining a downloads dataframe
downloads <- as.data.frame(file_names) %>%
  filter(!(str_detect(file_names, "\\.txt") & !str_detect(file_names, "\\.gz"))) %>%
  mutate(
    court = str_sub(file_names, 1, 3),
    court = case_when(
      court == "akd" ~ "D.Alaska",
      court == "azd" ~ "D.Ariz.",
      court == "cac" ~ "C.D.Cal.",
      court == "cae" ~ "E.D.Cal.",
      court == "can" ~ "N.D.Cal.",
      court == "cas" ~ "S.D.Cal.",
      court == "gud" ~ "D.Guam",
      court == "hid" ~ "D.Haw.",
      court == "idd" ~ "D.Idaho",
      court == "moe" ~ "E.D.Mo.",
      court == "mow" ~ "W.D.Mo.",
      court == "nmi" ~ "D.N.Mar.I.",
      court == "nvd" ~ "D.Nev.",
      court == "ord" ~ "D.Or.",
      court == "wae" ~ "E.D.Wash.",
      court == "waw" ~ "W.D.Wash.",
      TRUE ~ NA_character_
    ),
    civil_action_number = str_extract(file_names, "\\d+-\\d+-(cv|mc|cr|mj)-\\d+"),
    civil_action_number = sub("-", ":", civil_action_number),
    doc_number1 = as.integer(str_extract(file_names, "(?<= - )\\d+(?= - [^-]+(?:\\.[^.]+)+$)")),
    doc_number2 = as.integer(str_extract(file_names, "(?<= - )\\d+(?=(?:\\.[^.]+)+$)")),
    file_type = case_when(
      str_detect(file_names, "\\.pdf$") ~ "PDF",
      str_detect(file_names, "\\.txt|\\.gz") ~ "Text",
      TRUE ~ NA_character_
    ),
    primary = str_detect(file_names, "Primary"),
    file_id = sapply(file_names, digest, algo = "md5"),
    document_id = sapply(
      paste0(court, civil_action_number, doc_number1, doc_number2),
      digest,
      algo = "md5"
    ),
    case_id = sapply(
      paste0(court, civil_action_number),
      digest,
      algo = "md5"
    )
  )

# Adding orders to documents
pdf_order <- downloads %>%
  filter(file_type == "PDF") %>%
  arrange(court, civil_action_number, doc_number1, doc_number2) %>%
  group_by(court, civil_action_number) %>%
  mutate(
    order = row_number(),
    total_documents = n()
  ) %>%
  ungroup() %>%
  select(document_id, order, total_documents)

# Finalizing downloads
downloads <- downloads %>%
  left_join(pdf_order, by = "document_id") %>%
  select(
    file_id,
    document_id,
    case_id,
    file_names,
    file_type,
    order,
    total_documents
  )

# PDFs
pdf_documents <- downloads %>%
  filter(file_type == "PDF") %>%
  select(-file_type)

# Text files
text_documents <- downloads %>%
  filter(file_type != "PDF") %>%
  select(-file_type)

# ------------------------- GETTING TEXT --------------------------------------

# All texts
all_texts <- text_documents %>%
  mutate(
    file_path = file.path("/Users/eljahappelson/Desktop/lex_complaints/", file_names),
    text_content = map_chr(
      file_path,
      ~ tryCatch(
        read_file(gzfile(.x)), 
        error = function(e) NA_character_
      )
    )
  ) %>%
  select(case_id, document_id, file_id, file_names, text_content, order, total_documents)

# Filtered texts
filtered_texts <- all_texts %>%
  filter(case_id %in% filtered_cases$case_id)

# -------------------------- EXPORTING DATA ------------------------------------

# Downloading cleaned data
dir.create("data", showWarnings = FALSE)
filtered_texts$file_id
write_csv(filtered_cases, "data/filtered_cases.csv")
write_csv(text_documents, "data/text_documents.csv")
write_csv(pdf_documents, "data/pdf_documents.csv")
write_csv(filtered_texts, "data/filtered_texts.csv")