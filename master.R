# =============================================================================
# Runs the full pipeline in order. 
# =============================================================================

run_step <- function(step, label, expr) {
  cat(sprintf("\n[%d/4] %s...\n", step, label))
  start <- Sys.time()
  expr
  cat(sprintf("      done in %.1fs\n", as.numeric(Sys.time() - start, units = "secs")))
}

cat("============================================\n")
cat(sprintf(" Pipeline started: %s\n", Sys.time()))
cat("============================================\n")

run_step(1, "Loading and cleaning raw data",  source("01_data_prep.R"))
run_step(2, "Running LLM extraction", system("python 02_extraction.py"))
run_step(3, "Parsing LLM outputs", system("python 03_parse.py"))
run_step(4, "Joining and summarising", source("04_cleaning.R"))

cat("\n============================================\n")
cat(sprintf(" Pipeline complete: %s\n", Sys.time()))
cat("============================================\n")
