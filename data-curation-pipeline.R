# -----------------------------------------------------------------------------
# ABOUT:
# * Documents the sequence at which different steps are performed
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Run pipeline
# -----------------------------------------------------------------------------
source(file.path("other-scripts","create-masterlist.R"))
rm(list = ls())

source(file.path("other-scripts", "read_in_battery_data.R"))
rm(list = ls())

source(file.path("other-scripts", "processing_battery_data_into_bins.R"))
rm(list = ls())

source(file.path("ema-scripts","read-raw-data-cc1.R"))
rm(list = ls())

source(file.path("ema-scripts","parse-ema-questionnaire-cc1.R"))
rm(list = ls())

source(file.path("ema-scripts","generate-ema-datasets-cc1.R"))
rm(list = ls())

source(file.path("other-scripts","calculate_study_day_ema_blocks_cc1.R"))
rm(list = ls())

source(file.path("ema-scripts","read-raw-data-cc2.R"))
rm(list = ls())

source(file.path("ema-scripts","parse-ema-questionnaire-cc2.R"))
rm(list = ls())

source(file.path("ema-scripts","generate-ema-datasets-cc2.R"))
rm(list = ls())

source(file.path("other-scripts","calculate_study_day_ema_blocks_cc2.R"))
rm(list = ls())

source(file.path("biomarker-smoking-scripts","read-raw-data-cc1.R"))
rm(list = ls())

source(file.path("biomarker-smoking-scripts","generate-biomarker-smoking-dataset-cc1.R"))
rm(list = ls())

source(file.path("biomarker-smoking-scripts","read-raw-data-cc2.R"))
rm(list = ls())

source(file.path("biomarker-smoking-scripts","generate-biomarker-smoking-dataset-cc2.R"))
rm(list = ls())

#combines more than just EMA data, so I put this in other-scripts folder
source(file.path("other-scripts","combine-cc1-cc2.R"))
rm(list = ls())

#you can ignore "There were 50 or more warnings (use warnings() to see the first 50)". I didn't yet put in code to supress those specific warnings.
source(file.path("ema-scripts","create-recalculated-ema-vars.R"))
rm(list = ls())

source(file.path("ema-scripts","create-ema-per-study-design.R"))
rm(list = ls())

source(file.path("ema-scripts","create-random-only-ema.R"))
rm(list = ls())

source(file.path("other-scripts","create-codebook.R"))
rm(list = ls())

source(file.path("ema-scripts","create-ema-datasets-with-integers.R"))
rm(list = ls())

source(file.path("other-scripts","output-formatted-database.R"))
rm(list = ls())

# Note: Results of tests will display on console
source(file.path("tests","test-timevars-ema.R"))
rm(list = ls())

source(file.path("tests","test-duplicates.R"))
rm(list = ls())
