library(dplyr)
library(lubridate)
library(purrr)
source("paths.R")
source(file.path("collect-functions","new-variable-utils.R"))

# Load previously parsed data
load(file.path(path_breakfree_staged_data, "masterlist.RData"))
load(file.path(path_breakfree_staged_data, "ema_raw_data_cc2.RData"))
load(file.path(path_breakfree_staged_data, "ema_questionnaires_cc2.RData"))

# Participant IDs for data collected using CC2 platform
ids_cc2 <- list.files(path = path_breakfree_cc2_input_data)
ids_cc2 <- ids_cc2[grepl("aa_", ids_cc2)]
ids_cc2 <- ids_cc2[!grepl("test", ids_cc2)]

# Create an empty list to contain all the parsed EMA data
list_all_ema_data <- list()

# -----------------------------------------------------------------------------
# Select EMA type on which we will continue the parsing process: Random EMA
# -----------------------------------------------------------------------------
all_status_files_cc2 <- all_random_ema_status_files_cc2
all_response_files_cc2 <- all_random_ema_response_files_cc2
ema_items_cc2 <- random_ema_items_cc2
use_ema_type <- "RANDOM"

source(file.path("ema-scripts", "parse-ema-responses-cc2.R"))
list_all_ema_data <- append(list_all_ema_data, list(df_collect_all))

# -----------------------------------------------------------------------------
# Select EMA type on which we will continue the parsing process: Smoking EMA
# -----------------------------------------------------------------------------
all_status_files_cc2 <- all_smoking_ema_status_files_cc2
all_response_files_cc2 <- all_smoking_ema_response_files_cc2
ema_items_cc2 <- smoking_ema_items_cc2
use_ema_type <- "SMOKING"

source(file.path("ema-scripts", "parse-ema-responses-cc2.R"))
list_all_ema_data <- append(list_all_ema_data, list(df_collect_all))

# -----------------------------------------------------------------------------
# Select EMA type on which we will continue the parsing process: Stress EMA
# -----------------------------------------------------------------------------
all_status_files_cc2 <- all_stress_ema_status_files_cc2
all_response_files_cc2 <- all_stress_ema_response_files_cc2
ema_items_cc2 <- stress_ema_items_cc2
use_ema_type <- "STRESS"

source(file.path("ema-scripts", "parse-ema-responses-cc2.R"))
list_all_ema_data <- append(list_all_ema_data, list(df_collect_all))

# -----------------------------------------------------------------------------
# Aggregate data coming from all kinds of EMA
# -----------------------------------------------------------------------------
all_ema_data <- do.call(rbind, list_all_ema_data)
all_ema_data <- all_ema_data %>% arrange(participant_id, end_unixts)

#tb test start
# -----------------------------------------------------------------------------
# Calculate length of Survey Completion Time
# -----------------------------------------------------------------------------

all_ema_data <- all_ema_data %>% 
  mutate (survey_length_minutes = as.numeric(end_hrts_AmericaChicago - begin_hrts_AmericaChicago)/60)
#tb test end

# -----------------------------------------------------------------------------
# Adding curated withdrew date to be used in exclusion rules
# -----------------------------------------------------------------------------
dat_master <- dat_master %>% 
  mutate(
    last_day_date_w_withdrew = case_when(
      withdrew_date < last_day_date ~ withdrew_date,
      T ~ last_day_date),
    .after = withdrew_date)

# -----------------------------------------------------------------------------
# Apply our exclusion rules
# -----------------------------------------------------------------------------
# all_ema_data <- semi_join(x = all_ema_data, 
#                           y = dat_master %>%
#                             select(participant_id, withdrew) %>%
#                             filter(withdrew == 0), 
#                           by = "participant_id")

# Note that not all EMAs have a value for begin_hrts
# For example, 'MISSED' EMAs will not have begin_hrts
# since this timestamp will not be applicable to that EMA
all_ema_data <- all_ema_data %>% 
  left_join(x = .,
            y = dat_master %>% 
              select(participant_id, cc_indicator, last_day_date,
                     first_day_date, last_day_date_w_withdrew),
            by = "participant_id") %>%
  filter(as_date(end_hrts_AmericaChicago) >= first_day_date) %>%  
  filter(as_date(end_hrts_AmericaChicago) <= last_day_date_w_withdrew) %>%   # Right censor on withdrew date
  #filter(as_date(end_hrts_AmericaChicago) <= last_day_date) %>%  # Right censor on last_day_date only
  select(-first_day_date, -last_day_date_w_withdrew, -last_day_date) %>%
  select(participant_id, cc_indicator, everything()) %>% 
  arrange(participant_id, end_unixts)

# -----------------------------------------------------------------------------
# Save output
# -----------------------------------------------------------------------------
all_ema_data_cc2 <- all_ema_data
save(all_ema_data_cc2, file = file.path(path_breakfree_staged_data, "all_ema_data_cc2.RData"))


