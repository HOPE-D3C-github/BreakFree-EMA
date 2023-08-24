library(dplyr)
library(lubridate)
library(magrittr)
library(purrr)
source("paths.R")
source(file.path("collect-functions","new-variable-utils.R"))

# We've begun a preliminary parsing process; load in data from that step
load(file.path(path_breakfree_staged_data, "masterlist.RData"))
load(file.path(path_breakfree_staged_data, "ema_responses_raw_data_cc1.RData"))
load(file.path(path_breakfree_staged_data, "ema_questionnaires_cc1.RData"))

# Create an empty list to contain all the parsed EMA data
list_all_ema_data <- list()

# -----------------------------------------------------------------------------
# Select EMA type on which we will continue the parsing process: Random EMA
# -----------------------------------------------------------------------------
all_response_files_cc1 <- all_random_ema_response_files_cc1
use_ema_type <- "RANDOM"
ema_items_cc1 <- random_ema_items_cc1
source(file.path("ema-scripts", "parse-ema-responses-cc1.R"))
list_all_ema_data <- append(list_all_ema_data, list(df_collect_all))

# -----------------------------------------------------------------------------
# Select EMA type on which we will continue the parsing process: Smoking EMA
# -----------------------------------------------------------------------------
all_response_files_cc1 <- all_smoking_ema_response_files_cc1
use_ema_type <- "SMOKING"
ema_items_cc1 <- smoking_ema_items_cc1
source(file.path("ema-scripts", "parse-ema-responses-cc1.R"))
list_all_ema_data <- append(list_all_ema_data, list(df_collect_all))

# -----------------------------------------------------------------------------
# Select EMA type on which we will continue the parsing process: Stress EMA
# -----------------------------------------------------------------------------
all_response_files_cc1 <- all_stress_ema_response_files_cc1
use_ema_type <- "STRESS"
ema_items_cc1 <- stress_ema_items_cc1
source(file.path("ema-scripts", "parse-ema-responses-cc1.R"))
list_all_ema_data <- append(list_all_ema_data, list(df_collect_all))

# -----------------------------------------------------------------------------
# Aggregate data coming from all kinds of EMA
# -----------------------------------------------------------------------------
all_ema_data <- do.call(rbind, list_all_ema_data)
all_ema_data <- all_ema_data %>% arrange(participant_id, end_unixts)

# -----------------------------------------------------------------------------
# Calculate length of Survey Completion Time
# -----------------------------------------------------------------------------

all_ema_data <- all_ema_data %>% 
  mutate (survey_length_minutes = as.numeric(end_hrts_AmericaChicago - begin_hrts_AmericaChicago)/60)

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
  filter(as_date(end_hrts_AmericaChicago) <= last_day_date_w_withdrew) %>%   # right censoring after withdrew date if its before the last day date
  #filter(as_date(end_hrts_AmericaChicago) <= v_last_day_date) %>%  # This right censors only on v_last_day_date
  select(-first_day_date, -last_day_date_w_withdrew, -last_day_date) %>%
  select(participant_id, cc_indicator, everything()) %>% 
  arrange(participant_id, end_unixts)

# -----------------------------------------------------------------------------
# Save output
# -----------------------------------------------------------------------------
all_ema_data_cc1 <- all_ema_data
save(all_ema_data_cc1, file = file.path(path_breakfree_staged_data, "all_ema_data_cc1.RData"))


