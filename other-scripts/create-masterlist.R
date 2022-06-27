library(dplyr)
library(lubridate)
library(readxl)
source("paths.R")

default_warning_setting <- getOption("warn")
options(warn = -1)
# Temporarily suppress the display of warning messages
# Note that read_xlsx will throw a warning message if data types in the raw data
# columns do not always match up with the column types specified within read_xlsx
# In fact, this will be the case since in the raw data, some rows of columns
# containing dates will have text. In this case, the specification of the arguments
# in read_xlsx below will result in the text being recoded to "NA" (missing)
# but the dates will be read in correctly. At the moment, we simply attempt to
# correctly extract the dates from the raw data file.
# dat_raw_dates <- read_xlsx(path = file.path(path_breakfree_other_input_data, "Project AA Participant Visit Date Data.xlsx"), 
#                            sheet = "Visit Dates",
#                            col_types = c("numeric", "date", "date", "date", "date", "date", "date", "date", "text"))
options(warn = default_warning_setting)

### Tonys updates to import the curated v1 and enrolled dataset
file.copy(file.path(path_breakfree_visit_summary_input_data, "data_all.rds"), path_breakfree_other_input_data, overwrite = TRUE)
dat_curated_dates <- readRDS(file = file.path(path_breakfree_other_input_data, "data_all.rds")) %>% filter(enrolled == TRUE) %>% 
  select(participant_id, ID_RSR, ID_enrolled, cc_indicator, eligible_scr, enrolled, 
         #in_ematimes, We verified this is true for everyone so we don't need this variable. In future pipelines we should double check that we have EMA data for everyone in this dataset
         v1_date_calc, v1_attend_calc, v2_date_calc, v2_attend_ind_calc, v3_date_calc, v3_attend_ind_calc, v4_date_calc, v4_attend_ind_calc, withdrew_calc, withdrew_date_calc) %>% 
    rename(v1_date = v1_date_calc, v1_attend = v1_attend_calc,
           v2_date = v2_date_calc, v2_attend = v2_attend_ind_calc,
           v3_date = v3_date_calc, v3_attend = v3_attend_ind_calc,
           v4_date = v4_date_calc, v4_attend = v4_attend_ind_calc,
           withdrew = withdrew_calc, withdrew_date = withdrew_date_calc)

dat_master <- dat_curated_dates %>% select(participant_id, ID_enrolled, ID_RSR, cc_indicator, v1_date, v1_attend, v2_date,
                                           v2_attend, v3_date, v3_attend, v4_date, v4_attend, withdrew, withdrew_date) %>%
  mutate(begin_date_ema_data_collection = v1_date)


dat_master <- dat_master %>% arrange(cc_indicator, participant_id)

# Now, construct time variables
dat_master <- dat_master %>%
  mutate(first_day_hrts_AmericaChicago = force_tz(begin_date_ema_data_collection, tzone = "America/Chicago"),
         first_day_date = as_date(first_day_hrts_AmericaChicago))

# Edit to use withdraw date as the last day
dat_master <- dat_master %>%
  mutate(quit_day_hrts_AmericaChicago = first_day_hrts_AmericaChicago + days(4),
         last_day_hrts_AmericaChicago = first_day_hrts_AmericaChicago + days(13), #14 days of study
         last_day_date = as_date(last_day_hrts_AmericaChicago))

# reverted edit above
# dat_master <- dat_master %>% 
#   mutate(quit_day_hrts_AmericaChicago = first_day_hrts_AmericaChicago + days(4),
#          last_day_hrts_AmericaChicago = case_when(
#            withdrew_date <= first_day_hrts_AmericaChicago + days(14) ~ force_tz(withdrew_date, tzone = "America/Chicago"),
#            T ~ first_day_hrts_AmericaChicago + days(14)),
#          last_day_date = as_date(last_day_hrts_AmericaChicago))

hour(dat_master$quit_day_hrts_AmericaChicago) <- 4

dat_master <- dat_master %>%
  mutate(first_day_unixts = as.double(first_day_hrts_AmericaChicago),
         quit_day_unixts = as.double(quit_day_hrts_AmericaChicago),
         last_day_unixts = as.double(last_day_hrts_AmericaChicago)) %>%
  mutate(first_day_hrts_UTC = with_tz(first_day_hrts_AmericaChicago, tzone = "UTC"),
         quit_day_hrts_UTC = with_tz(quit_day_hrts_AmericaChicago, tzone = "UTC"),
         last_day_hrts_UTC = with_tz(last_day_hrts_AmericaChicago, tzone = "UTC"))

# Clean up data frame
dat_master <- dat_master %>% 
  select(-begin_date_ema_data_collection) %>%
  select(ID_RSR, ID_enrolled, participant_id, cc_indicator, withdrew, withdrew_date, 
         first_day_date, last_day_date,
         quit_day_unixts, quit_day_hrts_UTC, quit_day_hrts_AmericaChicago,
        v1_date, v1_attend, v2_date, v2_attend, v3_date, v3_attend, v4_date, v4_attend)

# Save to RData file
save(dat_master, file = file.path(path_breakfree_staged_data, "masterlist.RData"))

