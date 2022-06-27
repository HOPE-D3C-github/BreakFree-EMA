library(dplyr)
library(lubridate)
source("paths.R")

load(file.path(path_breakfree_staged_data, "masterlist.RData"))
load(file.path(path_breakfree_staged_data, "online_puffmarker_episode_raw_data_cc2.RData"))

df_raw <- do.call(rbind, online_puffmarker_episode_files_cc2)

# -----------------------------------------------------------------------------
# Construct time variables
# -----------------------------------------------------------------------------
df_raw <- df_raw %>%
  mutate(onlinepuffm_unixts = V1/1000) %>%
  mutate(onlinepuffm_hrts_UTC = as.POSIXct(onlinepuffm_unixts, tz = "UTC", origin="1970-01-01")) %>%
  mutate(onlinepuffm_hrts_AmericaChicago = with_tz(onlinepuffm_hrts_UTC, tzone = "America/Chicago")) %>%
  select(participant_id, onlinepuffm_unixts, onlinepuffm_hrts_UTC, onlinepuffm_hrts_AmericaChicago)

# -----------------------------------------------------------------------------
# Apply our exclusion rules
# -----------------------------------------------------------------------------
#NOTE: Not including the withdrew variable until it's curated
# df_analysis <- semi_join(x = df_raw, 
#                          y = dat_master %>% 
#                            select(participant_id, withdrew) %>%
#                            filter(withdrew == 0), 
#                          by = "participant_id")

df_analysis <- df_raw %>%         #df_analysis %>%
  left_join(x = .,
            y = dat_master %>% 
              select(participant_id, cc_indicator, 
                     first_day_date, 
                     last_day_date),
            by = "participant_id") %>%
  filter(onlinepuffm_hrts_AmericaChicago >= first_day_date) %>%
  filter(onlinepuffm_hrts_AmericaChicago <= last_day_date) %>%
  select(-first_day_date, -last_day_date) %>%
  select(participant_id, cc_indicator, everything())

# -----------------------------------------------------------------------------
# Drop duplicate records
# -----------------------------------------------------------------------------
idx_duplicates <- df_analysis %>%
  select(participant_id, onlinepuffm_unixts) %>%
  duplicated(.)

df_analysis <- df_analysis %>% filter(!idx_duplicates)

online_puffmarker_episode_data_cc2 <- df_analysis
save(online_puffmarker_episode_data_cc2, file = file.path(path_breakfree_staged_data, "online_puffmarker_episode_data_cc2.RData"))

