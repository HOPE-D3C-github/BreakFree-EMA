library(dplyr)
library(lubridate)
library(tidyr)

source("paths.R")

load(file.path(path_breakfree_staged_data, "filtered_battery_data.RData"))

filtered_battery_data_v2 <- filtered_battery_data %>% 
  mutate(
    bat_bin = case_when(
      #battery_percent == 0 ~ "Dead Battery",
      lag_diff_secs > 90 ~ "No Battery Data",
      battery_percent <= 100 & battery_percent > 10 ~ "10-100%",
      battery_percent <= 10 & battery_percent >= 0 ~ "0-10%"
    )
  )

# -------------------------------------------------------
# Use the bat_bin variable to reduce down to time ranges in each category
# -------------------------------------------------------

# Shell to add to with each participant
battery_data_binned_ranges <- data.frame(
  participant_id = character(),
  unix_datetime_start = integer(),
  unix_datetime_end = integer(),
  datetime_hrts_UTC_start = as_datetime(character()),
  datetime_hrts_UTC_end = as_datetime(character()),
  battery_status = character(),
  time_duration_minutes = numeric()
) 

for (participant in unique(filtered_battery_data_v2$participant_id)){
  print(participant)
  # Add a lead variable to see if the battery status changed from the current record to the next
  dat_battery_participant <- filtered_battery_data_v2 %>% filter(participant_id == participant) %>% mutate(bat_bin_lead = lead(bat_bin))
  
  participant_battery_binned_ranges <- dat_battery_participant %>% 
    # Get one record for each battery_status change + one for the ending status
    filter(bat_bin != bat_bin_lead | row_number() == nrow(dat_battery_participant)) %>%  
    # rename variables to match the shell dataframe
    rename(
      unix_datetime_end = datetime,
      datetime_hrts_UTC_end = datetime_hrts_UTC,
      battery_status = bat_bin
      ) %>%
    mutate(unix_datetime_start = lag(unix_datetime_end),
           datetime_hrts_UTC_start = lag(datetime_hrts_UTC_end)
           ) %>% 
    # Keep only the desired variables
    select(participant_id, unix_datetime_start, unix_datetime_end, datetime_hrts_UTC_start, datetime_hrts_UTC_end, battery_status)
  
  # The first record won't get a value from the lags done above, need to put the first record's timestamp manually
  participant_battery_binned_ranges$unix_datetime_start[1] <- dat_battery_participant$datetime[1]
  participant_battery_binned_ranges$datetime_hrts_UTC_start[1] <- dat_battery_participant$datetime_hrts_UTC[1] 
  
  # Add variable for the time duration in minutes spanned by the battery status 
  participant_battery_binned_ranges <- participant_battery_binned_ranges %>% 
    mutate(
      time_duration_minutes = round(time_length(datetime_hrts_UTC_end - datetime_hrts_UTC_start, "minutes"), digits = 1)
    )
  
  # Add the participant's data into the dataframe for all participants
  battery_data_binned_ranges <- battery_data_binned_ranges %>% 
    add_row(participant_battery_binned_ranges)
}

battery_data_binned_ranges <- battery_data_binned_ranges %>% 
  rename(battery_unix_datetime_start = unix_datetime_start,
         battery_unix_datetime_end = unix_datetime_end,
         battery_hrts_UTC_start = datetime_hrts_UTC_start,
         battery_hrts_UTC_end = datetime_hrts_UTC_end,
         battery_time_duration_minutes = time_duration_minutes) %>% 
  mutate(
    battery_hrts_AmerChi_start = with_tz(battery_hrts_UTC_start, tzone = "America/Chicago"),
    battery_hrts_AmerChi_end = with_tz(battery_hrts_UTC_end, tzone = "America/Chicago"),
    .after = battery_unix_datetime_end)

save(battery_data_binned_ranges,
     file = file.path(path_breakfree_staged_data, "battery_data_binned.RData"))

