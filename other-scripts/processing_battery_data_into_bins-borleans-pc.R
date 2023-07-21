library(dplyr)
library(lubridate)
library(tidyr)

source("paths.R")

#load(file.path(path_breakfree_staged_data, "filtered_battery_data.RData"))

if(T){load(file.path(path_breakfree_staged_data, "filtered_battery_data_test.RData"))}

load(file.path(path_breakfree_staged_data, "masterlist.RData"))

filtered_battery_data_v2 <- filtered_battery_data %>% 
  mutate(
    bat_bin = case_when(
      #battery_percent == 0 ~ "Dead Battery",
      lag_diff_secs > 300 ~ "No Battery Data",
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

for (participant in dat_master$participant_id){
  print(participant)
  dat_battery_participant_v1 <- filtered_battery_data_v2 %>% filter(participant_id == participant)
  
  # # # Add a lead variable to see if the battery status changed from the current record to the next
  # dat_battery_participant_v2 <- dat_battery_participant_v1  %>%
  #   add_row( 
  #     data.frame(participant_id = participant, datetime = 1640995201, battery_percent = NA, datetime_hrts_UTC = as_datetime(1640995201, origin = lubridate::origin),
  #                lag_diff_secs = NA, lag_diff_battery_percent = NA, lead_diff_battery_percent = NA, lag_battery_percent = NA, lead_battery_percent = NA,
  #                bat_bin = "After Battery Data Collection Ended")) %>% 
  #   mutate(bat_bin_lead = lead(bat_bin))
  # 
  # dat_battery_participant_v3 <- dat_battery_participant_v2 %>% 
  #   add_row(
  #     data.frame(participant_id = participant, datetime = 1420070401, battery_percent = NA, datetime_hrts_UTC = as_datetime(1420070401, origin = lubridate::origin),
  #                lag_diff_secs = NA, lag_diff_battery_percent = NA, lead_diff_battery_percent = NA, lag_battery_percent = NA, lead_battery_percent = NA, 
  #                bat_bin = "Before Battery Data Collection Began"), bat_bin_lead = "Before Battery Data Collection Began", .before = 1)
  # 
  #     
      
   
  
  # # Add a lead variable to see if the battery status changed from the current record to the next
  dat_battery_participant_v2 <- dat_battery_participant_v1  %>% mutate(bat_bin_lead = lead(bat_bin),
                                                                       bat_bin_lag = lag(bat_bin))
  
  participant_battery_binned_ranges_test <- dat_battery_participant_v2 %>% 
    filter(bat_bin != bat_bin_lead | bat_bin != bat_bin_lag | row_number() == 1 | row_number()==nrow(.)) %>% 
    rename(
      unix_datetime_start = datetime,
      datetime_hrts_UTC_start = datetime_hrts_UTC,
      battery_status = bat_bin
    ) %>% 
    mutate(
      unix_datetime_end = lead(unix_datetime_start),
      datetime_hrts_UTC_end = lead(datetime_hrts_UTC_start), 
      .after = datetime_hrts_UTC_start
    ) %>% select(-lag_diff_secs, -lag_diff_battery_percent, -lead_diff_battery_percent, -lead_battery_percent, -battery_status) %>% 
    rename(battery_status = bat_bin_lead) %>% 
    mutate(
      unix_datetime_end = case_when(
        row_number() == nrow(.) ~ 1640995201,
        T ~ unix_datetime_end),
      datetime_hrts_UTC_end = case_when(
        row_number() == nrow(.) ~ as_datetime(1640995201, origin = lubridate::origin),
        T ~ datetime_hrts_UTC_end),
      battery_status = case_when(
        row_number() == nrow(.) ~ "After Battery Data Collection Ended",
        T ~ battery_status)
      )
  
  participant_battery_binned_ranges_test_v2 <- participant_battery_binned_ranges_test %>% 
    add_row(
      data.frame(
        participant_id = participant, unix_datetime_start = 1262304001, battery_percent = NA, datetime_hrts_UTC_start = as_datetime(1262304001, origin = lubridate::origin),
        unix_datetime_end = participant_battery_binned_ranges_test$unix_datetime_start[1], datetime_hrts_UTC_end = participant_battery_binned_ranges_test$datetime_hrts_UTC_start[1],
        lag_battery_percent = NA, battery_status = "Before Battery Data Collection Began", bat_bin_lag = NA), 
      .before = 1
    ) %>% select(-lag_battery_percent, -bat_bin_lag, -battery_percent)
  
  
  
  # participant_battery_binned_ranges <- dat_battery_participant_v3 %>% 
  #   # Get one record for each battery_status change + one for the ending status
  #   filter(bat_bin != bat_bin_lead | bat_bin != bat_bin_lag | row_number() == nrow(dat_battery_participant_v3) | row_number() == 1L) %>%  
  #   # rename variables to match the shell dataframe
  #   rename(
  #     unix_datetime_start = datetime,
  #     datetime_hrts_UTC_start = datetime_hrts_UTC,
  #     battery_status = bat_bin
  #     ) %>%
  #   mutate(unix_datetime_end = lead(unix_datetime_start),
  #          datetime_hrts_UTC_end = lead(datetime_hrts_UTC_start)
  #          ) %>% 
  #   # Keep only the desired variables
  #   select(participant_id, unix_datetime_start, unix_datetime_end, datetime_hrts_UTC_start, datetime_hrts_UTC_end, battery_status)
  # 
  # # The first record won't get a value from the lags done above, need to put the first record's timestamp manually
  # participant_battery_binned_ranges$unix_datetime_start[1] <- dat_battery_participant_v3$datetime[1]
  # participant_battery_binned_ranges$datetime_hrts_UTC_start[1] <- dat_battery_participant_v3$datetime_hrts_UTC[1] 
  # 
  # Add variable for the time duration in minutes spanned by the battery status 
  participant_battery_binned_ranges <- participant_battery_binned_ranges_test_v2 %>% 
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

