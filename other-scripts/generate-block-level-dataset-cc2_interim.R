# This file is sourced from the end of "calculate_study_day_ema_blocks_cc2.R"
library(lubridate)
library(sqldf)

# -----------------------------------------------------------------------------
# Merge the ema dataset with calculated blocks to 
# a dataset of all blocks calculated from study day start times
# and prepolutated blocks from 4 random x
# -----------------------------------------------------------------------------

load(file.path(path_breakfree_staged_data, "masterlist.RData"))
load(file.path(path_breakfree_staged_data, "battery_data_binned.RData"))

# -----------------------------------------------------------------------------

dat_master_start_end_dates_cc2 <- dat_master %>% filter(cc_indicator == 2) %>% select(participant_id, first_day_date, last_day_date, withdrew, withdrew_date)

dates_w_block_long <- data.frame(participant_id = character(),
                               study_date = as.Date(character()),
                               study_day_int = integer(),
                               block = integer(),
                               withdrew_date = as.Date(character()))

for (i in 1:nrow(dat_master_start_end_dates_cc2)){
  dates_w_block_long <- dates_w_block_long %>% 
    add_row(expand_grid(participant_id = dat_master_start_end_dates_cc2$participant_id[i], 
                        study_date = as_date(dat_master_start_end_dates_cc2$first_day_date[i] : dat_master_start_end_dates_cc2$last_day_date[i]),
                        block = 1:4,
                        withdrew_date = dat_master_start_end_dates_cc2$withdrew_date[i]) %>%
              mutate(study_day_int = ceiling(row_number()/4), .after = study_date))
}

# ------------------------------------------------------
# START Imputing Blocks based on Start times
# ------------------------------------------------------

block_level_block_calc_wide <- day_start_and_end_cc2_v1d %>% mutate(
  study_day_start_date = as_date(day_start_hrts_AmericaChicago),
  block_1_end_dt = day_start_hrts_AmericaChicago + hours(4),
  block_2_end_dt = day_start_hrts_AmericaChicago + hours(8),
  block_3_end_dt = day_start_hrts_AmericaChicago + hours(12),
  block_4_end_dt = case_when(
    !is.na(extra_day_start_dt) ~ day_start_hrts_AmericaChicago + hours(16) + time_between_multiple_day_starts,
    T ~ day_start_hrts_AmericaChicago + hours(16),
    ),
  extra_day_start_block_number = case_when(
    is.na(extra_day_start_dt) ~ NA_integer_,
    extra_day_start_dt > day_start_hrts_AmericaChicago & extra_day_start_dt <= block_1_end_dt ~ 1L, # Occured in Block 1
    extra_day_start_dt > block_1_end_dt & extra_day_start_dt <= block_2_end_dt ~ 2L, # Occured in Block 2
    extra_day_start_dt > block_2_end_dt & extra_day_start_dt <= block_3_end_dt ~ 3L, # Occured in Block 3
    extra_day_start_dt > block_3_end_dt & extra_day_start_dt <= block_4_end_dt ~ 4L, # Occured in Block 4
  ), .after = day_start_hrts_AmericaChicago)

# The dataset is wide and without clear block numbers

# ---------------------------------------------------------------------
# Transform the dataset from wide to tall (with block numbers)
# ---------------------------------------------------------------------
df_shell <- block_level_block_calc_wide %>% 
  select(participant_id, study_day_start_date, day_start_hrts_AmericaChicago, day_end_hrts, block_1_end_dt, block_2_end_dt) %>%
  rename(block_start_hrts_AmericaChicago = block_1_end_dt,
         block_end_hrts_AmericaChicago = block_2_end_dt) %>% 
  slice(0) %>% 
  mutate(block_calc = as.character(),
         multi_day_start = as.logical())

block_level_block_calc_tall <- df_shell

# For loop to break out the wide dataset into tall with block numbers calculated
for (i in 1:nrow(block_level_block_calc_wide)){
  row_i <- block_level_block_calc_wide %>% slice(i)
  if (!is.na(row_i$block_1_end_dt)){
    block_1_row <- df_shell %>% add_row(participant_id = row_i$participant_id,
                                        study_day_start_date = row_i$study_day_start_date,
                                        day_start_hrts_AmericaChicago = row_i$day_start_hrts_AmericaChicago,
                                        day_end_hrts = row_i$day_end_hrts,
                                        block_start_hrts_AmericaChicago = row_i$day_start_hrts_AmericaChicago,
                                        block_end_hrts_AmericaChicago = row_i$block_1_end_dt,
                                        block_calc = "1",
                                        multi_day_start = case_when(row_i$extra_day_start_block_number == 1 ~ TRUE,
                                                                    T ~ F))
    block_level_block_calc_tall <- block_level_block_calc_tall %>% add_row(block_1_row)
    
    if (!is.na(row_i$block_2_end_dt)){
      block_2_row <- df_shell %>% add_row(participant_id = row_i$participant_id,
                                          study_day_start_date = row_i$study_day_start_date,
                                          day_start_hrts_AmericaChicago = row_i$day_start_hrts_AmericaChicago,
                                          day_end_hrts = row_i$day_end_hrts,
                                          block_start_hrts_AmericaChicago = row_i$block_1_end_dt,
                                          block_end_hrts_AmericaChicago = row_i$block_2_end_dt,
                                          block_calc = "2",
                                          multi_day_start = case_when(row_i$extra_day_start_block_number <= 2 ~ TRUE,
                                                                      T ~ F))
      block_level_block_calc_tall <- block_level_block_calc_tall %>% add_row(block_2_row)
      
      if (!is.na(row_i$block_3_end_dt)){
        block_3_row <- df_shell %>% add_row(participant_id = row_i$participant_id,
                                            study_day_start_date = row_i$study_day_start_date,
                                            day_start_hrts_AmericaChicago = row_i$day_start_hrts_AmericaChicago,
                                            day_end_hrts = row_i$day_end_hrts,
                                            block_start_hrts_AmericaChicago = row_i$block_2_end_dt,
                                            block_end_hrts_AmericaChicago = row_i$block_3_end_dt,
                                            block_calc = "3",
                                            multi_day_start = case_when(row_i$extra_day_start_block_number <= 3 ~ TRUE,
                                                                        T ~ F))
        block_level_block_calc_tall <- block_level_block_calc_tall %>% add_row(block_3_row)
        
        if (!is.na(row_i$block_4_end_dt)){
          block_4_row <- df_shell %>% add_row(participant_id = row_i$participant_id,
                                              study_day_start_date = row_i$study_day_start_date,
                                              day_start_hrts_AmericaChicago = row_i$day_start_hrts_AmericaChicago,
                                              day_end_hrts = row_i$day_end_hrts,
                                              block_start_hrts_AmericaChicago = row_i$block_3_end_dt,
                                              block_end_hrts_AmericaChicago = row_i$block_4_end_dt,
                                              block_calc = "4",
                                              multi_day_start = case_when(row_i$extra_day_start_block_number <= 4 ~ TRUE,
                                                                          T ~ F))
          block_level_block_calc_tall <- block_level_block_calc_tall %>% add_row(block_4_row)
        }
      }
    }
  }
}

block_level_block_calc_tall <- block_level_block_calc_tall[!duplicated(block_level_block_calc_tall),]

# ------------------------------------------------------------------------------------------
# Join the block_level_block_calc_tall data with the preprocessed ema data with block calcs
# ------------------------------------------------------------------------------------------

block_level_ema_dataset <- full_join(block_level_block_calc_tall, all_ema_data_wblocks_cc2 , by = c("participant_id", "day_start_hrts_AmericaChicago", "block_calc"))


# -----------------------------------------------------------------------------------------
# Update "Status" variable for not-sent random EMAs
#   - Differentiate between these three cases:
#        A. Block occured after the "end of day" button was pressed - "Not Sent - After End of Day"
#        B. End of Day button pressed within the block 
#        C. All else - "Not sent - Study Plan Noncompliance"
#
# And impute other variable values for blocks without EMAs
# -----------------------------------------------------------------------------------------

block_level_ema_dataset <- block_level_ema_dataset %>% 
  mutate(status = case_when(
    !is.na(status) ~ status,
    day_end_hrts < block_start_hrts_AmericaChicago.x ~ "UNDELIVERED - AFTER END OF DAY",
    day_end_hrts > block_start_hrts_AmericaChicago.x & day_end_hrts <= block_end_hrts_AmericaChicago.x ~ "UNDELIVERED - END OF DAY DURING BLOCK",
   T ~ "UNDELIVERED - STUDY PLAN NONCOMPLIANCE"),
   ema_type = case_when(
     !is.na(ema_type) ~ ema_type,
     T ~ "RANDOM"),
   cc_indicator = 1,
   block_start_hrts_AmericaChicago.y = case_when(
     !is.na(block_start_hrts_AmericaChicago.y) ~ block_start_hrts_AmericaChicago.y,
     T ~ block_start_hrts_AmericaChicago.x),
   block_end_hrts_AmericaChicago.y = case_when(
     !is.na(block_end_hrts_AmericaChicago.y) ~ block_end_hrts_AmericaChicago.y,
     T ~ block_end_hrts_AmericaChicago.x),
   multi_day_start.y = case_when(
     !is.na(multi_day_start.y) ~ multi_day_start.y,
     T ~ multi_day_start.x),
   extra_ema = case_when(
     !is.na(extra_ema) ~ extra_ema,
     T ~ FALSE)
   )

block_level_ema_dataset <- block_level_ema_dataset %>% 
  arrange(participant_id, ymd_hms(day_start_hrts_AmericaChicago)) %>% 
  group_by(participant_id, day_start_hrts_AmericaChicago) %>% 
  mutate(multi_day_start_on_study_day = any(multi_day_start.y)) %>% 
  mutate(extra_ema_on_study_day = any(extra_ema)) %>% 
  ungroup()

block_level_ema_dataset <- block_level_ema_dataset %>% select(-block_start_hrts_AmericaChicago.x, -block_end_hrts_AmericaChicago.x, -multi_day_start.x, #-study_day_start_date, 
                                                              -day_end_hrts)
block_level_ema_dataset <- block_level_ema_dataset %>% 
  relocate(block_calc, .before = extra_ema) %>% 
  relocate(day_start_hrts_AmericaChicago, .before = block_start_hrts_AmericaChicago.y)

block_level_ema_dataset <- block_level_ema_dataset %>% rename(
  block_start_hrts_AmericaChicago = block_start_hrts_AmericaChicago.y,
  block_end_hrts_AmericaChicago = block_end_hrts_AmericaChicago.y,
  multi_day_start = multi_day_start.y) 


block_level_ema_dataset %>% group_by(status) %>% summarize(n = n(), percent = n()/nrow(.)*100) %>% arrange(desc(percent))

# ------------------------------------------------------------------------------
# Join with "dates_w_block_long" for days without start day data
# ------------------------------------------------------------------------------
dates_w_block_long <- dates_w_block_long %>% mutate(block = as.character(block)) %>% 
  left_join(y = block_level_block_calc_tall,
            by = c("participant_id", "study_date" = "study_day_start_date", "block" = "block_calc"))

dates_w_block_long_v2 <- dates_w_block_long %>% mutate(ema_type = "RANDOM") %>% 
  add_row(
    dates_w_block_long %>% mutate(ema_type = "SMOKING")
  ) %>% 
  add_row(
    dates_w_block_long %>% mutate(ema_type = "STRESS")
  )


full_block_level_ema_dataset <- full_join(x = dates_w_block_long_v2 %>%  select(-day_end_hrts), 
                                          y = block_level_ema_dataset, 
                                          by = c("participant_id", "study_date" = "study_day_start_date",  "block" = "block_calc", "ema_type", 
                                                 "day_start_hrts_AmericaChicago", "block_start_hrts_AmericaChicago", "multi_day_start")) %>% 
  filter(ema_type == "RANDOM" | (ema_type != "RANDOM" & !is.na(end_hrts_AmericaChicago))) %>% 
  mutate(
    block_end_hrts_AmericaChicago.y = case_when(
      is.na(block_end_hrts_AmericaChicago.y) & !is.na(block_end_hrts_AmericaChicago.x) ~ block_end_hrts_AmericaChicago.x,
      T ~ block_end_hrts_AmericaChicago.y)
    ) %>% 
  rename(block_end_hrts_AmericaChicago = block_end_hrts_AmericaChicago.y) %>% 
  select(-block_end_hrts_AmericaChicago.x) %>% 
  arrange(participant_id, study_day_int, block) %>% 
  relocate(day_start_hrts_AmericaChicago,block_start_hrts_AmericaChicago, block_end_hrts_AmericaChicago,
           multi_day_start, .after = extra_ema_on_study_day) %>% 
  mutate(status = case_when(
    study_date > withdrew_date ~ "UNDELIVERED - AFTER WITHDREW DATE",
    T ~ status))

  
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Join with battery status data - original method required splitting the dataset 
# and joining with battery twice.
#
# Improved version used time_software_calc for the start and end times
# for records with time_software_calc, then others had the start and
# end calculated using available information (block start and end, etc.)
# So the block level ema dataset is merged with the battery data once
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

if(F){view(full_block_level_ema_dataset)}

# before joining battery data, compute time ranges as above,
# but for blocks with an EMA, the time range start and end
# will be the time_software_calc


full_block_level_ema_dataset_prebatt_v1 <- full_block_level_ema_dataset %>% 
  mutate(
  time_range_AmerChi_start = case_when(
    !is.na(time_software_calc) ~ time_software_calc,
    !is.na(block_start_hrts_AmericaChicago) ~ block_start_hrts_AmericaChicago,
    !is.na(lag(time_software_calc)) & lag(participant_id) == participant_id ~ lag(time_software_calc),
    lag(participant_id) == participant_id ~ lag(time_software_calc)),
  time_range_AmerChi_end = case_when(
    !is.na(time_software_calc) ~ time_software_calc,
    !is.na(block_end_hrts_AmericaChicago) ~ block_end_hrts_AmericaChicago,
    !is.na(lead(time_software_calc)) & lead(participant_id) == participant_id ~ lead(time_software_calc),
    lead(participant_id) == participant_id ~ lead(time_software_calc)),
  .after = study_date) %>% 
  relocate(time_software_calc, .after = study_date) %>% 
  mutate(
    time_range_AmerChi_start = case_when(
      is.na(time_software_calc) & is.na(time_range_AmerChi_start) & lag(participant_id) == participant_id ~ lag(time_range_AmerChi_end),
      T ~ time_range_AmerChi_start
    ),
    time_range_AmerChi_end = case_when(
      is.na(time_software_calc) & is.na(time_range_AmerChi_end) & lead(participant_id) == participant_id ~ lead(time_range_AmerChi_start),
      T ~ time_range_AmerChi_end
    )
  ) %>% 
  mutate(
    time_range_AmerChi_start = case_when(
      block == 1 & force_tz(study_date, tzone =  "America/Chicago") > time_range_AmerChi_start ~ force_tz(study_date, tzone =  "America/Chicago"),
      block == 1 & is.na(time_range_AmerChi_start) ~ force_tz(study_date, tzone =  "America/Chicago"),
      T ~ time_range_AmerChi_start),
    time_range_AmerChi_end = case_when(
      is.na(time_range_AmerChi_end) & block == 4 ~ force_tz(study_date, tzone = "America/Chicago") + hours(40),
      T ~ time_range_AmerChi_end)
  )

# without_battery_matches <- full_block_ema_w_battery_v2 %>% filter(is.na(end_hrts_AmericaChicago))
# with_battery_matches <- full_block_ema_w_battery_v2 %>% filter(!is.na(end_hrts_AmericaChicago))


# Fill in start and end time ranges for continuous durations
for (i in 1:nrow(full_block_level_ema_dataset_prebatt_v1)){
  print(i)
  i_time_range_AmerChi_end <- full_block_level_ema_dataset_prebatt_v1$time_range_AmerChi_end[i]
  if (is.na(i_time_range_AmerChi_end)){
    x <- 0
    while (is.na(i_time_range_AmerChi_end) & i + x <= nrow(full_block_level_ema_dataset_prebatt_v1)){
      x <- x+1
      i_time_range_AmerChi_end <- full_block_level_ema_dataset_prebatt_v1$time_range_AmerChi_end[i+x]
    } # while breaks when row i+x has a non-NA value
    full_block_level_ema_dataset_prebatt_v1$time_range_AmerChi_end[i:(i+x)] <- full_block_level_ema_dataset_prebatt_v1$time_range_AmerChi_end[i+x]
    full_block_level_ema_dataset_prebatt_v1$time_range_AmerChi_start[i:(i+x)] <- full_block_level_ema_dataset_prebatt_v1$time_range_AmerChi_start[i]
  }
}


# Join with the battery data using the time ranges for the day-blocks

full_block_ema_w_battery_v1 <- sqldf(
  "select bat.participant_id participant_id_bat, bat.battery_unix_datetime_start, bat.battery_unix_datetime_end,
  bat.battery_hrts_AmerChi_start, bat.battery_hrts_AmerChi_end, bat.battery_hrts_UTC_start, bat.battery_hrts_UTC_end,
  bat.battery_status,
  block.*
  from full_block_level_ema_dataset_prebatt_v1 block
    left outer join battery_data_binned_ranges bat on
      block.participant_id = bat.participant_id and
      ((block.time_range_AmerChi_start >= bat.battery_hrts_AmerChi_start and bat.battery_hrts_AmerChi_end >= block.time_range_AmerChi_end) or 
      (bat.battery_hrts_AmerChi_end >= block.time_range_AmerChi_start and bat.battery_hrts_AmerChi_end <= block.time_range_AmerChi_end) or
      (bat.battery_hrts_AmerChi_start >= block.time_range_AmerChi_start and bat.battery_hrts_AmerChi_start <= block.time_range_AmerChi_end))") %>% 
  as_tibble


#sqldf loses timezones
full_block_ema_w_battery_v1 <- full_block_ema_w_battery_v1  %>%
  mutate(begin_hrts_AmericaChicago = with_tz(begin_hrts_AmericaChicago, "America/Chicago"),
         end_hrts_AmericaChicago =  with_tz(end_hrts_AmericaChicago, "America/Chicago"),
         begin_hrts_UTC = with_tz(begin_hrts_UTC, "UTC"),
         end_hrts_UTC = with_tz(end_hrts_UTC, "UTC"),
         day_start_hrts_AmericaChicago = with_tz(day_start_hrts_AmericaChicago, "America/Chicago"),
         block_start_hrts_AmericaChicago = with_tz(block_start_hrts_AmericaChicago, "America/Chicago"),
         block_end_hrts_AmericaChicago = with_tz(block_end_hrts_AmericaChicago, "America/Chicago"),
         time_software_calc = with_tz(time_software_calc, "America/Chicago"),
         battery_hrts_AmerChi_start = with_tz(battery_hrts_AmerChi_start, tzone = "America/Chicago"),
         battery_hrts_AmerChi_end = with_tz(battery_hrts_AmerChi_end, tzone = "America/Chicago"),
         battery_hrts_UTC_start = with_tz(battery_hrts_UTC_start, "UTC"),
         battery_hrts_UTC_end = with_tz(battery_hrts_UTC_end, "UTC"),
         time_range_AmerChi_start = with_tz(time_range_AmerChi_start, "America/Chicago"),
         time_range_AmerChi_end = with_tz(time_range_AmerChi_end, "America/Chicago")) 


nrow(full_block_level_ema_dataset_prebatt_v1) # 8431
nrow(full_block_ema_w_battery_v1) # 12141

full_block_ema_w_battery_v1 %>% count(is.na(participant_id_bat))



full_block_ema_w_battery_v2 <- full_block_ema_w_battery_v1 %>% 
  mutate(
    battery_status = case_when(
      is.na(battery_status) ~ "No Battery Data",
      T ~ battery_status)
  ) %>% 
  group_by(participant_id, study_date, block, end_hrts_AmericaChicago) %>% 
  mutate(
    time_in_battery_range_mins = case_when(
      battery_hrts_AmerChi_start <= time_range_AmerChi_start & battery_hrts_AmerChi_end < time_range_AmerChi_end ~ difftime(battery_hrts_AmerChi_end, time_range_AmerChi_start, units = "mins"),
      battery_hrts_AmerChi_start > time_range_AmerChi_start & battery_hrts_AmerChi_end >= time_range_AmerChi_end ~ difftime(time_range_AmerChi_end, battery_hrts_AmerChi_start, units = "mins"),
      battery_hrts_AmerChi_start > time_range_AmerChi_start & battery_hrts_AmerChi_end < time_range_AmerChi_end ~ difftime(battery_hrts_AmerChi_end, battery_hrts_AmerChi_start, units = "mins")
    ),
    time_mins_10_100 = case_when(
      !is.na(time_in_battery_range_mins) & battery_status == "10-100%" ~ time_in_battery_range_mins
    ),
    time_mins_0_10 = case_when(
      !is.na(time_in_battery_range_mins) & battery_status == "0-10%" ~ time_in_battery_range_mins
    ),
    time_mins_no_battery_data = case_when(
      !is.na(time_in_battery_range_mins) & battery_status == "No Battery Data" ~ time_in_battery_range_mins
    ),
    block_time_mins = difftime(time_range_AmerChi_end, time_range_AmerChi_start, units = "mins"),
    .after = battery_status) %>% 
  mutate(
    time_10_100_all = case_when(
      !is.na(time_in_battery_range_mins) ~ sum(time_mins_10_100, na.rm = T)),
    time_0_10_all = case_when(
      !is.na(time_in_battery_range_mins) ~ sum(time_mins_0_10, na.rm = T)),
    time_no_battery_data_all = case_when(
      !is.na(time_in_battery_range_mins) ~ sum(time_mins_no_battery_data, na.rm = T)),
    .after = time_mins_no_battery_data) %>% 
  mutate(
    percent_time_battery_at_10_100 = as.numeric(time_10_100_all) / as.numeric(block_time_mins) * 100,
    percent_time_battery_at_0_10 = as.numeric(time_0_10_all) / as.numeric(block_time_mins) * 100,
    percent_time_battery_at_no_battery_data = as.numeric(time_no_battery_data_all) / as.numeric(block_time_mins) * 100, 
    sum_percent = percent_time_battery_at_10_100 + percent_time_battery_at_0_10 + percent_time_battery_at_no_battery_data,
    percent_time_battery_at_no_battery_data = case_when(
      sum_percent < 100 ~ percent_time_battery_at_no_battery_data + (100 - sum_percent),
      T ~ percent_time_battery_at_no_battery_data),
    sum_percent = percent_time_battery_at_10_100 + percent_time_battery_at_0_10 + percent_time_battery_at_no_battery_data,
    percent_time_battery_at_10_100 = round(percent_time_battery_at_10_100, digits = 1),
    percent_time_battery_at_0_10 = round(percent_time_battery_at_0_10, digits = 1),
    percent_time_battery_at_no_battery_data = round(percent_time_battery_at_no_battery_data, digits = 1),
    .after = time_no_battery_data_all
  )

full_block_ema_w_battery_v3 <- full_block_ema_w_battery_v2 %>% 
  mutate(
    battery_status = case_when(
      n() > 1 ~ NA_character_,
      T ~ battery_status)
  ) %>% 
  filter(row_number() == 1) %>% 
  ungroup() %>% 
  select(-participant_id_bat, -battery_unix_datetime_start, -battery_unix_datetime_end, -battery_hrts_AmerChi_start,
         -battery_hrts_AmerChi_end, -battery_hrts_UTC_start, -battery_hrts_UTC_end, -time_in_battery_range_mins, 
         -time_mins_10_100, -time_mins_0_10, -time_mins_no_battery_data, -time_10_100_all, -time_0_10_all, 
         -time_no_battery_data_all, -block_time_mins, -sum_percent, #-time_range_AmerChi_start, -time_range_AmerChi_end
         ) %>% 
  relocate(battery_status, percent_time_battery_at_10_100, percent_time_battery_at_0_10, percent_time_battery_at_no_battery_data, .after = with_any_response) %>% 
  relocate(end_hrts_AmericaChicago, .after = begin_hrts_AmericaChicago) %>% 
  mutate(
    status = case_when(
      status == "UNDELIVERED - STUDY PLAN NONCOMPLIANCE" ~ NA_character_, 
      T ~ status),
    battery_status = case_when(
      percent_time_battery_at_no_battery_data == 100 ~ "No Battery Data",
      percent_time_battery_at_10_100 == 100 ~ "10-100%",
      T ~ battery_status
    ),
    # percent_time for the 3 categories will be NA only when there is mixed battery data
    # at this stage, if there is mixed battery data, then the battery_status is NA
    # Need to modify for cases where the percent was 100 for No Battery Data
    # or 10-100%, because that is not mixed
    percent_time_battery_at_10_100 = case_when(
      !is.na(battery_status) ~ NA_real_,
      T ~ percent_time_battery_at_10_100
    ),
    percent_time_battery_at_0_10 = case_when(
      !is.na(battery_status) ~ NA_real_,
      T ~ percent_time_battery_at_0_10
    ),
    percent_time_battery_at_no_battery_data = case_when(
      !is.na(battery_status) ~ NA_real_,
      T ~ percent_time_battery_at_no_battery_data
    )
  ) %>% 
  # all days without a day start have no EMAs
  mutate(
    status = case_when(
      is.na(day_start_hrts_AmericaChicago) & is.na(status) ~ "UNDELIVERED - NO DAY START",
      T ~ status)
  ) %>% 
  mutate(
    cc_indicator = 2,
    status = case_when(
      is.na(status) & battery_status == "No Battery Data" ~ "UNDELIVERED - NO BATTERY DATA",
      is.na(status) & battery_status == "Dead Battery" ~ "UNDELIVERED - DEAD BATTERY",
      is.na(status) & battery_status == "0-10%" ~ "UNDELIVERED - LOW BATTERY",
      is.na(status) & battery_status == "10-100%" ~ "UNDELIVERED - SUFFICIENT BATTERY / OTHER CAUSE",
      is.na(status) & percent_time_battery_at_no_battery_data > 0 & percent_time_battery_at_no_battery_data < 100 ~ "UNDELIVERED - PARTIALLY MISSING BATTERY DATA",
      is.na(status) & percent_time_battery_at_no_battery_data == 0 & percent_time_battery_at_0_10 > 0 ~ "UNDELIVERED - PARTIALLY LOW BATTERY",
      T ~ status), 
    with_any_response = case_when(
      is.na(with_any_response) ~ 0,
      T ~ with_any_response), 
    extra_ema = case_when(
      !is.na(extra_ema) ~ extra_ema,
      T ~ FALSE),
    battery_status = case_when(
      is.na(battery_status) ~ "Mixed Battery Data",
      T ~ battery_status)
  ) %>% 
  group_by(participant_id, study_day_int) %>% 
  mutate(multi_day_start_on_study_day = any(multi_day_start)) %>% 
  mutate(extra_ema_on_study_day = any(extra_ema)) %>% 
  ungroup() %>% 
  filter(!is.na(study_day_int))

# End of adding battery data
# Most of the Undelivered EMAs are reconciled

full_block_ema_w_battery_v3_cc2 <- full_block_ema_w_battery_v3
  
full_block_ema_w_battery_v3 %>% count(status)
full_block_ema_w_battery_v3 %>% count(battery_status)

save(full_block_ema_w_battery_v3_cc2,
    file = file.path(path_breakfree_staged_data, "full_block_ema_w_battery_v3.RData"))

# -------------------------------------------------------------------
# -------------------------------------------------------------------
# Exploring Log Files
# Can only work for CC2, because CC1 had a different structure
# -------------------------------------------------------------------
# -------------------------------------------------------------------


load(file = file.path(path_breakfree_staged_data, "ema_raw_data_cc2.RData"))

remove(dat_file_counts_cc2,
       all_random_ema_status_files_cc2,
       all_random_ema_response_files_cc2,
       all_smoking_ema_status_files_cc2,
       all_smoking_ema_response_files_cc2,
       all_stress_ema_status_files_cc2,
       all_stress_ema_response_files_cc2,
       all_day_start_files_cc2,
       all_day_end_files_cc2)

full_block_ema_w_battery_v3 %>% nrow()
all_ema_conditions_files_cc2 %>% nrow()

colnames(all_ema_conditions_files_cc2) <- paste(colnames(all_ema_conditions_files_cc2), "log", sep = "_")

# ----------------------------------------------------------------
# Join the conditions file to the participant start/end date to 
# censor for enrolled pts and records within study period
# ----------------------------------------------------------------

conditions_data_w_datmaster_v1 <- all_ema_conditions_files_cc2 %>% 
  right_join(y = dat_master_start_end_dates_cc2,
             by = c("participant_id_log" = "participant_id"))


conditions_data_w_datmaster_v2 <- conditions_data_w_datmaster_v1 %>% 
  mutate(
    current_time_hrts_AmericaChicago_log = as_datetime(mCerebrum_timestamp_log/1000, tz = "America/Chicago"),
    date_log = as_date(current_time_hrts_AmericaChicago_log),
    .after = mCerebrum_timestamp_log
      ) %>% 
  mutate(block_log = block_number_log + 1, # block_number ranges from 0 to 3, instead of 1 to 4
         .after = block_number_log) %>% 
  mutate(
    ema_type_log = str_remove(ema_type_log, pattern = "EMA-")
  )

conditions_data_w_datmaster_v3 <- conditions_data_w_datmaster_v2 %>% 
  #filter(between(date_log, first_day_date, last_day_date)) %>% 
  filter(date_log > first_day_date & date_log < last_day_date) %>% 
  select(-withdrew_date)

# Create new variable for context on condition == 0
conditions_data_w_datmaster_v4 <- conditions_data_w_datmaster_v3 %>% 
  mutate(
   privacy_on = is_privacy_on_log == "1 [ema privacy enabled]",
   battery_insufficient = !(as.integer(GET_PHONE_BATTERY_log) > 10),
   driving = str_split_fixed(is_driving_log, pattern = " ", n = 2)[,1] == "true",
   condition_0_reason = case_when(
     # all(privacy_on, battery_insufficient, driving) ~ "privacy_on, battery_insufficient, driving",
     # privacy_on & battery_insufficient & !driving ~ "privacy_on, battery_insufficient",
     # privacy_on & !battery_insufficient & driving ~ "privacy_on, driving",
     # !privacy_on & battery_insufficient & driving ~ "battery_insufficient, driving",
     # privacy_on & !battery_insufficient & !driving ~ "privacy_on",
     # !privacy_on & battery_insufficient & !driving ~ "battery_insufficient",
     # !privacy_on & !battery_insufficient & driving ~ "driving"
     privacy_on ~ "privacy on",
     battery_insufficient ~ "battery insufficient",
     driving ~ "driving",
     condition_log == "1" ~ "Conditions Okay",
     is.na(condition_log) ~ "No condition data",
     condition_log == "0" ~ "Condition 0 - other"
   )
  )

# ---------------------------------------------------------------
# Join the conditions data with the block level dataset
# to try to reconcile the unreconciled undelivered random EMAs
# ---------------------------------------------------------------
full_block_ema_w_battery_v4 <- full_block_ema_w_battery_v3 %>% 
  mutate(
    undelivered = str_detect(status, pattern = "UNDELIVERED"),
    .after = status
  )

 
full_block_ema_w_battery_conditions_v1 <- sqldf(
  "select ema.*,
  cond.*
  from full_block_ema_w_battery_v4 ema
    left outer join conditions_data_w_datmaster_v4 cond on
    ema.participant_id = cond.participant_id_log and
    ema.ema_type = cond.ema_type_log and 
    (ema.time_range_AmerChi_start <= cond.current_time_hrts_AmericaChicago_log and 
        ema.time_range_AmerChi_end >= cond.current_time_hrts_AmericaChicago_log)") %>% 
  as_tibble


full_block_ema_w_battery_conditions_v1 %>% group_by(participant_id, study_day_int, block, ema_type, status) %>% mutate(n = n()) %>% filter(n>1 & undelivered) %>% View

full_block_ema_w_battery_conditions_v1 %>% filter(undelivered) %>% count(status, condition_0_reason) %>% View

# ----------------------------------------------------------------
# Start aggregating  
# ----------------------------------------------------------------

full_block_ema_w_battery_conditions_v2 <- full_block_ema_w_battery_conditions_v1 %>% 
  group_by(participant_id, study_day_int, block, ema_type, time_software_calc, status) %>% 
  mutate(n_conditions = n()) %>% 
  mutate(
    condition_0_reasons = paste(list(na.omit(unique(condition_0_reason)))),
    condition_0_reasons = case_when(
      condition_0_reasons == "character(0)" ~ NA_character_,
      T ~ condition_0_reasons)
    ) %>% 
  filter(row_number() == 1) %>% 
  ungroup() %>% 
  relocate(condition_0_reasons, .after = status) 

full_block_ema_w_battery_conditions_v3 <- full_block_ema_w_battery_conditions_v2 %>% 
  mutate(
    status = case_when(
      !(status %in% c("UNDELIVERED - PARTIALLY LOW BATTERY", "UNDELIVERED - PARTIALLY MISSING BATTERY DATA", "UNDELIVERED - SUFFICIENT BATTERY / OTHER CAUSE")) ~ status,
      condition_0_reasons == "driving" ~ "UNDELIVERED - Driving",
      condition_0_reasons == "battery insufficient" ~ "UNDELIVERED - Battery Insufficient",
      status == "UNDELIVERED - SUFFICIENT BATTERY / OTHER CAUSE" & condition_0_reasons == "Conditions Okay" ~ "UNDELIVERED - Unknown Cause - Missing Data: No corresponding EMA to Condition Record - SUFFICIENT BATTERY",
      status == "UNDELIVERED - SUFFICIENT BATTERY / OTHER CAUSE" & is.na(condition_0_reasons) ~ "UNDELIVERED - Unknown Cause - Missing Data: No Corresponding Conditions Data - SUFFICIENT BATTERY",
      status == "UNDELIVERED - SUFFICIENT BATTERY / OTHER CAUSE" & condition_0_reasons == "c(\"driving\", \"Conditions Okay\")" ~ "UNDELIVERED - Unknown Cause - Missing Data: No corresponding EMA to Condition Record - SUFFICIENT BATTERY",
      status == "UNDELIVERED - PARTIALLY MISSING BATTERY DATA" & 
        condition_0_reasons %in% c("c(\"Conditions Okay\", \"battery insufficient\")", "c(\"battery insufficient\", \"Conditions Okay\")", "c(\"Conditions Okay\", \"driving\")", "c(\"driving\", \"Conditions Okay\")")  ~ "UNDELIVERED - Unknown Cause - Missing Data: No corresponding EMA to Condition Record - PARTIALLY MISSING BATTERY DATA",
      #| condition_0_reasons == "c(\"battery insufficient\", \"Conditions Okay\")")
      # status == "UNDELIVERED - PARTIALLY MISSING BATTERY DATA" & 
      #   condition_0_reasons %in% c("c(\"Conditions Okay\", \"driving\")", "c(\"driving\", \"Conditions Okay\")") ~ "UNDELIVERED - PARTIALLY LOW BATTERY - Conditions Mixed - Partially Driving",
      status == "UNDELIVERED - PARTIALLY MISSING BATTERY DATA" & condition_0_reasons == "Conditions Okay" ~ "UNDELIVERED - Unknown Cause - Missing Data: No corresponding EMA to Condition Record - PARTIALLY MISSING BATTERY DATA",
      status == "UNDELIVERED - PARTIALLY MISSING BATTERY DATA" & is.na(condition_0_reasons) ~ "UNDELIVERED - Unknown Cause - Missing Data: No Corresponding Conditions Data - PARTIALLY MISSING BATTERY DATA",
      status == "UNDELIVERED - PARTIALLY LOW BATTERY" & condition_0_reasons == "Conditions Okay" ~ "UNDELIVERED - Unknown Cause - Missing Data: No corresponding EMA to Condition Record - PARTIALLY LOW BATTERY",
      status == "UNDELIVERED - PARTIALLY LOW BATTERY" & is.na(condition_0_reasons) ~ "UNDELIVERED - Unknown Cause - Missing Data: No Corresponding Conditions Data - PARTIALLY LOW BATTERY",
      T ~ status
    )
  )

full_block_ema_w_battery_conditions_v3 %>% count(status) %>% View

# rename status and create undelivered indicators to match cc1 format
full_block_ema_w_battery_conditions_v4 <- full_block_ema_w_battery_conditions_v3 %>% 
  mutate(status_raw = status,
         status = str_split_fixed(status_raw, pattern = " - ", n = 4)[,1],
         undelivered_rsn = str_split_fixed(status_raw, pattern = " - ", n = 4)[,2],
         conditions_stream_summary = str_split_fixed(status_raw, pattern = " - ", n = 4)[,3],
         battery_stream_summary = str_split_fixed(status_raw, pattern = " - ", n = 4)[,4],
         
         .after = status
         )
  
  
full_block_ema_w_battery_conditions_v4 %>% count(status, undelivered_rsn, conditions_stream_summary, battery_stream_summary) %>% View


save(full_block_ema_w_battery_conditions_v4,
     file = file.path(path_breakfree_staged_data, "block_level_ema_cc2.RData"))







