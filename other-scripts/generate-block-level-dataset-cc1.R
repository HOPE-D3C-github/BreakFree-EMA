# This file is sourced from the end of "calculate_study_day_ema_blocks_cc1.R"

library(sqldf)
library(rlang)
require(data.table)

# -----------------------------------------------------------------------------
# Merge the ema dataset with calculated blocks to 
# a dataset of all blocks calculated from study day start times
# and prepolutated blocks from 4 random x
# -----------------------------------------------------------------------------

load(file.path(path_breakfree_staged_data, "masterlist.RData"))
load(file.path(path_breakfree_staged_data, "battery_data_binned.RData"))

# -----------------------------------------------------------------------------

dat_master_start_end_dates_cc1 <- dat_master %>% filter(cc_indicator == 1) %>% select(participant_id, first_day_date, last_day_date, withdrew, withdrew_date)

dates_w_block_long <- data.frame(participant_id = character(),
                               study_date = as.Date(character()),
                               study_day_int = integer(),
                               block = integer(),
                               withdrew_date = as.Date(character()))

for (i in 1:nrow(dat_master_start_end_dates_cc1)){
  dates_w_block_long <- dates_w_block_long %>% 
    add_row(expand_grid(participant_id = dat_master_start_end_dates_cc1$participant_id[i], 
                        study_date = as_date(dat_master_start_end_dates_cc1$first_day_date[i] : dat_master_start_end_dates_cc1$last_day_date[i]),
                        block = 1:4,
                        withdrew_date = dat_master_start_end_dates_cc1$withdrew_date[i]) %>%
              mutate(study_day_int = ceiling(row_number()/4), .after = study_date))
}

# ------------------------------------------------------
# START Imputing Blocks based on Start times
# ------------------------------------------------------

block_level_block_calc_wide <- day_start_and_end_cc1 %>% mutate(
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

# Manual updates for daylight savings on March 10,2019
block_level_block_calc_wide <- block_level_block_calc_wide %>% 
  mutate(block_4_end_dt = case_when(
      participant_id == "3160" & study_day_start_date == "2019-03-09" ~ day_start_hrts_AmericaChicago + hours(17),
      T ~ block_4_end_dt))

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

block_level_ema_dataset <- full_join(block_level_block_calc_tall, all_ema_data_wblocks_cc1, by = c("participant_id", "day_start_hrts_AmericaChicago", "block_calc"))


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
    cc_indicator = 1,
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
# CC1:
# 182 out of 3676 undelivered EMAs (4.95%) 
# yet to be reconciled
# CC2: (requires updates to remove NA study_day_int)
# 272 out of 4,774 undelivered EMAs (5.7%) 
# yet to be reconciled

full_block_ema_w_battery_v3_cc1 <- full_block_ema_w_battery_v3
  
full_block_ema_w_battery_v3 %>% 
  filter(!(status %in% c('ABANDONED_BY_TIMEOUT', 'ABANDONED_BY_USER', 'COMPLETED', 'MISSED'))) %>% 
  summarise(undel_not_reconciled = sum(status %in% c('UNDELIVERED - PARTIALLY LOW BATTERY', 'UNDELIVERED - PARTIALLY MISSING BATTERY DATA', 'UNDELIVERED - SUFFICIENT BATTERY / OTHER CAUSE')),
            total_undelivered = n(),
            percent_undelivered_not_reconciled = round(undel_not_reconciled / total_undelivered * 100, digits = 2))


full_block_ema_w_battery_v3 %>% count(battery_status)

save(full_block_ema_w_battery_v3_cc1,
     file = file.path(path_breakfree_staged_data, "full_block_ema_w_battery_v3_cc1.RData"))

# -------------------------------------------------------------------
# -------------------------------------------------------------------
# Exploring Log Phone File
# Can only work for CC1, because CC2 had a different structure
# -------------------------------------------------------------------
# -------------------------------------------------------------------

log_phone_data2_v2 <- log_phone_data2 %>% rename(current_time_hrts_AmericaChicago = sftware_init_hrts_AmericaChicago) %>% select(-current_time) %>% 
  mutate(ema_type = str_remove(id, pattern = "_EMA"))

colnames(log_phone_data2_v2) <- paste(colnames(log_phone_data2_v2), "log", sep = "_")

full_block_ema_w_battery_cc1_v4 <- full_block_ema_w_battery_v3 %>% 
  mutate(
    undelivered = str_detect(status, pattern = "UNDELIVERED"),
    .after = status
  )

# finished prepping both datasets for the merge

full_block_ema_w_battery_log_phone_cc1_v1 <- sqldf(
  "select ema.*, 
  log.*
  from full_block_ema_w_battery_cc1_v4 ema
    left outer join log_phone_data2_v2 log on
     ema.participant_id = log.participant_id_log and
     ema.ema_type = log.ema_type_log and
      (ema.time_range_AmerChi_start <= log.current_time_hrts_AmericaChicago_log and 
        ema.time_range_AmerChi_end >= log.current_time_hrts_AmericaChicago_log)") %>% 
  as_tibble

full_block_ema_w_battery_log_phone_cc1_v1 <- full_block_ema_w_battery_log_phone_cc1_v1 %>% 
  relocate(contains("log"), .after = status)


full_block_ema_w_battery_log_phone_cc1_v1 %>% count(is.na(participant_id_log), undelivered)

full_block_ema_w_battery_log_phone_cc1_v2 <- full_block_ema_w_battery_log_phone_cc1_v1 %>% 
  select(-c(VALID_BLOCK.VALID_BLOCK_STRESS_EMA..STATUS_log, VALID_BLOCK.VALID_BLOCK_STRESS_EMA..CONDITION_log, VALID_BLOCK.VALID_BLOCK_SMOKING_EMA..STATUS_log,
                 VALID_BLOCK.VALID_BLOCK_SMOKING_EMA..CONDITION_log, LAST_EMA_EMI.STRESS_EMA_5..STATUS_log, LAST_EMA_EMI.STRESS_EMA_5..CONDITION_log,
                 LAST_EMA_EMI.SMOKING_EMA_5..STATUS_log, LAST_EMA_EMI.SMOKING_EMA_5..CONDITION_log, LAST_EMA_EMI.STRESS_EMA_30..STATUS_log, LAST_EMA_EMI.STRESS_EMA_30..CONDITION_log,
                 EMA.STRESS_EMA..STATUS_log, EMA.STRESS_EMA..CONDITION_log, LAST_EMA_EMI.SMOKING_EMA_30..STATUS_log, LAST_EMA_EMI.SMOKING_EMA_30..CONDITION_log,
                 EMA.SMOKING_EMA..STATUS_log, EMA.SMOKING_EMA..CONDITION_log)) %>% 
  filter(!is.na(study_day_int))

test <- full_block_ema_w_battery_log_phone_cc1_v2 %>% 
  group_by(participant_id, ema_type, study_day_int, block, end_hrts_AmericaChicago, status) %>% 
  summarize(n = n())

max(test$n)

# 764 log file records for the time range for
# participant_id == "3008" & study_day_int == 10 & block == 4 
# all noted as "new day is not started" and false for valid block.
# block occurs before midnight and crosses to the next calendar day
if(F){full_block_ema_w_battery_log_phone_cc1_v2 %>% filter(participant_id == "3008" & study_day_int == 10 & block == 4) %>% View()}

if(F){full_block_ema_w_battery_log_phone_cc1_v2 %>% filter(participant_id == "3060" & study_day_int == 7 & block == 3) %>% View()}

test2 <- full_block_ema_w_battery_log_phone_cc1_v2 %>% 
  group_by(participant_id, ema_type, study_day_int, block, end_hrts_AmericaChicago, status) %>%
  mutate(
    all_log_valid_block_is_false = all(VALID_BLOCK.VALID_BLOCK_RANDOM_EMA..STATUS_log == "false"),
    any_log_valid_block_is_false = any(VALID_BLOCK.VALID_BLOCK_RANDOM_EMA..STATUS_log == "false"),
    .after = VALID_BLOCK.VALID_BLOCK_RANDOM_EMA..STATUS_log
    ) %>% ungroup()

test2 %>% count(all_log_valid_block_is_false, any_log_valid_block_is_false)

if(F){test2 %>% filter(any_log_valid_block_is_false) %>% View()}


if(F){test2 %>% filter(!all_log_valid_block_is_false & any_log_valid_block_is_false &
                         status %in% c("UNDELIVERED - SUFFICIENT BATTERY / OTHER CAUSE", "UNDELIVERED - PARTIALLY LOW BATTERY", "UNDELIVERED - PARTIALLY MISSING BATTERY DATA")) %>% View()}

# The above view only corresponds to one participant/day/block
# Participant_id 3115 on study day 13, block 4
# about half of the status logs say, "block(4) triggered: 0 expected: 1" 
# while the second half say, "new day is not started"

if(F){test2 %>% filter(status %in% c("UNDELIVERED - SUFFICIENT BATTERY / OTHER CAUSE", "UNDELIVERED - PARTIALLY LOW BATTERY", "UNDELIVERED - PARTIALLY MISSING BATTERY DATA")) %>% View()}

#filter out 

full_block_ema_w_battery_log_phone_cc1_v2 %>% filter(participant_id != "3115" & status %in% c("UNDELIVERED - SUFFICIENT BATTERY / OTHER CAUSE", "UNDELIVERED - PARTIALLY LOW BATTERY", "UNDELIVERED - PARTIALLY MISSING BATTERY DATA")) %>% 
  count(VALID_BLOCK.VALID_BLOCK_RANDOM_EMA..STATUS_log)
  
# NA's represent blocks which had no corresponding log data or were undelivered in log data and labelled block "-1"
# All remaining have a true status - which is entirely contradictory

full_block_ema_w_battery_log_phone_cc1_v2 %>% filter(participant_id != "3115" & status %in% c("UNDELIVERED - SUFFICIENT BATTERY / OTHER CAUSE", "UNDELIVERED - PARTIALLY LOW BATTERY", "UNDELIVERED - PARTIALLY MISSING BATTERY DATA")) %>% 
  count(status, status_log, VALID_BLOCK.VALID_BLOCK_RANDOM_EMA..STATUS_log)

# Shows that there are 40 blocks where log file says an EMA was delivered - comes back to the data discordance seen in the joins
full_block_ema_w_battery_log_phone_cc1_v2 %>% filter(participant_id != "3115" &
     status %in% c("UNDELIVERED - SUFFICIENT BATTERY / OTHER CAUSE", "UNDELIVERED - PARTIALLY LOW BATTERY", "UNDELIVERED - PARTIALLY MISSING BATTERY DATA",
                   "UNDELIVERED - AFTER END OF DAY", "UNDELIVERED - END OF DAY DURING BLOCK", "UNDELIVERED - LOW BATTERY", 
                   "UNDELIVERED - NO BATTERY DATA", "UNDELIVERED - NO DAY START")) %>% 
  count(status_log, VALID_BLOCK.VALID_BLOCK_RANDOM_EMA..STATUS_log)  
  
full_block_ema_w_battery_log_phone_cc1_v2 %>% filter(participant_id != "3115" &
                                                       status %in% c("UNDELIVERED - SUFFICIENT BATTERY / OTHER CAUSE", "UNDELIVERED - PARTIALLY LOW BATTERY", "UNDELIVERED - PARTIALLY MISSING BATTERY DATA",
                                                                     "UNDELIVERED - AFTER END OF DAY", "UNDELIVERED - END OF DAY DURING BLOCK", "UNDELIVERED - LOW BATTERY", 
                                                                     "UNDELIVERED - NO BATTERY DATA", "UNDELIVERED - NO DAY START")) %>% 
  filter(status_log == "ABANDONED_BY_TIMEOUT") %>% View()

# ---------------------------------------------------------------------------------------
# Merge the dataset with the start and end dates from dat_master,
# then censor the data to the study period
# this step already occurred for the EMA data, but has not occurred for the battery nor
# log file data
# ---------------------------------------------------------------------------------------
full_block_ema_w_battery_log_phone_cc1_v3 <- full_block_ema_w_battery_log_phone_cc1_v2 %>% 
  left_join(y = dat_master_start_end_dates_cc1 %>% select(participant_id, first_day_date, last_day_date),
            by = c("participant_id"))

full_block_ema_w_battery_log_phone_cc1_v4 <- full_block_ema_w_battery_log_phone_cc1_v3 %>% 
  filter(!is.na(first_day_date)) %>%   #remove participants who were never formally enrolled
  filter((between(as_date(current_time_hrts_AmericaChicago_log), first_day_date, last_day_date) | is.na(current_time_hrts_AmericaChicago_log)) &
           (between(as_date(time_software_calc), first_day_date, last_day_date) | is.na(time_software_calc))) %>% 
  filter(!is.na(study_day_int))

# ----------------------------------------------------------------------------------------
# Start aggregating log status info 
#
# Currently, one EMA could repeat for each corresponding log status 
# ----------------------------------------------------------------------------------------

ema_blocklevel_w_log_v1 <-  full_block_ema_w_battery_log_phone_cc1_v4

if(F){ema_blocklevel_w_log_v1 %>% 
  group_by(participant_id, ema_type, study_day_int, block, end_hrts_AmericaChicago, status) %>% 
  filter(undelivered & n() == 1) %>% View()}

if(F){ema_blocklevel_w_log_v1 %>% 
  group_by(participant_id, ema_type, study_day_int, block, end_hrts_AmericaChicago, status) %>% 
  filter(undelivered & n() > 1) %>% ungroup() %>% View()}

if(F){ema_blocklevel_w_log_v1 %>% 
  group_by(participant_id, ema_type, study_day_int, block, end_hrts_AmericaChicago, status) %>% 
  filter(undelivered & n() > 1) %>%
  filter(status %in% c("UNDELIVERED - PARTIALLY LOW BATTERY", "UNDELIVERED - PARTIALLY MISSING BATTERY DATA", "UNDELIVERED - SUFFICIENT BATTERY / OTHER CAUSE")) %>% 
  ungroup() %>% 
  View()}

if(TRUE){
  ema_blocklevel_w_log_v2 <- ema_blocklevel_w_log_v1 %>% mutate(current_time_hrts_AmericaChicago_log = as.character(current_time_hrts_AmericaChicago_log)) %>% group_by(participant_id, ema_type, study_day_int, block, end_hrts_AmericaChicago, status)
  for (variable in colnames(ema_blocklevel_w_log_v2)){
    if (str_ends(variable, pattern = "_log")){
      varname = paste0(variable, "_agg")    # Create name of the aggregated variable
      ema_blocklevel_w_log_v2 <- ema_blocklevel_w_log_v2 %>% 
        mutate(
          !!varname := list(unique(na.omit(unlist(list(!! rlang::sym(variable)))))), # The new name = list of all the values for the given variable
          .after = variable) #%>% 
        #select(-!!variable)   # remove original variable
    }
  }
  ema_blocklevel_w_log_v3 <- ema_blocklevel_w_log_v2 %>% 
    filter(row_number() == 1) %>% 
    ungroup()
}

log_status_colnames <- colnames(ema_blocklevel_w_log_v3)[str_ends(colnames(ema_blocklevel_w_log_v3), pattern = "..STATUS_log_agg")]

ema_blocklevel_w_log_v4 <- ema_blocklevel_w_log_v3 %>% mutate(log_statuses_w_false = NA_character_, .after = status_log_agg)
for (i in 1:nrow(ema_blocklevel_w_log_v4)){
  row_i_status_vars <- ema_blocklevel_w_log_v4 %>% select(all_of(log_status_colnames)) %>% slice(i)
  log_status_colnames_w_false = log_status_colnames[which(row_i_status_vars == "false")]
  
  if(length(log_status_colnames_w_false) > 0){
    ema_blocklevel_w_log_v4$log_statuses_w_false[i] <- as.character(paste(list(log_status_colnames_w_false)))
  }
}

ema_blocklevel_w_log_v5 <- ema_blocklevel_w_log_v4 %>% 
  mutate(undelivered_log_reason = case_when(
    !undelivered ~ NA_character_,
    log_statuses_w_false == "DATA_QUALITY.DATA_QUALITY_5..STATUS_log_agg" ~ "Poor Data Quality",
    log_statuses_w_false == "VALID_BLOCK.VALID_BLOCK_RANDOM_EMA..STATUS_log_agg" ~ "Invalid Block",
    log_statuses_w_false == "NOT_DRIVING.NOT_DRIVING_5..STATUS_log_agg" ~ "Driving",
    log_statuses_w_false == "PHONE_BATTERY.PHONE_BATTERY_10..STATUS_log_agg" ~ "Insufficient Phone Battery",
    log_statuses_w_false == "c(\"DATA_QUALITY.DATA_QUALITY_5..STATUS_log_agg\", \"PHONE_BATTERY.PHONE_BATTERY_10..STATUS_log_agg\")" ~ "Poor Data Quality and Insufficient Phone Battery",
    log_statuses_w_false == "c(\"DATA_QUALITY.DATA_QUALITY_5..STATUS_log_agg\", \"NOT_DRIVING.NOT_DRIVING_5..STATUS_log_agg\")" ~ "Poor Data Quality and Driving",
    unixtime_log_agg == 'character(0)' ~ paste("Unknown Cause - Missing Data: No Corresponding Conditions Data", 
                                               str_split_fixed(status, pattern = " - ", n = 2)[,2], sep = " - "),
    BLOCK_log_agg == "-1" ~ paste("Unknown Cause - Missing Data: Block Negative 1", 
                                  str_split_fixed(status, pattern = " - ", n = 2)[,2], sep = " - "),
    #T ~ paste("Unknown Cause - Conditions =", status_log_agg)
    T ~ paste("Unknown Cause - No corresponding EMA to Condition Record", str_split_fixed(status, pattern = " - ", n = 2)[,2], sep = " - ")
  ),
  .before = log_statuses_w_false)

ema_blocklevel_w_log_v5 %>% count(status)

ema_blocklevel_w_log_v5 %>% 
  filter(status %in% c('UNDELIVERED - PARTIALLY LOW BATTERY', 'UNDELIVERED - PARTIALLY MISSING BATTERY DATA', 'UNDELIVERED - SUFFICIENT BATTERY / OTHER CAUSE')) %>% #View()
  count(undelivered_log_reason)

ema_blocklevel_w_log_v5 %>% 
  filter(status %in% c('UNDELIVERED - PARTIALLY LOW BATTERY', 'UNDELIVERED - PARTIALLY MISSING BATTERY DATA', 'UNDELIVERED - SUFFICIENT BATTERY / OTHER CAUSE')) %>% 
  filter(is.na(undelivered_log_reason)) %>% View()


ema_blocklevel_w_log_v6 <- ema_blocklevel_w_log_v5 %>% 
  mutate(status = case_when(
    !undelivered ~ status,
    !(status %in% c('UNDELIVERED - PARTIALLY LOW BATTERY', 'UNDELIVERED - PARTIALLY MISSING BATTERY DATA', 'UNDELIVERED - SUFFICIENT BATTERY / OTHER CAUSE')) ~ status,
    str_detect(undelivered_log_reason, pattern = "Undetermined") ~ paste(status, undelivered_log_reason, sep = " - "), 
    T ~ paste0("UNDELIVERED - ", undelivered_log_reason)
  ))

ema_blocklevel_w_log_v6 %>% count(status) %>% View  
#ema_blocklevel_w_log_v6 %>% filter(status == "UNDELIVERED - Unknown Cause - No corresponding EMA to Condition Record") %>% select(status_log_agg) %>% View
ema_blocklevel_w_log_v6_cc1 <- ema_blocklevel_w_log_v6

# Cleanup undelivered to match CC2 format and create undelivered indicators

ema_blocklevel_w_log_v7 <- ema_blocklevel_w_log_v6 %>% 
  mutate(status_raw = status, 
         status = str_split_fixed(status_raw, pattern = " - ", n = 4)[,1],
         undelivered_rsn = str_split_fixed(status_raw, pattern = " - ", n = 4)[,2],
         conditions_stream_summary = str_split_fixed(status_raw, pattern = " - ", n = 4)[,3],
         battery_stream_summary = str_split_fixed(status_raw, pattern = " - ", n = 4)[,4],
         
         .after = status
         )

ema_blocklevel_w_log_v7 %>% count(status, undelivered_rsn, conditions_stream_summary, battery_stream_summary) %>% View

ema_blocklevel_w_log_v7_cc1 <- ema_blocklevel_w_log_v7

save(ema_blocklevel_w_log_v7_cc1,
     file = file.path(path_breakfree_staged_data, "block_level_ema_cc1.RData"))



