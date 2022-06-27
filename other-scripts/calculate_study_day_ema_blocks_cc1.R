library(dplyr)
library(lubridate)
library(tidyr)
library(sqldf)
library(readr)
library(testthat)

source("paths.R")
source(file.path("collect-functions", "io-utils.R"))
load(file.path(path_breakfree_staged_data, "ema_responses_raw_data_cc1.RData"))
load(file.path(path_breakfree_staged_data, "all_ema_data_cc1.RData"))
remove(all_random_ema_response_files_cc1, all_smoking_ema_response_files_cc1, all_stress_ema_response_files_cc1)

log_phone_data1 <- do.call(bind_rows, all_log_phone_files_cc1)
log_phone_data2 <- log_phone_data1
# Replace "" with NA
log_phone_data2[log_phone_data2==""]<-NA

log_phone_data2 <- log_phone_data2 %>% relocate(BLOCK, .before = current_time) %>% 
  mutate(sftware_init_hrts_AmericaChicago =  as.POSIXct(as.numeric(unixtime), tz = "UTC", origin="1970-01-01") %>% with_tz(., "America/Chicago"), .after = current_time)

log_phone_data3 <- log_phone_data2 %>% filter(status != "NOT_DELIVERED") %>% 
  group_by(participant_id) %>% 
  arrange(participant_id, sftware_init_hrts_AmericaChicago) %>% 
  mutate(lead_sftware_init_hrts_AmericaChicago = lead(sftware_init_hrts_AmericaChicago, 
                             default = as.POSIXct(as.numeric("1996356467"), tz = "UTC", origin="1970-01-01") %>% with_tz(., "America/Chicago")), 
         .after = sftware_init_hrts_AmericaChicago) %>% 
  ungroup()

lj <- sqldf(
    "select log.participant_id, ema.participant_id participant_id_ema, log.study_day, log.BLOCK, log.sftware_init_hrts_AmericaChicago, log.lead_sftware_init_hrts_AmericaChicago, log.id, log.status as status_log,
      iif(log.participant_id is null,0,1) in_log, iif(ema.participant_id is null,0,1) in_ema,
      ema.ema_type, ema.status status_ema,  ema.end_hrts_AmericaChicago
      from log_phone_data3 log
        left outer join all_ema_data_cc1 ema on 
            log.participant_id = ema.participant_id and 
            (log.sftware_init_hrts_AmericaChicago <= ema.end_hrts_AmericaChicago and log.lead_sftware_init_hrts_AmericaChicago >= ema.end_hrts_AmericaChicago)") %>% 
  as_tibble

lj %>% count(in_log, in_ema)
if(F){lj %>% filter(in_ema==0) %>% View}
lj %>% filter(is.na(participant_id_ema)) %>% count(id, status_log)


rj <- sqldf(
  "select log.participant_id participant_id_log, log.study_day, log.BLOCK, log.sftware_init_hrts_AmericaChicago, log.lead_sftware_init_hrts_AmericaChicago, log.id, log.status as status_log,
      iif(log.participant_id is null,0,1) in_log, iif(ema.participant_id is null,0,1) in_ema,
      ema.*
      from all_ema_data_cc1 ema
        left outer join log_phone_data3 log on 
            log.participant_id = ema.participant_id and
            log.status = ema.status and
            (log.sftware_init_hrts_AmericaChicago <= ema.end_hrts_AmericaChicago and log.lead_sftware_init_hrts_AmericaChicago >= ema.end_hrts_AmericaChicago)") %>% 
  as_tibble

test_that(desc = "Did not create extra rows during the merge",{
  expect_equal(object = nrow(rj), expected = nrow(all_ema_data_cc1))
})

mismatched_ema_type_indices <- c()
for (i in 1:nrow(rj)){
  if (!is.na(rj$id[i])){
    if (str_remove(rj$id[i], "_EMA") != rj$ema_type[i]){
      mismatched_ema_type_indices <- append(mismatched_ema_type_indices, i)
    }
  }
}


rj2 <- rj %>% mutate(
  study_day = ifelse(row_number() %in% mismatched_ema_type_indices, NA, study_day),
  BLOCK = ifelse(row_number() %in% mismatched_ema_type_indices, NA, BLOCK),
  sftware_init_hrts_AmericaChicago = case_when(!(row_number() %in% mismatched_ema_type_indices) ~ sftware_init_hrts_AmericaChicago),
  lead_sftware_init_hrts_AmericaChicago = case_when(!(row_number() %in% mismatched_ema_type_indices) ~ lead_sftware_init_hrts_AmericaChicago),
  id = ifelse(row_number() %in% mismatched_ema_type_indices, NA, id),
  status_log = ifelse(row_number() %in% mismatched_ema_type_indices, NA, status_log),
  in_log = ifelse(row_number() %in% mismatched_ema_type_indices, 0, status_log)
)

rj %>% count(in_log, in_ema)


test_that(desc = "EMA type and status matches",{
  for (i in 1:nrow(rj2)){
    if (!is.na(rj2$id[i])){
      expect_equal(object = str_remove(rj2$id[i], "_EMA"),
                   expected = rj2$ema_type[i])
      expect_equal(object = rj2$status_log[i],
                   expected = rj2$status[i])
    }}})

rj2 <- rj2 %>% mutate(end_hrts_AmericaChicago =  with_tz(end_hrts_AmericaChicago, "America/Chicago"),
                     begin_hrts_AmericaChicago = with_tz(begin_hrts_AmericaChicago, "America/Chicago"),
                      sftware_init_hrts_AmericaChicago = with_tz(sftware_init_hrts_AmericaChicago, "America/Chicago"),
                      lead_sftware_init_hrts_AmericaChicago = with_tz(lead_sftware_init_hrts_AmericaChicago, "America/Chicago")) %>% 
  relocate(participant_id, .before = participant_id_log) %>% 
  select(-participant_id_log)

day_start_data_cc1 <- do.call(rbind, all_day_start_files_cc1) #convert list of tibbles to one tibble for all participants
day_end_data_cc1 <- do.call(rbind, all_day_end_files_cc1)

day_start_data_cc1_v2 <- day_start_data_cc1 %>% 
  mutate(X1hrts = as.POSIXct(as.numeric(X1/1000), tz = "UTC", origin="1970-01-01") %>% with_tz(., "America/Chicago"),
         X3hrts = as.POSIXct(as.numeric(X3/1000), tz = "UTC", origin="1970-01-01") %>% with_tz(., "America/Chicago"))

day_end_data_cc1_v2 <- day_end_data_cc1 %>% 
  mutate(X1hrts = as.POSIXct(as.numeric(X1/1000), tz = "UTC", origin="1970-01-01") %>% with_tz(., "America/Chicago"),
         X3hrts = as.POSIXct(as.numeric(X3/1000), tz = "UTC", origin="1970-01-01") %>% with_tz(., "America/Chicago"))

test1 <- test_that(desc = "The two timestamp values per row are within 1 second of eachother", {
  for (i in 1:length(day_start_data_cc1_v2)){
    expect_lt(
      abs(day_start_data_cc1_v2$X1hrts[i] - day_start_data_cc1_v2$X3hrts[i]), as.difftime(1, units = "secs"))
  }
  for (i in 1:length(day_end_data_cc1_v2)){
    expect_lt(
      abs(day_end_data_cc1_v2$X1hrts[i] - day_end_data_cc1_v2$X3hrts[i]), as.difftime(1, units = "secs"))
  }
})

test2 <- test_that(desc = "No missing timestamps",{
  expect_true(any(is.na(day_start_data_cc1_v2$X1hrts)) == FALSE)
  expect_true(any(is.na(day_end_data_cc1_v2$X1hrts)) == FALSE)
})

if(test1 & test2){
  # Adding lead for day_start_hrts_AmericaChicago, so the day_end can be crosswalked between a day_start and the next day_start (the lead)
  day_start_data_cc1_v3 <- day_start_data_cc1_v2 %>% 
    select(participant_id, X1, X1hrts) %>% 
    rename(day_start_unixts = X1, day_start_hrts_AmericaChicago = X1hrts) %>% 
    arrange(participant_id, day_start_unixts) %>%
    group_by(participant_id) %>% 
    mutate(lead_day_start_hrts_AmericaChicago = lead(day_start_hrts_AmericaChicago, 
                                      default = as.POSIXct(as.numeric("1996356467"), tz = "UTC", origin="1970-01-01") %>% with_tz(., "America/Chicago")),
           .after = day_start_hrts_AmericaChicago) %>% 
    ungroup()
  
  day_start_data_cc1_v3 <- day_start_data_cc1_v3 %>% filter(!(lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago < minutes(1)))
  
  day_end_data_cc1_v3 <- day_end_data_cc1_v2 %>% 
    select(participant_id, X1, X1hrts) %>% 
    rename(day_end_unixts = X1, day_end_hrts = X1hrts) %>% 
    arrange(participant_id, day_end_unixts) %>%
    group_by(participant_id) %>% 
    mutate(lead_day_end_hrts = lead(day_end_hrts, 
                                      default = as.POSIXct(as.numeric("1996356467"), tz = "UTC", origin="1970-01-01") %>% with_tz(., "America/Chicago")),
           .after = day_end_hrts) %>% 
    ungroup()
  
  day_end_data_cc1_v3 <- day_end_data_cc1_v3 %>% filter(!(lead_day_end_hrts - day_end_hrts < minutes(1)))
}

day_start_data_cc1_v3 <- day_start_data_cc1_v3 %>% mutate(start_date = as_date(day_start_hrts_AmericaChicago))

# Looking at days where a participant has multiple day_start records - Already removed duplicates (within 60 seconds of another)
multiple_day_start_same_day <- day_start_data_cc1_v3 %>% filter(lag(start_date) == start_date | lead(start_date) == start_date)

# The most extra day_starts per day is 2
day_start_data_cc1_v3 %>% arrange(participant_id, day_start_unixts) %>% 
  group_by(participant_id, start_date) %>% mutate(n = n()) %>% ungroup() %>% select(n) %>% summary()

# The log_phone data does not have a consistent pattern related to starting over counting blocks 
# when there are multiple start times on a given day. 
# Participant "3014" has the first day_start blocks continue through the second day_start
# Participants "3028" and "3131" have the blocks reset at the second day_start
#
# After discussions with the team, it was decided that the multiple day_start will
# be handled in the following manner:
#   + The blocks will continue from the first day_start timestamp
#   + A flag will be added to show instances of a second day_start
#   + Block 4 will be extended from a 4 hour window from hour 12 to up to hour 16,
#     to being from hour 12 to up to hour 28

day_start_data_cc1_v4 <- day_start_data_cc1_v3 %>% 
  arrange(participant_id, day_start_unixts) %>%
  group_by(participant_id, start_date) %>%
  mutate(
    day_start_numbr = row_number(),
    extra_day_start_dt = case_when(
      n() == 2 & row_number() == 1 ~ day_start_hrts_AmericaChicago[2]
    )) %>% 
  ungroup()

# Remove rows that are not the first day_start per participant per day
day_start_data_cc1_v4 <- day_start_data_cc1_v4 %>% 
  filter(day_start_numbr == 1)

# Recalculate the lead for day_start_hrts_AmericaChicago
day_start_data_cc1_v4 <-  day_start_data_cc1_v4 %>% 
  arrange(participant_id, day_start_unixts) %>%
    group_by(participant_id) %>% 
    mutate(lead_day_start_hrts_AmericaChicago = lead(day_start_hrts_AmericaChicago, 
                                      default = day_start_hrts_AmericaChicago[n()] + hours(28))) %>% 
    ungroup()

# Join the day_start data with the day_end data
# Used similar logic as the earlier join, using time between time and lead time
# End of days (where that data existed) were joined when fitting between day_start and lead_day_start
day_start_and_end_cc1 <- sqldf(
  "select start.participant_id, end.participant_id participant_id_end, start.day_start_unixts, 
      start.day_start_hrts_AmericaChicago, start.lead_day_start_hrts_AmericaChicago, start.extra_day_start_dt,
      end.day_end_unixts, end.day_end_hrts, end.lead_day_end_hrts,
      iif(start.participant_id is null,0,1) in_start, iif(end.participant_id is null,0,1) in_end
      from day_start_data_cc1_v4 start
        left outer join day_end_data_cc1_v3 end on
            end.participant_id = start.participant_id and
            (start.day_start_hrts_AmericaChicago <= end.day_end_hrts and start.lead_day_start_hrts_AmericaChicago >= end.day_end_hrts)") %>% tibble()

# The join reverted hrts timezone from Chicago to Mountain
day_start_and_end_cc1 <-day_start_and_end_cc1 %>% 
  mutate(day_start_hrts_AmericaChicago =  with_tz(day_start_hrts_AmericaChicago, "America/Chicago"),
                                lead_day_start_hrts_AmericaChicago = with_tz(lead_day_start_hrts_AmericaChicago, "America/Chicago"),
                                day_end_hrts = with_tz(day_end_hrts, "America/Chicago"),
                                lead_day_end_hrts = with_tz(lead_day_end_hrts, "America/Chicago"))

# The join created 4 additional rows from multiple day_end matched to one day_start window
# Will keep the latest day_end times
if(F){day_start_and_end_cc1 %>% group_by(participant_id, day_start_hrts_AmericaChicago) %>% 
    arrange(day_end_hrts) %>% filter(n()>1) %>% View()}

day_start_and_end_cc1 <- day_start_and_end_cc1 %>%
  group_by(participant_id, day_start_hrts_AmericaChicago) %>% 
  arrange(day_end_hrts) %>% 
  slice(n()) %>% 
  ungroup()

day_start_and_end_cc1 %>% count(in_start, in_end)

test_that(desc = "Did not create extra rows during the merge",{
  expect_equal(object = nrow(day_start_and_end_cc1), expected = nrow(day_start_data_cc1_v4))
})

# ------------------------------------------------------
# START Imputing Blocks based on Start (and end) times
# ------------------------------------------------------
#   Psuedocode for block_1_end_dt.
#   (For block 2,3,4: update all values by 4 hours)
#   + if end of day data (day_end) exists:
#     ++ if day_end is greater than day_start plus 4 hours,
#         then the end timestamp for block 1 is day_start plus 4 hours.
#     ++ if day_end is less than day_start plus 4 hours, 
#         but greater than day_start plus 0 hours,
#         then the end timestamp for block 1 is day_end.
#   + if end of day data does not exist:
#     ++ if the next day_start (lead_day_start) is greater than
#         day_start plus 4 hours, then the end timestamp 
#         for block 1 is day_start plus 4 hours
#
#   UPDATE: block 4 extended from up hours 12-16 to 12-16 + delta time from first start day button press to second day start button press
# ------------------------------------------------------
day_start_and_end_cc1 <- day_start_and_end_cc1 %>% mutate(extra_day_start_dt = with_tz(extra_day_start_dt, "America/Chicago"))

day_start_and_end_cc1 <- day_start_and_end_cc1 %>% mutate(
  time_between_multiple_day_starts = case_when(
    !is.na(extra_day_start_dt) ~ extra_day_start_dt - day_start_hrts_AmericaChicago
  ))

day_start_and_end_cc1_v2 <- day_start_and_end_cc1 %>% mutate(
  study_day_start_date = as_date(day_start_hrts_AmericaChicago),
  block_1_end_dt = case_when(
    day_start_hrts_AmericaChicago + hours(4) < day_end_hrts ~ day_start_hrts_AmericaChicago + hours(4),
    day_end_hrts - day_start_hrts_AmericaChicago >= hours(0) & day_end_hrts - day_start_hrts_AmericaChicago < hours(4) ~ day_end_hrts,
    is.na(day_end_hrts) & day_start_hrts_AmericaChicago + hours(4) < lead_day_start_hrts_AmericaChicago ~ day_start_hrts_AmericaChicago + hours(4),
    is.na(day_end_hrts) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago > hours(0) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago < hours(4) ~ lead_day_start_hrts_AmericaChicago
    ),
  block_2_end_dt = case_when(
    day_start_hrts_AmericaChicago + hours(8) < day_end_hrts ~ day_start_hrts_AmericaChicago + hours(8),
    day_end_hrts - day_start_hrts_AmericaChicago >= hours(4) & day_end_hrts - day_start_hrts_AmericaChicago < hours(8) ~ day_end_hrts,
    is.na(day_end_hrts) & day_start_hrts_AmericaChicago + hours(8) < lead_day_start_hrts_AmericaChicago ~ day_start_hrts_AmericaChicago + hours(8),
    is.na(day_end_hrts) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago >= hours(4) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago < hours(8) ~ lead_day_start_hrts_AmericaChicago
  ),
  block_3_end_dt = case_when(
    day_start_hrts_AmericaChicago + hours(12) < day_end_hrts ~ day_start_hrts_AmericaChicago + hours(12),
    day_end_hrts - day_start_hrts_AmericaChicago >= hours(8) & day_end_hrts - day_start_hrts_AmericaChicago < hours(12) ~ day_end_hrts,
    is.na(day_end_hrts) & day_start_hrts_AmericaChicago + hours(12) < lead_day_start_hrts_AmericaChicago ~ day_start_hrts_AmericaChicago + hours(12),
    is.na(day_end_hrts) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago > hours(8) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago < hours(12) ~ lead_day_start_hrts_AmericaChicago
  ),
  block_4_end_dt = case_when(
    !is.na(extra_day_start_dt) & day_start_hrts_AmericaChicago + hours(16) + time_between_multiple_day_starts < day_end_hrts ~ day_start_hrts_AmericaChicago + hours(16) + time_between_multiple_day_starts,
    !is.na(extra_day_start_dt) & day_end_hrts - day_start_hrts_AmericaChicago >= hours(12) & day_end_hrts - day_start_hrts_AmericaChicago < hours(16) + as.period(time_between_multiple_day_starts) ~ day_end_hrts,
    !is.na(extra_day_start_dt) & is.na(day_end_hrts) & day_start_hrts_AmericaChicago + hours(16) + time_between_multiple_day_starts < lead_day_start_hrts_AmericaChicago ~ day_start_hrts_AmericaChicago + hours(16) + time_between_multiple_day_starts,
    !is.na(extra_day_start_dt) & is.na(day_end_hrts) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago > hours(12) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago < hours(16) + as.period(time_between_multiple_day_starts) ~ lead_day_start_hrts_AmericaChicago,
    day_start_hrts_AmericaChicago + hours(16) < day_end_hrts ~ day_start_hrts_AmericaChicago + hours(16),
    day_end_hrts - day_start_hrts_AmericaChicago >= hours(12) & day_end_hrts - day_start_hrts_AmericaChicago < hours(16) ~ day_end_hrts,
    is.na(day_end_hrts) & day_start_hrts_AmericaChicago + hours(16) < lead_day_start_hrts_AmericaChicago ~ day_start_hrts_AmericaChicago + hours(16),
    is.na(day_end_hrts) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago > hours(12) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago < hours(16) ~ lead_day_start_hrts_AmericaChicago
  ),
  extra_day_start_block_number = case_when(
    is.na(extra_day_start_dt) ~ NA_integer_,
    extra_day_start_dt > day_start_hrts_AmericaChicago & extra_day_start_dt <= block_1_end_dt ~ 1L, # Occured in Block 1
    extra_day_start_dt > block_1_end_dt & extra_day_start_dt <= block_2_end_dt ~ 2L, # Occured in Block 2
    extra_day_start_dt > block_2_end_dt & extra_day_start_dt <= block_3_end_dt ~ 3L, # Occured in Block 3
    extra_day_start_dt > block_3_end_dt & extra_day_start_dt <= block_4_end_dt ~ 4L, # Occured in Block 4
  ), .after = day_start_hrts_AmericaChicago)

# Manual updates for daylight savings on March 10,2019
day_start_and_end_cc1_v2 <- day_start_and_end_cc1_v2 %>% 
  mutate(block_4_end_dt = case_when(
    participant_id == "3160" & study_day_start_date == "2019-03-09" ~ day_start_hrts_AmericaChicago + hours(17),
    T ~ block_4_end_dt))

# De-duplication done earlier made it so there are no NA values for block_1
day_start_and_end_cc1_v2 %>% filter(is.na(block_1_end_dt)) %>% count()

# The dataset is wide and without clear block numbers

# ---------------------------------------------------------------------
# Transform the dataset from wide to tall (with block numbers)
# ---------------------------------------------------------------------
df_shell <- day_start_and_end_cc1_v2 %>% 
  select(participant_id, study_day_start_date, day_start_hrts_AmericaChicago, block_1_end_dt, block_2_end_dt) %>%
  rename(block_start_hrts_AmericaChicago = block_1_end_dt,
         block_end_hrts_AmericaChicago = block_2_end_dt) %>% 
  slice(0) %>% 
  mutate(block_calc = as.character(),
         multi_day_start = as.logical())

day_blocks_calc_tall <- df_shell

# For loop to break out the wide dataset into tall with block numbers calculated
for (i in 1:nrow(day_start_and_end_cc1_v2)){
  row_i <- day_start_and_end_cc1_v2 %>% slice(i)
  if (!is.na(row_i$block_1_end_dt)){
    block_1_row <- df_shell %>% add_row(participant_id = row_i$participant_id,
                                       study_day_start_date = row_i$study_day_start_date,
                                       day_start_hrts_AmericaChicago = row_i$day_start_hrts_AmericaChicago,
                                       block_start_hrts_AmericaChicago = row_i$day_start_hrts_AmericaChicago,
                                       block_end_hrts_AmericaChicago = row_i$block_1_end_dt,
                                       block_calc = "1",
                                       multi_day_start = case_when(row_i$extra_day_start_block_number == 1 ~ TRUE,
                                                                          T ~ F))
    day_blocks_calc_tall <- day_blocks_calc_tall %>% add_row(block_1_row)
    
    if (!is.na(row_i$block_2_end_dt)){
      block_2_row <- df_shell %>% add_row(participant_id = row_i$participant_id,
                                         study_day_start_date = row_i$study_day_start_date,
                                         day_start_hrts_AmericaChicago = row_i$day_start_hrts_AmericaChicago,
                                         block_start_hrts_AmericaChicago = row_i$block_1_end_dt,
                                         block_end_hrts_AmericaChicago = row_i$block_2_end_dt,
                                         block_calc = "2",
                                         multi_day_start = case_when(row_i$extra_day_start_block_number <= 2 ~ TRUE,
                                                                            T ~ F))
      day_blocks_calc_tall <- day_blocks_calc_tall %>% add_row(block_2_row)
      
      if (!is.na(row_i$block_3_end_dt)){
        block_3_row <- df_shell %>% add_row(participant_id = row_i$participant_id,
                                           study_day_start_date = row_i$study_day_start_date,
                                           day_start_hrts_AmericaChicago = row_i$day_start_hrts_AmericaChicago,
                                           block_start_hrts_AmericaChicago = row_i$block_2_end_dt,
                                           block_end_hrts_AmericaChicago = row_i$block_3_end_dt,
                                           block_calc = "3",
                                           multi_day_start = case_when(row_i$extra_day_start_block_number <= 3 ~ TRUE,
                                                                              T ~ F))
        day_blocks_calc_tall <- day_blocks_calc_tall %>% add_row(block_3_row)
        
        if (!is.na(row_i$block_4_end_dt)){
          block_4_row <- df_shell %>% add_row(participant_id = row_i$participant_id,
                                             study_day_start_date = row_i$study_day_start_date,
                                             day_start_hrts_AmericaChicago = row_i$day_start_hrts_AmericaChicago,
                                             block_start_hrts_AmericaChicago = row_i$block_3_end_dt,
                                             block_end_hrts_AmericaChicago = row_i$block_4_end_dt,
                                             block_calc = "4",
                                             multi_day_start = case_when(row_i$extra_day_start_block_number <= 4 ~ TRUE,
                                                                                T ~ F))
          day_blocks_calc_tall <- day_blocks_calc_tall %>% add_row(block_4_row)
        }
      }
    }
  }
}

day_blocks_calc_tall <- day_blocks_calc_tall[!duplicated(day_blocks_calc_tall),]

# ------------------------------------------------------------------------------------------
# Join the day_blocks_calc_tall data with the cc1 ema_data & log_phone data from above (rj2)
# ------------------------------------------------------------------------------------------
rj3_sftwr_time <- rj2 %>% 
  mutate(
    time_software_calc = case_when(
      is.na(sftware_init_hrts_AmericaChicago) & !is.na(begin_hrts_AmericaChicago) ~ begin_hrts_AmericaChicago,
      is.na(sftware_init_hrts_AmericaChicago) & is.na(begin_hrts_AmericaChicago) ~ end_hrts_AmericaChicago,
      end_hrts_AmericaChicago - sftware_init_hrts_AmericaChicago < minutes(30) ~ sftware_init_hrts_AmericaChicago,
      end_hrts_AmericaChicago - sftware_init_hrts_AmericaChicago >= minutes(30) & !is.na(begin_hrts_AmericaChicago) ~ begin_hrts_AmericaChicago,
      end_hrts_AmericaChicago - sftware_init_hrts_AmericaChicago >= minutes(30) ~ end_hrts_AmericaChicago
    ), .before = study_day)


rj3_blocks_calc_sftwr_time <- sqldf(
  "select rj3.*, blocks.participant_id participant_id_blocks, blocks.study_day_start_date, blocks.day_start_hrts_AmericaChicago, blocks.block_start_hrts_AmericaChicago,
  blocks.block_end_hrts_AmericaChicago, blocks.block_calc, blocks.multi_day_start,
      iif(rj3.participant_id is null,0,1) in_rj3, iif(blocks.participant_id is null,0,1) in_blocks
      from rj3_sftwr_time rj3
        left outer join day_blocks_calc_tall blocks on
            rj3.participant_id = blocks.participant_id and
            (blocks.block_start_hrts_AmericaChicago <= rj3.time_software_calc and blocks.block_end_hrts_AmericaChicago >= rj3.time_software_calc)") %>% tibble()


rj3_blocks_calc_sftwr_time <- rj3_blocks_calc_sftwr_time %>%
  mutate(begin_hrts_AmericaChicago = with_tz(begin_hrts_AmericaChicago, "America/Chicago"),
         end_hrts_AmericaChicago =  with_tz(end_hrts_AmericaChicago, "America/Chicago"),
         begin_hrts_UTC = with_tz(begin_hrts_UTC, "UTC"),
         end_hrts_UTC = with_tz(end_hrts_UTC, "UTC"),
         sftware_init_hrts_AmericaChicago = with_tz(sftware_init_hrts_AmericaChicago, "America/Chicago"),
         lead_sftware_init_hrts_AmericaChicago = with_tz(lead_sftware_init_hrts_AmericaChicago, "America/Chicago"),
         day_start_hrts_AmericaChicago = with_tz(day_start_hrts_AmericaChicago, "America/Chicago"),
         block_start_hrts_AmericaChicago = with_tz(block_start_hrts_AmericaChicago, "America/Chicago"),
         block_end_hrts_AmericaChicago = with_tz(block_end_hrts_AmericaChicago, "America/Chicago"),
         time_software_calc = with_tz(time_software_calc, "America/Chicago"))

rj3_blocks_calc_sftwr_time %>% count(in_rj3, in_blocks)

# Cleaning up variable names and order
rj3_blocks_calc_sftwr_time <- rj3_blocks_calc_sftwr_time %>% 
  relocate(block_calc, .after = BLOCK) %>% 
  arrange(participant_id, end_hrts_AmericaChicago) %>% 
  relocate(end_hrts_AmericaChicago, .after = participant_id) %>% 
  relocate(in_log, in_ema, .before = in_rj3) %>% 
  relocate(status_log, ema_type, .after = block_calc)

rj3_blocks_calc_sftwr_time_2 <- rj3_blocks_calc_sftwr_time %>% 
  mutate(blocks_agree_logphone_daystart = BLOCK == block_calc, .after = block_calc)

if(F){rj3_blocks_calc_sftwr_time_2 %>% filter(BLOCK != "-1") %>%  filter(!blocks_agree_logphone_daystart) %>% View()}

rj3_blocks_calc_sftwr_time_2 <- rj3_blocks_calc_sftwr_time_2 %>% 
  arrange(participant_id, ymd_hms(end_hrts_AmericaChicago)) %>% 
  group_by(participant_id, ema_type, study_day_start_date, block_calc) %>% 
  mutate(extra_ema = case_when(
    !is.na(block_calc) ~ row_number()>1
  ), .after = block_calc) %>% 
  ungroup

rj3_blocks_calc_sftwr_time_2 %>% count(extra_ema)

extra_ema_data_cc1 <- rj3_blocks_calc_sftwr_time_2

# Add variable to track extra emas on the day level
extra_ema_data_cc1 <- extra_ema_data_cc1 %>% 
  arrange(participant_id, ymd_hms(end_hrts_AmericaChicago)) %>% 
  group_by(participant_id, ema_type, study_day_start_date) %>%
  mutate(extra_ema_on_study_day = any(extra_ema), .after = extra_ema) %>% 
  ungroup()

#Add variable to track multiple day_starts on the day level
extra_ema_data_cc1 <- extra_ema_data_cc1 %>% 
  arrange(participant_id, ymd_hms(end_hrts_AmericaChicago)) %>% 
  group_by(participant_id, study_day_start_date) %>% 
  mutate(multi_day_start_on_study_day = any(multi_day_start), .after = multi_day_start) %>% 
  ungroup() 

test_no_invalid_end_day_in_cc1 <- test_that(desc = "No invalid Day End in CC1",{
  cc1_na_block_count <- extra_ema_data_cc1 %>% filter(is.na(block_calc)) %>% nrow()
  expect_equal(object = cc1_na_block_count, expected = 0L)
})

if(test_no_invalid_end_day_in_cc1){
  extra_ema_data_cc1 <- extra_ema_data_cc1 %>% mutate(
    invalid_end_day = FALSE,
    invalid_end_day_on_study_day = FALSE
  )
}

# ----------------------------------------------------------------------------
# Prep Dataset for remainder of the pipeline
#   *remove extra columns unnecessary to carry through pipeline
# ----------------------------------------------------------------------------
all_ema_data_wblocks_cc1 <- extra_ema_data_cc1 %>% 
  select(participant_id, cc_indicator, ema_type, status, with_any_response, begin_unixts, end_unixts,
         begin_hrts_UTC, end_hrts_UTC, begin_hrts_AmericaChicago, end_hrts_AmericaChicago, 
         time_software_calc, block_calc, extra_ema, extra_ema_on_study_day,
         day_start_hrts_AmericaChicago, block_start_hrts_AmericaChicago, block_end_hrts_AmericaChicago, 
         invalid_end_day, invalid_end_day_on_study_day,
         multi_day_start, multi_day_start_on_study_day, everything())

all_ema_data_wblocks_cc1 <- all_ema_data_wblocks_cc1 %>% 
  select(-BLOCK, -lead_sftware_init_hrts_AmericaChicago, -blocks_agree_logphone_daystart, 
         -status_log, -participant_id_blocks, -study_day, -in_log, -in_ema, -in_rj3, -in_blocks, 
         -study_day_start_date, -id, -sftware_init_hrts_AmericaChicago)

# ----------------------------------------------------------------------------
# Source in code for the block level dataset here
#   - it uses many of the variables currently loaded/generated in the lines above
# ----------------------------------------------------------------------------
if(F){source("generate-block-level-dataset-cc1.R")}

# ----------------------------------------------------------------------------
# Save Dataset
# ----------------------------------------------------------------------------
save(all_ema_data_wblocks_cc1,
     file = file.path(path_breakfree_staged_data, "all_ema_data_wblocks_cc1.RData"))

