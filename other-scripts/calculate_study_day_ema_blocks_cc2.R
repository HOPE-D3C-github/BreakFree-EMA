library(dplyr)
library(lubridate)
library(tidyr)
library(sqldf)
library(readr)
library(testthat)
library(hms)

source("paths.R")

load(file = file.path(path_breakfree_staged_data, "ema_raw_data_cc2.RData"))
remove(all_random_ema_response_files_cc2, all_random_ema_status_files_cc2, all_smoking_ema_response_files_cc2, all_smoking_ema_status_files_cc2,
         all_stress_ema_response_files_cc2, all_stress_ema_status_files_cc2, dat_file_counts_cc2)

load(file.path(path_breakfree_staged_data, "all_ema_data_cc2.RData"))

# -----------------------------------------------------------------------------
# Reformat raw day_start and day_end data
#------------------------------------------------------------------------------
day_start_data_cc2 <- do.call(rbind, all_day_start_files_cc2) #convert list of tibbles to one tibble for all participants
day_end_data_cc2 <- do.call(rbind, all_day_end_files_cc2)

day_start_data_cc2_v2 <- day_start_data_cc2 %>% 
  mutate(V1hrts_AmericaChicago = as.POSIXct(as.numeric(V1/1000), tz = "UTC", origin="1970-01-01") %>% with_tz(., "America/Chicago"),
         V3hrts_AmericaChicago = as.POSIXct(as.numeric(V3/1000), tz = "UTC", origin="1970-01-01") %>% with_tz(., "America/Chicago"))

day_end_data_cc2_v2 <- day_end_data_cc2 %>% 
  mutate(V1hrts_AmericaChicago = as.POSIXct(as.numeric(V1/1000), tz = "UTC", origin="1970-01-01") %>% with_tz(., "America/Chicago"),
         V3hrts_AmericaChicago = as.POSIXct(as.numeric(V3/1000), tz = "UTC", origin="1970-01-01") %>% with_tz(., "America/Chicago"))

test1 <- test_that(desc = "The two timestamp values per row are within 1 second of eachother", {
  for (i in 1:length(day_start_data_cc2_v2)){
    expect_lt(
      abs(day_start_data_cc2_v2$V1hrts_AmericaChicago[i] - day_start_data_cc2_v2$V3hrts_AmericaChicago[i]), as.difftime(1, units = "secs"))
  }
  for (i in 1:length(day_end_data_cc2_v2)){
    expect_lt(
      abs(day_end_data_cc2_v2$V1hrts_AmericaChicago[i] - day_end_data_cc2_v2$V3hrts_AmericaChicago[i]), as.difftime(1, units = "secs"))
  }
})

test2 <- test_that(desc = "No missing timestamps",{
  expect_true(any(is.na(day_start_data_cc2_v2$V1hrts)) == FALSE)
  expect_true(any(is.na(day_end_data_cc2_v2$V1hrts)) == FALSE)
})

if(test1 & test2){
  # Adding lead for day_start_hrts, so the day_end can be crosswalked between a day_start and the next day_start (the lead)
  # Also running de-duplication steps here
  day_start_data_cc2_v3 <- day_start_data_cc2_v2 %>% 
    select(participant_id, V1, V1hrts_AmericaChicago) %>% 
    rename(day_start_unixts = V1, day_start_hrts_AmericaChicago = V1hrts_AmericaChicago) %>% 
    arrange(participant_id, day_start_unixts) %>%
    group_by(participant_id) %>% 
    mutate(lead_day_start_hrts_AmericaChicago = lead(day_start_hrts_AmericaChicago, 
                                              default = as.POSIXct(as.numeric("1996356467"), tz = "UTC", origin="1970-01-01") %>% with_tz(., "America/Chicago")),
           .after = day_start_hrts_AmericaChicago) %>% 
    ungroup()
  
  #remove duplicates
  day_start_data_cc2_v3 <- day_start_data_cc2_v3 %>% filter(!(lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago < minutes(1)))

  day_end_data_cc2_v3 <- day_end_data_cc2_v2 %>% 
    select(participant_id, V1, V1hrts_AmericaChicago) %>% 
    rename(day_end_unixts = V1, day_end_hrts_AmericaChicago = V1hrts_AmericaChicago) %>% 
    arrange(participant_id, day_end_unixts) %>% 
    group_by(participant_id) %>% 
    mutate(lead_day_end_hrts_AmericaChicago = lead(day_end_hrts_AmericaChicago,
                                            default = as.POSIXct(as.numeric("1996356467"), tz = "UTC", origin="1970-01-01") %>% with_tz(., "America/Chicago"))) %>% 
    ungroup()
  
  # remove duplicates
  day_end_data_cc2_v3 <-  day_end_data_cc2_v3 %>% filter(!(lead_day_end_hrts_AmericaChicago - day_end_hrts_AmericaChicago < minutes(1)))
}

day_start_data_cc2_v3 <- day_start_data_cc2_v3 %>% mutate(start_date = as_date(day_start_hrts_AmericaChicago))

# Looking at days where a participant has multiple day_start records - Already removed duplicates (within 60 seconds of another)
multiple_day_start_same_day <- day_start_data_cc2_v3 %>% filter(lag(start_date) == start_date | lead(start_date) == start_date)

# The most extra day_starts per day is 2
day_start_data_cc2_v3 %>% arrange(participant_id, day_start_unixts) %>% 
  group_by(participant_id, start_date) %>% mutate(n = n()) %>% ungroup() %>% select(n) %>% summary()

# Following the same rules as CC1 for handling multiple day_starts on a given day
#
# After discussions with the team, it was decided that the multiple day_start will
# be handled in the following manner:
#   + The blocks will continue from the first day_start timestamp
#   + A flag will be added to show instances of a second day_start
#   + Block 4 will be extended from a 4 hour window from hour 12 to up to hour 16,
#     to being from hour 12 to up to hour 28

day_start_data_cc2_v4 <- day_start_data_cc2_v3 %>% 
  arrange(participant_id, day_start_unixts) %>%
  group_by(participant_id, start_date) %>%
  mutate(
    day_start_numbr = row_number(),
    extra_day_start_dt = case_when(
      n() == 2 & row_number() == 1 ~ day_start_hrts_AmericaChicago[2]
    )) %>% 
  ungroup()

# Remove rows that are not the first day_start per participant per day
day_start_data_cc2_v4 <- day_start_data_cc2_v4 %>% 
  filter(day_start_numbr == 1)

# Recalculate the lead for day_start_hrts
day_start_data_cc2_v4 <-  day_start_data_cc2_v4 %>% 
  arrange(participant_id, day_start_unixts) %>%
  group_by(participant_id) %>% 
  mutate(lead_day_start_hrts_AmericaChicago = lead(day_start_hrts_AmericaChicago, 
                                    default = day_start_hrts_AmericaChicago[n()] + hours(28))) %>% 
  ungroup()

# ----------------------------------------------------------------------------
# Join the day_start data with the day_end data
# ----------------------------------------------------------------------------
day_start_and_end_cc2 <- sqldf(
  "select start.participant_id, end.participant_id participant_id_end, start.day_start_unixts, 
      start.day_start_hrts_AmericaChicago, start.lead_day_start_hrts_AmericaChicago, start.extra_day_start_dt,
      end.day_end_unixts, end.day_end_hrts_AmericaChicago, end.lead_day_end_hrts_AmericaChicago,
      iif(start.participant_id is null,0,1) in_start, iif(end.participant_id is null,0,1) in_end
      from day_start_data_cc2_v4 start
        left outer join day_end_data_cc2_v3 end on
            end.participant_id = start.participant_id and
            (start.day_start_hrts_AmericaChicago <= end.day_end_hrts_AmericaChicago and start.lead_day_start_hrts_AmericaChicago >= end.day_end_hrts_AmericaChicago)") %>% tibble()


# The join reverted hrts timezone from Chicago to Mountain
day_start_and_end_cc2 <-day_start_and_end_cc2 %>% 
  mutate(day_start_hrts_AmericaChicago =  with_tz(day_start_hrts_AmericaChicago, "America/Chicago"),
         lead_day_start_hrts_AmericaChicago = with_tz(lead_day_start_hrts_AmericaChicago, "America/Chicago"),
         day_end_hrts_AmericaChicago = with_tz(day_end_hrts_AmericaChicago, "America/Chicago"),
         lead_day_end_hrts_AmericaChicago = with_tz(lead_day_end_hrts_AmericaChicago, "America/Chicago"))


day_start_and_end_cc2 %>% count(in_start, in_end) 

# Test that day ends are matched with day starts within 25 hours - not matched for an end to a different day
test3 <- test_that(desc = "Day ends are within 25 hours of it's associated day start",{
  expect_equal(object = day_start_and_end_cc2 %>% filter(day_end_hrts_AmericaChicago - day_start_hrts_AmericaChicago > hours(25)) %>% nrow(), expected = 0)
})
if (!test3){
  day_start_and_end_cc2 <- day_start_and_end_cc2 %>% filter(!(!is.na(day_end_hrts_AmericaChicago) & day_end_hrts_AmericaChicago - day_start_hrts_AmericaChicago > hours(30)))
}

# The merge added new rows. This is from multiple day_end matching to the time window for a given day_start-lead_day_start
# Deduplication method: keep the last day_end record
if(F){day_start_and_end_cc2 %>% group_by(participant_id, day_start_hrts_AmericaChicago) %>% 
  arrange(day_end_hrts_AmericaChicago) %>% filter(n()>1) %>% View()}

day_start_and_end_cc2 <- day_start_and_end_cc2 %>%
  group_by(participant_id, day_start_hrts_AmericaChicago) %>% 
  arrange(day_end_hrts_AmericaChicago) %>% 
  slice(n()) %>% 
  ungroup()

test_that(desc = "Did not create extra rows during the merge",{
  expect_equal(object = nrow(day_start_and_end_cc2), expected = nrow(day_start_data_cc2_v4))
})

# Now the merge is complete and QC'd. Ready to calculate blocks

# ---------------------------------------------------------------------------
# Remove Inconsistent End of Day timestamps, and add flag
# day level and block level
# ---------------------------------------------------------------------------

df_revisions_to_end_days <- data.frame(
  participant_id = c("aa_176", "aa_210", "aa_210", "aa_236", "aa_244", "aa_249", "aa_252", "aa_252", "aa_265", "aa_265", "aa_284", "aa_288", "aa_301"),
  study_date = c("2019-04-23", "2019-06-24", "2019-06-28", "2019-07-24", "2019-08-04", "2019-08-19", "2019-08-20", 
                 "2019-08-21", "2019-09-13", "2019-09-17", "2019-10-18", "2019-10-24", "2019-12-07")
)

day_start_and_end_cc2_v1b <- day_start_and_end_cc2 %>% mutate(study_date = as_date(day_start_hrts_AmericaChicago),
                                                              .after = day_start_hrts_AmericaChicago)

(row_indexes_to_update <- which(paste(day_start_and_end_cc2_v1b$participant_id, day_start_and_end_cc2_v1b$study_date) %in% paste(df_revisions_to_end_days$participant_id, df_revisions_to_end_days$study_date)))

day_start_and_end_cc2_v1c <- day_start_and_end_cc2_v1b %>% 
  mutate(invalid_end_day_on_study_day = 
           case_when(
             row_number() %in% row_indexes_to_update ~ TRUE,
             T ~ FALSE
           ), .after = study_date) %>% 
  select(-study_date)


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
#   UPDATE: block 4 extended from up hours 12-16 to 12-28
# ------------------------------------------------------
day_start_and_end_cc2_v1d <- day_start_and_end_cc2_v1c %>% mutate(extra_day_start_dt = with_tz(extra_day_start_dt, "America/Chicago"))

day_start_and_end_cc2_v1d <- day_start_and_end_cc2_v1d %>% mutate(
  time_between_multiple_day_starts = case_when(
    !is.na(extra_day_start_dt) ~ extra_day_start_dt - day_start_hrts_AmericaChicago
  ))

day_start_and_end_cc2_v2 <- day_start_and_end_cc2_v1d %>% mutate(
  study_day_start_date = as_date(day_start_hrts_AmericaChicago),
  block_1_end_dt = case_when(
    invalid_end_day_on_study_day ~ day_start_hrts_AmericaChicago + hours(4),
    day_start_hrts_AmericaChicago + hours(4) < day_end_hrts_AmericaChicago ~ day_start_hrts_AmericaChicago + hours(4),
    day_end_hrts_AmericaChicago - day_start_hrts_AmericaChicago >= hours(0) & day_end_hrts_AmericaChicago - day_start_hrts_AmericaChicago < hours(4) ~ day_end_hrts_AmericaChicago,
    is.na(day_end_hrts_AmericaChicago) & day_start_hrts_AmericaChicago + hours(4) < lead_day_start_hrts_AmericaChicago ~ day_start_hrts_AmericaChicago + hours(4),
    is.na(day_end_hrts_AmericaChicago) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago > hours(0) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago < hours(4) ~ lead_day_start_hrts_AmericaChicago
  ),
  block_2_end_dt = case_when(
    invalid_end_day_on_study_day ~ day_start_hrts_AmericaChicago + hours(8),
    day_start_hrts_AmericaChicago + hours(8) < day_end_hrts_AmericaChicago ~ day_start_hrts_AmericaChicago + hours(8),
    day_end_hrts_AmericaChicago - day_start_hrts_AmericaChicago >= hours(4) & day_end_hrts_AmericaChicago - day_start_hrts_AmericaChicago < hours(8) ~ day_end_hrts_AmericaChicago,
    is.na(day_end_hrts_AmericaChicago) & day_start_hrts_AmericaChicago + hours(8) < lead_day_start_hrts_AmericaChicago ~ day_start_hrts_AmericaChicago + hours(8),
    is.na(day_end_hrts_AmericaChicago) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago >= hours(4) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago < hours(8) ~ lead_day_start_hrts_AmericaChicago
  ),
  block_3_end_dt = case_when(
    invalid_end_day_on_study_day ~ day_start_hrts_AmericaChicago + hours(12),
    day_start_hrts_AmericaChicago + hours(12) < day_end_hrts_AmericaChicago ~ day_start_hrts_AmericaChicago + hours(12),
    day_end_hrts_AmericaChicago - day_start_hrts_AmericaChicago >= hours(8) & day_end_hrts_AmericaChicago - day_start_hrts_AmericaChicago < hours(12) ~ day_end_hrts_AmericaChicago,
    is.na(day_end_hrts_AmericaChicago) & day_start_hrts_AmericaChicago + hours(12) < lead_day_start_hrts_AmericaChicago ~ day_start_hrts_AmericaChicago + hours(12),
    is.na(day_end_hrts_AmericaChicago) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago > hours(8) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago < hours(12) ~ lead_day_start_hrts_AmericaChicago
  ),
  block_4_end_dt = case_when(
    invalid_end_day_on_study_day ~ day_start_hrts_AmericaChicago + hours(16),
    !is.na(extra_day_start_dt) & day_start_hrts_AmericaChicago + hours(16) + time_between_multiple_day_starts < day_end_hrts_AmericaChicago ~ day_start_hrts_AmericaChicago + hours(16) + time_between_multiple_day_starts,
    !is.na(extra_day_start_dt) & day_end_hrts_AmericaChicago - day_start_hrts_AmericaChicago >= hours(12) & day_end_hrts_AmericaChicago - day_start_hrts_AmericaChicago < hours(16) + as.period(time_between_multiple_day_starts) ~ day_end_hrts_AmericaChicago,
    !is.na(extra_day_start_dt) & is.na(day_end_hrts_AmericaChicago) & day_start_hrts_AmericaChicago + hours(16) + time_between_multiple_day_starts < lead_day_start_hrts_AmericaChicago ~ day_start_hrts_AmericaChicago + hours(16) + time_between_multiple_day_starts,
    !is.na(extra_day_start_dt) & is.na(day_end_hrts_AmericaChicago) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago > hours(12) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago < hours(16) + as.period(time_between_multiple_day_starts) ~ lead_day_start_hrts_AmericaChicago,
    day_start_hrts_AmericaChicago + hours(16) < day_end_hrts_AmericaChicago ~ day_start_hrts_AmericaChicago + hours(16),
    day_end_hrts_AmericaChicago - day_start_hrts_AmericaChicago >= hours(12) & day_end_hrts_AmericaChicago - day_start_hrts_AmericaChicago < hours(16) ~ day_end_hrts_AmericaChicago,
    is.na(day_end_hrts_AmericaChicago) & day_start_hrts_AmericaChicago + hours(16) < lead_day_start_hrts_AmericaChicago ~ day_start_hrts_AmericaChicago + hours(16),
    is.na(day_end_hrts_AmericaChicago) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago > hours(12) & lead_day_start_hrts_AmericaChicago - day_start_hrts_AmericaChicago < hours(16) ~ lead_day_start_hrts_AmericaChicago
  ),
  extra_day_start_block_number = case_when(
    is.na(extra_day_start_dt) ~ NA_integer_,
    extra_day_start_dt > day_start_hrts_AmericaChicago & extra_day_start_dt <= block_1_end_dt ~ 1L, # Occured in Block 1
    extra_day_start_dt > block_1_end_dt & extra_day_start_dt <= block_2_end_dt ~ 2L, # Occured in Block 2
    extra_day_start_dt > block_2_end_dt & extra_day_start_dt <= block_3_end_dt ~ 3L, # Occured in Block 3
    extra_day_start_dt > block_3_end_dt & extra_day_start_dt <= block_4_end_dt ~ 4L, # Occured in Block 4
  ),
  invalid_end_day_on_block_1 = (invalid_end_day_on_study_day & block_1_end_dt > day_end_hrts_AmericaChicago),
  invalid_end_day_on_block_2 = (invalid_end_day_on_study_day & block_2_end_dt > day_end_hrts_AmericaChicago),
  invalid_end_day_on_block_3 = (invalid_end_day_on_study_day & block_3_end_dt > day_end_hrts_AmericaChicago),
  invalid_end_day_on_block_4 = (invalid_end_day_on_study_day & block_4_end_dt > day_end_hrts_AmericaChicago),
  .after = day_start_hrts_AmericaChicago)

# De-duplication done earlier made it so there are no NA values for block_1
day_start_and_end_cc2_v2 %>% filter(is.na(block_1_end_dt)) %>% count()

# The dataset is wide and without clear block numbers

# ---------------------------------------------------------------------
# Transform the dataset from wide to tall (with block numbers)
# ---------------------------------------------------------------------
df_shell <- day_start_and_end_cc2_v2 %>% 
  select(participant_id, study_day_start_date, day_start_hrts_AmericaChicago, day_end_hrts_AmericaChicago, block_1_end_dt, block_2_end_dt, invalid_end_day_on_block_1, invalid_end_day_on_study_day) %>%
  rename(block_start_hrts_AmericaChicago = block_1_end_dt,
         block_end_hrts_AmericaChicago = block_2_end_dt,
         invalid_end_day_on_block = invalid_end_day_on_block_1) %>% 
  slice(0) %>% 
  mutate(block_calc = as.character(),
         multi_day_start = as.logical())

day_blocks_calc_tall <- df_shell

# For loop to break out the wide dataset into tall with block numbers calculated
for (i in 1:nrow(day_start_and_end_cc2_v2)){
  row_i <- day_start_and_end_cc2_v2 %>% slice(i)
  if (!is.na(row_i$block_1_end_dt)){
    block_1_row <- df_shell %>% add_row(participant_id = row_i$participant_id,
                                        study_day_start_date = row_i$study_day_start_date,
                                        day_start_hrts_AmericaChicago = row_i$day_start_hrts_AmericaChicago,
                                        day_end_hrts_AmericaChicago = row_i$day_end_hrts_AmericaChicago,
                                        block_start_hrts_AmericaChicago = row_i$day_start_hrts_AmericaChicago,
                                        block_end_hrts_AmericaChicago = row_i$block_1_end_dt,
                                        invalid_end_day_on_block = row_i$invalid_end_day_on_block_1,
                                        invalid_end_day_on_study_day = row_i$invalid_end_day_on_study_day,
                                        block_calc = "1",
                                        multi_day_start = case_when(row_i$extra_day_start_block_number == 1 ~ TRUE,
                                                                           T ~ F))
    day_blocks_calc_tall <- day_blocks_calc_tall %>% add_row(block_1_row)
    
    if (!is.na(row_i$block_2_end_dt)){
      block_2_row <- df_shell %>% add_row(participant_id = row_i$participant_id,
                                          study_day_start_date = row_i$study_day_start_date,
                                          day_start_hrts_AmericaChicago = row_i$day_start_hrts_AmericaChicago,
                                          day_end_hrts_AmericaChicago = row_i$day_end_hrts_AmericaChicago,
                                          block_start_hrts_AmericaChicago = row_i$block_1_end_dt,
                                          block_end_hrts_AmericaChicago = row_i$block_2_end_dt,
                                          invalid_end_day_on_block = row_i$invalid_end_day_on_block_2,
                                          invalid_end_day_on_study_day = row_i$invalid_end_day_on_study_day,
                                          block_calc = "2",
                                          multi_day_start = case_when(row_i$extra_day_start_block_number <= 2 ~ TRUE,
                                                                             T ~ F))
      day_blocks_calc_tall <- day_blocks_calc_tall %>% add_row(block_2_row)
      
      if (!is.na(row_i$block_3_end_dt)){
        block_3_row <- df_shell %>% add_row(participant_id = row_i$participant_id,
                                            study_day_start_date = row_i$study_day_start_date,
                                            day_start_hrts_AmericaChicago = row_i$day_start_hrts_AmericaChicago,
                                            day_end_hrts_AmericaChicago = row_i$day_end_hrts_AmericaChicago,
                                            block_start_hrts_AmericaChicago = row_i$block_2_end_dt,
                                            block_end_hrts_AmericaChicago = row_i$block_3_end_dt,
                                            invalid_end_day_on_block = row_i$invalid_end_day_on_block_3,
                                            invalid_end_day_on_study_day = row_i$invalid_end_day_on_study_day,
                                            block_calc = "3",
                                            multi_day_start = case_when(row_i$extra_day_start_block_number <= 3 ~ TRUE,
                                                                               T ~ F))
        day_blocks_calc_tall <- day_blocks_calc_tall %>% add_row(block_3_row)
        
        if (!is.na(row_i$block_4_end_dt)){
          block_4_row <- df_shell %>% add_row(participant_id = row_i$participant_id,
                                              study_day_start_date = row_i$study_day_start_date,
                                              day_start_hrts_AmericaChicago = row_i$day_start_hrts_AmericaChicago,
                                              day_end_hrts_AmericaChicago = row_i$day_end_hrts_AmericaChicago,
                                              block_start_hrts_AmericaChicago = row_i$block_3_end_dt,
                                              block_end_hrts_AmericaChicago = row_i$block_4_end_dt,
                                              invalid_end_day_on_block = row_i$invalid_end_day_on_block_4,
                                              invalid_end_day_on_study_day = row_i$invalid_end_day_on_study_day,
                                              block_calc = "4",
                                              multi_day_start = case_when(row_i$extra_day_start_block_number <= 4 ~ TRUE,
                                                                                 T ~ F))
          day_blocks_calc_tall <- day_blocks_calc_tall %>% add_row(block_4_row)
        }
      }
    }
  }
}

# Should not have any duplicates
sum(duplicated(day_blocks_calc_tall))
#day_blocks_calc_tall <- day_blocks_calc_tall[!duplicated(day_blocks_calc_tall),]
day_blocks_calc_tall %>% group_by(participant_id, study_day_start_date, block_calc) %>% count() %>% summary()

# ------------------------------------------------------------------------------------------
# Join the day_blocks_calc_tall data with the cc2 ema_data 
# ------------------------------------------------------------------------------------------
all_ema_data_cc2_sftwr_time <- all_ema_data_cc2 %>% 
  mutate(time_software_calc = case_when(
      is.na(begin_hrts_AmericaChicago) ~ end_hrts_AmericaChicago,
      T ~ begin_hrts_AmericaChicago))

cc2_ema_time_blocks_calc <- sqldf(
  "select ema.*, blocks.participant_id participant_id_blocks, blocks.study_day_start_date, blocks.day_start_hrts_AmericaChicago, blocks.day_end_hrts_AmericaChicago,
      blocks.block_start_hrts_AmericaChicago, blocks.block_end_hrts_AmericaChicago, blocks.block_calc, blocks.multi_day_start, blocks.invalid_end_day_on_block, blocks.invalid_end_day_on_study_day,
      iif(ema.participant_id is null,0,1) in_ema, iif(blocks.participant_id is null,0,1) in_blocks
      from all_ema_data_cc2_sftwr_time ema
        left outer join day_blocks_calc_tall blocks on
            ema.participant_id = blocks.participant_id and
            (blocks.block_start_hrts_AmericaChicago <= ema.time_software_calc and blocks.block_end_hrts_AmericaChicago >= ema.time_software_calc)") %>% tibble()

# The join reverted Chicago timezone to Mountain
cc2_ema_time_blocks_calc <- cc2_ema_time_blocks_calc %>%
  mutate(begin_hrts_AmericaChicago = with_tz(begin_hrts_AmericaChicago, "America/Chicago"),
         end_hrts_AmericaChicago =  with_tz(end_hrts_AmericaChicago, "America/Chicago"),
         begin_hrts_UTC = with_tz(begin_hrts_UTC, "UTC"),
         end_hrts_UTC = with_tz(end_hrts_UTC, "UTC"),
         day_start_hrts_AmericaChicago = with_tz(day_start_hrts_AmericaChicago, "America/Chicago"),
         day_end_hrts_AmericaChicago = with_tz(day_end_hrts_AmericaChicago, "America/Chicago"),
         block_start_hrts_AmericaChicago = with_tz(block_start_hrts_AmericaChicago, "America/Chicago"),
         block_end_hrts_AmericaChicago = with_tz(block_end_hrts_AmericaChicago, "America/Chicago"),
         time_software_calc = with_tz(time_software_calc, "America/Chicago"))

cc2_ema_time_blocks_calc %>% count(in_ema, in_blocks)

# Cleaning up order
cc2_ema_time_blocks_calc_v1b <- cc2_ema_time_blocks_calc %>% 
  relocate(block_calc, .after = study_day_start_date) %>% 
  relocate(day_start_hrts_AmericaChicago, day_end_hrts_AmericaChicago, .before = in_ema) %>% 
  relocate(end_hrts_AmericaChicago, .after = participant_id) %>% 
  select(-participant_id_blocks)

# Ensure every EMA was mapped to a block number
cc2_ema_time_blocks_calc_v1b %>% filter(is.na(block_calc)) %>% count()

cc2_ema_time_blocks_calc_v2 <- cc2_ema_time_blocks_calc_v1b %>% 
  arrange(participant_id, ymd_hms(end_hrts_AmericaChicago)) %>% 
  group_by(participant_id, ema_type, study_day_start_date, block_calc) %>% 
  mutate(extra_ema = case_when(
    !is.na(block_calc) ~ row_number()>1
  ), .after = block_calc) %>% 
  ungroup

# Add variable to track extra emas on the day level
cc2_ema_time_blocks_calc_v2 <- cc2_ema_time_blocks_calc_v2 %>% 
  arrange(participant_id, ymd_hms(end_hrts_AmericaChicago)) %>% 
  group_by(participant_id, ema_type, study_day_start_date) %>%
  mutate(extra_ema_on_study_day = any(extra_ema), .after = extra_ema) %>% ungroup()

#Add variable to track multiple day_starts on the day level
cc2_ema_time_blocks_calc_v2 <- cc2_ema_time_blocks_calc_v2 %>% 
  arrange(participant_id, ymd_hms(end_hrts_AmericaChicago)) %>% 
  group_by(participant_id, study_day_start_date) %>% 
  mutate(multi_day_start_on_study_day = any(multi_day_start), .after = multi_day_start) %>% 
  ungroup() 

# ----------------------------------------------------------------------------
# Add variable to track EMAs occuring after the End of Day timestamp
# ----------------------------------------------------------------------------

cc2_ema_time_blocks_calc_v3 <- cc2_ema_time_blocks_calc_v2 %>% 
  mutate(invalid_end_day = (invalid_end_day_on_block & time_software_calc > day_end_hrts_AmericaChicago)) %>% 
  select(-invalid_end_day_on_block)

cc2_ema_time_blocks_calc_v3 %>% count(invalid_end_day, invalid_end_day_on_study_day)

cc2_ema_time_blocks_calc_v3 %>% count(block_calc)

if(F){cc2_ema_time_blocks_calc_v3 %>% filter(extra_ema_on_study_day) %>% View()}

cc2_ema_time_blocks_calc_v3 %>% filter(extra_ema) %>% count()

cc2_ema_time_blocks_calc_v3 %>% filter(extra_ema) %>% group_by(participant_id) %>% count() %>% summary()

cc2_ema_time_blocks_calc_v3 %>% filter(extra_ema) %>% group_by(participant_id) %>% count() %>% arrange(desc(n))


extra_ema_data_cc2 <- cc2_ema_time_blocks_calc_v3
# ----------------------------------------------------------------------------
# CC2 Multiple Day Start per Day stats
# ----------------------------------------------------------------------------
extra_ema_data_cc2 %>% count(multi_day_start)
extra_ema_data_cc2 %>% count(multi_day_start, extra_ema)

# ----------------------------------------------------------------------------
# Prep Dataset for remainder of the pipeline
#   *remove extra columns unnecessary to carry through pipeline
# ----------------------------------------------------------------------------
extra_ema_data_cc2 %>% colnames()

all_ema_data_wblocks_cc2 <- extra_ema_data_cc2 %>% 
  select(participant_id, cc_indicator, ema_type, status, with_any_response, begin_unixts, end_unixts,
         begin_hrts_UTC, end_hrts_UTC, begin_hrts_AmericaChicago, end_hrts_AmericaChicago, 
         time_software_calc, block_calc, extra_ema, extra_ema_on_study_day,
         day_start_hrts_AmericaChicago, block_start_hrts_AmericaChicago, 
         block_end_hrts_AmericaChicago, invalid_end_day, invalid_end_day_on_study_day, 
         multi_day_start, multi_day_start_on_study_day, everything())

all_ema_data_wblocks_cc2 <- all_ema_data_wblocks_cc2 %>% 
  select(-in_ema, -in_blocks, -study_day_start_date, -day_end_hrts_AmericaChicago)

all_ema_data_wblocks_cc2 %>% colnames()

# ----------------------------------------------------------------------------
# Placeholder to source in code for the block level dataset here
#   - it uses many of the variables currently loaded/generated in the lines above
# ----------------------------------------------------------------------------
#day_start_and_end_cc2 <- day_start_and_end_cc2 %>% rename(day_end_hrts = day_end_hrts_AmericaChicago)
day_start_and_end_cc2_v1d <- day_start_and_end_cc2_v1d %>% rename(day_end_hrts = day_end_hrts_AmericaChicago)
# Discuss the 16 NA block Calcs. Appear to occur after the end of day button was pressed, but otherwise within the window for the block
# test <- all_ema_data_wblocks_cc2 %>% mutate(date = as_date(end_hrts_AmericaChicago)) %>% group_by(participant_id, date) %>% filter(any(is.na(block_calc)))

if(F){source("generate-block-level-dataset-cc2.R")}

# ----------------------------------------------------------------------------
# Save Dataset
# ----------------------------------------------------------------------------
save(all_ema_data_wblocks_cc2,
  file = file.path(path_breakfree_staged_data, "all_ema_data_wblocks_cc2.RData"))
