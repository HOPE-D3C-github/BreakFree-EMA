# Now, move on to parse out responses from EMA in which a participant
# provided a response
N <- length(all_response_files_cc2)
list_df_reference <- list()

for(i in 1:N){
  df_raw <- all_response_files_cc2[[i]]
  
  # Create an id-variable for each ema in the raw data having
  # COMPLETED and ABANDONED_BY_TIMEOUT EMAs for merging responses
  # within a participant
  df_raw$merge_id <- 1:nrow(df_raw)
  
  # Create outcome variable/target
  check1 <- grepl("_response_", colnames(df_raw))
  check2 <- (!(grepl("_response_option_", colnames(df_raw))))
  keep_these_cols <- check1 & check2
  keep_these_cols <- colnames(df_raw)[keep_these_cols]
  # Calling the function CheckAnyResponse() creates with_any_response
  df_raw <- CheckAnyResponse(df = df_raw, keep_cols = keep_these_cols)
  
  # Bring time variables to the left of the data frame
  # delivered_unixts and begin_unixts are two time variables used to anchor analyses
  df_raw <- df_raw %>% 
    select(participant_id, status, start_time, end_time, everything()) %>%
    mutate(status = as.character(status)) %>%
    mutate(begin_unixts = NA_real_,
           end_unixts = NA_real_)
  
  all_col_names <- colnames(df_raw)
  check <- (("questions_0_response_0") %in% all_col_names)
  
  if(isTRUE(check)){
    # In raw data: questions_0_prompt_time = 0 all throughout
    df_raw <- df_raw %>% 
      mutate(begin_unixts = questions_0_finish_time) %>%
      mutate(begin_unixts = replace(begin_unixts, 
                                    (status=="ABANDONED_BY_TIMEOUT") & (questions_0_finish_time==0), 
                                    NA_real_)) %>%
      mutate(end_unixts = end_time)
  } else {                    ### NOTE: Tony added this to try to avoid end_unixts from being NA when end_timestamp exists
    df_raw <- df_raw %>%
      mutate(end_unixts = end_time)
  }
  
  
  # Rearrange columns
  df_raw <- df_raw %>%
    select(participant_id, merge_id, status, 
           begin_unixts, end_unixts, everything())
  
  df_ref <- df_raw %>%
    mutate(offset_secs = mCerebrum_offset/1000) %>%
    select(participant_id, merge_id, status, 
           begin_unixts, end_unixts, offset_secs,
           with_any_response, start_time)
  
  # Save changes
  all_response_files_cc2[[i]] <- df_raw
  list_df_reference <- append(list_df_reference, list(df_ref))
}

df_reference <- bind_rows(list_df_reference)

# -----------------------------------------------------------------------------
# Create a list to collect various types of responses
# -----------------------------------------------------------------------------
list_collect_all <- list()

# -----------------------------------------------------------------------------
# Grab raw data responses for each participant i and discard extraneous info:
# multiple_choice items
# -----------------------------------------------------------------------------
find_this_string <- "multiple_choice"

# collect_idx are question ids with response type find_this_string
collect_idx <- ema_items_cc2 %>% 
  filter(question_type==find_this_string) %>%
  select(question_id)

# Change collect_idx from data frame to numeric array
collect_idx <- c(as.matrix(collect_idx))

# Grab items with responses find_this_string
source(file.path("ema-scripts", "grab-single-response-items-cc2.R"))
df_resp_cc2 <- df_resp_cc2 %>% mutate(across(grep(pattern = "item_", x = .), as.character))
list_collect_all <- append(list_collect_all, list(df_resp_cc2))

# Remove these variables from environment so that they can be reused
remove(list_resp_cc2, df_resp, df, df_resp_cc2)

# -----------------------------------------------------------------------------
# Grab raw data responses for each participant i and discard extraneous info:
# number_picker items
# -----------------------------------------------------------------------------
find_this_string <- "number_picker"

# collect_idx are question ids with response type find_this_string
collect_idx <- ema_items_cc2 %>% 
  filter(question_type==find_this_string) %>%
  select(question_id)

# Change collect_idx from data frame to numeric array
collect_idx <- c(as.matrix(collect_idx))

# Grab items with responses find_this_string
source(file.path("ema-scripts", "grab-single-response-items-cc2.R"))
df_resp_cc2 <- df_resp_cc2 %>% mutate(across(grep(pattern = "item_", x = .), as.character))
list_collect_all <- append(list_collect_all, list(df_resp_cc2))

# Remove these variables from environment so that they can be reused
remove(list_resp_cc2, df_resp, df, df_resp_cc2)

# -----------------------------------------------------------------------------
# Grab raw data responses for each participant i and discard extraneous info:
# hour_minute items
# -----------------------------------------------------------------------------
find_this_string <- "hour_minute"

# collect_idx are question ids with response type find_this_string
collect_idx <- ema_items_cc2 %>% 
  filter(question_type==find_this_string) %>%
  select(question_id)

# Change collect_idx from data frame to numeric array
collect_idx <- c(as.matrix(collect_idx))

# Grab items with responses find_this_string
source(file.path("ema-scripts", "grab-single-response-items-cc2.R"))
df_resp_cc2 <- df_resp_cc2 %>% mutate(across(grep(pattern = "item_", x = .), as.character))
list_collect_all <- append(list_collect_all, list(df_resp_cc2))

# Remove these variables from environment so that they can be reused
remove(list_resp_cc2, df_resp, df, df_resp_cc2)

# -----------------------------------------------------------------------------
# Grab raw data responses for each participant i and discard extraneous info:
# multiple_select items
# -----------------------------------------------------------------------------
find_this_string <- "multiple_select"

# collect_idx are question ids with response type find_this_string
collect_idx <- ema_items_cc2 %>% 
  filter(question_type==find_this_string) %>%
  select(question_id)

# Change collect_idx from data frame to numeric array
collect_idx <- c(as.matrix(collect_idx))

# Grab items with responses find_this_string
source(file.path("ema-scripts", "grab-multiple-response-items-cc2.R"))
df_resp_cc2 <- df_resp_cc2 %>% mutate(across(grep(pattern = "item_", x = .), as.character))
list_collect_all <- append(list_collect_all, list(df_resp_cc2))

# Remove these variables from environment so that they can be reused
remove(list_resp_cc2, df_resp, df, df_resp_cc2)

# -----------------------------------------------------------------------------
# Merge different types of information into one data frame per participant
# -----------------------------------------------------------------------------
df_collect_all <- list_collect_all %>% purrr::reduce(left_join, by = c("participant_id", "merge_id"))
df_collect_all <- left_join(x = df_collect_all, y = df_reference, by = c("participant_id", "merge_id"))
df_collect_all <- df_collect_all %>% select(-merge_id)

# Remove these variables from environment 
remove(list_collect_all)

# -----------------------------------------------------------------------------
# Convert timestamps from milliseconds to seconds
# and add human-readable time
# -----------------------------------------------------------------------------

df_collect_all <- df_collect_all %>%
  select(participant_id, status, with_any_response,
         begin_unixts, end_unixts, offset_secs, 
         everything())

df_collect_all <- df_collect_all %>%
  mutate(begin_unixts = if_else((with_any_response==1) & (is.na(begin_unixts)), start_time, begin_unixts)) %>%
  select(-start_time)

df_collect_all <- df_collect_all %>% 
  arrange(participant_id, begin_unixts) %>%
  mutate(begin_unixts = begin_unixts/1000,
         end_unixts = end_unixts/1000) 

df_collect_all <- df_collect_all %>% 
  mutate(begin_hrts_UTC = as.POSIXct(begin_unixts, tz = "UTC", origin="1970-01-01"),
         end_hrts_UTC = as.POSIXct(end_unixts, tz = "UTC", origin="1970-01-01")) %>%
  select(participant_id, status, with_any_response,
         begin_unixts, end_unixts, offset_secs, 
         begin_hrts_UTC, end_hrts_UTC,
         everything())

df_collect_all <- df_collect_all %>%
  mutate(begin_hrts_AmericaChicago = with_tz(begin_hrts_UTC, tzone = "America/Chicago"),
         end_hrts_AmericaChicago = with_tz(end_hrts_UTC, tzone = "America/Chicago")) %>%
  select(participant_id, status, with_any_response,
         begin_unixts, end_unixts, offset_secs, 
         begin_hrts_UTC, end_hrts_UTC,
         begin_hrts_AmericaChicago,
         end_hrts_AmericaChicago,
         everything())

# -----------------------------------------------------------------------------
# Other data preparation steps
# -----------------------------------------------------------------------------
# Add EMA type
df_collect_all <- df_collect_all %>% mutate(ema_type = use_ema_type)

# Rearrange columns
tmp_idx <- grep(pattern = "item_", x = colnames(df_collect_all))
tmp_item_numbers <- substring(colnames(df_collect_all)[tmp_idx], first = 6)
tmp_item_numbers <- as.numeric(tmp_item_numbers)
tmp_item_numbers <- tmp_item_numbers[order(tmp_item_numbers)]
tmp_item_names <- paste("item_",tmp_item_numbers,sep="")

df_collect_all <- df_collect_all %>%
  select(participant_id, 
         ema_type,
         status, 
         with_any_response,
         begin_unixts, 
         end_unixts,
         offset_secs,
         begin_hrts_UTC, 
         end_hrts_UTC,
         begin_hrts_AmericaChicago,
         end_hrts_AmericaChicago,
         all_of(tmp_item_names),
         everything()) %>%
  select(-offset_secs)

# -----------------------------------------------------------------------------
# Extract the unique rows corresponding to the moment in time when the
# data collection software regards an EMA as missed or cancelled
# -----------------------------------------------------------------------------
# Note that 'null' connotes the end of a particular sequence of state transitions
all_status_files_cc2 <- all_status_files_cc2 %>%
  filter((V4=="MISSED" & V5=="null")|(V4=="Cancel" & V5=="null")) %>%
  mutate(status = V4) %>%
  mutate(with_any_response = 0)

# Now, construct end time
all_status_files_cc2 <- all_status_files_cc2 %>%
  mutate(V1 = as.numeric(V1),
         V2 = as.numeric(V2)) %>%
  mutate(end_unixts = (V1)/1000) %>%
  mutate(end_hrts_UTC = as.POSIXct(x = end_unixts, tz = "UTC", origin = "1970-01-01")) %>%
  mutate(end_hrts_AmericaChicago = with_tz(end_hrts_UTC, tzone = "America/Chicago"))

# Construct remaining time variables 
all_status_files_cc2 <- all_status_files_cc2 %>%
  mutate(ema_type = use_ema_type) %>%
  mutate(begin_unixts = NA, begin_hrts_UTC = NA, begin_hrts_AmericaChicago = NA)

#build vars to QC duplicates EMAs that are within 1 second of each other
all_status_files_cc2 <- all_status_files_cc2 %>% 
  arrange(participant_id,ema_type,end_hrts_AmericaChicago) %>% 
  group_by(participant_id,ema_type,status) %>% 
  mutate(temp_ematime_sincelast_dup = c(F,diff(end_hrts_AmericaChicago)< seconds(1)),
        temp_ematime_id = cumsum(!temp_ematime_sincelast_dup)) %>% 
  ungroup
  #extra vars are OK because of select statement below

#Tony's edit
sum(all_status_files_cc2$temp_ematime_sincelast_dup) # test case (random EMA): 13 before change made and 13 after the "<1" ==> "< seconds(1)" change was made
# [END] Tony's edit

#remove extraneous EMAs based on duplicate vars
all_status_files_cc2 <- all_status_files_cc2 %>% 
  filter(!temp_ematime_sincelast_dup)

#Tony's edit
sum(all_status_files_cc2$temp_ematime_sincelast_dup) # confirmed all duplicates were removed
# [END] Tony's edit

# Select relevant columns
all_status_files_cc2 <- all_status_files_cc2 %>%
  select(participant_id, 
         ema_type, 
         status, 
         with_any_response, 
         begin_unixts, 
         end_unixts, 
         begin_hrts_UTC,
         end_hrts_UTC,
         begin_hrts_AmericaChicago,
         end_hrts_AmericaChicago)

# Prepare to append with df_collect_all
add_these_dummy_cols <- setdiff(colnames(df_collect_all), colnames(all_status_files_cc2))
dummy_matrix <- matrix(rep(NA, length(add_these_dummy_cols) * nrow(all_status_files_cc2)),
                       ncol = length(add_these_dummy_cols), nrow = nrow(all_status_files_cc2))
dummy_matrix <- as.data.frame(dummy_matrix)
colnames(dummy_matrix)<- add_these_dummy_cols
all_status_files_cc2 <- cbind(all_status_files_cc2, dummy_matrix)

# -----------------------------------------------------------------------------
# Now, place completed, missed, cancelled, abandoned by time out EMAs in one
# data frame
# -----------------------------------------------------------------------------
ordered_colnames <- colnames(df_collect_all)
all_status_files_cc2 <- all_status_files_cc2 %>% select(all_of(ordered_colnames))
df_collect_all <- rbind(df_collect_all, all_status_files_cc2)

