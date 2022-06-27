# -----------------------------------------------------------------------------
# Data pre-processing steps
# -----------------------------------------------------------------------------
N <- length(all_response_files_cc1)
list_df_reference <- list()

for(i in 1:N){
  df_raw <- all_response_files_cc1[[i]]
  
  # Create an id-variable for each ema in the raw data having
  # for merging responses within a participant
  df_raw$merge_id <- 1:nrow(df_raw)
  
  # Create outcome variable/target
  check1 <- grepl("_response_", colnames(df_raw))
  check2 <- (!(grepl("_response_option_", colnames(df_raw))))
  keep_these_cols <- check1 & check2
  keep_these_cols <- colnames(df_raw)[keep_these_cols]
  # Calling the function CheckAnyResponse() creates with_any_response
  df_raw <- CheckAnyResponse(df = df_raw, keep_cols = keep_these_cols)
  
  # Proceed with remaining steps 
  # Bring time variables to the left of the data frame
  df_raw <- df_raw %>% 
    select(participant_id, status,
           start_timestamp, end_timestamp,
           everything()) %>%
    mutate(status = as.character(status)) %>%
    mutate(begin_unixts = NA_real_,
           end_unixts = NA_real_)
  
  all_col_names <- colnames(df_raw)
  check <- (("question_answers_0_response_0") %in% all_col_names)
  
  if(isTRUE(check)){
    # Use question_answers_0_finish_time instead of 
    # question_answers_0_prompt_time for CC1 data
    # since in CC2 data, prompt times are zero throughout
    df_raw <- df_raw %>% 
      mutate(begin_unixts = question_answers_0_finish_time) %>%
      mutate(begin_unixts = replace(begin_unixts, 
                                    (status == "ABANDONED_BY_TIMEOUT") & (question_answers_0_finish_time==-1), 
                                    NA_real_)) %>%
      mutate(end_unixts = end_timestamp)
  } else {                    ### NOTE: Tony added this to try to avoid end_unixts from being NA when end_timestamp exists
    df_raw <- df_raw %>%
      mutate(end_unixts = end_timestamp)
  }
  
  # Rearrange columns
  df_raw <- df_raw %>%
    select(participant_id, merge_id, status, 
           begin_unixts, end_unixts, everything())
  
  df_ref <- df_raw %>%
    mutate(offset_secs = mCerebrum_offset*60) %>%
    select(participant_id, merge_id, status, 
           begin_unixts, end_unixts, offset_secs,
           with_any_response, start_timestamp)
  
  # Save changes
  all_response_files_cc1[[i]] <- df_raw
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
collect_idx <- ema_items_cc1 %>% 
  filter(question_type==find_this_string) %>%
  select(question_id)

# Change collect_idx from data frame to numeric array
collect_idx <- c(as.matrix(collect_idx))

# Grab items with responses find_this_string
source(file.path("ema-scripts", "grab-single-response-items-cc1.R"))
df_resp_cc1 <- df_resp_cc1 %>% mutate(across(grep(pattern = "item_", x = .), as.character))
list_collect_all <- append(list_collect_all, list(df_resp_cc1))

# Remove these variables from environment so that they can be reused
remove(list_resp_cc1, df_resp, df, df_resp_cc1)

# -----------------------------------------------------------------------------
# Grab raw data responses for each participant i and discard extraneous info:
# text_numeric items
# -----------------------------------------------------------------------------
find_this_string <- "text_numeric"

# collect_idx are question ids with response type find_this_string
collect_idx <- ema_items_cc1 %>% 
  filter(question_type==find_this_string) %>%
  select(question_id)

# Change collect_idx from data frame to numeric array
collect_idx <- c(as.matrix(collect_idx))

# Grab items with responses find_this_string
source(file.path("ema-scripts", "grab-single-response-items-cc1.R"))
df_resp_cc1 <- df_resp_cc1 %>% mutate(across(grep(pattern = "item_", x = .), as.character))
list_collect_all <- append(list_collect_all, list(df_resp_cc1))

# Remove these variables from environment so that they can be reused
remove(list_resp_cc1, df_resp, df, df_resp_cc1)

# -----------------------------------------------------------------------------
# Grab raw data responses for each participant i and discard extraneous info:
# multiple_select items
# -----------------------------------------------------------------------------
find_this_string <- "multiple_select"

# collect_idx are question ids with response type find_this_string
collect_idx <- ema_items_cc1 %>% 
  filter(question_type==find_this_string) %>%
  select(question_id)

# Change collect_idx from data frame to numeric array
collect_idx <- c(as.matrix(collect_idx))

# Grab items with responses find_this_string
source(file.path("ema-scripts", "grab-multiple-response-items-cc1.R"))
df_resp_cc1 <- df_resp_cc1 %>% mutate(across(grep(pattern = "item_", x = .), as.character))
list_collect_all <- append(list_collect_all, list(df_resp_cc1))

# Remove these variables from environment so that they can be reused
remove(list_resp_cc1, df_resp, df, df_resp_cc1)

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
  mutate(begin_unixts = if_else((with_any_response==1) & (is.na(begin_unixts)), start_timestamp, begin_unixts)) %>%
  select(-start_timestamp)

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


