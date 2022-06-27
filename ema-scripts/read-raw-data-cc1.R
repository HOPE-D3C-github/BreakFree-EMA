library(dplyr)
library(readr)
source("paths.R")
source(file.path("collect-functions", "io-utils.R"))

# Participant IDs for data collected using CC1 platform
ids_cc1 <- list.files(path = path_breakfree_cc1_input_data)

# -----------------------------------------------------------------------------
# Check for existence of files and determine whether each participant has a
# unique copy of each file
# -----------------------------------------------------------------------------
dat_file_counts_cc1 <- data.frame(participant_id = ids_cc1,
                                  count_random_ema_file = NA,
                                  count_smoking_ema_file = NA,
                                  count_stress_ema_file = NA,
                                  count_log_phone_file = NA)

for(i in 1:length(ids_cc1)){
  this_id <- ids_cc1[i]
  
  n_random <- CountFile(participant_id = this_id, 
                        file_name = "EMA+RANDOM_EMA+PHONE+processed.csv", 
                        directory = path_breakfree_cc1_input_data)
  
  n_smoking <- CountFile(participant_id = this_id, 
                         file_name = "EMA+SMOKING_EMA+PHONE+processed.csv", 
                         directory = path_breakfree_cc1_input_data)
  
  n_stress <- CountFile(participant_id = this_id, 
                        file_name = "EMA+STRESS_EMA+PHONE+processed.csv", 
                        directory = path_breakfree_cc1_input_data)
  
  n_log_phone <- CountFile(participant_id = this_id, 
                     file_name = "LOG+PHONE+analysis.csv", 
                     directory = path_breakfree_cc1_input_data)
  
  dat_file_counts_cc1 <- dat_file_counts_cc1 %>%
    mutate(count_random_ema_file = replace(count_random_ema_file, participant_id == this_id, n_random),
           count_smoking_ema_file = replace(count_smoking_ema_file, participant_id == this_id, n_smoking),
           count_stress_ema_file = replace(count_stress_ema_file, participant_id == this_id, n_stress),
           count_log_phone_file = replace(count_stress_ema_file, participant_id == this_id, n_stress))
}

# Calculate summary statistics
# If maximum number displayed is 1, then there are no duplicate files
dat_file_counts_cc1 %>%
  summarise(max_random = max(count_random_ema_file),
            max_smoking = max(count_smoking_ema_file),
            max_stress = max(count_stress_ema_file),
            max_log_phone = max(count_log_phone_file))

# -----------------------------------------------------------------------------
# Read CC1 random EMA raw data: responses to Random EMA questionnaire
# -----------------------------------------------------------------------------
list_df_raw <- list()

# Specify data stream of interest
this_string <- "EMA+RANDOM_EMA+PHONE+processed.csv"

for(i in 1:length(ids_cc1)){
  this_id <- ids_cc1[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_breakfree_cc1_input_data, this_id))
  # Pick out file names related to data stream of interest
  idx <- grepl(pattern = this_string, 
               x = all_files, 
               fixed = TRUE)
  # Pick out corresponding files
  this_file <- all_files[idx]
  is_any_file <- length(this_file)
  
  if(is_any_file == 1){
    df_raw <- read.csv(file.path(path_breakfree_cc1_input_data, this_id, this_file), 
                       header = TRUE,
                       sep = ",")  
    
    # Add column to record participant ID
    df_raw <- df_raw %>% 
      mutate(participant_id = this_id) %>% 
      select(participant_id, everything())
    
    #deduplicate entirely duplicated rows
    df_raw <- df_raw[!duplicated(df_raw),]
    
    list_df_raw <- append(list_df_raw, list(df_raw))
  }else{
    # In this case, the file we are looking for does not exist for this participant
    next
  }
}

cnt <- lapply(list_df_raw, ncol)
cnt <- unlist(cnt)
# Note that each participant's data frame will have varying number of columns
# due to the way the raw data is structured. Hence, we do not call do.call()
# and leave responses in list form
print(cnt)

all_random_ema_response_files_cc1 <- list_df_raw
remove(list_df_raw)

# -----------------------------------------------------------------------------
# Read CC1 smoking EMA raw data: responses to smoking EMA questionnaire
# -----------------------------------------------------------------------------
list_df_raw <- list()

# Specify data stream of interest
this_string <- "EMA+SMOKING_EMA+PHONE+processed.csv"

for(i in 1:length(ids_cc1)){
  this_id <- ids_cc1[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_breakfree_cc1_input_data, this_id))
  # Pick out file names related to data stream of interest
  idx <- grepl(pattern = this_string, 
               x = all_files, 
               fixed = TRUE)
  # Pick out corresponding files
  this_file <- all_files[idx]
  is_any_file <- length(this_file)
  
  if(is_any_file == 1){
    df_raw <- read.csv(file.path(path_breakfree_cc1_input_data, this_id, this_file), 
                       header = TRUE,
                       sep = ",")  
    
    # Add column to record participant ID
    df_raw <- df_raw %>% 
      mutate(participant_id = this_id) %>% 
      select(participant_id, everything())
    
    #deduplicate entirely duplicated rows
    df_raw <- df_raw[!duplicated(df_raw),]
    
    list_df_raw <- append(list_df_raw, list(df_raw))
  }else{
    # In this case, the file we are looking for does not exist for this participant
    next
  }
}

cnt <- lapply(list_df_raw, ncol)
cnt <- unlist(cnt)
# Note that each participant's data frame will have varying number of columns
# due to the way the raw data is structured. Hence, we do not call do.call()
# and leave responses in list form
print(cnt)

all_smoking_ema_response_files_cc1 <- list_df_raw
remove(list_df_raw)

# -----------------------------------------------------------------------------
# Read CC1 stress EMA raw data: responses to stress EMA questionnaire
# -----------------------------------------------------------------------------
list_df_raw <- list()

# Specify data stream of interest
this_string <- "EMA+STRESS_EMA+PHONE+processed.csv"

for(i in 1:length(ids_cc1)){
  this_id <- ids_cc1[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_breakfree_cc1_input_data, this_id))
  # Pick out file names related to data stream of interest
  idx <- grepl(pattern = this_string, 
               x = all_files, 
               fixed = TRUE)
  # Pick out corresponding files
  this_file <- all_files[idx]
  is_any_file <- length(this_file)
  
  if(is_any_file == 1){
    df_raw <- read.csv(file.path(path_breakfree_cc1_input_data, this_id, this_file), 
                       header = TRUE,
                       sep = ",")  
    
    # Add column to record participant ID
    df_raw <- df_raw %>% 
      mutate(participant_id = this_id) %>% 
      select(participant_id, everything())
    
    #deduplicate entirely duplicated rows
    df_raw <- df_raw[!duplicated(df_raw),]
    
    list_df_raw <- append(list_df_raw, list(df_raw))
  }else{
    # In this case, the file we are looking for does not exist for this participant
    next
  }
}

cnt <- lapply(list_df_raw, ncol)
cnt <- unlist(cnt)
# Note that each participant's data frame will have varying number of columns
# due to the way the raw data is structured. Hence, we do not call do.call()
# and leave responses in list form
print(cnt)

all_stress_ema_response_files_cc1 <- list_df_raw
remove(list_df_raw)

# -----------------------------------------------------------------------------
# Read CC1 phone log raw data: Log about EMA Scheduler and Block #
# -----------------------------------------------------------------------------
list_df_raw <- list()

# Specify data stream of interest
this_string <- "LOG+PHONE+analysis.csv"

for(i in 1:length(ids_cc1)){
  this_id <- ids_cc1[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_breakfree_cc1_input_data, this_id))
  # Pick out file names related to data stream of interest
  idx <- grepl(pattern = this_string, 
               x = all_files, 
               fixed = TRUE)
  # Pick out corresponding files
  this_file <- all_files[idx]
  is_any_file <- length(this_file)
  
  if(is_any_file == 1){
    df_raw <- read.csv(file.path(path_breakfree_cc1_input_data, this_id, this_file), 
                       header = TRUE,
                       sep = ",")  
    
    # Add column to record participant ID
    df_raw <- df_raw %>% 
      mutate(participant_id = this_id) %>% 
      select(participant_id, everything())
    
    df_raw <- df_raw %>% 
      mutate(across(everything(), as.character))
    
    #deduplicate entirely duplicated rows
    df_raw <- df_raw[!duplicated(df_raw),]
    
    list_df_raw <- append(list_df_raw, list(df_raw))
  }else{
    # In this case, the file we are looking for does not exist for this participant
    next
  }
}

cnt <- lapply(list_df_raw, ncol)
cnt <- unlist(cnt)
# Note that each participant's data frame will have varying number of columns
# due to the way the raw data is structured. Hence, we do not call do.call()
# and leave responses in list form
print(cnt)

all_log_phone_files_cc1 <- list_df_raw
remove(list_df_raw)

# ------------------------------------------------
# Read in day_start and day_end data for CC1 ####
# ------------------------------------------------
## Participant IDs for data collected using CC1 platform
## ids_cc1 <- all_ema_data_cc1 %>% distinct(participant_id) %>% unlist()

# Check for existence of files and determine whether each participant has a unique copy of each file 
dat_zipped_file_counts_cc1 <- data.frame(participant_id = ids_cc1,
                                         count_day_start_zip_csv_file = NA,
                                         count_day_end_zip_csv_file = NA)

for(i in 1:length(ids_cc1)){
  this_id <- ids_cc1[i]
  
  n_day_start_zip_csv <- CountFile(participant_id = this_id, 
                                   file_name = "DAY_START+PHONE.csv.zip", 
                                   directory = path_breakfree_cc1_input_data)
  
  n_day_end_zip_csv <- CountFile(participant_id = this_id,
                                 file_name = "DAY_END+PHONE.csv.zip", 
                                 directory = path_breakfree_cc1_input_data)
  
  
  dat_zipped_file_counts_cc1 <- dat_zipped_file_counts_cc1 %>%
    mutate(count_day_start_zip_csv_file = replace(count_day_start_zip_csv_file, participant_id == this_id, n_day_start_zip_csv),
           count_day_end_zip_csv_file = replace(count_day_end_zip_csv_file, participant_id == this_id, n_day_end_zip_csv))
}

# Calculate summary statistics
# If max and min numbers displayed are 1, then there are no duplicate files and each participant has a file
dat_zipped_file_counts_cc1 %>%
  summary()

# Read CC1 start and end day data ####
list_df_raw_day_start <- list()
list_df_raw_day_end <- list()

# Specify data stream of interest
these_strings <- c("DAY_START+PHONE.csv", "DAY_END+PHONE.csv")

for(i in 1: length(ids_cc1)){
  this_id <- ids_cc1[i]
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_breakfree_cc1_input_data, this_id)) 
  for(string_i in these_strings){   # For each participant, iterate once for the day_start file and another time for day_end file
    # Pick out file names related to data stream of interest
    idx <- grepl(pattern = string_i, 
                 x = all_files, 
                 fixed = TRUE)
    # Pick out corresponding files
    this_file <- all_files[idx]
    is_any_file <- length(this_file)
    if(is_any_file == 1){
      tmp <- try(read_csv(file.path(path_breakfree_cc1_input_data, this_id, this_file), col_names = FALSE, show_col_types = FALSE))
      if (!inherits(tmp, 'try-error')){
        df_raw <- tmp
      } else {
        df_raw <- NULL
      }
      if (!is.null(df_raw)){
        # Add column to record participant ID
        df_raw <- df_raw %>% 
          mutate(participant_id = this_id) %>% 
          select(participant_id, everything())
        #deduplicate entirely duplicated rows
        df_raw <- df_raw[!duplicated(df_raw),]
        if (string_i == "DAY_START+PHONE.csv"){
          list_df_raw_day_start <- append(list_df_raw_day_start, list(df_raw))
        } else {
          list_df_raw_day_end <- append(list_df_raw_day_end, list(df_raw))
        }
      }
    }
  }
}
all_day_start_files_cc1 <- list_df_raw_day_start
all_day_end_files_cc1 <- list_df_raw_day_end


# -----------------------------------------------------------------------------
# Save to RData file
# -----------------------------------------------------------------------------
save(dat_file_counts_cc1,
     all_random_ema_response_files_cc1,
     all_smoking_ema_response_files_cc1,
     all_stress_ema_response_files_cc1,
     all_log_phone_files_cc1,
     all_day_start_files_cc1,
     all_day_end_files_cc1,
     file = file.path(path_breakfree_staged_data, "ema_responses_raw_data_cc1.RData"))


