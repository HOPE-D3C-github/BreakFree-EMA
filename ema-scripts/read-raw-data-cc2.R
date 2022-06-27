library(dplyr)
source("paths.R")
source(file.path("collect-functions", "io-utils.R"))

# Participant IDs for data collected using CC2 platform
ids_cc2 <- list.files(path = path_breakfree_cc2_input_data)
ids_cc2 <- ids_cc2[grepl("aa_", ids_cc2)]
ids_cc2 <- ids_cc2[!grepl("test", ids_cc2)]

# -----------------------------------------------------------------------------
# Check for existence of files and determine whether each participant has a
# unique copy of each file
# -----------------------------------------------------------------------------
dat_file_counts_cc2 <- data.frame(participant_id = ids_cc2,
                                  count_random_ema_data_file = NA,
                                  count_smoking_ema_data_file = NA,
                                  count_stress_ema_data_file = NA,
                                  count_random_ema_status_file = NA,
                                  count_smoking_ema_status_file = NA,
                                  count_stress_ema_status_file = NA)

for(i in 1:length(ids_cc2)){
  this_id <- ids_cc2[i]
  
  n_random_data <- CountFile(participant_id = this_id, 
                             file_name = "EMA_RANDOM--DATA--org.md2k.ema.csv.bz2", 
                             directory = path_breakfree_cc2_input_data)
  
  n_smoking_data <- CountFile(participant_id = this_id, 
                              file_name = "EMA_SMOKING--DATA--org.md2k.ema.csv.bz2", 
                              directory = path_breakfree_cc2_input_data)
  
  n_stress_data <- CountFile(participant_id = this_id, 
                             file_name = "EMA_STRESS--DATA--org.md2k.ema.csv.bz2", 
                             directory = path_breakfree_cc2_input_data)
  
  n_random_status <- CountFile(participant_id = this_id, 
                               file_name = "EMA_RANDOM--STATUS--org.md2k.scheduler.csv.bz2", 
                               directory = path_breakfree_cc2_input_data)
  
  n_smoking_status <- CountFile(participant_id = this_id, 
                                file_name = "EMA_SMOKING--STATUS--org.md2k.scheduler.csv.bz2", 
                                directory = path_breakfree_cc2_input_data)
  
  n_stress_status <- CountFile(participant_id = this_id, 
                               file_name = "EMA_STRESS--STATUS--org.md2k.scheduler.csv.bz2", 
                               directory = path_breakfree_cc2_input_data)
  
  dat_file_counts_cc2 <- dat_file_counts_cc2 %>%
    mutate(count_random_ema_data_file = replace(count_random_ema_data_file, participant_id == this_id, n_random_data),
           count_smoking_ema_data_file = replace(count_smoking_ema_data_file, participant_id == this_id, n_smoking_data),
           count_stress_ema_data_file = replace(count_stress_ema_data_file, participant_id == this_id, n_stress_data),
           count_random_ema_status_file = replace(count_random_ema_status_file, participant_id == this_id, n_random_status),
           count_smoking_ema_status_file = replace(count_smoking_ema_status_file, participant_id == this_id, n_smoking_status),
           count_stress_ema_status_file = replace(count_stress_ema_status_file, participant_id == this_id, n_stress_status))
}

# Calculate summary statistics
# If maximum number displayed is 1, then there are no duplicate files
dat_file_counts_cc2 %>%
  summarise(max_random_data = max(count_random_ema_data_file),
            max_smoking_data = max(count_smoking_ema_data_file),
            max_stress_data = max(count_stress_ema_data_file),
            max_random_status = max(count_random_ema_status_file),
            max_smoking_status = max(count_smoking_ema_status_file),
            max_stress_status = max(count_stress_ema_status_file))

# -----------------------------------------------------------------------------
# Read CC2 random EMA raw data: STATUS file
# -----------------------------------------------------------------------------
list_df_raw <- list()

# Specify data stream of interest
this_string <- "EMA_RANDOM--STATUS--org.md2k.scheduler.csv.bz2"

for(i in 1:length(ids_cc2)){
  this_id <- ids_cc2[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_breakfree_cc2_input_data, this_id))
  # Pick out file names related to data stream of interest
  idx <- match(x=this_string, table=all_files)
  # Pick out corresponding files
  this_file <- all_files[idx]
  
  # Read file if it exists for given participant
  if(!is.na(this_file)){
    # Simply using read.csv's default value for sep (the comma: ",") will result in
    # some entries being viewed as corresponding to two cells when they should 
    # correspond to only one cell. This can happen, for example, 
    df_raw <- read.csv(file.path(path_breakfree_cc2_input_data, this_id, this_file), 
                       header = FALSE, # STATUS files do not contain column names
                       sep = ";")  
    
    list_split <- strsplit(x = df_raw[["V1"]], split = ", total_incentive=")
    list_split <- lapply(list_split, function(x){
      if(length(x) == 2){
        new_x <- paste(x[1], paste(" total = ", x[2]), sep = "")
      }else{
        new_x <- x
      }
      return(new_x)
    })
    
    df_raw <- do.call(rbind, list_split)
    list_split <- strsplit(x = df_raw[,1], split = ",")
    list_split <- lapply(list_split, function(x){
      if(length(x)==4){
        # a minimal amount of rows will have four columns
        # at this point; put a placeholder value of 999
        # in a fifth column
        x <- c(x, "999")
      }
      return(x)
    })
    df_raw <- do.call(rbind, list_split)
    
    # Add column to record participant ID
    df_raw <- as.data.frame(df_raw) %>% 
      mutate(participant_id = this_id) %>% 
      select(participant_id, everything())
    
    #deduplicate entirely duplicated rows
    df_raw <- df_raw[!duplicated(df_raw),]
    
    # Add df_raw to collection
    list_df_raw <- append(list_df_raw, list(df_raw))
  }else{
    next
  }
}

all_random_ema_status_files_cc2 <- do.call(rbind, list_df_raw)

# -----------------------------------------------------------------------------
# Read CC2 random EMA raw data: DATA file
# -----------------------------------------------------------------------------
list_df_raw <- list()

# Specify data stream of interest
this_string <- "EMA_RANDOM--DATA--org.md2k.ema.csv"

for(i in 1:length(ids_cc2)){
  this_id <- ids_cc2[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_breakfree_cc2_input_data, this_id))
  # Pick out file names related to data stream of interest
  # Note: The file EMA_RANDOM--DATA--org.md2k.ema.csv will not contain MISSED
  # or CANCELLED Random EMA. WHence, we needed to bring in information from
  # the EMA_RANDOM--STATUS--org.md2k.scheduler.csv.bz2 file above which would
  # contain information related to MISSED or CANCELLED Random EMA
  idx <- match(x=this_string, table=all_files)
  # Pick out corresponding files
  this_file <- all_files[idx]
  
  # Read file if it exists for given participant
  if(!is.na(this_file)){
    df_raw <- read.csv(file.path(path_breakfree_cc2_input_data, this_id, this_file), 
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
    next
  }
}


cnt <- lapply(list_df_raw, ncol)
cnt <- unlist(cnt)
# Note that each participant's data frame will have varying number of columns
# due to the way the raw data is structured. Hence, we do not call do.call()
# and leave responses in list form
print(cnt)

all_random_ema_response_files_cc2 <- list_df_raw

# -----------------------------------------------------------------------------
# Read CC2 smoking EMA raw data: STATUS file
# -----------------------------------------------------------------------------
list_df_raw <- list()

# Specify data stream of interest
this_string <- "EMA_SMOKING--STATUS--org.md2k.scheduler.csv.bz2"

for(i in 1:length(ids_cc2)){
  this_id <- ids_cc2[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_breakfree_cc2_input_data, this_id))
  # Pick out file names related to data stream of interest
  idx <- match(x=this_string, table=all_files)
  # Pick out corresponding files
  this_file <- all_files[idx]
  
  # Read file if it exists for given participant
  if(!is.na(this_file)){
    # Simply using read.csv's default value for sep (the comma: ",") will result in
    # some entries being viewed as corresponding to two cells when they should 
    # correspond to only one cell. This can happen, for example, 
    df_raw <- read.csv(file.path(path_breakfree_cc2_input_data, this_id, this_file), 
                       header = FALSE, # STATUS files do not contain column names
                       sep = ";")  
    
    list_split <- strsplit(x = df_raw[["V1"]], split = ", total_incentive=")
    list_split <- lapply(list_split, function(x){
      if(length(x) == 2){
        new_x <- paste(x[1], paste(" total = ", x[2]), sep = "")
      }else{
        new_x <- x
      }
      return(new_x)
    })
    
    df_raw <- do.call(rbind, list_split)
    list_split <- strsplit(x = df_raw[,1], split = ",")
    list_split <- lapply(list_split, function(x){
      if(length(x)==4){
        # a minimal amount of rows will have four columns
        # at this point; put a placeholder value of 999
        # in a fifth column
        x <- c(x, "999")
      }
      return(x)
    })
    df_raw <- do.call(rbind, list_split)
    
    # Add column to record participant ID
    df_raw <- as.data.frame(df_raw) %>% 
      mutate(participant_id = this_id) %>% 
      select(participant_id, everything())
    
    #deduplicate entirely duplicated rows
    df_raw <- df_raw[!duplicated(df_raw),]
    
    # Add df_raw to collection
    list_df_raw <- append(list_df_raw, list(df_raw))
  }else{
    next
  }
}

all_smoking_ema_status_files_cc2 <- do.call(rbind, list_df_raw)

# -----------------------------------------------------------------------------
# Read CC2 smoking EMA raw data: DATA file
# -----------------------------------------------------------------------------
list_df_raw <- list()

# Specify data stream of interest
this_string <- "EMA_SMOKING--DATA--org.md2k.ema.csv"

for(i in 1:length(ids_cc2)){
  this_id <- ids_cc2[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_breakfree_cc2_input_data, this_id))
  # Pick out file names related to data stream of interest
  # Note: The file EMA_RANDOM--DATA--org.md2k.ema.csv will not contain MISSED
  # or CANCELLED Random EMA. WHence, we needed to bring in information from
  # the EMA_RANDOM--STATUS--org.md2k.scheduler.csv.bz2 file above which would
  # contain information related to MISSED or CANCELLED Random EMA
  idx <- match(x=this_string, table=all_files)
  # Pick out corresponding files
  this_file <- all_files[idx]
  
  # Read file if it exists for given participant
  if(!is.na(this_file)){
    df_raw <- read.csv(file.path(path_breakfree_cc2_input_data, this_id, this_file), 
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
    next
  }
}


cnt <- lapply(list_df_raw, ncol)
cnt <- unlist(cnt)
# Note that each participant's data frame will have varying number of columns
# due to the way the raw data is structured. Hence, we do not call do.call()
# and leave responses in list form
print(cnt)

all_smoking_ema_response_files_cc2 <- list_df_raw

# -----------------------------------------------------------------------------
# Read CC2 stress EMA raw data: STATUS file
# -----------------------------------------------------------------------------
list_df_raw <- list()

# Specify data stream of interest
this_string <- "EMA_STRESS--STATUS--org.md2k.scheduler.csv.bz2"

for(i in 1:length(ids_cc2)){
  this_id <- ids_cc2[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_breakfree_cc2_input_data, this_id))
  # Pick out file names related to data stream of interest
  idx <- match(x=this_string, table=all_files)
  # Pick out corresponding files
  this_file <- all_files[idx]
  
  # Read file if it exists for given participant
  if(!is.na(this_file)){
    # Simply using read.csv's default value for sep (the comma: ",") will result in
    # some entries being viewed as corresponding to two cells when they should 
    # correspond to only one cell. This can happen, for example, 
    df_raw <- read.csv(file.path(path_breakfree_cc2_input_data, this_id, this_file), 
                       header = FALSE, # STATUS files do not contain column names
                       sep = ";")  
    
    list_split <- strsplit(x = df_raw[["V1"]], split = ", total_incentive=")
    list_split <- lapply(list_split, function(x){
      if(length(x) == 2){
        new_x <- paste(x[1], paste(" total = ", x[2]), sep = "")
      }else{
        new_x <- x
      }
      return(new_x)
    })
    
    df_raw <- do.call(rbind, list_split)
    list_split <- strsplit(x = df_raw[,1], split = ",")
    list_split <- lapply(list_split, function(x){
      if(length(x)==4){
        # a minimal amount of rows will have four columns
        # at this point; put a placeholder value of 999
        # in a fifth column
        x <- c(x, "999")
      }
      return(x)
    })
    df_raw <- do.call(rbind, list_split)
    
    # Add column to record participant ID
    df_raw <- as.data.frame(df_raw) %>% 
      mutate(participant_id = this_id) %>% 
      select(participant_id, everything())
    
    #deduplicate entirely duplicated rows
    df_raw <- df_raw[!duplicated(df_raw),]
    
    # Add df_raw to collection
    list_df_raw <- append(list_df_raw, list(df_raw))
  }else{
    next
  }
}

all_stress_ema_status_files_cc2 <- do.call(rbind, list_df_raw)

# -----------------------------------------------------------------------------
# Read CC2 stress EMA raw data: DATA file
# -----------------------------------------------------------------------------
list_df_raw <- list()

# Specify data stream of interest
this_string <- "EMA_STRESS--DATA--org.md2k.ema.csv"

for(i in 1:length(ids_cc2)){
  this_id <- ids_cc2[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_breakfree_cc2_input_data, this_id))
  # Pick out file names related to data stream of interest
  # Note: The file EMA_RANDOM--DATA--org.md2k.ema.csv will not contain MISSED
  # or CANCELLED Random EMA. WHence, we needed to bring in information from
  # the EMA_RANDOM--STATUS--org.md2k.scheduler.csv.bz2 file above which would
  # contain information related to MISSED or CANCELLED Random EMA
  idx <- match(x=this_string, table=all_files)
  # Pick out corresponding files
  this_file <- all_files[idx]
  
  # Read file if it exists for given participant
  if(!is.na(this_file)){
    df_raw <- read.csv(file.path(path_breakfree_cc2_input_data, this_id, this_file), 
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
    next
  }
}


cnt <- lapply(list_df_raw, ncol)
cnt <- unlist(cnt)
# Note that each participant's data frame will have varying number of columns
# due to the way the raw data is structured. Hence, we do not call do.call()
# and leave responses in list form
print(cnt)

all_stress_ema_response_files_cc2 <- list_df_raw


# Participant IDs for data collected using CC2 platform
ids_cc2 <- list.files(path = path_breakfree_cc2_input_data)
ids_cc2 <- ids_cc2[grepl("aa_", ids_cc2)]
ids_cc2 <- ids_cc2[!grepl("test", ids_cc2)]

# -----------------------------------------------------------------------------
# Read CC2 day_start and day_end raw data
# -----------------------------------------------------------------------------
list_df_raw_day_start <- list()
list_df_raw_day_end <- list()

# Specify data stream of interest
these_strings <- c("DAY--START--org.md2k.studywithema.csv.bz2", "DAY--END--org.md2k.studywithema.csv.bz2")

for(i in 1:length(ids_cc2)){
  this_id <- ids_cc2[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_breakfree_cc2_input_data, this_id))
  for(string_i in these_strings){   # For each participant, iterate once for the day_start file and another time for day_end file
    # Pick out file names related to data stream of interest
    idx <- match(x=string_i, table=all_files)
    # Pick out corresponding files
    this_file <- all_files[idx]
    
    # Read file if it exists for given participant
    if(!is.na(this_file)){
      df_raw <- read.csv(file.path(path_breakfree_cc2_input_data, this_id, this_file),
                         header = FALSE,
                         sep = ",")
      # Add column to record participant ID
      df_raw <- df_raw %>%
        mutate(participant_id = this_id) %>%
        select(participant_id, everything())
      
      #deduplicate entirely duplicated rows
      df_raw <- df_raw[!duplicated(df_raw),]
      
      if (string_i == "DAY--START--org.md2k.studywithema.csv.bz2"){
        list_df_raw_day_start <- append(list_df_raw_day_start, list(df_raw))
      } else {
        list_df_raw_day_end <- append(list_df_raw_day_end, list(df_raw))
      }
    }
  }
}
all_day_start_files_cc2 <- list_df_raw_day_start
all_day_end_files_cc2 <- list_df_raw_day_end

# -----------------------------------------------------------------------------
# Read CC2 EMA Conditions raw data: DATA file
# -----------------------------------------------------------------------------
list_df_raw <- list()

# Specify data stream of interest
this_string <- 'ema_conditions.csv'

for(i in 1:length(ids_cc2)){
  this_id <- ids_cc2[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_breakfree_cc2_input_data, this_id))
  # Pick out file names related to data stream of interest
  # Note: The file EMA_RANDOM--DATA--org.md2k.ema.csv will not contain MISSED
  # or CANCELLED Random EMA. WHence, we needed to bring in information from
  # the EMA_RANDOM--STATUS--org.md2k.scheduler.csv.bz2 file above which would
  # contain information related to MISSED or CANCELLED Random EMA
  idx <- match(x=this_string, table=all_files)
  # Pick out corresponding files
  this_file <- all_files[idx]
  
  # Read file if it exists for given participant
  if(!is.na(this_file)){
    df_raw <- read.csv(file.path(path_breakfree_cc2_input_data, this_id, this_file), 
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
    next
  }
}


cnt <- lapply(list_df_raw, ncol)
cnt <- unlist(cnt)
# Note that each participant's data frame will have varying number of columns
# due to the way the raw data is structured. Hence, we do not call do.call()
# and leave responses in list form
print(cnt)

all_ema_conditions_files_cc2 <- do.call(rbind, list_df_raw)

# -----------------------------------------------------------------------------
# Save to RData file
# -----------------------------------------------------------------------------
save(dat_file_counts_cc2,
     all_random_ema_status_files_cc2,
     all_random_ema_response_files_cc2,
     all_smoking_ema_status_files_cc2,
     all_smoking_ema_response_files_cc2,
     all_stress_ema_status_files_cc2,
     all_stress_ema_response_files_cc2,
     all_day_start_files_cc2,
     all_day_end_files_cc2,
     all_ema_conditions_files_cc2,
     file = file.path(path_breakfree_staged_data, "ema_raw_data_cc2.RData"))


