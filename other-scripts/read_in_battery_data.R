library(dplyr)
library(lubridate)
library(tidyr)

source("paths.R")
source(file.path("collect-functions", "io-utils.R"))

dat_master <- readRDS(file = file.path("C:/Users/u1330076/Box/Break Free curated data/EMA/data for analysis", "masterlist.rds"))

#### Looking at how to import Battery data ####
# Notes:
# I added all the raw data for each participant to a dataframe,
# created a list of all those dataframes, then rbind-ed them 
# to create one dataframe. The resulting dataframe was 

# -----------------------------------------------------------------------------
# Starting with CC1 ####
# "...+BATTERY+PHONE.json" contains metadata
# "...+BATTERY+PHONE.csv" is a zipped folder containing a csv of battery data
# -----------------------------------------------------------------------------

# Participant IDs for data collected using CC1 platform
ids_cc1 <- dat_master %>% filter(cc_indicator == 1) %>% .$participant_id

# -----------------------------------------------------------------------------
# Check for existence of files and determine whether each participant has a unique copy of each file ####
# -----------------------------------------------------------------------------
dat_zipped_file_counts_cc1 <- data.frame(participant_id = ids_cc1,
                                  count_battery_zip_csv_file = NA)

for(i in 1:length(ids_cc1)){
  this_id <- ids_cc1[i]
  
  n_battery_zip_csv <- CountFile(participant_id = this_id, 
                        file_name = "BATTERY+PHONE.csv.zip", 
                        directory = path_breakfree_cc1_input_data)

  dat_zipped_file_counts_cc1 <- dat_zipped_file_counts_cc1 %>%
    mutate(count_battery_zip_csv_file = replace(count_battery_zip_csv_file, participant_id == this_id, n_battery_zip_csv))
}

# Calculate summary statistics
# If maximum number displayed is 1, then there are no duplicate files
dat_zipped_file_counts_cc1 %>%
  summarise(max_zip_csv = max(count_battery_zip_csv_file),
            min_zip_csv = min(count_battery_zip_csv_file))


# ----------------------------------------------------------------------------- 
# Check for existence of more than one csv in each participants zipped battery folder ####
# -----------------------------------------------------------------------------
if (T){
  dat_csv_file_counts_cc1 <- data.frame(participant_id = ids_cc1,
                                  count_battery_csv_file = NA)

  this_string <- "BATTERY+PHONE.csv"
  
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
      unzipped_all <- unzip(zipfile = file.path(path_breakfree_cc1_input_data, this_id, this_file), list = TRUE)
      unzipped_filenames <- unzipped_all$Name
      n_battery_csv <- length(unzipped_filenames)
      
      dat_csv_file_counts_cc1 <- dat_csv_file_counts_cc1 %>%
        mutate(count_battery_csv_file = replace(count_battery_csv_file, participant_id == this_id, n_battery_csv))
    }
  }
  
  dat_csv_file_counts_cc1 %>%
    summarise(max_zip_csv = max(count_battery_csv_file),
              min_zip_csv = min(count_battery_csv_file))}


# -----------------------------------------------------------------------------
# Read CC1 battery raw data ####
# -----------------------------------------------------------------------------
list_df_filtered <- list()

# Specify data stream of interest
this_string <- "BATTERY+PHONE.csv"

start_time <- Sys.time()
for(i in 1: length(ids_cc1)){
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
    unzipped_all <- unzip(zipfile = file.path(path_breakfree_cc1_input_data, this_id, this_file), list = TRUE)
    unzipped_filenames <- unzipped_all$Name
    
    tmp <- try(read.csv(unz(file.path(path_breakfree_cc1_input_data, this_id, this_file),
                            unzipped_filenames), sep = ","))
    if (!inherits(tmp, 'try-error')){
      df_raw <- tmp
    } else {
      df_raw <- NULL
    }
    
    if (!is.null(df_raw) & ncol(df_raw) == 5){   
      # Ran into an error when there are no lines of data in the file ""no lines available in input". this logical allows the process to continue
      # Another issue where the data file didn't have all the 5 columns (no headers in the csv)
      df_raw <- df_raw %>%  
        stats::setNames(c("datetime", "unk_1", "battery_percent", "battery_voltage", "unk_2")) %>% 
        select(datetime, battery_percent)
      
      df_raw  <- df_raw %>% mutate(datetime = datetime/1000)  # convert from unix with milliseconds to unix with seconds
      
      df_filtered <- data.frame(datetime = numeric(),
                                  battery_percent = integer())
      
      for(indx in 1:nrow(df_raw)){
        if (indx == 1){   #add first timestamp
          df_filtered <- df_filtered %>% add_row(datetime = df_raw$datetime[indx],
                                                     battery_percent = df_raw$battery_percent[indx])
        } else if(as.integer(df_raw$datetime[indx]) %% 60 == 0){  #timestamp is divisible by 60 
          df_filtered <- df_filtered %>% add_row(datetime = df_raw$datetime[indx],
                                                     battery_percent = df_raw$battery_percent[indx])
        } else if(df_raw$datetime[indx] - df_filtered$datetime[nrow(df_filtered)] >= 60){  #last added timestamp was more than 60 seconds ago, then add it
          df_filtered <- df_filtered %>% add_row(datetime = df_raw$datetime[indx],
                                                     battery_percent = df_raw$battery_percent[indx])
          }
        }
      
      remove(df_raw)
      
      # Add column to record participant ID
      df_filtered <- df_filtered %>% 
        mutate(participant_id = this_id) %>% 
        select(participant_id, everything())
      
      #deduplicate entirely duplicated rows
      df_filtered <- df_filtered[!duplicated(df_filtered),]
      
      list_df_filtered <- append(list_df_filtered, list(df_filtered))
    }
  }else{
    # In this case, the file we are looking for does not exist for this participant
    next
  }
}
end_time <- Sys.time()
end_time - start_time    # Time difference of 10.6 mins                      ## old - 2.839069 mins

list_battery_data_cc1 <- list_df_filtered
remove(list_df_filtered)
remove(df_filtered)

if(F){save(list_battery_data_cc1,
           file = file.path("C:/Users/u1330076/Box/Break Free curated data/EMA/Tony - demo/staged", "battery_raw_filtered_data_cc1.RData"))}

# -----------------------------------------------------------------------------
# END cc1 read-raw-data steps (equivalent to "read-raw-data-cc1.R") ####
# -----------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# START cc2 read-raw-data steps ####
# Files are:
# "BATTERY--org.md2k.phonesensor--PHONE.json" - metadata
# "BATTERY--org.md2k.phonesensor--PHONE.csv.bz2" - battery data
# -----------------------------------------------------------------------------

# Participant IDs for data collected using CC2 platform
ids_cc2 <- dat_master %>% filter(cc_indicator == 2) %>% .$participant_id

# -----------------------------------------------------------------------------
# Check for existence of files and determine whether each participant has a
# unique copy of each file
# -----------------------------------------------------------------------------
dat_csv_file_counts_cc2 <- data.frame(participant_id = ids_cc2,
                                         count_battery_csv_file = NA)

for(i in 1:length(ids_cc2)){
  this_id <- ids_cc2[i]
  
  n_battery_csv <- CountFile(participant_id = this_id, 
                                 file_name = "BATTERY--org.md2k.phonesensor--PHONE.csv.bz2", 
                                 directory = path_breakfree_cc2_input_data)
  
  dat_csv_file_counts_cc2 <- dat_csv_file_counts_cc2 %>%
    mutate(count_battery_csv_file = replace(count_battery_csv_file, participant_id == this_id, n_battery_csv))
}

# Calculate summary statistics
# If maximum number displayed is 1, then there are no duplicate files
dat_csv_file_counts_cc2 %>%
  summarise(max_csv = max(count_battery_csv_file),
            min_csv = min(count_battery_csv_file))

# -----------------------------------------------------------------------------
# Read CC2 battery raw data
# -----------------------------------------------------------------------------

list_df_filtered <- list()

# Specify file of interest
this_file <- "BATTERY--org.md2k.phonesensor--PHONE.csv.bz2"

start_time <- Sys.time()
for(i in 1: length(ids_cc2)){
  this_id <- ids_cc2[i]
  
  
  tmp <- try(read.csv(file.path(path_breakfree_cc2_input_data, this_id, this_file), 
                      header = FALSE, 
                      sep = ","))
  if (!inherits(tmp, 'try-error')){
    df_raw <- tmp
  } else {
    df_raw <- NULL
  }
  
  if (!is.null(df_raw) & ncol(df_raw) == 5){   
    # Ran into an error when there are no lines of data in the file ""no lines available in input". this logical allows the process to continue
    # Another issue where the data file didn't have all the 5 columns (no headers in the csv)
    df_raw <- df_raw %>%  
      stats::setNames(c("datetime", "unk_1", "battery_percent", "battery_voltage", "unk_2")) %>% 
      select(datetime, battery_percent)

    df_raw  <- df_raw %>% mutate(datetime = datetime/1000)  # convert from unix with milliseconds to unix with seconds
    
    df_filtered <- data.frame(datetime = numeric(),
                              battery_percent = integer())
    
    for(indx in 1:nrow(df_raw)){
      if (indx == 1){   #add first timestamp
        df_filtered <- df_filtered %>% add_row(datetime = df_raw$datetime[indx],
                                               battery_percent = df_raw$battery_percent[indx])
      } else if(as.integer(df_raw$datetime[indx]) %% 60 == 0){  #timestamp is divisible by 60 
        df_filtered <- df_filtered %>% add_row(datetime = df_raw$datetime[indx],
                                               battery_percent = df_raw$battery_percent[indx])
      } else if(df_raw$datetime[indx] - df_filtered$datetime[nrow(df_filtered)] >= 60){  #last added timestamp was more than 60 seconds ago, then add it
        df_filtered <- df_filtered %>% add_row(datetime = df_raw$datetime[indx],
                                               battery_percent = df_raw$battery_percent[indx])
      }
    }
    
    remove(df_raw)
    
    # Add column to record participant ID
    df_filtered <- df_filtered %>% 
      mutate(participant_id = this_id) %>% 
      select(participant_id, everything())
    
    #deduplicate entirely duplicated rows
    df_filtered <- df_filtered[!duplicated(df_filtered),]
    
    list_df_filtered <- append(list_df_filtered, list(df_filtered))
    
    # # Add column to record participant ID
    # df_raw <- df_raw %>% 
    #   mutate(participant_id = this_id) %>% 
    #   select(participant_id, everything())
    # 
    # #deduplicate entirely duplicated rows
    # df_raw <- df_raw[!duplicated(df_raw),]
    # 
    # list_df_raw <- append(list_df_raw, list(df_raw))
    }
}
end_time <- Sys.time()
end_time - start_time    # Time difference of 30.4 mins                      #old -8.23 mins

list_battery_data_cc2 <- list_df_filtered
remove(list_df_filtered)
remove(df_filtered)


if(F){save(list_battery_data_cc2,
     file = file.path("C:/Users/u1330076/Box/Break Free curated data/EMA/Tony - demo/staged", "battery_raw_filtered_data_cc2.RData"))}


load(file = file.path("C:/Users/u1330076/Box/Break Free curated data/EMA/Tony - demo/staged", "battery_raw_filtered_data_cc2.RData"))




# -----------------------------------------------------------------------------
# Combine cc1 and cc2 data and rbind the list into one dataframe ####
# -----------------------------------------------------------------------------
list_all_battery_data <- list()
list_all_battery_data <- append(list_all_battery_data, list_battery_data_cc1)
#remove(list_battery_data_cc1)
list_all_battery_data <- append(list_all_battery_data, list_battery_data_cc2) 
#remove(list_battery_data_cc2)

all_battery_data <- do.call(rbind, list_all_battery_data)
#remove(list_all_battery_data)

# CC2 was ran for all participants with a folder, not the updated logic to only include participants from dat_master
# This semijoin removes the participants not in dat_master
all_battery_data <- all_battery_data %>% semi_join(y = dat_master, by = "participant_id")  

all_battery_data <- all_battery_data  %>% 
  mutate(datetime_hrts_UTC = as.POSIXct(datetime, tz = "UTC", origin="1970-01-01"))

all_battery_data <- all_battery_data %>%
  group_by(participant_id) %>% 
  #arrange(datetime) %>% 
  mutate(lag_diff_secs = datetime - lag(datetime, order_by = datetime)) %>% ungroup

all_battery_data %>% summarise(min_lag=min(replace_na(lag_diff_secs, 0)),
                               max_lag=max(replace_na(lag_diff_secs, 0)))

all_battery_data %>% select(lag_diff_secs) %>% summary()
# lag_diff_secs        
# Min.   :0.000e+00  # I replaced the NAs with 0
# 1st Qu.:6.000e+01  # 60 seconds
# Median :6.000e+01  # 60 seconds
# Mean   :1.906e+04  # 19060 seconds - inflated by extremely large (although infrequent) values. median is better for summary
# 3rd Qu.:6.000e+01  # 60 seconds
# Max.   :1.576e+09  
# NA's   :289 

all_battery_data %>% select(battery_percent) %>% summary #Some had battery % over 100 - need to investigate

all_battery_data %>% filter(battery_percent > 100) %>% View


names_w_badvalues <- all_battery_data %>% filter(battery_percent > 100) %>% select(participant_id) %>% unique  # 30 participants; They were all from CC2

all_battery_data %>% filter(participant_id %in% names_w_badvalues$participant_id) %>% View

# 1,919 records have no datetime and their battery percentages are usually out of the scale - will drop those here, but also can change upstream when time filtering
# Exclusively happening from CC2
all_battery_data %>% filter(datetime == 0) %>% View  

# Dropping rows with 0 (or really small value; "1.4e-4") datetime
all_battery_data <- all_battery_data %>% filter(datetime > 1)

names_w_badvalues2 <- all_battery_data %>% filter(battery_percent > 100) %>% select(participant_id) %>% unique  # reduced to 21 participants - 

all_battery_data %>% filter(battery_percent > 100) %>% nrow  # 54 observations with battery over 100% - Not possible; erroneous record

all_battery_data %>% filter(battery_percent < 0) %>% nrow  #65 observations with battery below 0% - Not possible; erroneous record

all_battery_data %>% filter(battery_percent <= 100 & battery_percent >= 0) %>% summary

all_battery_data <- all_battery_data %>% filter(battery_percent <= 100 & battery_percent >= 0)

all_battery_data <- all_battery_data %>%
  group_by(participant_id) %>% 
  #arrange(datetime) %>% 
  mutate(lag_diff_battery_percent = battery_percent - lag(battery_percent, order_by = datetime),
         lead_diff_battery_percent = lead(battery_percent, order_by = datetime) - battery_percent,
         lag_battery_percent = lag(battery_percent, order_by = datetime),
         lead_battery_percent = lead(battery_percent, order_by = datetime)) %>% ungroup

# 54 instances where the battery % decreased by more than 5% from the previous reading, then increased over 5% on the next reading, and the previous time reading was within 90 seconds
all_battery_data %>% filter(lag_diff_battery_percent < -5 & lead_diff_battery_percent >5 & lag_diff_secs < 90) %>% View() 
all_battery_data <- all_battery_data %>% filter(!(lag_diff_battery_percent < -5 & lead_diff_battery_percent >5 & lag_diff_secs < 90))
all_battery_data %>% filter(lag_diff_battery_percent < -5 & lead_diff_battery_percent >5 & lag_diff_secs < 90) %>% View()


# Run back through to recalculate the lag and lead
all_battery_data <- all_battery_data %>%
  group_by(participant_id) %>% 
  #arrange(datetime) %>% 
  mutate(lag_diff_secs = datetime - lag(datetime, order_by = datetime),
         lag_diff_battery_percent = battery_percent - lag(battery_percent, order_by = datetime),
         lead_diff_battery_percent = lead(battery_percent, order_by = datetime) - battery_percent,
         lag_battery_percent = lag(battery_percent, order_by = datetime),
         lead_battery_percent = lead(battery_percent, order_by = datetime)) %>% ungroup

# if(F){save(all_battery_data,
#            file = file.path("C:/Users/u1330076/Box/Break Free curated data/EMA/Tony - demo/staged", "all_battery_data.RData"))}
# 

filtered_battery_data <- all_battery_data %>% filter(lag_diff_battery_percent != 0) #filter for rows with change in battery percent from the previous (chronologically and grouped per participant id) observation

# if(F){save(filtered_battery_data,
#            file = file.path("C:/Users/u1330076/Box/Break Free curated data/EMA/Tony - demo/staged", "filtered_battery_data.RData"))}

if(F){save(filtered_battery_data,
           file = file.path(path_breakfree_staged_data, "filtered_battery_data.RData"))}