library(dplyr)
source("paths.R")

# Participant IDs for data collected using CC1 platform
ids_cc1 <- list.files(path = path_breakfree_cc1_input_data)

# -----------------------------------------------------------------------------
# Read raw data: smoking episodes detected by biomarker
# -----------------------------------------------------------------------------
this_string <- "PUFFMARKER_SMOKING_EPISODE+PHONE.csv.zip"

list_df_raw <- list()

for(i in 1:length(ids_cc1)){
  this_id <- ids_cc1[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_breakfree_cc1_input_data, this_id))
  # Pick out file names related to data stream of interest
  idx <- grepl(pattern = this_string, x = all_files, fixed=TRUE)
  # Pick out corresponding files
  this_file <- all_files[idx]
  len <- length(this_file)
  
  # Check whether file exists for this given participant
  if(len>0){
    # Create a new temporary folder named tmp_unzipped
    # tmp_unzipped will contain this_file
    # this_file is a zipped file which will be unzipped with unzip()
    unzip(zipfile = file.path(path_breakfree_cc1_input_data, this_id, this_file),
          exdir =  file.path(path_breakfree_staged_data, "tmp_unzipped"),
          overwrite = TRUE)
    
    # The call to unzip() will produce folder_here inside tmp_unzipped
    # folder_here is a folder which contains the csv file file.here
    # file_here is the csv file we would like to read for participant this_id
    folder_here <- list.files(path = file.path(path_breakfree_staged_data, "tmp_unzipped"))
    file_here <- list.files(path = file.path(path_breakfree_staged_data, "tmp_unzipped", folder_here))
    idx <- grep(this_string, file_here, invert = FALSE)
    
    # Finally, we have located file_here
    # Now, let's read this csv file into the df_raw variable
    df_raw <- try(read.csv(file.path(path_breakfree_staged_data, "tmp_unzipped", folder_here, file_here), 
                           header = FALSE), 
                  silent=TRUE)
    
    # A try-error occurs when file_here exists but does not have any data recorded
    if(class(df_raw) == "try-error"){
      unlink(x = file.path(path_breakfree_staged_data, "tmp_unzipped"), recursive = TRUE)
      next
    }else{
      # Add column to record participant ID
      df_raw <- df_raw %>% mutate(participant_id = this_id) %>% select(participant_id, everything())
      
      #deduplicate entirely duplicated rows
      df_raw <- df_raw[!duplicated(df_raw),]
      
      # Clean up afterwards: unlink() deletes the folder tmp_unzipped
      unlink(x = file.path(path_breakfree_staged_data, "tmp_unzipped"), recursive = TRUE)
      # Add df_raw to collection
      list_df_raw <- append(list_df_raw, list(df_raw))
    }
  }else{
    # In this case, the file  does not exist
    next
  }
}

online_puffmarker_episode_files_cc1 <- list_df_raw

# -----------------------------------------------------------------------------
# Save RData files
# -----------------------------------------------------------------------------
save(online_puffmarker_episode_files_cc1,
     file = file.path(path_breakfree_staged_data, "online_puffmarker_episode_raw_data_cc1.RData"))


