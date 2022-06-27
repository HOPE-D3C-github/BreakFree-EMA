# io-utils.R -- "Input-Output Utility Functions in R"

CountFile <- function(participant_id, file_name, directory){
  
  # Args:
  # participant_id (character string) - participant's ID
  # file_name (character string) - file name
  # directory (character string) - directory/folder where file_name is located
  
  # List all file names within folder
  all_files <- list.files(file.path(directory, participant_id))
  # Pick out file names related to data stream of interest
  idx <- grep(pattern = file_name, x = all_files, fixed = TRUE)
  
  # Count number of times this_file exists
  # This check allows us to see whether any duplicate copies might exist
  # Note that grep checks for files having "file_name" as a substring
  # in their file name. Hence, one should prefer specifying the longest 
  # possible substring in "file_name".
  # For example, if one specifies "my_file.csv" in "file_name"
  # and two files named "my_file_01.csv" and "my_file_02.csv"
  # both exist, then the function CountFile() will return a value of 2
  # Another example would be when one specifies "my_file.csv" in "file_name"
  # and two files named "my_file_01.csv" and "my_file_02.csv.bz2"
  # both exist (i.e., a csv file and its zipped version both exist), 
  # then the function CountFile() will return a value of 2
  count <- sum(!is.na(idx))
  
  return(count)
}




