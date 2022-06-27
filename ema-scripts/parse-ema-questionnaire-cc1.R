library(dplyr)
source("paths.R")

# Load previously parsed data
load(file.path(path_breakfree_staged_data, "ema_responses_raw_data_cc1.RData"))

# Participant IDs for data collected using CC1 platform
ids_cc1 <- list.files(path = path_breakfree_cc1_input_data)

# -----------------------------------------------------------------------------
# Create a file containing the actual questions utilized in each kind
# of EMA questionnaire: Random EMA
# -----------------------------------------------------------------------------

# First, check the number of columns for each participant
# Ensure that you do not select a data frame with less than 10 columns
# from which you will parse the questionnaire from
lapply(all_random_ema_response_files_cc1, ncol)

# Pick one individual
df_raw <- all_random_ema_response_files_cc1[[3]]  

list_items <- list()
all_col_names <- colnames(df_raw)
# There are 54 items in a Random EMA for CC1 participants
# The last item is a thank you message
# Thus, idx ranges from 0 to 52 (numbering begins at 0, not 1)

for(idx in 0:52){  
  these_col_names <- grepl(paste("_",idx,"_",sep=""), all_col_names)
  these_col_names <- all_col_names[these_col_names]
  df_item <- df_raw %>% select(all_of(these_col_names))  
  df_item <- df_item[1,] # Just grab the first row
  
  # Note: Column names from CC1 to CC2 changed
  # For each of the EMA items, get:
  # question number: question_answers_idx_question_id
  # question text: question_answers_idx_question_text
  # question type: question_answers_idx_question_type
  # options for possible responses: question_answers_idx_response_option_nn
  idx_id <- match(x = paste("question_answers_",idx,"_question_id", sep=""), table = these_col_names)
  idx_text <- match(x = paste("question_answers_",idx,"_question_text", sep=""), table = these_col_names)
  idx_type <- match(x = paste("question_answers_",idx,"_question_type", sep=""), table = these_col_names)
  idx_options <- grep(pattern = paste("question_answers_",idx,"_response_option_", sep=""), x = these_col_names)
  
  # Collect info into one data frame
  question_id <- df_item[these_col_names[idx_id]]
  question_text <- df_item[these_col_names[idx_text]]
  question_type <- df_item[these_col_names[idx_type]]
  question_options <- df_item[these_col_names[idx_options]]
  
  question_id <- as.numeric(as.matrix(question_id))
  question_text <- as.character(as.matrix(question_text))
  question_type <- as.character(as.matrix(question_type))
  question_options <- as.character(as.matrix(question_options))
  question_options <- paste(question_options, collapse = "},{")
  question_options <- paste("{", question_options, "}", sep="")
  
  df_collect <- data.frame(question_id = question_id,
                           question_text = question_text,
                           question_type = question_type,
                           question_options = question_options)
  
  list_items <- append(list_items, list(df_collect))
}

random_ema_items_cc1 <- do.call(rbind, list_items)

# -----------------------------------------------------------------------------
# Create a file containing the actual questions utilized in each kind
# of EMA questionnaire: Smoking EMA
# -----------------------------------------------------------------------------

# First, check the number of columns for each participant
# Ensure that you do not select a data frame with less than 10 columns
# from which you will parse the questionnaire from
lapply(all_smoking_ema_response_files_cc1, ncol)

# Pick one individual
df_raw <- all_smoking_ema_response_files_cc1[[3]]  # Pick one individual

list_items <- list()
all_col_names <- colnames(df_raw)
# There are 54 items in a Random EMA for CC1 participants
# The last item is a thank you message
# Thus, idx ranges from 0 to 52 (numbering begins at 0, not 1)

for(idx in 0:52){  
  these_col_names <- grepl(paste("_",idx,"_",sep=""), all_col_names)
  these_col_names <- all_col_names[these_col_names]
  df_item <- df_raw %>% select(all_of(these_col_names))  
  df_item <- df_item[1,] # Just grab the first row
  
  # Note: Column names from CC1 to CC2 changed
  # For each of the EMA items, get:
  # question number: question_answers_idx_question_id
  # question text: question_answers_idx_question_text
  # question type: question_answers_idx_question_type
  # options for possible responses: question_answers_idx_response_option_nn
  idx_id <- match(x = paste("question_answers_",idx,"_question_id", sep=""), table = these_col_names)
  idx_text <- match(x = paste("question_answers_",idx,"_question_text", sep=""), table = these_col_names)
  idx_type <- match(x = paste("question_answers_",idx,"_question_type", sep=""), table = these_col_names)
  idx_options <- grep(pattern = paste("question_answers_",idx,"_response_option_", sep=""), x = these_col_names)
  
  # Collect info into one data frame
  question_id <- df_item[these_col_names[idx_id]]
  question_text <- df_item[these_col_names[idx_text]]
  question_type <- df_item[these_col_names[idx_type]]
  question_options <- df_item[these_col_names[idx_options]]
  
  question_id <- as.numeric(as.matrix(question_id))
  question_text <- as.character(as.matrix(question_text))
  question_type <- as.character(as.matrix(question_type))
  question_options <- as.character(as.matrix(question_options))
  question_options <- paste(question_options, collapse = "},{")
  question_options <- paste("{", question_options, "}", sep="")
  
  df_collect <- data.frame(question_id = question_id,
                           question_text = question_text,
                           question_type = question_type,
                           question_options = question_options)
  
  list_items <- append(list_items, list(df_collect))
}

smoking_ema_items_cc1 <- do.call(rbind, list_items)

# -----------------------------------------------------------------------------
# Create a file containing the actual questions utilized in each kind
# of EMA questionnaire: Stress EMA
# -----------------------------------------------------------------------------

# First, check the number of columns for each participant
# Ensure that you do not select a data frame with less than 10 columns
# from which you will parse the questionnaire from
lapply(all_stress_ema_response_files_cc1, ncol)

# Pick one individual
df_raw <- all_stress_ema_response_files_cc1[[3]]  # Pick one individual

list_items <- list()
all_col_names <- colnames(df_raw)
# There are 54 items in a Random EMA for CC1 participants
# The last item is a thank you message
# Thus, idx ranges from 0 to 52 (numbering begins at 0, not 1)

for(idx in 0:52){  
  these_col_names <- grepl(paste("_",idx,"_",sep=""), all_col_names)
  these_col_names <- all_col_names[these_col_names]
  df_item <- df_raw %>% select(all_of(these_col_names))  
  df_item <- df_item[1,] # Just grab the first row
  
  # Note: Column names from CC1 to CC2 changed
  # For each of the EMA items, get:
  # question number: question_answers_idx_question_id
  # question text: question_answers_idx_question_text
  # question type: question_answers_idx_question_type
  # options for possible responses: question_answers_idx_response_option_nn
  idx_id <- match(x = paste("question_answers_",idx,"_question_id", sep=""), table = these_col_names)
  idx_text <- match(x = paste("question_answers_",idx,"_question_text", sep=""), table = these_col_names)
  idx_type <- match(x = paste("question_answers_",idx,"_question_type", sep=""), table = these_col_names)
  idx_options <- grep(pattern = paste("question_answers_",idx,"_response_option_", sep=""), x = these_col_names)
  
  # Collect info into one data frame
  question_id <- df_item[these_col_names[idx_id]]
  question_text <- df_item[these_col_names[idx_text]]
  question_type <- df_item[these_col_names[idx_type]]
  question_options <- df_item[these_col_names[idx_options]]
  
  question_id <- as.numeric(as.matrix(question_id))
  question_text <- as.character(as.matrix(question_text))
  question_type <- as.character(as.matrix(question_type))
  question_options <- as.character(as.matrix(question_options))
  question_options <- paste(question_options, collapse = "},{")
  question_options <- paste("{", question_options, "}", sep="")
  
  df_collect <- data.frame(question_id = question_id,
                           question_text = question_text,
                           question_type = question_type,
                           question_options = question_options)
  
  list_items <- append(list_items, list(df_collect))
}

stress_ema_items_cc1 <- do.call(rbind, list_items)

# -----------------------------------------------------------------------------
# Save to RData file
# -----------------------------------------------------------------------------
save(random_ema_items_cc1, 
     smoking_ema_items_cc1, 
     stress_ema_items_cc1,
     file = file.path(path_breakfree_staged_data, "ema_questionnaires_cc1.RData"))


