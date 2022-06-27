library(dplyr)
source("paths.R")

# Load previously parsed data
load(file.path(path_breakfree_staged_data, "ema_raw_data_cc2.RData"))

# Participant IDs for data collected using CC2 platform
ids_cc2 <- list.files(path = path_breakfree_cc2_input_data)
ids_cc2 <- ids_cc2[grepl("aa_", ids_cc2)]
ids_cc2 <- ids_cc2[!grepl("test", ids_cc2)]

# -----------------------------------------------------------------------------
# Create a file containing the actual questions utilized in each kind
# of EMA questionnaire: Random EMA
# -----------------------------------------------------------------------------

# Pick one individual
df_raw <- all_random_ema_response_files_cc2[[15]]  

list_items <- list()
all_col_names <- colnames(df_raw)
# There are 65 items in a Random EMA for CC2 participants
# The last item is a thank you message
# Thus, idx ranges from 0 to 63 (numbering begins at 0, not 1)

for(idx in 0:63){  
  these_col_names <- grepl(paste("_",idx,"_",sep=""), all_col_names)
  these_col_names <- all_col_names[these_col_names]
  df_item <- df_raw %>% select(all_of(these_col_names))  
  df_item <- df_item[1,] # Just grab the first row
  
  # Note: Column names from CC1 to CC2 changed
  # For each of the EMA items, get:
  # question number: questions_idx_question_id
  # question text: questions_idx_question_text
  # question type: questions_idx_question_type
  # options for possible responses: questions_idx_response_option_nn
  idx_id <- match(x = paste("questions_",idx,"_question_id", sep=""), table = these_col_names)
  idx_text <- match(x = paste("questions_",idx,"_question_text", sep=""), table = these_col_names)
  idx_type <- match(x = paste("questions_",idx,"_question_type", sep=""), table = these_col_names)
  idx_options <- grep(pattern = paste("questions_",idx,"_response_option_", sep=""), x = these_col_names)
  
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

random_ema_items_cc2 <- do.call(rbind, list_items)

# -----------------------------------------------------------------------------
# Create a file containing the actual questions utilized in each kind
# of EMA questionnaire: Smoking EMA
# -----------------------------------------------------------------------------

# Pick one individual
df_raw <- all_smoking_ema_response_files_cc2[[15]]  

list_items <- list()
all_col_names <- colnames(df_raw)
# There are 65 items in a Random EMA for CC2 participants
# The last item is a thank you message
# Thus, idx ranges from 0 to 63 (numbering begins at 0, not 1)

for(idx in 0:63){  
  these_col_names <- grepl(paste("_",idx,"_",sep=""), all_col_names)
  these_col_names <- all_col_names[these_col_names]
  df_item <- df_raw %>% select(all_of(these_col_names))  
  df_item <- df_item[1,] # Just grab the first row
  
  # Note: Column names from CC1 to CC2 changed
  # For each of the EMA items, get:
  # question number: questions_idx_question_id
  # question text: questions_idx_question_text
  # question type: questions_idx_question_type
  # options for possible responses: questions_idx_response_option_nn
  idx_id <- match(x = paste("questions_",idx,"_question_id", sep=""), table = these_col_names)
  idx_text <- match(x = paste("questions_",idx,"_question_text", sep=""), table = these_col_names)
  idx_type <- match(x = paste("questions_",idx,"_question_type", sep=""), table = these_col_names)
  idx_options <- grep(pattern = paste("questions_",idx,"_response_option_", sep=""), x = these_col_names)
  
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

smoking_ema_items_cc2 <- do.call(rbind, list_items)

# -----------------------------------------------------------------------------
# Create a file containing the actual questions utilized in each kind
# of EMA questionnaire: Stress EMA
# -----------------------------------------------------------------------------

# Pick one individual
df_raw <- all_stress_ema_response_files_cc2[[15]]  

list_items <- list()
all_col_names <- colnames(df_raw)
# There are 65 items in a Random EMA for CC2 participants
# The last item is a thank you message
# Thus, idx ranges from 0 to 63 (numbering begins at 0, not 1)

for(idx in 0:63){  
  these_col_names <- grepl(paste("_",idx,"_",sep=""), all_col_names)
  these_col_names <- all_col_names[these_col_names]
  df_item <- df_raw %>% select(all_of(these_col_names))  
  df_item <- df_item[1,] # Just grab the first row
  
  # Note: Column names from CC1 to CC2 changed
  # For each of the EMA items, get:
  # question number: questions_idx_question_id
  # question text: questions_idx_question_text
  # question type: questions_idx_question_type
  # options for possible responses: questions_idx_response_option_nn
  idx_id <- match(x = paste("questions_",idx,"_question_id", sep=""), table = these_col_names)
  idx_text <- match(x = paste("questions_",idx,"_question_text", sep=""), table = these_col_names)
  idx_type <- match(x = paste("questions_",idx,"_question_type", sep=""), table = these_col_names)
  idx_options <- grep(pattern = paste("questions_",idx,"_response_option_", sep=""), x = these_col_names)
  
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

stress_ema_items_cc2 <- do.call(rbind, list_items)

# -----------------------------------------------------------------------------
# Save to RData file
# -----------------------------------------------------------------------------
save(random_ema_items_cc2, 
     smoking_ema_items_cc2, 
     stress_ema_items_cc2,
     file = file.path(path_breakfree_staged_data, "ema_questionnaires_cc2.RData"))


