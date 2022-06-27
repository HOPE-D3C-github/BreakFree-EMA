# List to collect item responses
list_resp_cc2 <- list()
N <- length(all_response_files_cc2)

for(i in 1:N){
  df_raw <- all_response_files_cc2[[i]]
  
  all_col_names <- colnames(df_raw)
  check <- (("questions_0_response_0") %in% all_col_names)
  
  if(isTRUE(check)){
    list_items <- list()
    # There are 64 items in a Random EMA for CC2 participants
    # The last item is a thank you message
    # Thus, idx ranges from 1 to 63
    for(idx in collect_idx){  
      these_col_names <- grepl(paste("_",idx,"_",sep=""), all_col_names)
      these_col_names <- all_col_names[these_col_names]
      df_item <- df_raw %>% select(all_of(these_col_names)) 
      
      # Note: Column names from CC1 to CC2 changed
      # For each of the EMA items, get:
      # question response: question_idx_response_0
      idx_resp <- match(x = paste("questions_",idx,"_response_0", sep=""), table = these_col_names)
      
      if(!is.na(idx_resp)){
        # Collect info altogether
        question_resp <- df_item[,idx_resp]
        question_resp <- as.character(question_resp)
        question_resp <- replace(question_resp, question_resp=="", NA_character_)
        question_resp <- as.matrix(question_resp)
        colnames(question_resp) <- paste("item_",idx, sep="")
        list_items <- append(list_items,list(question_resp))
      }else{
        question_resp <- rep(NA_character_, times = nrow(df_item))
        question_resp <- as.matrix(question_resp)
        colnames(question_resp) <- paste("item_",idx, sep="")
        list_items <- append(list_items,list(question_resp))
      }
    }  # End looping through collect_idx
    
    df_resp <- do.call(cbind, list_items)
    df_resp <- as.data.frame(df_resp)
    df <- df_raw %>% select(participant_id, merge_id)
    df <- cbind(df, df_resp)
    list_resp_cc2 <- append(list_resp_cc2, list(df))
  }else{
    df_resp <- rep(NA, nrow(df_raw)*length(collect_idx))
    df_resp <- matrix(df_resp, ncol = length(collect_idx))
    df_resp <- as.data.frame(df_resp)
    colnames(df_resp) <- paste("item_",collect_idx, sep="")
    df <- df_raw %>% select(participant_id, merge_id)
    df <- cbind(df, df_resp) 
    list_resp_cc2 <- append(list_resp_cc2, list(df))
  }
}

df_resp_cc2 <- do.call(rbind, list_resp_cc2)

