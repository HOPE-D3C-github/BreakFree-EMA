# List to collect item responses
list_resp_cc1 <- list()
N <- length(all_response_files_cc1)

for(i in 1:N){
  df_raw <- all_response_files_cc1[[i]]
  
  all_col_names <- colnames(df_raw)
  check <- (("question_answers_0_response_0") %in% all_col_names)
  
  if(isTRUE(check)){
    list_items <- list()
    # There are 54 items in a Random EMA for CC1 participants
    # The last item is a thank you message
    # Item numbering begins at 0: 0, 1, ..., 53
    # Thus, idx ranges from 0 to 52
    for(idx in collect_idx){  
      these_col_names <- grepl(paste("_",idx,"_",sep=""), all_col_names)
      these_col_names <- all_col_names[these_col_names]
      df_item <- df_raw %>% select(all_of(these_col_names)) 
      
      # This step allows us to determine the total number of options
      # available to the participant to select
      idx_options <- grep(pattern = paste("question_answers_",idx,"_response_option_", sep=""), x = these_col_names)
      # number_options is a count of the total number of options available to the participant to select
      number_options <- length(these_col_names[idx_options])
      all_possible_columns <- paste("question_answers_",idx,"_response_",0:(number_options-1), sep="")
      in_data <- all_possible_columns[all_possible_columns %in% these_col_names]
      
      if(length(in_data)>0){
        list_collect_chosen_options <- list()
        
        for(j in 1:length(in_data)){
          idx_resp <- which(these_col_names==in_data[j])
          question_resp <- df_item[,idx_resp]
          question_resp <- as.character(question_resp)
          question_resp <- replace(question_resp, question_resp=="", NA_character_)
          list_collect_chosen_options <- append(list_collect_chosen_options, list(question_resp))
        }
        
        collect_chosen_options <- do.call(cbind, list_collect_chosen_options)
        collect_chosen_options <- as.matrix(collect_chosen_options)
        chosen_options <- apply(collect_chosen_options, 1, function(this_row){
          
          picked <- this_row[!is.na(this_row)]
          
          if(length(picked)==0){
            combined <- NA_character_
          }else if(length(picked)==1){
            combined <- paste("{",picked,"}",sep="")
          }else{
            combined <- paste("{",picked[1],"}",sep="")
            for(k in 2:length(picked)){
              combined <- paste(combined,paste("{",picked[k],"}",sep=""), sep=",")
            }
          }
          
          return(combined)
        })
        
        question_resp <- as.matrix(chosen_options)
        colnames(question_resp) <- paste("item_",idx, sep="")
        list_items <- append(list_items,list(question_resp))
      }else{
        question_resp <- rep(NA_character_, times = nrow(df_item))
        question_resp <- as.matrix(question_resp)
        colnames(question_resp) <- paste("item_",idx, sep="")
        list_items <- append(list_items,list(question_resp))
      }
    }
    
    df_resp <- do.call(cbind, list_items)
    df_resp <- as.data.frame(df_resp)
    df <- df_raw %>% select(participant_id, merge_id)
    df <- cbind(df, df_resp)
    list_resp_cc1 <- append(list_resp_cc1, list(df))
  }else{
    df_resp <- rep(NA, nrow(df_raw)*length(collect_idx))
    df_resp <- matrix(df_resp, ncol = length(collect_idx))
    df_resp <- as.data.frame(df_resp)
    colnames(df_resp) <- paste("item_",collect_idx, sep="")
    df <- df_raw %>% select(participant_id, merge_id)
    df <- cbind(df, df_resp) 
    list_resp_cc1 <- append(list_resp_cc1, list(df))
  }
}

df_resp_cc1 <- do.call(rbind, list_resp_cc1)

