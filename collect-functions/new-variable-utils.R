# new-variable-utils.R -- "New Variable Creation Utility Functions in R"

CheckAnyResponse <- function(df, keep_cols){
  # About: Check whether there is a recorded response to each row of the 
  #   data frame df
  # Args:
  #   df: data frame containing raw data
  #   keep_cols: names of columns to keep from df when checking 
  #     whether any response was recorded in each row
  # Output:
  #   df with a new columns with_any_response: this variable is equal to 1
  #   if there is a record of at least one question completed and 0 if there
  #   is no record of any question completed
  
  # ---------------------------------------------------------------------------
  # Begin tasks
  # ---------------------------------------------------------------------------
  df_items <- df %>% select(all_of(keep_cols))
  
  if(ncol(df_items)>0){
    df_items <- apply(df_items, 2, function(x){
      x <- as.character(x)
      x <- ifelse(is.na(x), NA_character_,
                  ifelse(x=="", NA_character_, x))
      return(x)
    })
    
    # Note that nrow(df_items)==NULL if df.items contains only 1 row
    if(is.null(nrow(df_items))){
      df_items <- as.matrix(df_items)
      df_items <- t(df_items)
    }
    
    count_num_response <- df_items %>% is.na(.) %>% magrittr::not(.) %>% rowSums(.)
    with_any_response <- if_else(count_num_response > 0, 1, 0)
    
  }else{
    with_any_response <- rep(0, times = nrow(df)) 
  }
  df <- df %>% mutate(with_any_response = with_any_response)
  
  return(df)
}


