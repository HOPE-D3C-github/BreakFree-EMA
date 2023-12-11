library(dplyr)
library(tidyr)
library(testthat)

source("paths.R")

load(file = file.path(path_breakfree_staged_data, "all_ema_data_D2_per_study_design.RData"))
load(file = file.path(path_breakfree_staged_data, "all_ema_data_D3_random_only.RData"))
load(file = file.path(path_breakfree_staged_data, "codebook.RData"))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 0. Initialize baseline QC metrics to compare against later ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
pre2_dim <- dim(all_ema_data_D2_per_study_design)
pre2_colnames <- colnames(all_ema_data_D2_per_study_design)
pre2_na_bycol <- data.frame(count_na = colSums(is.na(all_ema_data_D2_per_study_design)))
pre2_na_bycol$Variable <- row.names(pre2_na_bycol)
row.names(pre2_na_bycol) <- NULL
pre2_na_bycol <- pre2_na_bycol %>% relocate(Variable, .before = everything())

pre3_dim <- dim(all_ema_data_D3_random_only)
pre3_colnames <- colnames(all_ema_data_D3_random_only)
pre3_na_bycol <- data.frame(count_na = colSums(is.na(all_ema_data_D3_random_only)))
pre3_na_bycol$Variable <- row.names(pre3_na_bycol)
row.names(pre3_na_bycol) <- NULL
pre3_na_bycol <- pre3_na_bycol %>% relocate(Variable, .before = everything())

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 1. Use Codebook values to create factors of select variables with labels ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if(F){codebook %>% count(`Variable Category`)}

cb_vars_to_convert <- codebook %>% filter(`Variable Category` %in% c('EMA Aggregated Variable', 'EMA Item')) %>% # Only these categories need conversion
  filter(!`Question Type` %in% c('number_picker', 'hour_minute', 'text_numeric')) # these items dont need conversion

cb_vars_to_convert_v2 <- cb_vars_to_convert %>% 
  mutate(response_labels = coalesce(`Value Label`, `Question Options`)) %>% 
  mutate(response_labels = case_when(
    Variables == 'ea_patch_all_records' ~ "{No},{Mixed},{Yes}",
    Variables %in% c('ei_patch', 'ei_cig_yn') ~ "{No},{Yes}",
    T ~ response_labels
  ))

# NOTE: multiple_select question options need to be treated differently
if(F){cb_vars_to_convert_v2 %>% select(Variables, `Question Type`, response_labels) %>% arrange(desc(`Question Type`)) %>% View}

# (str_test <- cb_vars_to_convert_v2$response_labels[1])
# 
# str_extract_all(str_test, "\\{.+?\\}", simplify = T)

cb_vars_to_convert_v3 <- cb_vars_to_convert_v2 %>%
  mutate(
    response_labels_formatted_4_factors <- str_extract_all(response_labels, "\\{.+?\\}"))

# fix column naming issue
colnames(cb_vars_to_convert_v3)[ncol(cb_vars_to_convert_v3)] <- "response_labels_formatted_4_factors"

df_updated_responses_cw <- data.frame(Variables = character(), question_type = character(), response_values = character())

for (i in 1:nrow(cb_vars_to_convert_v3)){
  (responses_n_pre <- cb_vars_to_convert_v3$response_labels_formatted_4_factors[i])
  # unlist(responses_n)
  # if(replace_na(cb_vars_to_convert_v3$`Question Type`[i] != "multiple_select", T)){
  (responses_n_post <- str_remove_all(unlist(responses_n_pre), "\\{|\\}"))
  # }else{
  #   (responses_n_post <- unlist(responses_n_pre))
  # }
  df_updated_responses_cw <- df_updated_responses_cw %>% 
    add_row(data.frame(Variables = cb_vars_to_convert_v3$Variables[i], 
                       question_type = cb_vars_to_convert_v3$`Question Type`[i],
                       response_values = responses_n_post))
  
  if(i == nrow(cb_vars_to_convert_v3)){
    df_updated_responses_cw <- df_updated_responses_cw %>% 
      mutate(value = row_number()-1, .by = Variables) %>% 
      mutate(response_values = str_remove(response_values, "<UNSELECT_OTHER>"))
  }
}

# Creates df with 1 row per response option per variable
head(df_updated_responses_cw)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 2. Update the EMA data from labels to values
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
all_ema_data_D2_per_study_design_integers_pre <- all_ema_data_D2_per_study_design

for (i in 1:ncol(all_ema_data_D2_per_study_design)){
  var_i <- colnames(all_ema_data_D2_per_study_design)[i]
  if(var_i %in% cb_vars_to_convert_v3$Variables){ # skip any variables that are not in cb_vars_to_convert_v3 as they do not need values converted to integers
    # Any column names that made it to this point are those which need their values labels updated to integers
    # Will need to treat the multiple select options differently
    vector_i_char <- all_ema_data_D2_per_study_design[,i]
    cw_var_i <- df_updated_responses_cw %>% filter(Variables == var_i)
    if(replace_na(cw_var_i$question_type[1] != "multiple_select", T)){ 
      # If the response options were not multiple select, then we can convert value labels to integers using a join
      df_i <- vector_i_char %>% 
        left_join(y = cw_var_i %>% select(response_values, value),
                  by = join_by(!!var_i == "response_values"))
    } 
    else{
      # When the response option is multiple select, we need to find and replace to convert the value labels to integers as there could be multiple individual responses selected by the participant
      df_i <- vector_i_char 
      df_i[,2] <- df_i[,1]
      colnames(df_i)[2] <- "temp"
      for (j in 1:length(cw_var_i$response_values)){ # for each unique response value, do a find and replace 
        response_value_j <- cw_var_i$response_values[j]
        integer_value_j <- cw_var_i$value[j]
        df_i <- df_i %>% 
          mutate( temp = str_remove_all(str_replace_all(string = temp, 
                                                        pattern = response_value_j, 
                                                        replacement = as.character(integer_value_j))
                                        , "\\{|\\}")
          )
      }
    }
    # Outside of the if/else conditioned on the Response Options
    vector_i_int <- df_i[,2]
    all_ema_data_D2_per_study_design_integers_pre[,i] <- vector_i_int # substitute the original column vector with the integer-converted one
  }
  if(i == ncol(all_ema_data_D2_per_study_design)){
    # after all else on the last column, create the updated dataset 
    all_ema_data_D2_per_study_design_integers <- all_ema_data_D2_per_study_design_integers_pre
    remove(all_ema_data_D2_per_study_design_integers_pre)
  }
}

# repeat steps but for the random only dataset (D3)
all_ema_data_D3_random_only_integers_pre <- all_ema_data_D3_random_only

for (i in 1:ncol(all_ema_data_D3_random_only)){
  var_i <- colnames(all_ema_data_D3_random_only)[i]
  if(var_i %in% cb_vars_to_convert_v3$Variables){ # skip any variables that are not in cb_vars_to_convert_v3 as they do not need values converted to integers
    # Any column names that made it to this point are those which need their values labels updated to integers
    # Will need to treat the multiple select options differently
    vector_i_char <- all_ema_data_D3_random_only[,i]
    cw_var_i <- df_updated_responses_cw %>% filter(Variables == var_i)
    if(replace_na(cw_var_i$question_type[1] != "multiple_select", T)){ 
      # If the response options were not multiple select, then we can convert value labels to integers using a join
      df_i <- vector_i_char %>% 
        left_join(y = cw_var_i %>% select(response_values, value),
                  by = join_by(!!var_i == "response_values"))
    } 
    else{
      # When the response option is multiple select, we need to find and replace to convert the value labels to integers as there could be multiple individual responses selected by the participant
      df_i <- vector_i_char 
      df_i[,2] <- df_i[,1]
      colnames(df_i)[2] <- "temp"
      for (j in 1:length(cw_var_i$response_values)){ # for each unique response value, do a find and replace 
        response_value_j <- cw_var_i$response_values[j]
        integer_value_j <- cw_var_i$value[j]
        df_i <- df_i %>% 
          mutate( temp = str_remove_all(str_replace_all(string = temp, 
                                         pattern = response_value_j, 
                                         replacement = as.character(integer_value_j))
                                         , "\\{|\\}")
                  )
      }
    }
    # Outside of the if/else conditioned on the Response Options
    vector_i_int <- df_i[,2]
    all_ema_data_D3_random_only_integers_pre[,i] <- vector_i_int # substitute the original column vector with the integer-converted one
  }
  if(i == ncol(all_ema_data_D3_random_only)){
    # after all else on the last column, create the updated dataset 
    all_ema_data_D3_random_only_integers <- all_ema_data_D3_random_only_integers_pre
    remove(all_ema_data_D3_random_only_integers_pre)
  }
}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 3. QC Check
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Create post vars
# Per Study Design (D2)
post2_dim <- dim(all_ema_data_D2_per_study_design_integers)
test2_dim <- test_that("test dimensions remained the same", {
  expect_equal(post2_dim, pre2_dim)
})


post2_colnames <- colnames(all_ema_data_D2_per_study_design_integers)
test2_colnames <- test_that("test column names remained the same", {
  expect_equal(post2_colnames, pre2_colnames)
})

post2_na_bycol <- data.frame(count_na = colSums(is.na(all_ema_data_D2_per_study_design_integers)))
post2_na_bycol$Variable <- row.names(post2_na_bycol)
row.names(post2_na_bycol) <- NULL
post2_na_bycol <- post2_na_bycol %>% relocate(Variable, .before = everything())

test2_na_bycol <- test_that("counts of na values remained the same", {
  expect_equal(post2_na_bycol, pre2_na_bycol)
})

# Random Only (D3)
post3_dim <- dim(all_ema_data_D3_random_only_integers)
test3_dim <- test_that("test dimensions remained the same", {
  expect_equal(post3_dim, pre3_dim)
})


post3_colnames <- colnames(all_ema_data_D3_random_only_integers)
test3_colnames <- test_that("test column names remained the same", {
  expect_equal(post3_colnames, pre3_colnames)
})

post3_na_bycol <- data.frame(count_na = colSums(is.na(all_ema_data_D3_random_only_integers)))
post3_na_bycol$Variable <- row.names(post3_na_bycol)
row.names(post3_na_bycol) <- NULL
post3_na_bycol <- post3_na_bycol %>% relocate(Variable, .before = everything())

test3_na_bycol <- test_that("counts of na values remained the same", {
  expect_equal(post3_na_bycol, pre3_na_bycol)
})

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 4. Output Updated Dataset and Crosswalk
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if(test2_dim & test2_colnames & test2_na_bycol & test3_dim & test3_colnames & test3_na_bycol){
  save(all_ema_data_D2_per_study_design_integers,
       file = file.path(path_breakfree_staged_data, "all_ema_data_D2_per_study_design_integers.RData"))
  
  save(all_ema_data_D3_random_only_integers,
       file = file.path(path_breakfree_staged_data, "all_ema_data_D3_random_only_integers.RData"))
  print("Integer version of datasets saved to staged folder")
} else{print("1+ test failed.")}
