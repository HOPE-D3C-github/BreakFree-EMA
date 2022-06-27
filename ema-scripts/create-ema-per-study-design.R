library(dplyr)
library(tibble)
library(lubridate)
library(tidyr)
library(stringr)
library(testthat)
library(haven)

source("paths.R",echo = F)

aggregation_rules <- read.csv(file.path(path_breakfree_other_input_data, "EMA_aggregations_4 read in.csv")) %>% 
  select(Variables, AGGREGATION.METHOD.for.R.code, Question.Options) %>% rename(agg_rule = AGGREGATION.METHOD.for.R.code)

load(file = file.path(path_breakfree_staged_data, "all_ema_data_D1_all_delivered.RData"))

all_ema_data <- all_ema_data_D1_all_delivered

remove(conditions_applied_simple, unedited_and_clean_ema_vars_dat, all_ema_data_D1_all_delivered)

rule_types <- aggregation_rules %>% filter(agg_rule != "New Variable") %>% select(agg_rule) %>% unique() %>% arrange(agg_rule) %>% .$agg_rule
rule_variable_names <- aggregation_rules %>% filter(agg_rule != "New Variable") %>% select(Variables) %>% .$Variables


all_ema_data <- all_ema_data %>% 
  add_column(aggreg_num_extra_ema = 0,
             aggreg_num_invalid_end_day_ema = 0,
             patch_all_records = .$patch,
             anhedonia_agg_min = .$anhedonia,
             anhedonia_agg_max = .$anhedonia) %>% 
  relocate(patch_all_records, .after = patch) %>% 
  relocate(anhedonia_agg_min, anhedonia_agg_max, .after = anhedonia)
  

# LOOP STARTS HERE ######

ema_data_per_study_design <- all_ema_data[0,]  #Create shell that we will add modified ema data

for (participant in unique(all_ema_data$participant_id)){  #iterate over participant id's
  print(participant)  # progress bar analogy
  parts_all_ema_data <- all_ema_data %>% filter(participant_id == participant) %>% arrange(end_unixts)  # Dataframe of all ema data for the single participant
  temp_drop_ema <- parts_all_ema_data[0,]   # Create placeholder for extra EMAs 
  for (i in 1:nrow(parts_all_ema_data)){  #iterate over the records for the single participant
    ema_i <- parts_all_ema_data[i,]
    if (ema_i$extra_ema | ema_i$invalid_end_day){    # Extra EMA's and invalid end day come through
      if (ema_i$with_any_response == 1){   # Filter out EMA's without any completed fields
        temp_drop_ema <- temp_drop_ema %>% add_row(ema_i)
      }
    } else {   # Non-Extra EMA's come through to this else
      if (ema_i$with_any_response == 0){  
        # Non-Extra EMAs with all data missing have their record added but no Extra EMA will be aggregated to this record. 
        # It goes to the next random ema with some/all data
        ema_data_per_study_design <- ema_data_per_study_design %>% add_row(ema_i) 
        next
      }
      if (nrow(temp_drop_ema)>0){   #if TRUE, there is Extra EMA data (in temp_drop_ema) to include in the current Non-Extra EMA record (ema_i)
        updated_ema_i <- ema_i   # create the updated record as a copy of the current Non-Extra EMA - then modify it for applicable variables
        updated_ema_i$aggreg_num_extra_ema <- sum(temp_drop_ema$extra_ema)
        updated_ema_i$aggreg_num_invalid_end_day_ema <- sum(temp_drop_ema$invalid_end_day)
        for (variable in colnames(ema_i)){
          var_values <- temp_drop_ema[variable] %>% add_row(ema_i[variable])  # placeholder for all values between the Non-Extra EMA and the Extra EMA(s) being aggregated
          if (all(is.na(var_values))){  # All values are NA - leave the Non-Extra EMA's NA in place, proceed to next variable
            next
          }
          var_values <- var_values %>% drop_na()   # Remove NA here so remaining logic is pre-filtered from NAs
          if (!(variable %in% rule_variable_names)){   # If the variable is NOT in the variable_rules list, then it is not a response to an EMA question. Preserve Non-Extra EMA value
            next
          } 
          if (length(aggregation_rules$agg_rule[aggregation_rules$Variables == variable]) == 0){   # new variables get filtered here - no action needed for those variable as they are updated elsewhere
            next
          } else if (aggregation_rules$agg_rule[aggregation_rules$Variables == variable] == "AGGREGATE ALL"){     # Aggregate all logic - combine all selected choices
            var_values_unique <- unique(unlist(str_split(unlist(var_values), pattern = ",")))  # Split compound originals, combine them into one list, then take only unique values
            unselected_list <- c("{I did not use other tobacco products}", "{Did not drink any alcohol}", "{NONE}")  # Add all *UNSELECTED* options to this list
            agg_text <-  NULL
            if (length(var_values_unique) > 1){
              for (value in var_values_unique){
                if (!(value %in% unselected_list)){   # If there are multiple unique options in agg_list, then don't keep the *UNSELECTED* option because that would contradict
                  if (length(agg_text) == 0){
                    agg_text <- value
                  } else {
                    agg_text <- paste(agg_text, value, sep = ",")
                  }
                }
              }
            } else {
              agg_text <- var_values_unique
            }
            updated_ema_i[variable] <- agg_text
          } else if (aggregation_rules$agg_rule[aggregation_rules$Variables == variable] == "MAX AFFIRMATIVE - LINKED"){
            if (variable == "discrim_unf"){
              linked_var_values <- temp_drop_ema %>% add_row(ema_i) %>%   # Create tibble of values for both linked variables
                select(discrim_unf, discrim_rsn) %>% filter(!is.na(discrim_unf))
              no_yesabsolutelysure_scale <- c("No","Yes Somewhat Sure","Yes Mostly Sure","Yes Absolutely Sure") # Scale used for the discrim_unf variable
              discrim_unf_values_fctr <- factor(linked_var_values$discrim_unf, levels = no_yesabsolutelysure_scale, ordered = TRUE) # Convert the discrim_unf values to factors
              max_affirm_discrim_unf_value <- as.character(max(discrim_unf_values_fctr))   # Retrieve the max (most affirmative) value
              # Retrieve the index (row) for the max affirmative value
              # If max affirmative value appears in more than one row, then take the most recent
              max_affirm_discrim_unf_index <- max(which(linked_var_values$discrim_unf == max_affirm_discrim_unf_value))  
              linked_discrim_rsn <- linked_var_values$discrim_rsn[max_affirm_discrim_unf_index]  # Retrieve the linked _rsn from the row with the max affirm _unf value
              
              updated_ema_i$discrim_unf <- max_affirm_discrim_unf_value 
              updated_ema_i$discrim_rsn <- linked_discrim_rsn
            }
          } else if (aggregation_rules$agg_rule[aggregation_rules$Variables == variable] == "MAX AFFIRMATIVE (Y/N)"){
            # if (all(is.na(ema_i[variable]), is.na(temp_drop_ema[variable]))){
            #   next
            # }
            updated_ema_i[variable] <- case_when(
              any(var_values == "Yes") ~ "Yes",
              T ~ "No"
            )
          } else if (aggregation_rules$agg_rule[aggregation_rules$Variables == variable] == "MAX AFFIRMATIVE"){  
            # Multiple different scales: 
            # 1. {Strongly Disagree},{Disagree},{Neutral},{Agree},{Strongly Agree}
            # 2. {Definitely No},{Mostly No},{Mostly Yes},{Definitely Yes}
            if (aggregation_rules$Question.Options[aggregation_rules$Variables == variable] == "{Definitely No},{Mostly No},{Mostly Yes},{Definitely Yes}"){
              defno_defyes_scale <- c("Definitely No","Mostly No","Mostly Yes","Definitely Yes")
              var_values_fctr <- factor(var_values[[variable]], levels = defno_defyes_scale, ordered = TRUE)
            } else if (aggregation_rules$Question.Options[aggregation_rules$Variables == variable] == "{Strongly Disagree},{Disagree},{Neutral},{Agree},{Strongly Agree}"){
              stronglydisagree_stronglyagree_scale <- c("Strongly Disagree","Disagree","Neutral","Agree","Strongly Agree")
              var_values_fctr <- factor(var_values[[variable]], levels = stronglydisagree_stronglyagree_scale, ordered = TRUE)
            }
            max_affirm_value <- as.character(max(var_values_fctr))
            updated_ema_i[variable] <- max_affirm_value
          } else if (aggregation_rules$agg_rule[aggregation_rules$Variables == variable] == "MAX VALUE - LOGICAL"){
            updated_ema_i[variable] <- any(var_values)
          } else if (aggregation_rules$agg_rule[aggregation_rules$Variables == variable] == "RECALCULATE"){
            # Recalculate values from other columns to get:
            # midpoint, and time for *_ago, *_first, *_recent
            # _ago is for one, otherwise first and recent are used
            # cig_* uses time durations; the other methods use hour_minute
            if (variable == "cig_ago"){
              # cig_ago may updated cig_recent & cig_first, so this can be done in one step, at variable == cig_ago, 
              # then it will pass at cig_recent & cig_first because it will already be updated
              # Will also update cig_ago_mid, cig_recent_mid, and cig_first_mid
              cig_time_vars <- temp_drop_ema %>% add_row(ema_i) %>% 
                select("begin_unixts", "end_unixts", "begin_hrts_UTC", "end_hrts_UTC", "begin_hrts_AmericaChicago", "end_hrts_AmericaChicago", 
                       "cig_yn", "cig_n_v1", "cig_n_v2", "cig_ago", "cig_recent", "cig_first")
              mapping_df <- data.frame(intervals = c("0 - 2 hrs","2 hrs - 4 hrs","4 hrs - 6 hrs","6 hrs - 8 hrs","8 hrs - 10 hrs","10 hrs - 12 hrs","More than 12 hrs"),
                                       midpoint = c(1, 3, 5, 7, 9, 11, 13))
              # Cases to consider: 1 cig_ago, 1+ cig_recent/first. cig_ago -> NA, update recent/first
              # A. 1 cig_ago, 0 cig_recent/first. cig_ago carry through the one record & update for time as needed, recent/first unchanged
              # B. 2+ cig_ago, 0 cig_recent/first. cig_ago -> NA, update recent/first
              # C. 1+ cig_ago, 1+ cig_recent/first. cig_ago -> NA, update recent/first
              # D. 0 cig_ago, 2+ cig_recent/first. cig_ago unchanged (NA), update recent/first
              if (sum(!is.na(cig_time_vars$cig_ago)) == 0 & sum(!is.na(cig_time_vars$cig_recent)) == 0){
                next # all values for cig_ago and cig_recent/first are NA - confirmed that all NA for cig_first correspond to NA for cig_recent
              } else if (sum(!is.na(cig_time_vars$cig_ago)) == 1 & all(is.na(cig_time_vars$cig_recent))){  
                # Exactly one record with cig_ago, and exactly 0 records with cig_recent. There was exactly one cig between aggregated random ema and last random ema  
                if (!is.na(cig_time_vars$cig_ago[nrow(cig_time_vars)])){ # the one record of cig_ago was from the random ema
                  updated_ema_i$cig_ago <- ema_i$cig_ago # don't need to add time because it was the record from the random ema
                } else { # the one record of cig_ago was not from the random ema. need to include the time between emas for the time interval
                  row_cig_ago_index <- which(!is.na(cig_time_vars$cig_ago))
                  row_cig_ago_value <- cig_time_vars$cig_ago[row_cig_ago_index]
                  # Retrieve the midpoint value from the interval
                  row_cig_ago_value_midpt <- mapping_df$midpoint[which(mapping_df$intervals == row_cig_ago_value)]  # Retrieve the midpoint value from the interval
                  # Calculate the time between the ema with the observed cig_ago, and the random ema
                  delta_time <- time_length(cig_time_vars$end_hrts_AmericaChicago[nrow(cig_time_vars)] - cig_time_vars$end_hrts_AmericaChicago[row_cig_ago_index], unit = "hour") 
                  # Add the midpoint (hours) to the time difference (hours)
                  updated_cig_ago_num <- row_cig_ago_value_midpt + delta_time
                  # Transform the numeric value back into a time range. 
                  # Use the interval corresponding to the midpoint with the smallest distance (absolute difference) to the updated numeric value
                  updated_cig_ago_int <- mapping_df$intervals[which.min(abs(mapping_df$midpoint - updated_cig_ago_num))]
                  updated_ema_i$cig_ago <- updated_cig_ago_int
                }
              } else { # End of Situation A (described above). All others will need to update cig_recent/first and cig_ago to NA (except for situation D, where it is already NA)
                # to make it to this else, there are more than 1 cig_ago or 1+ cig_recent/first.
                # All cig_ago will be NA after updating these - cig_recent/first to be updated as necessary
                
                # First, grab any cig_ago values. Copy the value into the row's cig_recent & cig_first vars for the calculation in the next portion
                for (indx in 1:nrow(cig_time_vars)){
                  if (!is.na(cig_time_vars$cig_ago[indx])){
                    cig_time_vars$cig_recent[indx] <- cig_time_vars$cig_ago[indx]
                    cig_time_vars$cig_first[indx] <- cig_time_vars$cig_ago[indx]
                  }
                } 
                updated_ema_i$cig_ago <- NA_character_  # cig_ago is NA, because recent and first will have values
                
                row_cig_first_index <- min(which(!is.na(cig_time_vars$cig_first)))  # index for the earliest (temporally) record of cig_first 
                row_cig_recent_index <- max(which(!is.na(cig_time_vars$cig_recent)))  # index for the latest (temporally) record of cig_recent
                
                row_cig_first_value <- cig_time_vars$cig_first[row_cig_first_index]
                row_cig_recent_value <- cig_time_vars$cig_recent[row_cig_recent_index]
                
                if (row_cig_first_index == nrow(cig_time_vars)){  # the earliest cig_first record is from the random ema - does not require updating
                  updated_ema_i$cig_first <- row_cig_first_value
                } else {  # the earliest cig_first record is not from the random ema - requires updating
                  # Retrieve the midpoint value from the interval
                  row_cig_first_value_midpt <- mapping_df$midpoint[which(mapping_df$intervals == row_cig_first_value)]  # Retrieve the midpoint value from the interval
                  # Calculate the time between the ema with the earliest cig_first, and the random ema
                  delta_time <- time_length(cig_time_vars$end_hrts_AmericaChicago[nrow(cig_time_vars)] - cig_time_vars$end_hrts_AmericaChicago[row_cig_first_index], unit = "hour") 
                  # Add the midpoint (hours) to the time difference (hours)
                  updated_cig_first_num <- row_cig_first_value_midpt + delta_time
                  # Transform the numeric value back into a time range. 
                  # Use the interval corresponding to the midpoint with the smallest distance (absolute difference) to the updated numeric value
                  updated_cig_first_int <- mapping_df$intervals[which.min(abs(mapping_df$midpoint - updated_cig_first_num))]
                  updated_ema_i$cig_first <- updated_cig_first_int
                } # Finished updates for cig_first
                # Start updating cig_recent (if applicable)
                if (row_cig_recent_index == nrow(cig_time_vars)){  # the latest cig_recent record is from the random ema - does not require updating
                  updated_ema_i$cig_recent <- row_cig_recent_value
                } else {  # the latest cig_recent record is not from the random ema - requires updating
                  # Retrieve the midpoint value from the interval
                  row_cig_recent_value_midpt <- mapping_df$midpoint[which(mapping_df$intervals == row_cig_recent_value)]  # Retrieve the midpoint value from the interval
                  # Calculate the time between the ema with the earliest cig_first, and the random ema
                  delta_time <- time_length(cig_time_vars$end_hrts_AmericaChicago[nrow(cig_time_vars)] - cig_time_vars$end_hrts_AmericaChicago[row_cig_recent_index], unit = "hour") 
                  # Add the midpoint (hours) to the time difference (hours)
                  updated_cig_recent_num <- row_cig_recent_value_midpt + delta_time
                  # Transform the numeric value back into a time range. 
                  # Use the interval corresponding to the midpoint with the smallest distance (absolute difference) to the updated numeric value
                  updated_cig_recent_int <- mapping_df$intervals[which.min(abs(mapping_df$midpoint - updated_cig_recent_num))]
                  updated_ema_i$cig_recent <- updated_cig_recent_int
                } # Finished updates for cig_recent
              } # Finished updates for cig_ago, cig_first, and cig_recent
              # Start updates on cig_ago_mid, cig_first_mid, and cig_recent_mid
              updated_ema_i <- updated_ema_i %>% mutate(
                cig_ago_mid = cig_ago,
                cig_first_mid = cig_first,
                cig_recent_mid = cig_recent
                ) %>% 
                mutate(across(c(cig_ago_mid,cig_first_mid,cig_recent_mid),
                                  ~recode(.,
                                          "0 - 2 hrs"=1,
                                          "2 hrs - 4 hrs"=3,
                                          "4 hrs - 6 hrs"=5,
                                          "6 hrs - 8 hrs"=7,
                                          "8 hrs - 10 hrs"=9,
                                          "10 hrs - 12 hrs"=11,
                                          "More than 12 hrs"=13)))
              # Finished updates for variable == "cig_ago"
            } else if (variable %in% c("othertob_cgr_ago", "other_ecig_recent", "othertob_marij_recent")){
              # variables "other_cgr_*", "other_ecig_*", and "othertob_marij_*" are listed as minutes, so no processing intervals as seen with "cig_*" variables
              # Can update all *_ago, *_recent, *_first variables with the same prefix in one pass through, 
              # so only running below when the variables are "othertob_cgr_ago", "other_ecig_recent", "othertob_marij_recent"
              # 
              if (variable == "othertob_cgr_ago"){
                variable_recent <- "othertob_cgr_recent"
                variable_first <- "othertob_cgr_first"
                
                # Create tibble of time and "othertob_cgr_*" variables
                othertob_time_vars <- temp_drop_ema %>% add_row(ema_i) %>% 
                  select("begin_unixts", "end_unixts", "begin_hrts_UTC", "end_hrts_UTC", "begin_hrts_AmericaChicago", "end_hrts_AmericaChicago",
                         othertob_cgr_ago, othertob_cgr_recent, othertob_cgr_first)
                
                # If applicable, update *_ago to *_recent and *_first 
                if (sum(!is.na(othertob_time_vars$othertob_cgr_ago)) == 1 & all(is.na(othertob_time_vars$othertob_cgr_recent))){
                  # Exactly one non-NA for *_cgr_ago, and all records of *_cgr_recent are NA
                  nonNA_cgr_ago_index <- which(!is.na(othertob_time_vars$othertob_cgr_ago))   # Get the row index for the Non-NA othertob_cgr_ago value
                  nonNA_cgr_ago_value <- othertob_time_vars$othertob_cgr_ago[nonNA_cgr_ago_index]  # Get the Non-NA othertob_cgr_ago value
                  if (nonNA_cgr_ago_index == nrow(othertob_time_vars)){  # The record with the Non-NA _cgr_ago is from ema_i. Don't need to add delta time
                    updated_ema_i$othertob_cgr_ago <- nonNA_cgr_ago_value
                  } else {
                    delta_time <- ceiling(time_length(othertob_time_vars$end_hrts_AmericaChicago[nrow(othertob_time_vars)] - othertob_time_vars$end_hrts_AmericaChicago[nonNA_cgr_ago_index], unit = "minute"))
                    updated_cgr_ago_value <- as.double(nonNA_cgr_ago_value + delta_time)
                    updated_ema_i$othertob_cgr_ago <- updated_cgr_ago_value  
                  }    
                } else {
                  for (indx in 1:nrow(othertob_time_vars)){
                    if (!is.na(othertob_time_vars$othertob_cgr_ago[indx])){
                      othertob_time_vars$othertob_cgr_recent[indx] <- othertob_time_vars$othertob_cgr_ago[indx]  # Move the value from ago to *_recent and *_first to be aggregated in next steps
                      othertob_time_vars$othertob_cgr_first[indx] <- othertob_time_vars$othertob_cgr_ago[indx]   # Move the value from ago to *_recent and *_first to be aggregated in next steps
                      updated_ema_i$othertob_cgr_ago <- NA_real_  # Will be updating for _recent and _first, so _ago must be NA
                    } 
                  }
                } # Finished restructuring othertob_cgr, so later lines can handle _cgr_, _ecig_, and _marij_ similarly
              } else {
                variable_recent <- variable
                if (variable == "othertob_ecig_recent"){
                  variable_first <- "othertob_ecig_first"
                } else if (variable == "othertob_marij_recent"){
                  variable_first <- "othertob_marij_first"
                }
                othertob_time_vars <- temp_drop_ema %>% add_row(ema_i) %>% 
                  select("begin_unixts", "end_unixts", "begin_hrts_UTC", "end_hrts_UTC", "begin_hrts_AmericaChicago", "end_hrts_AmericaChicago",
                         all_of(variable_recent), all_of(variable_first))
              } # Finished prepping _ecig_, and _marij_. All will have variable called othertob_time_vars
              if (!all(is.na(othertob_time_vars[variable_first]))){
                othertob_first_index <- min(which(!is.na(othertob_time_vars[variable_first])))  # index for the earliest record of *_first
                othertob_first_value <- othertob_time_vars[othertob_first_index, variable_first]
                # Updating *_first
                if (othertob_first_index == nrow(othertob_time_vars)){  # the earliest *_first non-NA record is from the Non-Extra EMA - does not require adding delta time
                  updated_ema_i[variable_first] <- othertob_first_value
                } else {  # The earliest *_first non-NA record is not from the Non-Extra EMA, requires adding delta time
                  delta_time <- ceiling(time_length(othertob_time_vars$end_hrts_AmericaChicago[nrow(othertob_time_vars)] - othertob_time_vars$end_hrts_AmericaChicago[othertob_first_index], unit = "minute"))
                  updated_othertob_first_value <- as.double(othertob_first_value + delta_time)
                  updated_ema_i[variable_first] <- updated_othertob_first_value
                }
              }
              if (!all(is.na(othertob_time_vars[variable_recent]))){
                othertob_recent_index <- max(which(!is.na(othertob_time_vars[variable_recent])))  # index for the latest record of *_recent
                othertob_recent_value <- othertob_time_vars[othertob_recent_index, variable_recent]
                # Updating *_recent
                if (othertob_recent_index == nrow(othertob_time_vars)){  # the earliest *_recent non-NA record is from the Non-Extra EMA - does not require adding delta time
                  updated_ema_i[variable_recent] <- othertob_recent_value
                } else {  # The earliest *_recent non-NA record is not from the Non-Extra EMA, requires adding delta time
                  delta_time <- ceiling(time_length(othertob_time_vars$end_hrts_AmericaChicago[nrow(othertob_time_vars)] - othertob_time_vars$end_hrts_AmericaChicago[othertob_recent_index], unit = "minute"))
                  updated_othertob_recent_value <- as.double(othertob_recent_value + delta_time)
                  updated_ema_i[variable_recent] <- updated_othertob_recent_value
                }  
              }  
            } # End of else if variable %in% c("othertob_cgr_ago", "other_ecig_recent", "othertob_marij_recent")
            # End of all rules for "RECALCULATE"
          } else if (aggregation_rules$agg_rule[aggregation_rules$Variables == variable] == "SKIP"){
            next
          } else if (aggregation_rules$agg_rule[aggregation_rules$Variables == variable] == "SKIP & AGGREGATE (new var)"){
            # This rule is used to skip aggregation for the variable, but populate a new variable with an aggregation
            # patch_all_records : aggregation of patch. Options are: "Yes", "No", "Mixed"
            # Yes: All records, random ema plus any aggregated non-random ema, show "Yes" for the patch variable
            # No: All records, random ema plus any aggregated non-random ema, show "Yes" for the patch variable
            # Mixed: All records, random ema plus any aggregated non-random ema, show "Yes" for at least one patch variable record and "No" for at least one patch variable record
            if (variable == "patch"){  # This is currently variable specific and only built for patch -> patch_all_records
              unique_patch <- unique(unlist(var_values))
              updated_ema_i$patch_all_records <- case_when(length(unique_patch) > 1 ~ "Mixed", 
                                                           length(unique_patch) == 1 ~ unique_patch[1],  
                                                           T ~ NA_character_)
            }
          } else if (aggregation_rules$agg_rule[aggregation_rules$Variables == variable] == "SKIP & AGGREGATE (new variable min and max)"){
            # Currently only used for "anhedonia" variable
            # Due to question phrasing & scale, will create a min and max
            # Minimum values correspond to higher anhedonia
            if (variable == "anhedonia"){
              anhedonia_scale <- c("0 (absolutely no pleasure)", "1", "2", "3", "4", "5 (extreme pleasure)")
              var_values_fctr <- factor(var_values$anhedonia, levels = anhedonia_scale, ordered = TRUE)  # Transform the values into factors of ordered levels
              updated_ema_i$anhedonia <- ema_i$anhedonia
              updated_ema_i$anhedonia_agg_min <- as.character(min(var_values_fctr))
              updated_ema_i$anhedonia_agg_max <- as.character(max(var_values_fctr))
            }
          } else if (aggregation_rules$agg_rule[aggregation_rules$Variables == variable] == "SUM"){
            # if (all(is.na(ema_i[variable]), is.na(temp_drop_ema[variable]))){   # If all any NA, then leave with the random ema's NA already in updated_ema_i
            #   next
            # } else {
            #   updated_ema_i[variable] <- as.character(sum(as.integer(ema_i[variable]),as.integer(unlist(temp_drop_ema[variable])), na.rm = T))
            #   print(paste("Updated record", i, "for participant", participant, "for", variable, "from", ema_i[variable], "to", updated_ema_i[variable]))
            # }
            if (is.numeric(ema_i[[variable]])){
              updated_ema_i[variable] <- sum(unlist(var_values))
            } else {
              updated_ema_i[variable] <- as.character(sum(as.integer(unlist(var_values))))
              }
          }
        } 
        ##### THIS IS THE END OF THE IF-ELSES FOR THE AGGREGATION RULE FOR EACH VARIABLE ######
        # Still within the if-statement for Non-Extra EMA with data and temp_drop_ema with >0 rows
        temp_drop_ema <- parts_all_ema_data[0,]   # Empty the placeholder for non-random emas
        ema_data_per_study_design <- ema_data_per_study_design %>% add_row(updated_ema_i) # Add the updated Non-Extra EMA record to the growing Non-Extra EMA dataset
      } else {  # if TRUE, there were no Extra EMA records to aggregate into the current Non-Extra EMA record
          ema_data_per_study_design <- ema_data_per_study_design %>% add_row(ema_i)
      }
    }
  }
}

ema_data_per_study_design %>% count(extra_ema, extra_ema_on_study_day)

# Replicated from "create-recalculated-ema-vars.R" to re-calculate "time since" variables
ema_data_per_study_design_cpl <- ema_data_per_study_design %>% 
  filter(with_any_response == 1) %>% 
  arrange(participant_id, end_unixts) %>% 
  group_by(participant_id) %>% 
  mutate(end_hrts_last = lag(end_hrts_AmericaChicago),
         minutes_since_last = interval(end_hrts_last,end_hrts_AmericaChicago) %/% minutes(1),  #floors minutes, or at least rounds
         hours_since_last = minutes_since_last/60) %>% ungroup()

ema_data_per_study_design_not_cpl <- ema_data_per_study_design %>% filter(with_any_response != 1)

ema_data_per_study_design2 <- bind_rows(ema_data_per_study_design_cpl, ema_data_per_study_design_not_cpl) %>% arrange(participant_id, end_unixts)


# Recalculate for *_err
# It uses hours_since_last or minutes_since_last, so this is outside the main for loop
ema_data_per_study_design3 <- ema_data_per_study_design2 %>% 
    rowwise() %>% 
    mutate(
      cig_err = max(replace_na(c(cig_ago_mid, cig_recent_mid, cig_first_mid), 0)) - 1 > hours_since_last,  #subtract 1 because using midpoint of intervals spanning 2 hours
      othertob_cgr_err = max(replace_na(c(othertob_cgr_ago, othertob_cgr_first, othertob_cgr_recent), 0)) > minutes_since_last, # replace NA with 0, if all NA then the max is 0 which will yield FALSE
      othertob_ecig_err = max(replace_na(c(othertob_ecig_first, othertob_ecig_recent), 0)) > minutes_since_last,  
      othertob_marij_err = max(replace_na(c(othertob_marij_first, othertob_marij_recent), 0)) > minutes_since_last  
    ) %>% ungroup()


# START Tests #####
test1 <- test_that(desc = "Correct number of records per participant?", {
  all_ema_minus_extra_nrow <- all_ema_data %>% filter(!extra_ema) %>% filter(!invalid_end_day) %>% group_by(participant_id) %>% summarize(nrows = n())
  ema_data_per_study_design3_nrow <- ema_data_per_study_design3 %>% group_by(participant_id) %>% summarize(nrows = n())
  expect_equal(object = ema_data_per_study_design3_nrow, 
               expected = all_ema_minus_extra_nrow)})


# test2 <- test_that(desc = "Check that we don't add missing values", {
#   # First, checking across all variables with orig and aggreg - except for the *_last variables
#   # "*_last_aggreg" can be NA when the "*_orig" was not, if the "_orig" value was tied to a non-random EMA and there are no random EMAs preceding
#   # *_err_aggreg can be NA when the *_orig was not, for the same reason described for _last_aggreg
#   # *_ago_aggreg can have less NA because *_ago goes to NA if there are more than one. In the aggregation, *_ago would go to NA, and _first, and _recent would go from NA to values
#   orig_vars_NA_n <- ema_data_per_study_design3 %>% select(contains("_orig")) %>% select(-all_of(contains(c("_last", "_err")))) %>% select(-contains("_ago")) %>% apply(., 2, function(x) sum(is.na(x)))
#   aggreg_vars_NA_n <- ema_data_per_study_design3 %>% select(contains(c("_aggreg", "_err"))) %>% select(-all_of(contains(c("_last", "_err")))) %>% select(-contains("_ago")) %>% apply(., 2, function(x) sum(is.na(x)))
#   for (i in 1: length(orig_vars_NA_n)){
#     expect_lte(object = aggreg_vars_NA_n[i], expected = orig_vars_NA_n[i])
#   }
#   # Second, checking across all records
#   orig_vars_NA_n <- ema_data_per_study_design3 %>% select(contains("_orig")) %>% select(-all_of(contains(c("_last", "_err")))) %>% apply(., 1, function(x) sum(is.na(x)))
#   aggreg_vars_NA_n <- ema_data_per_study_design3 %>% select(contains("_aggreg")) %>% select(-all_of(contains(c("_last", "_err")))) %>% apply(., 1, function(x) sum(is.na(x)))
#   for (i in 1: length(orig_vars_NA_n)){
#     expect_lte(object = aggreg_vars_NA_n[i], expected = orig_vars_NA_n[i])
#   }
# })


test2 <- test_that(desc = "If *_ago has value, then the corresponding *_first and *_recent are NA (and vice-versa)", {
  expect_equal(ema_data_per_study_design3 %>% filter(!is.na(cig_ago)) %>% filter(!is.na(cig_recent)) %>% count() %>% as.integer(), expected = 0)
  expect_equal(ema_data_per_study_design3 %>% filter(!is.na(cig_recent)) %>% filter(!is.na(cig_ago)) %>% count() %>% as.integer(), expected = 0)
  })


#### Visual QC A. Check that the "Aggregate All" compound values are as desired
if(F){ema_data_per_study_design3 %>% select(othertob_which_v1) %>% unique()}
if(F){ema_data_per_study_design3 %>% select(othertob_which_v2) %>% unique()}


all_ema_data_D2_per_study_design <- ema_data_per_study_design3 %>% select(-extra_ema, -invalid_end_day)

if(test1 & test2){
  save(all_ema_data_D2_per_study_design,
       file = file.path(path_breakfree_staged_data, "all_ema_data_D2_per_study_design.RData"))
  message("Succesfully saved RData file")
  }


