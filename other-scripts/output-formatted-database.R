library(dplyr)
library(haven)
library(stringr)
library(openxlsx)
source("paths.R")

# -----------------------------------------------------------------------------
# Masterlist of all participant IDs enrolled in the study
# For each participant ID listed, the dat at which EMA data collection began
# and ended, as well as quit date is also listed
# -----------------------------------------------------------------------------
load(file = file.path(path_breakfree_staged_data, "masterlist.RData"))

saveRDS(object = dat_master, file = file.path(path_breakfree_output_data, "masterlist.rds"))  # R/python users
write_dta(data = dat_master, path = file.path(path_breakfree_output_data, "masterlist.dta"))  # Stata/SAS users

# General users
dat_master %>%
  mutate(across(contains("hrts"), ~format(., format = "%Y-%m-%d %H:%M:%S"))) %>%
  write.csv(x = ., file.path(path_breakfree_output_data, "masterlist.csv"), row.names = FALSE, na = "")  

# -----------------------------------------------------------------------------
# EMA Questionnaires for each type of EMA (aka, the codebooks) -- CC1 & CC2 - output 4 data managers only
# -----------------------------------------------------------------------------
load(file = file.path(path_breakfree_staged_data, "combined_ema_data.RData"))
write.csv(ema_items_labelled, file.path(path_breakfree_output_data_4dm, "ema_items_all.csv"), row.names = FALSE, na = "")
# -----------------------------------------------------------------------------
# EMA skip logic issues - output 4 data managers only
# -----------------------------------------------------------------------------
# load(file = file.path(path_breakfree_staged_data, "combined_ema_data_clean.RData"))
# write.csv(conditions_applied_simple, file.path(path_breakfree_output_data_4dm, "skip_logic_errors_summary.csv"), row.names = FALSE, na = "")
# 
# # [Begin] Tony's addition
# write.csv(unedited_and_clean_ema_vars_dat, file.path(path_breakfree_output_data_4dm, "unedited_and_clean_ema_vars.csv"), row.names = FALSE, na = "")
# saveRDS(object = unedited_and_clean_ema_vars_dat, file = file.path(path_breakfree_output_data_4dm, "unedited_and_clean_ema_vars.rds"))  # R/python users
# write_dta(data = unedited_and_clean_ema_vars_dat, path = file.path(path_breakfree_output_data_4dm, "unedited_and_clean_ema_vars.dta"))  # Stata/SAS users
# # [End] Tony's addition

# -----------------------------------------------------------------------------
# EMA Dataset #1 - all delivered EMAs
#
# Before the aggregation and drops of extra EMAs and EMAs after the end of day
# -----------------------------------------------------------------------------
load(file = file.path(path_breakfree_staged_data, "all_ema_data_D1_all_delivered.RData"))

saveRDS(object = all_ema_data_D1_all_delivered, file = file.path(path_breakfree_output_data_4dm, "all_ema_data-1-all_delivered.rds"))  # R/python users
write_dta(data = all_ema_data_D1_all_delivered, path = file.path(path_breakfree_output_data_4dm, "all_ema_data-1-all_delivered.dta"))  # Stata/SAS users

# General users
all_ema_data_D1_all_delivered %>%
  mutate(across(contains("hrts"), ~format(., format = "%Y-%m-%d %H:%M:%S"))) %>%
  write.csv(x = ., file.path(path_breakfree_output_data_4dm, "all_ema_data-1-all_delivered.csv"), row.names = FALSE, na = "")

# -----------------------------------------------------------------------------
# EMA Dataset #2 - per Study Design
#
# Extra EMAs and EMAs after the End of Day, have been aggregated forward and dropped
# -----------------------------------------------------------------------------
load(file = file.path(path_breakfree_staged_data, "all_ema_data_D2_per_study_design.RData"))

saveRDS(object = all_ema_data_D2_per_study_design, file = file.path(path_breakfree_output_data, "all_ema_data-2-per_study_design.rds"))  # R/python users
write_dta(data = all_ema_data_D2_per_study_design, path = file.path(path_breakfree_output_data, "all_ema_data-2-per_study_design.dta"))  # Stata/SAS users

# General users
all_ema_data_D2_per_study_design %>%
  mutate(across(contains("hrts"), ~format(., format = "%Y-%m-%d %H:%M:%S"))) %>%
  write.csv(x = ., file.path(path_breakfree_output_data, "all_ema_data-2-per_study_design.csv"), row.names = FALSE, na = "")

### Integer values version
load(file = file.path(path_breakfree_staged_data, "all_ema_data_D2_per_study_design_integers.RData"))
load(file = file.path(path_breakfree_staged_data, "create_and_apply_value_labels_SAS_script.RData"))

saveRDS(object = all_ema_data_D2_per_study_design_integers, file = file.path(path_breakfree_output_data, "all_ema_data-2-per_study_design_integers.rds"))  # R/python users
write_dta(data = all_ema_data_D2_per_study_design_integers, path = file.path(path_breakfree_output_data, "all_ema_data-2-per_study_design_integers.dta"))  # Stata/SAS users
write_file(x = sas_script, file = file.path(path_breakfree_output_data,"create_and_apply_value_labels_SAS_script.sas"))

# General users
all_ema_data_D2_per_study_design_integers %>%
  mutate(across(contains("hrts"), ~format(., format = "%Y-%m-%d %H:%M:%S"))) %>%
  write.csv(x = ., file.path(path_breakfree_output_data, "all_ema_data-2-per_study_design_integers.csv"), row.names = FALSE, na = "")

# -----------------------------------------------------------------------------
# EMA Dataset #3 - Random Only EMA
#
# Non-Random EMAs have been aggregated, as well as aggregations for study design
# -----------------------------------------------------------------------------
load(file = file.path(path_breakfree_staged_data, "all_ema_data_D3_random_only.Rdata"))

saveRDS(object = all_ema_data_D3_random_only, file = file.path(path_breakfree_output_data, "all_ema_data-3-random_only_ema.rds"))  # R/python users
write_dta(data = all_ema_data_D3_random_only, path = file.path(path_breakfree_output_data, "all_ema_data-3-random_only_ema.dta"))  # Stata/SAS users

all_ema_data_D3_random_only %>% 
  mutate(across(contains("hrts"), ~format(., format = "%Y-%m-%d %H:%M:%S"))) %>%
  write.csv(x = ., file.path(path_breakfree_output_data, "all_ema_data-3-random_only_ema.csv"), row.names = FALSE, na = "")


### Integer values version
load(file = file.path(path_breakfree_staged_data, "all_ema_data_D3_random_only_integers.Rdata"))

saveRDS(object = all_ema_data_D3_random_only_integers, file = file.path(path_breakfree_output_data, "all_ema_data-3-random_only_ema_integers.rds"))  # R/python users
write_dta(data = all_ema_data_D3_random_only_integers, path = file.path(path_breakfree_output_data, "all_ema_data-3-random_only_ema_integers.dta"))  # Stata/SAS users

all_ema_data_D3_random_only_integers %>% 
  mutate(across(contains("hrts"), ~format(., format = "%Y-%m-%d %H:%M:%S"))) %>%
  write.csv(x = ., file.path(path_breakfree_output_data, "all_ema_data-3-random_only_ema_integers.csv"), row.names = FALSE, na = "")

# -----------------------------------------------------------------------------
# For CC1 & CC2:
# A record of all time-stamps classified as smoking episodes by a biomarker
# These time-stamps are the result of an 'online algorithm'
# -----------------------------------------------------------------------------
load(file = file.path(path_breakfree_staged_data, "combined_online_puffmarker_episode_data.RData"))

saveRDS(object = online_puffmarker_episode_data, file = file.path(path_breakfree_output_data, "online_puffmarker_episode_data.rds"))  # R/python users
write_dta(data = online_puffmarker_episode_data, path = file.path(path_breakfree_output_data, "online_puffmarker_episode_data.dta"))  # Stata/SAS users

# General users
online_puffmarker_episode_data %>%
  mutate(across(contains("hrts"), ~format(., format = "%Y-%m-%d %H:%M:%S"))) %>%
  write.csv(x = ., file.path(path_breakfree_output_data, "online_puffmarker_episode_data.csv"), row.names = FALSE, na = "")  

# -----------------------------------------------------------------------------
# Curated Codebook
# -----------------------------------------------------------------------------
load(file = file.path(path_breakfree_staged_data, "codebook.Rdata"))

saveRDS(object = codebook, file = file.path(path_breakfree_output_data, "codebook.rds"))  # R/python users

codebook %>% rename_all(~str_replace_all(.," ","_")) %>% write_dta(data = ., path = file.path(path_breakfree_output_data, "codebook.dta"))  # Stata/SAS users

# General users
# codebook %>%
#   mutate(across(contains("hrts"), ~format(., format = "%Y-%m-%d %H:%M:%S"))) %>%
#   write.csv(x = ., file.path(path_breakfree_output_data, "codebook.csv"), row.names = FALSE, na = "") 

wb <- createWorkbook()
addWorksheet(wb, "Codebook")
writeData(wb, sheet = 1, x = codebook)
setColWidths(wb, sheet = 1, cols = c(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15), widths = c(26,56,14,34,80,27,27,88,16,86,8,8,20,23,71))
freezePane(wb, sheet = 1, firstRow = TRUE, firstCol = TRUE)
header_style <- createStyle(textDecoration = "bold")
addStyle(wb, sheet = 1, header_style, rows = 1, cols = 1:14)
saveWorkbook(wb, file.path(path_breakfree_output_data, "codebook.xlsx"), overwrite = TRUE)

# -----------------------------------------------------------------------------
# Update Log
# -----------------------------------------------------------------------------
update_description <- readline(prompt = "Input update description: ")

write(paste(as.character(Sys.time()), update_description, sep = "\t"),
      file = file.path(path_breakfree_output_data_4dm, "log.txt"),
      append = TRUE)
