library(dplyr)
library(readxl)
library(stringr)
library(tidyr)
library(lubridate)

source("paths.R",echo = T)

load(file.path(path_breakfree_staged_data,"ema_questionnaires_cc1.RData"))
load(file.path(path_breakfree_staged_data,"ema_questionnaires_cc2.RData"))

#### check if all questions are same within a CC version 

#If not true, inspect the datasets individuall. each of the all_equal functions will indicate differences
(cc1_same <- isTRUE(all_equal(random_ema_items_cc1, smoking_ema_items_cc1,convert=T)) & isTRUE(all_equal(random_ema_items_cc1, stress_ema_items_cc1)))
(cc2_same <- isTRUE(all_equal(random_ema_items_cc2, smoking_ema_items_cc2)) & isTRUE(all_equal(random_ema_items_cc2, stress_ema_items_cc2)))

common_by_vars <- names(random_ema_items_cc1) #find common variables that I plan to merge by later -- doesn't matter which dataset

### prep datasets

random_ema_items_cc1$random_ema_cc1 = T
smoking_ema_items_cc1$smoking_ema_cc1 = T
stress_ema_items_cc1$stress_ema_cc1 = T

random_ema_items_cc2$random_ema_cc2 = T
smoking_ema_items_cc2$smoking_ema_cc2 = T
stress_ema_items_cc2$stress_ema_cc2 = T


if(cc1_same) {
  ema_items_cc1 <- random_ema_items_cc1 %>% full_join(smoking_ema_items_cc1, by = common_by_vars) %>% full_join(stress_ema_items_cc1, by = common_by_vars) %>% 
    mutate(cc1=T) %>% 
    rename(question_id_CC1 = question_id)
  print("CC1 Items were combined")
} else{print("code hasn't been built if cc1_same==F")}

if(cc2_same) {
  ema_items_cc2 <- random_ema_items_cc2 %>% full_join(smoking_ema_items_cc2, by = common_by_vars) %>% full_join(stress_ema_items_cc2, by = common_by_vars) %>% 
    mutate(cc2=T) %>% 
    rename(question_id_CC2 = question_id)
  print("CC2 Items were combined")
} else{print("code hasn't been built if cc2_same==F")}

#combine CC1 and 2
ema_items <- ema_items_cc1 %>% full_join(ema_items_cc2, by = c("question_text", "question_type", "question_options")) %>% #I combine by the actual questions
  #fill in missing indicators
  mutate(across(c("random_ema_cc1","smoking_ema_cc1","stress_ema_cc1","random_ema_cc2","smoking_ema_cc2","stress_ema_cc2","cc1","cc2"),
            ~if_else(is.na(.),F,.))) %>% 
  mutate(question_id_temp = coalesce(question_id_CC2,question_id_CC1)) %>% 
  arrange(question_id_temp, question_id_CC1) %>% 
  #create unifying ID across software types (CC1/CC2)
  mutate(question_id_breakfree = row_number()) %>% 
  mutate(varname_CC1 = if_else(!is.na(question_id_CC1),paste0("item_",question_id_CC1),NA_character_),
         varname_CC2 = if_else(!is.na(question_id_CC2),paste0("item_",question_id_CC2),NA_character_),
         varname_breakfree = if_else(!is.na(question_id_breakfree),paste0("item_",question_id_breakfree),NA_character_)) %>% 
  select(question_id_breakfree, question_id_CC1, question_id_CC2, everything())


#output excel
# write.csv(ema_items,file.path(path_breakfree_staged_data,"ema_items_raw.csv"), row.names = F)
ema_labels <- read_xlsx(file.path(path_breakfree_other_input_data,"ema_items_labelled_4_input.xlsx"))
ema_labels <- ema_labels %>% 
  select(question_text,question_type,question_options,scale_name, varname_desc)

#add ema labels
ema_items_labelled_1 <- ema_items %>%
  left_join(ema_labels, by = c("question_text", "question_type", "question_options")) %>% 
  mutate(varname_breakfree = varname_desc) %>% 
  arrange(question_id_breakfree)

#find items with missing custom variable names
if (nrow(ema_items_labelled_1 %>% filter(is.na(varname_desc)))>0){
  print("The following variables are missing custom variable names")
  ema_items_labelled_1 %>% filter(is.na(varname_desc)) %>% select(question_id_breakfree,question_id_CC1,question_id_CC2,question_text,varname_desc) %>% as_tibble()
  }

###################################################################################################################
###add skip logic

'%detect%' = function(alpha,bravo){str_detect(alpha,bravo)}
'!%detect%' = function(alpha,bravo){!str_detect(alpha,bravo)}


raw_cc1 <- read.csv(paste0(path_breakfree_cc1_input_data,"/3001/7c54c7d9-5e6a-33da-9aac-18478f533af1+10468+org.md2k.ema_scheduler+EMA+RANDOM_EMA+PHONE+processed.csv"), na="")
raw_cc2 <- read.csv(paste0(path_breakfree_cc2_input_data,"/aa_203/EMA_RANDOM--DATA--org.md2k.ema.csv"), na="")
skip_raw_cc1 <- raw_cc1 %>% as_tibble %>% select(contains("condition")) %>%
  filter(row_number()==1) %>% 
  pivot_longer(cols = everything(), names_to = "id_temp", values_to = "condition_raw") %>% 
  mutate(id = as.numeric(str_extract(id_temp, "[:digit:]+"))) %>% 
  select(-id_temp) %>% 
  rename(condition_raw_cc1 = condition_raw)

skip_raw_cc2 <- raw_cc2 %>% as_tibble %>% select(contains("condition")) %>%
  filter(row_number()==1) %>% 
  pivot_longer(cols = everything(), names_to = "id_temp", values_to = "condition_raw") %>% 
  mutate(id = as.numeric(str_extract(id_temp, "[:digit:]+"))) %>% 
  select(-id_temp) %>% 
  rename(condition_raw_cc2 = condition_raw)

ema_items_labelled_2 <- ema_items_labelled_1 %>% 
  left_join(skip_raw_cc1, by = c("question_id_CC1"="id")) %>% 
  left_join(skip_raw_cc2, by = c("question_id_CC2"="id")) %>% 
  mutate(condition_id_cc1 = str_extract(condition_raw_cc1,"^[:digit:]+"), #find #ID of condition
         condition_id_cc2 = str_extract(condition_raw_cc2,"^[:digit:]+"),
         #condition_varname = ema_items_labelled$varname_breakfree[match(condition_id, ema_items_labelled$question_id_CC2)],
         #replace number ID with text varname
         condition_cc1 = str_replace(condition_raw_cc1, "^[:digit:]+", ema_items_labelled_1$varname_desc[match(condition_id_cc1, ema_items_labelled_1$question_id_CC1)]),
         #replace not equal to operand, w/ single quote
         condition_cc1 = str_replace_all(condition_cc1, ":~"," != '"),
         #replace equal to operand, w/ single quote
         condition_cc1 = str_replace_all(condition_cc1, ":"," == '"),
         #add single quote to the end
         condition_cc1 = str_c(condition_cc1,"'"),
         #
         condition_cc1 = if_else(ema_items_labelled_1$question_type[match(condition_id_cc1, ema_items_labelled_1$question_id_CC1)]=="multiple_select",str_replace(condition_cc1,"==","%detect%") %>% str_replace(.,"!=","%no_detect%"),condition_cc1),
         condition_cc2 = str_replace(condition_raw_cc2, "^[:digit:]+",ema_items_labelled_1$varname_desc[match(condition_id_cc2, ema_items_labelled_1$question_id_CC2)]),
         condition_cc2 = str_replace_all(condition_cc2, ":~"," != '"),
         condition_cc2 = str_replace_all(condition_cc2, ":"," == '"),
         condition_cc2 = str_c(condition_cc2,"'"),
         condition_cc2 = if_else(ema_items_labelled_1$question_type[match(condition_id_cc2, ema_items_labelled_1$question_id_CC2)]=="multiple_select",str_replace(condition_cc2,"==","%detect%") %>% str_replace(.,"!=","%no_detect%"),condition_cc2),
         #
         condition = case_when(is.na(condition_cc1)|is.na(condition_cc2) ~ coalesce(condition_cc1,condition_cc2),
                          (condition_cc1==condition_cc2) ~ condition_cc1,
                          T ~ str_c("(",condition_cc1,")|(",condition_cc2,")")),
    )

ema_items_labelled <- ema_items_labelled_2

###################################################################################################################
###update variable names in ema data to correspond to new variable names
# load(file.path(path_breakfree_staged_data,"all_ema_data_cc1.RData"))
# load(file.path(path_breakfree_staged_data,"all_ema_data_cc2.RData"))
#load(file.path(path_breakfree_staged_data, "all_ema_data_wblocks_cc1.RData"))
#load(file.path(path_breakfree_staged_data, "all_ema_data_wblocks_cc2.RData"))

block_level_ema_cc1 <- readRDS(file.path(path_breakfree_staged_data, "block_level_ema_cc1.Rds"))
block_level_ema_cc2 <- readRDS(file.path(path_breakfree_staged_data, "block_level_ema_cc2.Rds"))

#create renamed version of data
#all_ema_data_cc1_renamed <- all_ema_data_wblocks_cc1
#all_ema_data_cc2_renamed <- all_ema_data_wblocks_cc2

all_ema_data_cc1_renamed <- block_level_ema_cc1
all_ema_data_cc2_renamed <- block_level_ema_cc2


#for each column, define lookup in combined codebook
# cc1_lookups <- match(names(all_ema_data_wblocks_cc1),ema_items_labelled$varname_CC1)
# cc2_lookups <- match(names(all_ema_data_wblocks_cc2),ema_items_labelled$varname_CC2)
cc1_lookups <- match(names(block_level_ema_cc1),ema_items_labelled$varname_CC1)
cc2_lookups <- match(names(block_level_ema_cc2),ema_items_labelled$varname_CC2)


#replace column names in renamed dataset with non-missing lookup values
names(all_ema_data_cc1_renamed)[!is.na(cc1_lookups)] <- na.omit(ema_items_labelled$varname_breakfree[cc1_lookups])
names(all_ema_data_cc2_renamed)[!is.na(cc2_lookups)] <- na.omit(ema_items_labelled$varname_breakfree[cc2_lookups])
#stack data

all_ema_data <- bind_rows(all_ema_data_cc1_renamed, all_ema_data_cc2_renamed)
non_ema_vars <- setdiff(names(all_ema_data),ema_items_labelled$varname_breakfree)
all_ema_data <- all_ema_data %>% 
  #reorder columns
  select(all_of(c(non_ema_vars,ema_items_labelled$varname_breakfree)))

all_ema_data <- all_ema_data %>% 
  mutate(
    begin_hrts_AmericaChicago = as_datetime(begin_unixts, tz = "America/Chicago"),
    end_hrts_AmericaChicago = as_datetime(end_unixts, tz = "America/Chicago")
  )

save(all_ema_data, ema_items_labelled,
     file = file.path(path_breakfree_staged_data, "combined_ema_data.RData"))


###################################################################################################################
###combine puffmarker data
load(file = file.path(path_breakfree_staged_data, "online_puffmarker_episode_data_cc1.RData"))
load(file = file.path(path_breakfree_staged_data, "online_puffmarker_episode_data_cc2.RData"))

online_puffmarker_episode_data <- bind_rows(online_puffmarker_episode_data_cc1,online_puffmarker_episode_data_cc2)

save(online_puffmarker_episode_data,
     file = file.path(path_breakfree_staged_data, "combined_online_puffmarker_episode_data.RData"))

