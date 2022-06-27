library(dplyr)
library(stringr)
source("paths.R",echo = T)

cc1_raw <- read.csv(paste0(path_breakfree_cc1_input_data,"/3001/7c54c7d9-5e6a-33da-9aac-18478f533af1+10468+org.md2k.ema_scheduler+EMA+RANDOM_EMA+PHONE+processed.csv"), na="")
cc2_raw <- read.csv(paste0(path_breakfree_cc2_input_data,"/aa_203/EMA_RANDOM--DATA--org.md2k.ema.csv"), na="")
cc1_raw %>% select(contains("question_type")) %>% distinct_all %>% t

cc2_raw %>% select(contains("question_type")) %>% distinct_all %>% t %>% as.data.frame %>% mutate(q = rownames(.)) %>% distinct(V1)

np <- cc2_raw %>% select(contains("question_type")) %>% distinct_all %>% t %>% as.data.frame %>% mutate(q = rownames(.)) %>% filter(V1=="number_picker") %>% .$q %>% 
  str_remove(.,"_question_type")
cc2_raw %>% select(contains(np)) %>% select(contains(c("question_type","question_text","response_0"))) %>% View
#only seems to be an issue with question 55


hm <- cc2_raw %>% select(contains("question_type")) %>% distinct_all %>% t %>% as.data.frame %>% mutate(q = rownames(.)) %>% filter(V1=="hour_minute") %>% .$q %>% 
  str_remove(.,"_question_type")
cc2_raw %>% select(contains(hm)) %>% select(contains(c("question_type","question_text","response_0"))) #%>% View
cc2_raw #%>%View

#with this participant, I see one instance where they were asked questions about timing of single cigar smoke and multiple (first/last), but they entered they smoked 3.
#outside of this, there seems nothing systematic
