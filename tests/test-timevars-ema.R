library(testthat)
library(glue)
library(ggplot2)
library(dplyr)
source("paths.R")

# Load 'clean' data files
masterlist <- readRDS(file = file.path(path_breakfree_output_data, "masterlist.rds"))
all_ema_data <- readRDS(file = file.path(path_breakfree_output_data_4dm, "all_ema_data-1-all_delivered.rds"))

# Perform sanity checks on 'clean' data files and document these tests using the testthat package
test_that("Considering only 'COMPLETED' EMAs, begin time should fall prior to end time.", {
  for(idx_row in 1:nrow(all_ema_data)){
    if(all_ema_data[["status"]][idx_row] == "COMPLETED"){
      expect_lt(object = all_ema_data[["begin_hrts_AmericaChicago"]][idx_row], 
                expected = all_ema_data[["end_hrts_AmericaChicago"]][idx_row])
      
      expect_lt(object = all_ema_data[["begin_hrts_UTC"]][idx_row], 
                expected = all_ema_data[["end_hrts_UTC"]][idx_row])
    }
  }
})

test_that("Considering only 'ABANDONED_BY_TIMEOUT' EMAs having a response to at least 1 item (i.e., with_any_response=1), begin time should fall prior to end time.", {
  for(idx_row in 1:nrow(all_ema_data)){
    if(all_ema_data[["status"]][idx_row] == "ABANDONED_BY_TIMEOUT" & all_ema_data[["with_any_response"]][idx_row] == 1){
      expect_lt(object = all_ema_data[["begin_hrts_AmericaChicago"]][idx_row], 
                expected = all_ema_data[["end_hrts_AmericaChicago"]][idx_row])
      
      expect_lt(object = all_ema_data[["begin_hrts_UTC"]][idx_row], 
                expected = all_ema_data[["end_hrts_UTC"]][idx_row])
    }
  }
})
