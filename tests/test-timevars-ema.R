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
    if(all_ema_data[["ep_status"]][idx_row] == "EMA-COMPLETE"){
      expect_lt(object = all_ema_data[["ep_begin_hrts_AmChi"]][idx_row], 
                expected = all_ema_data[["ep_end_hrts_AmChi"]][idx_row])
      
      expect_lt(object = all_ema_data[["ep_begin_hrts_UTC"]][idx_row], 
                expected = all_ema_data[["ep_end_hrts_UTC"]][idx_row])
    }
  }
})

test_that("Considering only 'ABANDONED_BY_TIMEOUT' EMAs having a response to at least 1 item (i.e., with_any_response=1), begin time should fall prior to end time. 
          Note: the status 'ABANDONED_BY_TIMEOUT' was updated to 'EMA-PARTIALLY_COMPLETE' in version 2.0.0", {
  for(idx_row in 1:nrow(all_ema_data)){
    if(all_ema_data[["ep_status"]][idx_row] == "EMA-PARTIALLY_COMPLETE" & all_ema_data[["ep_with_any_response"]][idx_row] == 1){
      expect_lt(object = all_ema_data[["ep_begin_hrts_AmChi"]][idx_row], 
                expected = all_ema_data[["ep_end_hrts_AmChi"]][idx_row])
      
      expect_lt(object = all_ema_data[["ep_begin_hrts_UTC"]][idx_row], 
                expected = all_ema_data[["ep_end_hrts_UTC"]][idx_row])
    }
  }
})
