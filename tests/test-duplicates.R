library(testthat)
library(glue)
library(ggplot2)
library(dplyr)
source("paths.R")

# Load 'clean' data files
puffm_data <- readRDS(file = file.path(path_breakfree_output_data, "online_puffmarker_episode_data.rds"))


# Perform sanity checks on 'clean' data files and document these tests using the testthat package
test_that("Are there any duplicate rows in each file?", {
  output_val <- puffm_data %>%
    select(participant_id, onlinepuffm_unixts) %>%
    duplicated(.) %>%
    sum(.)
  
  expect_equal(object = output_val, expected = 0)
})