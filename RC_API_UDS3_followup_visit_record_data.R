#!/usr/bin/env Rscript

# LOAD USEFUL VARS ----
# source("~/Desktop/config.R")
# source("~/Desktop/helpers.R")
source("~/Box Sync/Documents/R_helpers/config.R")
source("~/Box Sync/Documents/R_helpers/helpers.R")
library(dplyr)
library(readr)
library(stringr)


# LOAD RECORDS + DATE ----
date <- Sys.Date()

# Get initial-visit UM IDs
records_fvp_raw <-
  read_csv(paste0("~/Box Sync/",
                  "Documents/",
                  "NACCulator/",
                  "NACCulator ", date, "/",
                  "df_upload_ready_", date, ".csv")) %>% 
  filter(redcap_event_name != "visit_1_arm_1") %>% 
  pull(ptid)

sum(duplicated(records_fvp_raw))
records_fvp_raw <- records_fvp_raw[!duplicated(records_fvp_raw)]
sum(duplicated(records_fvp_raw))
records_fvp_raw <- sort(records_fvp_raw)
records_fvp <- records_fvp_raw %>% paste(collapse = ",")


# GET DATA VIA REDCAP API ----
json_fvp <- 
  get_rc_data_api(uri = REDCAP_API_URI,
                  token = REDCAP_API_TOKEN_UDS3n,
                  records = records_fvp,
                  .opts = list(ssl.verifypeer = FALSE, verbose = FALSE))

# Clean out Windoze newlines from Excel copy-pastes: \r\n
json_fvp <- str_replace_all(json_fvp, fixed("\r\n"), " ")
df_fvp_raw <- jsonlite::fromJSON(json_fvp) %>% na_if("")


# CLEAN DATA ----

# Get records that are ready for NACC
df_fvp <- df_fvp_raw %>% 
  filter(redcap_event_name != "visit_1_arm_1",
                str_detect(redcap_event_name, "visit_\\d{1,2}_arm_1")) %>% 
  filter(header_complete == 2
                , fvp_a1_complete == 2
                , fvp_a1_complete == 2
                , fvp_a2_complete == 2
                , fvp_a4_complete == 2
                # , fvp_a5_complete == 2
                , fvp_b1_complete == 2
                , fvp_b4_complete == 2
                , fvp_b5_complete == 2
                , fvp_b6_complete == 2
                , fvp_b7_complete == 2
                , fvp_b8_complete == 2
                , fvp_b9_complete == 2
                # , fvp_c1_complete == 2
                , fvp_c2_complete == 2
                , fvp_d1_complete == 2
                , fvp_d2_complete == 2)

# Block records that shouldn't go to NACC b/c of unverified forms
df_fvp_block <- df_fvp_raw %>% 
  filter(redcap_event_name != "visit_1_arm_1") %>% 
  select(ptid
                , redcap_event_name 
                , form_date
                , header_complete
                , fvp_a1_complete
                , fvp_a2_complete
                , fvp_a3_complete
                , fvp_a4_complete
                , fvp_a5_complete
                , fvp_b1_complete
                , fvp_b4_complete
                , fvp_b5_complete
                , fvp_b6_complete
                , fvp_b7_complete
                , fvp_b8_complete
                , fvp_b9_complete
                , fvp_c1_complete
                , fvp_c2_complete
                , fvp_d1_complete
                , fvp_d2_complete) %>% 
  filter(header_complete != 2
                | fvp_a1_complete != 2
                | fvp_a1_complete != 2
                | fvp_a2_complete != 2
                | fvp_a4_complete != 2
                # | fvp_a5_complete != 2
                | fvp_b1_complete != 2
                | fvp_b4_complete != 2
                | fvp_b5_complete != 2
                | fvp_b6_complete != 2
                | fvp_b7_complete != 2
                | fvp_b8_complete != 2
                | fvp_b9_complete != 2
                # | fvp_c1_complete != 2
                | fvp_c2_complete != 2
                | fvp_d1_complete != 2
                | fvp_d2_complete != 2)


# WRITE TO CSV ----
write_csv(df_fvp, 
          paste0("~/Box\ Sync/Documents/NACCulator/NACCulator ", 
                 date, "/NACC30_fv_", date, ".csv"), 
          na = "")

write_csv(df_fvp_block, 
          paste0("~/Box\ Sync/Documents/NACCulator/NACCulator ", 
                 date, "/BLOCKED_NACC30_fv_", date, ".csv"), 
          na = "")


# RUN NACCulator via TERMINAL COMMANDS ----
ncltr_path <- "~/'Box Sync'/Documents/NACCulator/"

system(
  command = 
    paste0("PYTHONPATH=", ncltr_path, "nacculator/", 
           " ", ncltr_path, "nacculator/nacc/redcap2nacc.py", 
           " -f fixHeaders", 
           " -meta ", ncltr_path, "nacculator/nacculator_cfg_mich.ini", 
           " < ", ncltr_path, 
           "'NACCulator ", date, "'/NACC30_fv_", date, ".csv",
           " > ", ncltr_path, 
           "'NACCulator ", date, "'/NACC30_fv_", date, "_FILTERED.csv"))

system(
  command =
    paste0("PYTHONPATH=", ncltr_path, "nacculator/",
           " ", ncltr_path, "nacculator/nacc/redcap2nacc.py",
           " -fvp",
           " -file ", ncltr_path, 
           "'NACCulator ", date, "'/NACC30_fv_", date, "_FILTERED.csv",
           " > ", ncltr_path, 
           "'NACCulator ", date, "'/NACC30_fv_", date, ".txt"))


###@    #==--  :  --==#    @##==---==##@##==---==##@    #==--  :  --==#    @###
#==##@    #==-- --==#    @##==---==##@   @##==---==##@    #==-- --==#    @##==#
#--==##@    #==-==#    @##==---==##@   #   @##==---==##@    #==-==#    @##==--#
#=---==##@    #=#    @##==---==##@    #=#    @##==---==##@    #=#    @##==---=#
##==---==##@   #   @##==---==##@    #==-==#    @##==---==##@   #   @##==---==##
#@##==---==##@   @##==---==##@    #==-- --==#    @##==---==##@   @##==---==##@#
#  @##==---==##@##==---==##@    EXTRA  :  SPACE    @##==---==##@##==---==##@  #
#@##==---==##@   @##==---==##@    #==-- --==#    @##==---==##@   @##==---==##@#
##==---==##@   #   @##==---==##@    #==-==#    @##==---==##@   #   @##==---==##
#=---==##@    #=#    @##==---==##@    #=#    @##==---==##@    #=#    @##==---=#
#--==##@    #==-==#    @##==---==##@   #   @##==---==##@    #==-==#    @##==--#
#==##@    #==-- --==#    @##==---==##@   @##==---==##@    #==-- --==#    @##==#
###@    #==--  :  --==#    @##==---==##@##==---==##@    #==--  :  --==#    @###
#==##@    #==-- --==#    @##==---==##@   @##==---==##@    #==-- --==#    @##==#
#--==##@    #==-==#    @##==---==##@   #   @##==---==##@    #==-==#    @##==--#
#=---==##@    #=#    @##==---==##@    #=#    @##==---==##@    #=#    @##==---=#
##==---==##@   #   @##==---==##@    #==-==#    @##==---==##@   #   @##==---==##
#@##==---==##@   @##==---==##@    #==-- --==#    @##==---==##@   @##==---==##@#
#  @##==---==##@##==---==##@    EXTRA  :  SPACE    @##==---==##@##==---==##@  #
#@##==---==##@   @##==---==##@    #==-- --==#    @##==---==##@   @##==---==##@#
##==---==##@   #   @##==---==##@    #==-==#    @##==---==##@   #   @##==---==##
#=---==##@    #=#    @##==---==##@    #=#    @##==---==##@    #=#    @##==---=#
#--==##@    #==-==#    @##==---==##@   #   @##==---==##@    #==-==#    @##==--#
#==##@    #==-- --==#    @##==---==##@   @##==---==##@    #==-- --==#    @##==#
###@    #==--  :  --==#    @##==---==##@##==---==##@    #==--  :  --==#    @###
