#!/usr/bin/env Rscript

# LOAD USEFUL PACKAGES ----
library(dplyr)
library(readr)
library(stringr)


# LOAD USEFUL VARS ----
# source("~/Desktop/config.R")
# source("~/Desktop/helpers.R")
source("~/Box Sync/Documents/R_helpers/config.R")
source("~/Box Sync/Documents/R_helpers/helpers.R")


# LOAD RECORDS + DATE ----
date <- Sys.Date()

# Get initial-visit UM IDs
records_ivp_raw <-
  read_csv(paste0("~/Box Sync/Documents/NACCulator/",
                  "NACCulator ", date, "/",
                  "df_upload_ready_", date, ".csv")) %>% 
  filter(redcap_event_name == "visit_1_arm_1") %>% 
  pull(ptid)

sum(duplicated(records_ivp_raw))
records_ivp_raw <- records_ivp_raw[!duplicated(records_ivp_raw)]
sum(duplicated(records_ivp_raw))
records_ivp_raw <- sort(records_ivp_raw)
records_ivp <- records_ivp_raw %>% paste(collapse = ",")


# GET DATA VIA REDCAP API ----
json_ivp <- 
  get_rc_data_api(uri = REDCAP_API_URI,
                  token = REDCAP_API_TOKEN_UDS3n,
                  records = records_ivp,
                  .opts = list(ssl.verifypeer = FALSE, verbose = FALSE))
# Clean out Windoze newlines from Excel copy-pastes: \r\n
json_ivp <- str_replace_all(json_ivp, fixed("\r\n"), " ")
df_ivp_raw <- jsonlite::fromJSON(json_ivp) %>% na_if("")
# df_ivp_v1 <- df_ivp_raw %>% 
#   filter(redcap_event_name == "visit_1_arm_1",
#                 str_detect(redcap_event_name, "visit_\\d{1,2}_arm_1"))


# CLEAN DATA ----

# Get records that are ready for NACC
df_ivp <- df_ivp_raw %>% 
  filter(redcap_event_name == "visit_1_arm_1",
         str_detect(redcap_event_name, 
                    "visit_\\d{1,2}_arm_1")) %>% 
  filter(header_complete == 2
         , ivp_a1_complete == 2
         , ivp_a2_complete == 2
         , ivp_a3_complete == 2
         , ivp_a4_complete == 2
         , ivp_a5_complete == 2
         , ivp_b1_complete == 2
         , ivp_b4_complete == 2
         , ivp_b5_complete == 2
         , ivp_b6_complete == 2
         , ivp_b7_complete == 2
         , ivp_b8_complete == 2
         , ivp_b9_complete == 2
         , ivp_c2_complete == 2
         , ivp_d1_complete == 2
         , ivp_d2_complete == 2)
# Block records that shouldn't go to NACC b/c of unverified forms
df_ivp_block <- df_ivp_raw %>% 
  filter(redcap_event_name == "visit_1_arm_1") %>% 
  select(ptid
         , redcap_event_name 
         , form_date
         , header_complete
         , ivp_a1_complete
         , ivp_a2_complete
         , ivp_a3_complete
         , ivp_a4_complete
         , ivp_a5_complete
         , ivp_b1_complete
         , ivp_b4_complete
         , ivp_b5_complete
         , ivp_b6_complete
         , ivp_b7_complete
         , ivp_b8_complete
         , ivp_b9_complete
         , ivp_c2_complete
         , ivp_d1_complete
         , ivp_d2_complete) %>% 
  filter(header_complete != 2
         | ivp_a1_complete != 2
         | ivp_a2_complete != 2
         | ivp_a3_complete != 2
         | ivp_a4_complete != 2
         | ivp_a5_complete != 2
         | ivp_b1_complete != 2
         | ivp_b4_complete != 2
         | ivp_b5_complete != 2
         | ivp_b6_complete != 2
         | ivp_b7_complete != 2
         | ivp_b8_complete != 2
         | ivp_b9_complete != 2
         | ivp_c2_complete != 2
         | ivp_d1_complete != 2
         | ivp_d2_complete != 2)


# WRITE TO CSV ---- 
write_csv(df_ivp, 
          paste0("~/Box\ Sync/Documents/NACCulator/NACCulator ", 
                 date, "/NACC30_iv_", date, ".csv"), 
          na = "")

write_csv(df_ivp_block,
          paste0("~/Box\ Sync/Documents/NACCulator/NACCulator ", 
                 date, "/BLOCKED_NACC30_iv_", date, ".csv"), 
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
           "'NACCulator ", date, "'/NACC30_iv_", date, ".csv",
           " > ", ncltr_path, 
           "'NACCulator ", date, "'/NACC30_iv_", date, "_FILTERED.csv"))

system(
  command =
    paste0("PYTHONPATH=", ncltr_path, "nacculator/",
           " ", ncltr_path, "nacculator/nacc/redcap2nacc.py",
           " -ivp",
           " -file ", ncltr_path, 
           "'NACCulator ", date, "'/NACC30_iv_", date, "_FILTERED.csv",
           " > ", ncltr_path, 
           "'NACCulator ", date, "'/NACC30_iv_", date, ".txt"))


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
