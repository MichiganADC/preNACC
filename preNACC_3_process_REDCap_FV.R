#!/usr/bin/env Rscript

#@##==---==##@   @##==---==##@    #==-- --==#    @##==---==##@   @##==---==##@#
##==---==##@   #   @##==---==##@    #==-==#    @##==---==##@   #   @##==---==##
#=---==##@    #=#    @##==---==##@    #=#    @##==---==##@    #=#    @##==---=#
#--==##@    #==-==#    @##==---==##@   #   @##==---==##@    #==-==#    @##==--#
#==##@    #==-- --==#    @##==---==##@   @##==---==##@    #==-- --==#    @##==#
###@                                                                       @###
###                PROCESS FOLLOWUP VISIT REDCap RECORDS                    ###
###             Local REDCap Data => CSV => NACCulator => TXT               ###
###@                                                                       @###
#==##@    #==-- --==#    @##==---==##@   @##==---==##@    #==-- --==#    @##==#
#--==##@    #==-==#    @##==---==##@   #   @##==---==##@    #==-==#    @##==--#
#=---==##@    #=#    @##==---==##@    #=#    @##==---==##@    #=#    @##==---=#
##==---==##@   #   @##==---==##@    #==-==#    @##==---==##@   #   @##==---==##
#@##==---==##@   @##==---==##@    #==-- --==#    @##==---==##@   @##==---==##@#


# USEFUL LIBRARIES ----
library(dplyr)
library(readr)
library(stringr)
library(lubridate)


# USEFUL GLOBALS AND FUNCTIONS ----
source("~/Box Sync/Documents/R_helpers/config.R")
source("~/Box Sync/Documents/R_helpers/helpers.R")
DATE_CHAR <- as.character(Sys.Date())


# LOAD INITIAL VISIT RECORDS ----
records_fvp_raw <-
  read_csv(paste0("~/Box Sync/Documents/preNACC/",
                  "NACCulator ", DATE_CHAR, "/",
                  "df_redcap_yes_nacc_no_", DATE_CHAR, ".csv")) %>% 
  filter(redcap_event_name != "visit_1_arm_1" & visitnum != "001") %>%
  pull(ptid)

records_fvp <- records_fvp_raw %>% paste(collapse = ",")


# GET REDCap DATA via API ----

forms_fvp_raw <-
  c(
    "header"
    , "ivp_a1" # needed for NACCulator to work
    , "fvp_a1"
    , "fvp_a2"
    , "fvp_a3"
    , "fvp_a4"
    , "fvp_a5"
    , "fvp_b1"
    , "fvp_b4"
    , "fvp_b5"
    , "fvp_b6"
    , "fvp_b7"
    , "fvp_b8"
    , "fvp_b9"
    , "fvp_c1"
    , "fvp_c2"
    , "fvp_d1"
    , "fvp_d2"
    , "fvp_z1"
  )

forms_fvp <- forms_fvp_raw %>% paste(collapse = ",")

# Get the JSON
json_fvp <- 
  get_rc_data_api(uri     = REDCAP_API_URI,
                  token   = REDCAP_API_TOKEN_UDS3n,
                  forms   = forms_fvp,
                  records = records_fvp,
                  vp      = FALSE)

# Parse JSON to df
df_fvp_raw <- jsonlite::fromJSON(json_fvp) %>% as_tibble() %>%  na_if("")


# CLEAN DATA ----

# Get records that are ready for NACC
df_fvp <- df_fvp_raw %>% 
  select(-dob, -mrn, -madc_id, -cues_id, -tb_id, -cues_tbid,
         -paper_visit_num) %>% 
  filter(redcap_event_name != "visit_1_arm_1",
         str_detect(redcap_event_name, "visit_\\d{1,2}_arm_1")) %>% 
  filter(header_complete == 2
         , fvp_a1_complete == 2
         , fvp_a2_complete == 2
         , fvp_a3_complete == 2
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
         | fvp_a2_complete != 2
         | fvp_a3_complete != 2
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
          paste0("~/Box\ Sync/Documents/preNACC/NACCulator ", 
                 DATE_CHAR, "/NACC_UDS3_fvp_", DATE_CHAR, ".csv"), 
          na = "")

write_csv(df_fvp_block,
          paste0("~/Box\ Sync/Documents/preNACC/NACCulator ", 
                 DATE_CHAR, "/BLOCKED_NACC_UDS3_fvp_", DATE_CHAR, ".csv"), 
          na = "")


# RUN NACCulator via TERMINAL COMMANDS ----

ncltr_path <- "~/'Box Sync'/Documents/nacculator/"
prenacc_path <- "~/'Box Sync'/Documents/preNACC/"

system(
  command = 
    paste0("PYTHONPATH=", ncltr_path,
           " ", ncltr_path, "nacc/redcap2nacc.py", 
           " -f fixHeaders", 
           " -meta ", ncltr_path, "nacculator_cfg_mich.ini", 
           " < ", prenacc_path, 
           "'NACCulator ", DATE_CHAR, 
           "'/NACC_UDS3_fvp_", DATE_CHAR, ".csv",
           " > ", prenacc_path, 
           "'NACCulator ", DATE_CHAR, 
           "'/NACC_UDS3_fvp_", DATE_CHAR, "_FILTERED.csv"))

system(
  command =
    paste0("PYTHONPATH=", ncltr_path,
           " ", ncltr_path, "nacc/redcap2nacc.py",
           " -fvp",
           " -file ", prenacc_path, 
           "'NACCulator ", DATE_CHAR, 
           "'/NACC_UDS3_fvp_", DATE_CHAR, "_FILTERED.csv",
           " > ", prenacc_path, 
           "'NACCulator ", DATE_CHAR, 
           "'/NACC_UDS3_fvp_", DATE_CHAR, ".txt"))


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
