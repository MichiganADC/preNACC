#!/usr/bin/env Rscript

#@##==---==##@   @##==---==##@    #==-- --==#    @##==---==##@   @##==---==##@#
##==---==##@   #   @##==---==##@    #==-==#    @##==---==##@   #   @##==---==##
#=---==##@    #=#    @##==---==##@    #=#    @##==---==##@    #=#    @##==---=#
#--==##@    #==-==#    @##==---==##@   #   @##==---==##@    #==-==#    @##==--#
#==##@    #==-- --==#    @##==---==##@   @##==---==##@    #==-- --==#    @##==#
###@                                                                       @###
###                 PROCESS INITIAL VISIT REDCap RECORDS                    ###
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
records_ivp_raw <-
  read_csv(paste0("~/Box Sync/Documents/preNACC/",
                  "NACCulator ", DATE_CHAR, "/",
                  "df_redcap_yes_nacc_no_", DATE_CHAR, ".csv")) %>% 
  filter(redcap_event_name == "visit_1_arm_1" & visitnum == "001") %>%
  pull(ptid)

records_ivp <- records_ivp_raw %>% paste(collapse = ",")


# GET REDCap DATA via API ----

forms_ivp_raw <-
  c(
    "header"
    , "ivp_a1"
    , "ivp_a2"
    , "ivp_a3"
    , "ivp_a4"
    , "ivp_a5"
    , "ivp_b1"
    , "ivp_b4"
    , "ivp_b5"
    , "ivp_b6"
    , "ivp_b7"
    , "ivp_b8"
    , "ivp_b9"
    , "ivp_c2"
    , "ivp_d1"
    , "ivp_d2"
    , "ivp_z1"
  )

forms_ivp <- forms_ivp_raw %>% paste(collapse = ",")

# Get the JSON
json_ivp <- 
  get_rc_data_api(uri     = REDCAP_API_URI,
                  token   = REDCAP_API_TOKEN_UDS3n,
                  forms   = forms_ivp,
                  records = records_ivp,
                  vp      = FALSE)

# Parse JSON to df
df_ivp_raw <- jsonlite::fromJSON(json_ivp) %>% as_tibble() %>%  na_if("")


# CLEAN DATA ----

# Get records that are ready for NACC
df_ivp <- df_ivp_raw %>% 
  select(-dob, -mrn, -madc_id, -cues_id, -tb_id, -cues_tbid, 
         -paper_visit_num) %>% 
  filter(redcap_event_name == "visit_1_arm_1",
         str_detect(redcap_event_name, "visit_\\d{1,2}_arm_1")) %>% 
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
          paste0("~/Box\ Sync/Documents/preNACC/NACCulator ", 
                 DATE_CHAR, "/NACC_UDS3_ivp_", DATE_CHAR, ".csv"), 
          na = "")

write_csv(df_ivp_block,
          paste0("~/Box\ Sync/Documents/preNACC/NACCulator ", 
                 DATE_CHAR, "/BLOCKED_NACC_UDS3_ivp_", DATE_CHAR, ".csv"), 
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
           "'/NACC_UDS3_ivp_", DATE_CHAR, ".csv",
           " > ", prenacc_path, 
           "'NACCulator ", DATE_CHAR, 
           "'/NACC_UDS3_ivp_", DATE_CHAR, "_FILTERED.csv"))

system(
  command =
    paste0("PYTHONPATH=", ncltr_path,
           " ", ncltr_path, "nacc/redcap2nacc.py",
           " -ivp",
           " -file ", prenacc_path, 
           "'NACCulator ", DATE_CHAR, 
           "'/NACC_UDS3_ivp_", DATE_CHAR, "_FILTERED.csv",
           " > ", prenacc_path, 
           "'NACCulator ", DATE_CHAR, 
           "'/NACC_UDS3_ivp_", DATE_CHAR, ".txt"))


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
