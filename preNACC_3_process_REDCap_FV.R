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
suppressMessages( library(crayon) )
cat(green("Loading R packages...\n"))
suppressMessages( library(dplyr) )
suppressMessages( library(readr) )
suppressMessages( library(stringr) )
suppressMessages( library(lubridate) )


# USEFUL GLOBALS AND FUNCTIONS ----
cat(green("Loading globals and helper functions...\n"))
source("~/Box/Documents/R_helpers/config.R")
source("~/Box/Documents/R_helpers/helpers.R")
today_char <- as.character(today())
path_nacc_push <- paste0(here::here(), "/NACC_pushes")
path_nacc_push_today <- paste0(path_nacc_push, "/push_", today_char)


# LOAD FOLLOW-UP VISIT RECORDS ----
cat(green("Loading follow-up visit records...\n"))
records_fvp_raw <-
  read_csv(paste0(path_nacc_push_today,
                  "/df_redcap_yes_nacc_no_", today_char, ".csv"),
           col_types = cols(.default = col_guess())) %>% 
  filter(redcap_event_name != "visit_1_arm_1" & visitnum != "001") %>%
  pull(ptid)

records_fvp <- records_fvp_raw %>% paste(collapse = ",")


# GET REDCap DATA via API ----
cat(green("Retrieving latest UDS 3 data via REDCap API...\n"))
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
  export_redcap_records(uri     = REDCAP_API_URI,
                        token   = REDCAP_API_TOKEN_UDS3n,
                        forms   = forms_fvp,
                        records = records_fvp,
                        vp      = FALSE)

# Parse JSON to df
df_fvp_raw <- jsonlite::fromJSON(json_fvp) %>% as_tibble() %>% na_if("")


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
         , fvp_d2_complete == 2) %>% 
  # Coalesce A5 `fu_arthtype___X` fields to `fu_arthtype`
  mutate(fu_arthtype = case_when(
    fu_arthtype___1 == 1 ~ 1L,
    fu_arthtype___2 == 1 ~ 2L,
    fu_arthtype___3 == 3 ~ 3L,
    fu_arthtype___9 == 9 ~ 9L,
    TRUE ~ NA_integer_
  )) %>%
  select(-fu_arthtype___1, -fu_arthtype___2, 
         -fu_arthtype___3, -fu_arthtype___9) %>% 
  get_visit_n(ptid, form_date, Inf) %>% 
  # Coalesce D2 `fu_artype___X` fields to `fu_artype`
  mutate(fu_artype = case_when(
    fu_artype___1 == 1 ~ 1L,
    fu_artype___2 == 1 ~ 2L,
    fu_artype___3 == 3 ~ 3L,
    fu_artype___9 == 9 ~ 9L,
    TRUE ~ NA_integer_
  )) %>%
  select(-fu_artype___1, -fu_artype___2, -fu_artype___3, -fu_artype___9) %>% 
  get_visit_n(ptid, form_date, Inf)

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
cat(green("Writing relevant data frames to CSV...\n"))
write_csv(df_fvp, 
          paste0(path_nacc_push_today, 
                 "/NACC_UDS3_fvp_", today_char, ".csv"), 
          na = "")

write_csv(df_fvp_block,
          paste0(path_nacc_push_today, 
                 "/BLOCKED_NACC_UDS3_fvp_", today_char, ".csv"), 
          na = "")


# RUN NACCulator via TERMINAL COMMANDS ----
cat(green("Executing NACCulator commands...\n"))
ncltr_path <- "~/Box/Documents/nacculator"

system(
  command = 
    paste0("PYTHONPATH=", ncltr_path,
           " python2.7 ", ncltr_path, "/nacc/redcap2nacc.py", 
           " -f fixHeaders", 
           " -meta ", ncltr_path, "/nacculator_cfg_mich.ini", 
           " < ", 
           path_nacc_push_today,
           "/NACC_UDS3_fvp_", today_char, ".csv",
           " > ", 
           path_nacc_push_today,
           "/NACC_UDS3_fvp_", today_char, "_FILTERED.csv"))

system(
  command =
    paste0("PYTHONPATH=", ncltr_path,
           " python2.7 ", ncltr_path, "/nacc/redcap2nacc.py",
           " -fvp",
           " -file ",
           path_nacc_push_today,
           "/NACC_UDS3_fvp_", today_char, "_FILTERED.csv",
           " > ", 
           path_nacc_push_today,
           "/NACC_UDS3_fvp_", today_char, ".txt"))

cat(cyan("\nDone.\n\n"))


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
