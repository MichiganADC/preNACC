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
today_char <- as.character(today(tzone = "EST"))
path_nacc_push <- paste0(here::here(), "/NACC_pushes")
path_nacc_push_today <- paste0(path_nacc_push, "/push_", today_char)


# LOAD TELEPHONE FOLLOW-UP VISIT RECORDS ----
cat(green("Loading telephone follow-up visit records...\n"))

# records_tvp_raw <-
#   read_csv(paste0(path_nacc_push_today,
#                   "/df_redcap_yes_nacc_no_", today_char, ".csv"),
#            col_types = cols(.default = col_guess())) %>% 
#   filter(redcap_event_name != "visit_1_arm_1",
#          visitnum != "001",
#          tvp_z1_complete == "2") %>%
#   pull(ptid)
# 
# records_tvp <- records_tvp_raw %>% paste(collapse = ",")

df_redcap_tvp <-
  read_csv(paste0(path_nacc_push_today,
                  "/df_redcap_yes_nacc_no_", today_char, ".csv"),
           col_types = cols(.default = col_guess())) %>% 
  filter(redcap_event_name != "visit_1_arm_1",
         visitnum != "001",
         tvp_z1_complete == "2") %>% 
  select(ptid, visitnum)

records_tvp <- df_redcap_tvp %>% pull(ptid) %>% paste(collapse = ",")


# GET REDCap DATA via API ----
cat(green("Retrieving latest UDS 3 data via REDCap API...\n"))
forms_tvp_raw <- c(
  "header"
  , "ivp_a1" # needed for NACCulator to work
  , "tvp_t1"
  , "tvp_a1"
  , "tvp_a2"
  , "tvp_a3"
  , "tvp_a4"
  # , "tvp_a5"
  # , "tvp_b1"
  , "tvp_b4"
  , "tvp_b5"
  # , "tvp_b6"
  , "tvp_b7"
  # , "tvp_b8"
  , "tvp_b9"
  # , "tvp_c1"
  , "tvp_c2t"
  , "tvp_d1"
  , "tvp_d2"
  , "tvp_z1"
)

forms_tvp <- forms_tvp_raw %>% paste(collapse = ",")

# Get the JSON
json_tvp <- export_redcap_records(
  uri     = REDCAP_API_URI,
  token   = REDCAP_API_TOKEN_UDS3n,
  forms   = forms_tvp,
  records = records_tvp,
  vp      = FALSE
)

# Parse JSON to df
df_tvp_raw <- jsonlite::fromJSON(json_tvp) %>% as_tibble() %>% na_if("")


# CLEAN DATA ----

# Get records that are ready for NACC
df_tvp <- df_tvp_raw %>%
  inner_join(df_redcap_tvp, by = c("ptid" = "ptid", "visitnum" = "visitnum")) %>%
  select(-dob, -mrn, -madc_id, -cues_id, -tb_id, -cues_tbid,
         -paper_visit_num) %>% 
  filter(redcap_event_name != "visit_1_arm_1",
         str_detect(redcap_event_name, "visit_\\d{1,2}_arm_1")) %>% 
  filter(header_complete == 2
         , tvp_t1_complete == 2
         , tvp_a1_complete == 2
         , tvp_a2_complete == 2
         , tvp_a3_complete == 2
         , tvp_a4_complete == 2
         # , tvp_a5_complete == 2
         # , tvp_b1_complete == 2
         , tvp_b4_complete == 2
         , tvp_b5_complete == 2
         # , tvp_b6_complete == 2
         , tvp_b7_complete == 2
         # , tvp_b8_complete == 2
         , tvp_b9_complete == 2
         # , tvp_c1_complete == 2
         , tvp_c2t_complete == 2
         , tvp_d1_complete == 2
         , tvp_d2_complete == 2) %>% 
  # # Coalesce A5 `fu_arthtype___X` fields to `fu_arthtype`
  # mutate(tele_arthtype = case_when(
  #   tele_arthtype___1 == 1 ~ 1L,
  #   tele_arthtype___2 == 1 ~ 2L,
  #   tele_arthtype___3 == 1 ~ 3L,
  #   tele_arthtype___9 == 1 ~ 9L,
  #   TRUE ~ NA_integer_
  # )) %>%
  # select(-tele_arthtype___1, -tele_arthtype___2, 
  #        -tele_arthtype___3, -tele_arthtype___9) %>% 
  # get_visit_n(ptid, form_date, Inf) %>% 
  # Coalesce D2 `tele_artype___X` fields to `tele_artype`
  mutate(tele_artype = case_when(
    tele_artype___1 == 1 ~ 1L,
    tele_artype___2 == 1 ~ 2L,
    tele_artype___3 == 1 ~ 3L,
    tele_artype___9 == 1 ~ 9L,
    TRUE ~ NA_integer_
  )) %>%
  select(-tele_artype___1, -tele_artype___2, -tele_artype___3, -tele_artype___9) # %>% 
  # get_visit_n(ptid, form_date, Inf)

# Block records that shouldn't go to NACC b/c of unverified forms
df_tvp_block <- df_tvp_raw %>% 
  filter(redcap_event_name != "visit_1_arm_1") %>% 
  select(ptid
         , redcap_event_name 
         , form_date
         , header_complete
         , tvp_t1_complete
         , tvp_a1_complete
         , tvp_a2_complete
         , tvp_a3_complete
         , tvp_a4_complete
         # , tvp_a5_complete
         # , tvp_b1_complete
         , tvp_b4_complete
         , tvp_b5_complete
         # , tvp_b6_complete
         , tvp_b7_complete
         # , tvp_b8_complete
         , tvp_b9_complete
         # , tvp_c1_complete
         , tvp_c2t_complete
         , tvp_d1_complete
         , tvp_d2_complete) %>% 
  filter(header_complete != 2
         | tvp_t1_complete != 2
         | tvp_a1_complete != 2
         | tvp_a2_complete != 2
         | tvp_a3_complete != 2
         | tvp_a4_complete != 2
         # | tvp_a5_complete != 2
         # | tvp_b1_complete != 2
         | tvp_b4_complete != 2
         | tvp_b5_complete != 2
         # | tvp_b6_complete != 2
         | tvp_b7_complete != 2
         # | tvp_b8_complete != 2
         | tvp_b9_complete != 2
         # | tvp_c1_complete != 2
         | tvp_c2t_complete != 2
         | tvp_d1_complete != 2
         | tvp_d2_complete != 2)


# WRITE TO CSV ---- 
cat(green("Writing relevant data frames to CSV...\n"))
write_csv(df_tvp, 
          paste0(path_nacc_push_today, 
                 "/NACC_UDS3_tvpc2_", today_char, ".csv"), 
          na = "")

write_csv(df_tvp_block,
          paste0(path_nacc_push_today, 
                 "/BLOCKED_NACC_UDS3_tvpc2_", today_char, ".csv"), 
          na = "")


# RUN NACCulator via TERMINAL COMMANDS ----
cat(green("Executing NACCulator commands...\n"))
ncltr_path <- "~/Box/Documents/nacculator_1.7.0"

ncltr_cmd_1 <- paste0(
  "PYTHONPATH=", ncltr_path, " ",
  "python3 ", ncltr_path, "/nacc/redcap2nacc.py ",
  "-f fixHeaders ",
  "-meta ", ncltr_path, "/nacculator_cfg_mich.ini ",
  "< ", path_nacc_push_today, "/NACC_UDS3_tvpc2_", today_char, ".csv",
  "> ", path_nacc_push_today, "/NACC_UDS3_tvpc2_", today_char, "_FILTERED.csv"
)
system(ncltr_cmd_1)

ncltr_cmd_2 <- paste0(
  "PYTHONPATH=", ncltr_path, " ",
  "python3 ", ncltr_path, "/nacc/redcap2nacc.py ",
  "-tfp ",
  "-file ", path_nacc_push_today, "/NACC_UDS3_tvpc2_", today_char, "_FILTERED.csv",
  "> ", path_nacc_push_today, "/NACC_UDS3_tvpc2_", today_char, ".txt"
)
system(ncltr_cmd_2)

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
