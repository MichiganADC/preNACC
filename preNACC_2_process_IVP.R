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
suppressMessages( library(crayon) )
cat(green("Loading R packages...\n"))
suppressMessages( library(dplyr) )
suppressMessages( library(readr) )
suppressMessages( library(gitlabr) )
suppressMessages( library(stringr) )
suppressMessages( library(lubridate) )


# USEFUL GLOBALS AND FUNCTIONS ----
cat(green("Loading globals and helper functions...\n"))
source("~/Box/Documents/R_helpers/config.R")
# source("~/Box/Documents/R_helpers/helpers.R")
michmed_gitlab <- gl_connection(
  MICHMED_GITLAB_URL,
  private_token = MICHMED_GITLAB_TOKEN,
  api_version = "v4"
)
r_helpers_src <- michmed_gitlab(
  gl_get_file,
  project = "r-helpers",
  file_path = "helpers.R"
)
eval(parse(text = r_helpers_src))
today_char <- as.character(today(tzone = "EST"))
path_nacc_push <- paste0(here::here(), "/NACC_pushes")
path_nacc_push_today <- paste0(path_nacc_push, "/push_", today_char)


# LOAD INITIAL VISIT RECORDS ----
cat(green("Loading initial visit records...\n"))

# records_ivp_raw <-
#   read_csv(paste0(path_nacc_push_today,
#                   "/df_redcap_yes_nacc_no_", today_char, ".csv"),
#            col_types = cols(.default = col_guess())) %>% 
#   filter(redcap_event_name == "visit_1_arm_1",
#          visitnum == "001",
#          ivp_z1_complete == "2") %>%
#   pull(ptid)
# 
# records_ivp <- records_ivp_raw %>% paste(collapse = ",")

df_redcap_ivp <-
  read_csv(paste0(path_nacc_push_today,
                  "/df_redcap_yes_nacc_no_", today_char, ".csv"),
           col_types = cols(.default = col_guess())) %>% 
  filter(redcap_event_name == "visit_1_arm_1",
         visitnum == "001",
         ivp_z1_complete == "2") %>% 
  select(ptid, visitnum)

records_ivp <- df_redcap_ivp %>% pull(ptid) %>% paste(collapse = ",")


# GET REDCap DATA via API ----
cat(green("Retrieving latest UDS 3 data via REDCap API...\n"))
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
json_ivp <- export_redcap_records(
  uri     = REDCAP_API_URI,
  token   = REDCAP_API_TOKEN_UDS3n,
  forms   = forms_ivp,
  records = records_ivp,
  vp      = FALSE
)

# Parse JSON to df
df_ivp_raw <- jsonlite::fromJSON(json_ivp) %>% as_tibble() %>% na_if("")


# CLEAN DATA ----

# Get records that are ready for NACC
df_ivp <- df_ivp_raw %>%
  inner_join(df_redcap_ivp, by = c("ptid" = "ptid", "visitnum" = "visitnum")) %>% 
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
         , ivp_d2_complete == 2) %>% 
  # Coalesce Form A5 `arthtype___X` fields to `arthtype`
  mutate(arthtype = case_when(
    arthtype___1 == 1 ~ 1L,
    arthtype___2 == 1 ~ 2L,
    arthtype___3 == 1 ~ 3L,
    arthtype___9 == 1 ~ 9L,
    TRUE ~ NA_integer_
  )) %>%
  select(-arthtype___1, -arthtype___2, -arthtype___3, -arthtype___9) %>% 
  # Coalesce Form D2 `artype___X` fields to `arthtype`
  mutate(artype = case_when(
    artype___1 == 1 ~ 1L,
    artype___2 == 1 ~ 2L,
    artype___3 == 1 ~ 3L,
    artype___9 == 1 ~ 9L,
    TRUE ~ NA_integer_
  )) %>%
  select(-artype___1, -artype___2, -artype___3, -artype___9)


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
cat(green("Writing relevant data frames to CSV...\n"))
write_csv(df_ivp, 
          paste0(path_nacc_push_today, 
                 "/NACC_UDS3_ivp_", today_char, ".csv"), 
          na = "")

write_csv(df_ivp_block,
          paste0(path_nacc_push_today, 
                 "/BLOCKED_NACC_UDS3_ivp_", today_char, ".csv"), 
          na = "")


# RUN NACCulator via TERMINAL COMMANDS ----
cat(green("Executing NACCulator commands...\n"))
ncltr_path <- "~/Box/Documents/nacculator_1.7.0"

ncltr_cmd_1 <- paste0(
  "PYTHONPATH=", ncltr_path, " ",
  "python3 ", ncltr_path, "/nacc/redcap2nacc.py ",
  "-f fixHeaders ",
  "-meta ", ncltr_path, "/nacculator_cfg_mich.ini ",
  "< ", path_nacc_push_today, "/NACC_UDS3_ivp_", today_char, ".csv",
  "> ", path_nacc_push_today, "/NACC_UDS3_ivp_", today_char, "_FILTERED.csv"
)
system(ncltr_cmd_1)

ncltr_cmd_2 <- paste0(
  "PYTHONPATH=", ncltr_path, " ",
  "python3 ", ncltr_path, "/nacc/redcap2nacc.py ",
  "-ivp ",
  "-file ", path_nacc_push_today, "/NACC_UDS3_ivp_", today_char, "_FILTERED.csv",
  "> ", path_nacc_push_today, "/NACC_UDS3_ivp_", today_char, ".txt"
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
