#!/usr/bin/env Rscript

#@##==---==##@   @##==---==##@    #==-- --==#    @##==---==##@   @##==---==##@#
##==---==##@   #   @##==---==##@    #==-==#    @##==---==##@   #   @##==---==##
#=---==##@    #=#    @##==---==##@    #=#    @##==---==##@    #=#    @##==---=#
#--==##@    #==-==#    @##==---==##@   #   @##==---==##@    #==-==#    @##==--#
#==##@    #==-- --==#    @##==---==##@   @##==---==##@    #==-- --==#    @##==#
###@                                                                       @###
###                           RECONCILE RECORDS                             ###
###                 Local REDCap Data vs. National NACC Data                ###
###@                                                                       @###
#==##@    #==-- --==#    @##==---==##@   @##==---==##@    #==-- --==#    @##==#
#--==##@    #==-==#    @##==---==##@   #   @##==---==##@    #==-==#    @##==--#
#=---==##@    #=#    @##==---==##@    #=#    @##==---==##@    #=#    @##==---=#
##==---==##@   #   @##==---==##@    #==-==#    @##==---==##@   #   @##==---==##
#@##==---==##@   @##==---==##@    #==-- --==#    @##==---==##@   @##==---==##@#


# USEFUL LIBRARIES ----
library(dplyr)
library(readr)
library(crayon)
library(stringr)
library(lubridate)


# USEFUL GLOBALS AND FUNCTIONS
source("~/Box Sync/Documents/R_helpers/config.R")
source("~/Box Sync/Documents/R_helpers/helpers.R")
DATE_CHAR <- as.character(Sys.Date())

# GET DATA ----

# _ Latest NACC data ----

# Downloaded NACC Form A1 CSV should include:
# - Form A1
# - Working and Current packets
# - UDS Version 3 only
# - All UDS packets
# - Include headers

# Find latest NACC Form A1 CSV
df_nacc_a1_csvs <- 
  file.info(paste0("./NACC_A1_data/", list.files("./NACC_A1_data/"))) %>% 
  as_tibble(rownames = "filepath") %>%
  filter(!isdir) %>% 
  select(filepath, ctime)

df_nacc_a1_csvs_latest <- 
  df_nacc_a1_csvs %>% 
  arrange(desc(ctime)) %>% 
  mutate(cdate = as_date(ctime)) %>% 
  slice(1L)

# Throw a warning if the latest NACC Form A1 CSV wasn't downloaded today
if (df_nacc_a1_csvs_latest[["cdate"]] != Sys.Date()) {
  warning(
    bold(
      cyan(
        paste0("\n",
               strrep(" ", 19),
               "---------======== WARNING =======---------\n", 
               strrep(" ", 19),
               "The NACC A1 Form CSV was not created today\n",
               strrep(" ", 19),
               "---------======== WARNING =======---------\n"))))
}

filepath_latest_nacc_a1 <- 
  df_nacc_a1_csvs_latest %>% 
  pull(filepath)

# Read the NACC Form A1 CSV
df_nacc_a1 <-
  read_csv(filepath_latest_nacc_a1,
           col_types = cols(.default = col_character(),
                            VISITMO  = col_integer(),
                            VISITDAY = col_integer(),
                            VISITYR  = col_integer()))

# Clean the data from the NACC Form A1 CSV
df_nacc_a1_cln <- df_nacc_a1 %>% 
  select(PTID, VISITMO, VISITDAY, VISITYR, VISITNUM) %>% 
  transmute(ptid = PTID,
            form_date = mdy(paste0(VISITMO, "-", VISITDAY, "-", VISITYR)),
            visitnum = VISITNUM)

# _ REDCap UDS 3 data ----

fields_u3_raw <-
  c(
    "ptid"
    , "form_date"
    , "visitnum"
    , "ivp_z1_complete"
    , "fvp_z1_complete"
    , "tvp_z1_complete"
    , "m1_complete"
  )

fields_u3 <- fields_u3_raw %>% paste(collapse = ",")

json_u3 <- 
  get_rc_data_api(uri    = REDCAP_API_URI,
                  token  = REDCAP_API_TOKEN_UDS3n,
                  fields = fields_u3,
                  vp     = FALSE)

df_u3 <- jsonlite::fromJSON(json_u3) %>% as_tibble() %>% na_if("")


# CLEAN DATA ----

df_u3_cln <- df_u3 %>% 
  # Clean out DDE records (__1, __2)
  filter(str_detect(ptid, "^UM\\d{8}$")) %>%
  # Coerce `form_date` to date
  mutate(form_date = lubridate::as_date(form_date)) %>% 
  # Clean out records missing `form_date`s
  filter(!is.na(form_date)) %>%
  # Coerce `_complete` fields to integer
  mutate_at(vars(ends_with("_complete")), as.integer) %>% 
  # Clean out incomplete records
  filter(ivp_z1_complete == 2L |
           fvp_z1_complete == 2L |
           tvp_z1_complete == 2L |
           m1_complete == 2L)


# GET PARTICIPANT-VISITS TO MATCH, UPLOAD, OR RESOLVE ----

#              NACC
#         | No  || Yes |
#  R  ----+-----++-----+
#  E  No  | --- ||  O  |
#  D  ----+-----++-----+
#  C  ----+-----++-----+
#  a  Yes |  O  ||  O  |
#  p  ----+-----++-----+

# REDCap No  + NACC No  : Invisible & irrelevant
# REDCap No  + NACC Yes : Discuss/resolve with data manager
# REDCap Yes + NACC No  : Push records from REDCap to NACC
# REDCap Yes + NACC Yes : Records match in both DBs; nothing to do

# REDCap No  + NACC No  : Invisible & irrelevant
df_redcap_no_nacc_no <- 
  tibble(ptid = NA, form_date = NA, visitnum = NA, .rows = 0)

# REDCap No  + NACC Yes : Discuss/resolve with data manager
df_redcap_no_nacc_yes <- 
  anti_join(df_nacc_a1_cln, df_u3_cln, 
            by = c("ptid", "form_date", "visitnum")) %>% 
  select(ptid, form_date, visitnum)

# REDCap Yes + NACC No  : Push records from REDCap to NACC
df_redcap_yes_nacc_no <-
  anti_join(df_u3_cln, df_nacc_a1_cln, 
            by = c("ptid", "form_date", "visitnum")) %>% 
  select(ptid, form_date, visitnum, everything())

# REDCap Yes + NACC Yes : Records match in both DBs; nothing to do
df_redcap_yes_nacc_yes_1 <-
  semi_join(df_nacc_a1_cln, df_u3_cln, 
            by = c("ptid", "form_date", "visitnum")) %>% 
  select(ptid, form_date, visitnum) %>% 
  arrange(ptid, form_date, visitnum)
df_redcap_yes_nacc_yes_2 <-
  semi_join(df_u3_cln, df_nacc_a1_cln, 
            by = c("ptid", "form_date", "visitnum")) %>% 
  select(ptid, form_date, visitnum) %>% 
  arrange(ptid, form_date, visitnum)
# Verify that these match
n <- nrow(df_redcap_yes_nacc_yes_1)
identical(head(df_redcap_yes_nacc_yes_1, n), 
          head(df_redcap_yes_nacc_yes_2, n))


# WRITE DFs TO CSV ----
if (!dir.exists(paste0("./NACCulator ", DATE_CHAR))) {
  system(command = paste0("mkdir ",
                          "~/'Box Sync'/Documents/preNACC/",
                          "'NACCulator ", DATE_CHAR, "'"))
}

# If there are records in `df_redcap_no_nacc_yes`, 
# write a CSV to discuss with data manager
if (nrow(df_redcap_no_nacc_yes) != 0L) {
  warning(paste0("There are records in NACC that are NOT in REDCap UDS 3.\n",
                 "Resolve these discrepancies before doing a NACC push.\n"))
  write_csv(df_redcap_no_nacc_yes, 
            paste0("./NACCulator ", DATE_CHAR, "/", 
                   "df_redcap_no_nacc_yes_", DATE_CHAR, ".csv"), 
            na = "")
}

# Write CSV of participant-visit records that need to be uploaded to NACC
# This CSV will be used as a source of records by 
write_csv(df_redcap_yes_nacc_no, 
          paste0("./NACCulator ", DATE_CHAR, "/",
                 "df_redcap_yes_nacc_no_", DATE_CHAR, ".csv"), 
          na = "")


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
