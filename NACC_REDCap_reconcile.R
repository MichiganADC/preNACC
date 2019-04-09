#!/usr/bin/env Rscript

# USEFUL LIBRARIES
library(dplyr)
library(readr)
library(stringr)
library(lubridate)

# USEFUL GLOBALS AND FUNCTIONS
# source("~/Desktop/config.R")
# source("~/Desktop/helpers.R")
source("~/Box Sync/Documents/R_helpers/config.R")
source("~/Box Sync/Documents/R_helpers/helpers.R")

date_chr <- as.character(Sys.Date())

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
  warning(paste0("\n",
                 "---------======== WARNING =======---------\n", 
                 "The NACC A1 Form CSV was not created today\n",
                 "---------======== WARNING =======---------\n"))
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
                  .opts  = list(ssl.verifypeer = FALSE, verbose = FALSE))

df_u3 <- jsonlite::fromJSON(json_u3) %>% na_if("")


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


# GET PARTICIPANT-VISITS TO UPLOAD ----

# Create df of records in NACC that aren't in REDCap UDS 3
df_upload_conflict <- anti_join(df_nacc_a1_cln, df_u3_cln, 
                                # by = c("ptid", "form_date")) %>%
                                by = c("ptid", "form_date", "visitnum")) %>% 
  select(ptid, form_date, visitnum)

# If there aren't mismatched records in `df_upload_conflict`, 
# create df of records that need to be processed and uploaded;
# else throw a warning but create the df w/o the records in `df_upload_conflict`
if (nrow(df_upload_conflict) == 0) { 
  df_upload_ready <- anti_join(df_u3_cln, df_nacc_a1_cln, 
                               # by = c("ptid", "form_date")) %>%
                               by = c("ptid", "form_date", "visitnum")) %>% 
    select(ptid, form_date, visitnum)
} else {
  warning(paste0("There are records in NACC that are NOT in REDCap UDS 3.\n",
                 "These records have been removed from the upload df.\n",
                 "Resolve these discrepancies before doing a NACC push.\n"))
  df_upload_ready <- anti_join(df_u3_cln, df_nacc_a1_cln, 
                               # by = c("ptid", "form_date")) %>%
                               by = c("ptid", "form_date", "visitnum")) %>% 
    select(ptid, redcap_event_name, form_date, visitnum) %>% 
    filter(!(ptid %in% pull(distinct(df_upload_conflict, ptid))))
}


# WRITE DFs TO CSV ----
if (!dir.exists(paste0("./NACCulator ", date_chr))) {
  system(command = paste0("mkdir ",
                          "~/'Box Sync'/Documents/NACCulator/",
                          "'NACCulator ", date_chr, "'"))
}

write_csv(df_upload_conflict,
                 paste0("./NACCulator ", date_chr, "/",
                        "df_upload_conflict_", date_chr, ".csv"),
                 na = "")

write_csv(df_upload_ready, 
                 paste0("./NACCulator ", date_chr, "/",
                        "df_upload_ready_", date_chr, ".csv"), 
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
