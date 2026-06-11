# Setup -------------------------------------------------------------------
rm(list=ls()) # Clears workspace

# # Install/call libraries
# install.packages("renv") # Run if you have cloned repository and don't already have renv installed
# renv::restore() # Run once after cloning repository
# renv::install("package") # Run to install new packages
# renv::snapshot() # Run after installing new packages
# renv::init() # Only run when the repository is first created, don't run on cloning an existing repository

# Data for this repository is at https://drive.google.com/drive/folders/1syX_y2lMbo-ETNBXAo24m2FWUK1q60Ux?usp=sharing

pkgs<-c("tidyverse","arrow","sf")

missing<-pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly=TRUE)]
if (length(missing)>0) {
  stop("Missing packages: ", paste(missing, collapse=", "),
       "\nRun renv::restore()")
}

invisible(lapply(pkgs, library, character.only=TRUE))
rm(pkgs, missing)


# Load data --------------------------------------------------------------
source("Data_collation.R")
dgs<-read_parquet("Data/tdata-combined/Airportvisittrends2024_allterminals.parquet")
dgs<-dgs |> 
  mutate(datetime_et=as.POSIXct(TIMESTAMP_EPOCH_MS/1000,origin="1970-01-01",tz="UTC") |> 
        lubridate::with_tz("America/New_York"),
        airport_service_date=as.Date(datetime_et - lubridate::hours(3))) # Using 3am as the split between airport "days" to avoid single airport trips being split into 2 trips

mia_enplanements_monthly<-tibble( # https://www.miami-airport.com/airport_stats.asp
  year=2024,
  month=1:12,
  month_name=month.name,
  enplanements=c(
    1418709, # January
    1346432, # February
    1478927, # March
    1360958, # April
    1354102, # May
    1228149, # June
    1313402, # July
    1224626, # August
    1065864, # September
    1121247, # October
    1215227, # November
    1346432 # December
  ))
#  %>%
#   mutate(
#     month_date=as.Date(sprintf("%04d-%02d-01", year, month))
#   ) %>%
#   select(year, month, month_name, month_date, enplanements)


# Calibration ------------------------------------------------------------
fastest_ping<-dds %>%
  mutate(
    elapsed_hr=(TIMESTAMP_EPOCH_MS - matched_anchor_ms) / (1000 * 60 * 60),
    implied_kmh=dist_km / elapsed_hr
  ) %>%
  filter(
    elapsed_hr > 1, # There are no scheduled flights shorter than an hour and low elapsed durations with bouncing pings can inflate speeds 
    dist_km >= 25, # Filtering local noise
    is.finite(implied_kmh)
  ) %>%
  group_by(REGISTRATION_ID, matched_anchor_date, matched_anchor_ms) %>%
  slice_max(implied_kmh, n=1, with_ties=FALSE) %>%
  ungroup()

hist(fastest_ping$implied_kmh,breaks = 1000)











dgs$date<-as.Date(dgs$datetime_et)
dds$date<-
movement_summary <- dgs |> 
  group_by(REGISTRATION_ID, date) %>%
  slice_min(order_by = TIMESTAMP_EPOCH_MS, n = 1, with_ties = FALSE) %>%
  rename(
    LAT_REF = LATITUDE,
    LON_REF = LONGITUDE,
    FIRST_TIME_MS = TIMESTAMP_EPOCH_MS
  ) %>%
  select(REGISTRATION_ID, date, LAT_REF, LON_REF, FIRST_TIME_MS) %>%
  inner_join(dds, by = c("REGISTRATION_ID", "date")) %>%
  filter(TIMESTAMP_EPOCH_MS > FIRST_TIME_MS) %>%
  mutate(dist_from_ref_km = distHaversine(
    cbind(LONGITUDE, LATITUDE),
    cbind(LON_REF, LAT_REF)
  )/1000) %>%
  group_by(REGISTRATION_ID, date) %>%
  summarize(
    moved_far = any(dist_from_ref_km > 40),
    max_dist_m = max(dist_from_ref_km),
    .groups = "drop"
  )
