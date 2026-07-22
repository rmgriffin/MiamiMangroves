# Setup -------------------------------------------------------------------
rm(list=ls()) # Clears workspace

# # Install/call libraries
# install.packages("renv") # Run if you have cloned repository and don't already have renv installed
# renv::restore() # Run once after cloning repository
# renv::install("package") # Run to install new packages
# renv::snapshot() # Run after installing new packages
# renv::init() # Only run when the repository is first created, don't run on cloning an existing repository

# Data for this repository is at https://drive.google.com/drive/folders/1syX_y2lMbo-ETNBXAo24m2FWUK1q60Ux?usp=sharing

pkgs<-c("tidyverse","arrow","sf","osrm")

missing<-pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly=TRUE)]
if (length(missing)>0) {
  stop("Missing packages: ", paste(missing, collapse=", "),
       "\nRun renv::restore()")
}

invisible(lapply(pkgs, library, character.only=TRUE))
rm(pkgs, missing)


# Script options ---------------------------------------------------------
options(osrm.server="http://127.0.0.1:5000/") # docker run --rm -t -i -p 5000:5000 -v osrm_us_data:/data osrm/osrm-backend osrm-routed --max-table-size 10000 /data/us-latest.osrm
options(osrm.profile="driving")


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
    1310335, # February
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
max_implied_kmh<-1200
drive_time_buffer<-1.25 # 1.25 means a car is allowed to be 25% faster than OSRM's modeled duration (accounting for favorable traffic, speeding, etc.)
osrm_batch_size<-5000

fastest_ping<-dds %>%
  mutate(
    elapsed_hr=(TIMESTAMP_EPOCH_MS - matched_anchor_ms) / (1000 * 60 * 60),
    implied_kmh=dist_km / elapsed_hr
  ) %>%
  filter(
    elapsed_hr > 1,
    dist_km >= 25,
    is.finite(implied_kmh),
    implied_kmh <= max_implied_kmh
  ) %>%
  group_by(REGISTRATION_ID, matched_anchor_date, matched_anchor_ms) %>%
  slice_max(implied_kmh, n=1, with_ties=FALSE) %>%
  ungroup()

# fastest_ping_without_speed_cutoff<-dds %>%
#   mutate(
#     elapsed_hr=(TIMESTAMP_EPOCH_MS - matched_anchor_ms) / (1000 * 60 * 60),
#     implied_kmh=dist_km / elapsed_hr
#   ) %>%
#   filter(
#     elapsed_hr > 1,
#     dist_km >= 25,
#     is.finite(implied_kmh)
#   ) %>%
#   group_by(REGISTRATION_ID, matched_anchor_date, matched_anchor_ms) %>%
#   slice_max(implied_kmh, n=1, with_ties=FALSE) %>%
#   ungroup()

# tibble( # How many device days are lost due to upper limit implied speed cutoff?
#   n_device_days_no_cutoff=nrow(fastest_ping_without_speed_cutoff),
#   n_device_days_with_cutoff=nrow(fastest_ping),
#   n_device_days_lost_due_to_cutoff=nrow(fastest_ping_without_speed_cutoff) - nrow(fastest_ping),
#   pct_device_days_lost_due_to_cutoff=100 * (nrow(fastest_ping_without_speed_cutoff) - nrow(fastest_ping)) / nrow(fastest_ping_without_speed_cutoff)
# )

#hist(fastest_ping$implied_kmh,breaks = 1000)

fastest_ping<-fastest_ping %>%
  mutate(
    dest_region=case_when(
      LATITUDE >= 18 & LATITUDE <= 23 &
        LONGITUDE >= -161 & LONGITUDE <= -154 ~ "Hawaii",
      LATITUDE >= 51 & LATITUDE <= 72 &
        LONGITUDE >= -180 & LONGITUDE <= -129 ~ "Alaska",
      LATITUDE >= 17.8 & LATITUDE <= 18.6 &
        LONGITUDE >= -67.5 & LONGITUDE <= -65.1 ~ "Puerto_Rico",
      LATITUDE >= 17.5 & LATITUDE <= 18.5 &
        LONGITUDE >= -65.2 & LONGITUDE <= -64.4 ~ "USVI",
      LATITUDE >= 13.1 & LATITUDE <= 13.8 &
        LONGITUDE >= 144.5 & LONGITUDE <= 145.1 ~ "Guam",
      LATITUDE >= 14.0 & LATITUDE <= 21.0 &
        LONGITUDE >= 144.8 & LONGITUDE <= 146.2 ~ "CNMI",
      LATITUDE >= 24 & LATITUDE <= 50 &
        LONGITUDE >= -125 & LONGITUDE <= -66 ~ "CONUS",
      TRUE ~ "other_or_unclassified"), # Only a handful, over water near caribbean islands
    osrm_query=dest_region=="CONUS")


fastest_ping_conus<-fastest_ping %>%
  filter(osrm_query) %>%
  mutate(osrm_row_id=row_number())

fastest_ping_nonconus<-fastest_ping %>%
  filter(!osrm_query) %>%
  mutate(
    osrm_status="not_queried_nonconus",
    osrm_error=NA_character_,
    osrm_distance_km=NA_real_,
    osrm_duration_min=NA_real_,
    osrm_duration_hr=NA_real_,
    osrm_buffered_duration_hr=NA_real_,
    enplaned=TRUE,
    enplanement_rule="nonconus_treated_as_enplanement"
  )

mia_origin<-fastest_ping_conus %>%
  summarise(
    lon=median(anchor_lon, na.rm=TRUE),
    lat=median(anchor_lat, na.rm=TRUE)
  ) %>%
  mutate(id="MIA") %>%
  select(id, lon, lat) %>%
  st_as_sf(coords=c("lon","lat"), crs=4326)

dst_all<-fastest_ping_conus %>%
  transmute(
    osrm_row_id=as.character(osrm_row_id),
    lon=LONGITUDE,
    lat=LATITUDE
  )

get_osrm_table_batch<-function(dst_df) {
  
  dst<-dst_df %>%
    transmute(id=osrm_row_id, lon, lat) %>%
    st_as_sf(coords=c("lon","lat"), crs=4326)
  
  out<-tryCatch(
    osrmTable(
      src=mia_origin,
      dst=dst,
      measure=c("duration","distance")
    ),
    error=function(e) e
  )
  
  if(inherits(out, "error")) {
    return(tibble(
      osrm_row_id=dst_df$osrm_row_id,
      osrm_status="request_error",
      osrm_error=conditionMessage(out),
      osrm_duration_min=NA_real_,
      osrm_distance_km=NA_real_
    ))
  }
  
  tibble(
    osrm_row_id=dst_df$osrm_row_id,
    osrm_status="Ok",
    osrm_error=NA_character_,
    osrm_duration_min=as.numeric(out$durations[1, seq_len(nrow(dst_df))]),
    osrm_distance_km=as.numeric(out$distances[1, seq_len(nrow(dst_df))])
  )
}

dst_batches<-split(
  dst_all,
  ceiling(seq_len(nrow(dst_all)) / osrm_batch_size)
)

system.time({
  osrm_results<-map_dfr(dst_batches, get_osrm_table_batch)
})

fastest_ping_conus_osrm<-fastest_ping_conus %>%
  mutate(osrm_row_id=as.character(osrm_row_id)) %>%
  left_join(osrm_results, by="osrm_row_id") %>%
  mutate(
    osrm_duration_hr=osrm_duration_min / 60,
    osrm_buffered_duration_hr=osrm_duration_hr / drive_time_buffer,
    enplaned=case_when(
      osrm_status=="Ok" & elapsed_hr < osrm_buffered_duration_hr ~ TRUE,
      osrm_status=="Ok" ~ FALSE,
      TRUE ~ NA
    ),
    enplanement_rule=case_when(
      osrm_status=="Ok" & enplaned ~ "conus_osrm_drive_time_infeasible",
      osrm_status=="Ok" & !enplaned ~ "conus_osrm_drive_time_plausible",
      TRUE ~ "conus_osrm_failed"
    )
  ) %>%
  select(-osrm_row_id)

fastest_ping_osrm<-bind_rows(
  fastest_ping_conus_osrm,
  fastest_ping_nonconus
) %>%
  arrange(matched_anchor_date, REGISTRATION_ID, matched_anchor_ms)

enplanement_by_dist<-fastest_ping_osrm %>%
  mutate(
    dist_bin=cut(
      dist_km,
      breaks=c(0, 25, 50, 75, 100, 150, 200, 300, 500, 750, 1000, 2000, 3000, Inf),
      include.lowest=TRUE,
      right=FALSE
    )
  ) %>%
  group_by(dist_bin) %>%
  summarise(
    n=n(),
    n_classified=sum(!is.na(enplaned)),
    n_enplaned=sum(enplaned, na.rm=TRUE),
    n_not_enplaned=sum(enplaned==FALSE, na.rm=TRUE),
    n_unknown=sum(is.na(enplaned)),
    p_enplaned=n_enplaned / n_classified,
    dist_median_km=median(dist_km, na.rm=TRUE),
    elapsed_median_hr=median(elapsed_hr, na.rm=TRUE),
    osrm_duration_median_hr=median(osrm_duration_hr, na.rm=TRUE),
    osrm_buffered_duration_median_hr=median(osrm_buffered_duration_hr, na.rm=TRUE),
    .groups="drop"
  ) %>%
  arrange(dist_median_km)

ggplot(enplanement_by_dist, aes(x=dist_median_km, y=p_enplaned)) +
  geom_line() +
  geom_point() +
  geom_text(
    aes(label=paste0("n=", round(n_classified / 1000, 1), "k")),
    vjust=-0.7,
    size=5
  ) +
  scale_x_log10() +
  scale_y_continuous(labels=scales::percent_format()) +
  labs(
    x="Distance from MIA to fastest ping, km",
    y="Share classified as enplaned"
  )

conus_dist_margin<-fastest_ping_osrm %>%
  filter(dest_region=="CONUS", osrm_status=="Ok") %>%
  mutate(
    dist_bin=cut(dist_km, breaks=c(0, 25, 50, 75, 100, 150, 200, 300, 500, 750, 1000, 2000, 3000, Inf), include.lowest=TRUE, right=FALSE),
    drive_time_margin_hr=osrm_buffered_duration_hr - elapsed_hr
  ) %>%
  group_by(dist_bin) %>%
  summarise(
    n=n(),
    p_enplaned=mean(enplaned, na.rm=TRUE),
    dist_median_km=median(dist_km, na.rm=TRUE),
    median_drive_time_margin_hr=median(drive_time_margin_hr, na.rm=TRUE),
    p25_drive_time_margin_hr=quantile(drive_time_margin_hr, 0.25, na.rm=TRUE),
    p75_drive_time_margin_hr=quantile(drive_time_margin_hr, 0.75, na.rm=TRUE),
    .groups="drop")

ggplot(conus_dist_margin, aes(x=dist_median_km, y=median_drive_time_margin_hr)) +
  geom_hline(yintercept=0, linetype="dashed") +
  geom_line() +
  geom_point() +
  geom_text(
    aes(label=paste0("n=", round(n / 1000, 1), "k")),
    vjust=-0.7,
    size=5
  ) +
  scale_x_log10() +
  labs(
    x="Distance from MIA to fastest ping, km",
    y="Median drive-time margin, hours")

monthly_enplanements_device_data<-fastest_ping_osrm %>%
  mutate(
    month_date=as.Date(format(as.Date(matched_anchor_date), "%Y-%m-01"))
  ) %>%
  group_by(month_date) %>%
  summarise(
    n_enplaned=sum(enplaned, na.rm=TRUE),
    .groups="drop"
  ) %>%
  arrange(month_date)

mia_enplanements_monthly<-mia_enplanements_monthly %>%
  left_join(
    fastest_ping_osrm %>%
      mutate(month=lubridate::month(as.Date(matched_anchor_date))) %>%
      group_by(month) %>%
      summarise(device_enplanements=sum(enplaned, na.rm=TRUE), .groups="drop"),
    by="month"
  ) %>%
  mutate(
    device_enplanements=coalesce(device_enplanements, 0),
    enplanementperdevice=enplanements/device_enplanements
  )
