# Setup -------------------------------------------------------------------
rm(list=ls()) # Clears workspace

# # Install/call libraries
# install.packages("renv") # Run if you have cloned repository and don't already have renv installed
# renv::restore() # Run once after cloning repository
# renv::install("package") # Run to install new packages
# renv::snapshot() # Run after installing new packages
# renv::init() # Only run when the repository is first created, don't run on cloning an existing repository

# Data for this repository is at https://drive.google.com/drive/folders/1syX_y2lMbo-ETNBXAo24m2FWUK1q60Ux?usp=sharing

pkgs<-c("tidyverse","googledrive","arrow","furrr","sf","tigris","osrm")

missing<-pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly=TRUE)]
if (length(missing)>0) {
  stop("Missing packages: ", paste(missing, collapse=", "),
       "\nRun renv::restore()")
}

invisible(lapply(pkgs, library, character.only=TRUE))
rm(pkgs, missing)

# Download data ----------------------------------------------------------
# dir.create(file.path('Data'), recursive = TRUE)
# folder_url<-"https://drive.google.com/open?id=1syX_y2lMbo-ETNBXAo24m2FWUK1q60Ux"
# folder<-drive_get(as_id(folder_url))
# files<-drive_ls(folder)
# dl<-function(files){
#   walk(files, ~ drive_download(as_id(.x), overwrite = TRUE))
# }
# setwd("./Data")
# system.time(map(files$id,dl))
# system.time(unzip("GIS_data.zip", exdir = "."))
# file.remove("GIS_data.zip")
# system.time(unzip("tdata-combined.zip", exdir = "."))
# file.remove("tdata-combined.zip")
# setwd("..")
# rm(files, folder, folder_url, dl)

source("Modeling parameter settings.R") # Loading modeling parameter settings

# Loading data into workspace --------------------------------------------
pad_cbg<-function(x) { # Helper function to deal with NULLs and CBG identifiers where the leading zero was dropped
  x<-as.character(x)
  x<-trimws(x)
  x[x %in% c("", "NULL", "NA", "NaN")]<-NA_character_
  
  ifelse(
    !is.na(x) & grepl("^[0-9]+$", x) & nchar(x) < 12,
    paste0(strrep("0", 12 - nchar(x)), x),
    x
  )
}

dfs<-list.files(
  "Data/tdata-combined/",
  pattern="\\.parquet$",
  full.names=TRUE
) |>
  keep(~!str_detect(basename(.x), "single_row|batch|allterminals")) |> 
  map_dfr(function(f) {
    d.f<-read_parquet(f)
    
    d.f |> 
      mutate(
        FEATUREID=as.numeric(FEATUREID),
        CENSUS_BLOCK_GROUP_ID=pad_cbg(CENSUS_BLOCK_GROUP_ID),
        LATEST_OBSERVATION_OF_DAY=as.POSIXct(
          LATEST_OBSERVATION_OF_DAY,
          format="%Y-%m-%d %H:%M:%S",
          tz="UTC"
        ),
        EARLIEST_OBSERVATION_OF_DAY=as.POSIXct(
          EARLIEST_OBSERVATION_OF_DAY,
          format="%Y-%m-%d %H:%M:%S",
          tz="UTC"
        )
      )
  })

# dfs |> # CBG ID diagnostics
#   st_drop_geometry() |>
#   summarise(
#     n=n(),
#     n_missing=sum(is.na(CENSUS_BLOCK_GROUP_ID)),
#     min_nchar=min(nchar(CENSUS_BLOCK_GROUP_ID), na.rm=TRUE),
#     max_nchar=max(nchar(CENSUS_BLOCK_GROUP_ID), na.rm=TRUE),
#     n_12=sum(nchar(CENSUS_BLOCK_GROUP_ID) == 12, na.rm=TRUE),
#     n_not_12=sum(nchar(CENSUS_BLOCK_GROUP_ID) != 12, na.rm=TRUE)
#   )

df<-st_transform(st_read("Data/GIS_data/County_State_National_Municipal_v3.gpkg"), crs = 4326) # Merging to spatial data
df$id<-seq(1,nrow(df),1)

dfst<-left_join(dfs,df, join_by(FEATUREID == id))

# Airport visits and 24 hour window after movement
dds_path<-"Data/intermediate/airport_24h_post_terminal_observations.parquet"

dir.create(dirname(dds_path), recursive=TRUE, showWarnings=FALSE)

if(file.exists(dds_path)) {
  
  dds<-read_parquet(dds_path)
  
} else {
  mtdf<-read_parquet("Data/tdata-combined/Airportvisittrends2024_allterminals.parquet")
  mtdf<-mtdf %>%
    mutate(
      datetime_et=as.POSIXct(
        TIMESTAMP_EPOCH_MS/1000,
        origin="1970-01-01",
        tz="UTC"
      ) %>%
        lubridate::with_tz("America/New_York"),
      airport_service_date=as.Date(datetime_et - lubridate::hours(3)) # Using 3am as the split between airport "days" to avoid single airport trips being split into 2 trips
    )

  mtdf<-mtdf %>%
    arrange(REGISTRATION_ID, airport_service_date, desc(TIMESTAMP_EPOCH_MS)) %>%
    distinct(REGISTRATION_ID, airport_service_date, .keep_all=TRUE)

  haversine_m <- function(lat1, lon1, lat2, lon2) {
    r <- 6371000
    to_rad <- pi/180
    dlat <- (lat2 - lat1) * to_rad
    dlon <- (lon2 - lon1) * to_rad
    a <- sin(dlat/2)^2 + cos(lat1*to_rad) * cos(lat2*to_rad) * sin(dlon/2)^2
    2 * r * asin(pmin(1, sqrt(a)))
  }

  filter_parquet_folder <- function( # Reading in only observations within 24 hours into the future for last point seen in the terminal for a registration id on a given day 
      parquet_dir,
      ref_df,
      workers = 4
  ) {
    # 24h windows per anchor (no trimming → overlaps allowed)
    ref_windows <- ref_df %>%
      transmute(
        REGISTRATION_ID,
        start_ms   = as.numeric(TIMESTAMP_EPOCH_MS),
        end_ms     = start_ms + 24 * 3600 * 1000,
        anchor_lat = LATITUDE,
        anchor_lon = LONGITUDE
      ) %>%
      distinct()
    
    files <- list.files(parquet_dir, pattern = "^batch_.*\\.parquet$", full.names = TRUE)
    if (!length(files)) stop("No matching parquet files found")
    
    plan(multisession, workers = workers)
    
    pieces <- future_map(
      files,
      function(f) {
        ref_tab <- arrow_table(ref_windows) # Arrow objects must be constructed inside the worker
        
        tab <- tryCatch(read_parquet(f, as_data_frame = FALSE), error = function(e) NULL)
        if (is.null(tab) || tab$num_rows == 0L) return(NULL)
        
        needed <- c("REGISTRATION_ID", "TIMESTAMP_EPOCH_MS", "LATITUDE", "LONGITUDE")
        if (!all(needed %in% names(tab))) return(NULL)
        
        out<-tab %>%
          mutate(TIMESTAMP_EPOCH_MS=cast(TIMESTAMP_EPOCH_MS, float64())) %>%
          inner_join(ref_tab, by="REGISTRATION_ID") %>%
          filter(
            TIMESTAMP_EPOCH_MS > start_ms,
            TIMESTAMP_EPOCH_MS <= end_ms
          ) %>%
          mutate(
            matched_anchor_ms=start_ms
          ) %>%
          select(-start_ms, -end_ms) %>%
          collect()
        
        if (!nrow(out)) return(NULL)

        out %>%
          mutate(
            matched_anchor_time=as.POSIXct(
              matched_anchor_ms/1000,
              origin="1970-01-01",
              tz="UTC"
            ) %>%
              lubridate::with_tz("America/New_York"),
            matched_anchor_date=as.Date(matched_anchor_time),
            dist_km=haversine_m(anchor_lat, anchor_lon, LATITUDE, LONGITUDE)/1000
          )
      },
      .progress = TRUE,
      .options = furrr_options(seed = TRUE, packages = c("tidyverse","googledrive","arrow","lubridate"))
    )
    
    plan(sequential)
    pieces |> compact() |> bind_rows()
  }

  dds_time<-system.time({
      dds<-filter_parquet_folder(
        parquet_dir="Data/tdata-combined/",
        ref_df=mtdf,
        workers=10
      )
    })
    
    print(dds_time)

    write_parquet(dds, dds_path)
}


# Data cleaning ----------------------------------------------------------
dfst<-dfst |> mutate(timespan_min=(as.numeric(LATEST_OBSERVATION_OF_DAY) - as.numeric(EARLIEST_OBSERVATION_OF_DAY)) / 60) |> # Minutes between earliest and latest time in location
  filter(timespan_min > 5) |> # Dropping observations with gap in latest/earliest less than 5 minutes
  filter(CENSUS_BLOCK_GROUP_ID != "NULL")


# Multiple site visits on the same day -----------------------------------
# df_dups<-dfst %>% # Sites visited together on same day (raw data)
#   add_count(DEVICEID, DAY_IN_FEATURE, name="n_obs_device_day") %>%
#   filter(n_obs_device_day > 1) |> 
#   arrange(DEVICEID)

# poly_dup_compare<-dfst %>% # Comparison of polygon visits that are part of multi-site visits versus the only site visited per day
#   st_drop_geometry() %>%
#   count(FEATUREID, Name, Jurisdiction, name="n_total_visits") %>%
#   left_join(
#     df_dups %>%
#       st_drop_geometry() %>%
#       count(FEATUREID, Name, Jurisdiction, name="n_dup_context_visits"),
#     by=c("FEATUREID", "Name", "Jurisdiction")
#   ) %>%
#   mutate(
#     n_dup_context_visits=replace_na(n_dup_context_visits, 0L),
#     pct_dup_context=100 * n_dup_context_visits / n_total_visits
#   ) %>%
#   arrange(desc(pct_dup_context), desc(n_dup_context_visits))

# poly_sets<-dfst %>% # Sites visited together on the same day (summary)
#   sf::st_drop_geometry() %>% # remove this line if dfst is not an sf object
#   filter(!is.na(DEVICEID), !is.na(DAY_IN_FEATURE), !is.na(FEATUREID)) %>%
#   mutate(
#     polygon_name=if_else(is.na(Name), paste0("FEATUREID ", FEATUREID), Name)
#   ) %>%
#   distinct(DEVICEID, DAY_IN_FEATURE, FEATUREID, polygon_name) %>%
#   arrange(DEVICEID, DAY_IN_FEATURE, FEATUREID) %>%
#   group_by(DEVICEID, DAY_IN_FEATURE) %>%
#   summarise(
#     n_polygons=n_distinct(FEATUREID),
#     polygon_set_chr=paste(FEATUREID[!duplicated(FEATUREID)], collapse=" | "),
#     polygon_name_chr=paste(polygon_name[!duplicated(FEATUREID)], collapse=" | "),
#     .groups="drop"
#   ) %>%
#   filter(n_polygons > 1) %>%
#   count(polygon_set_chr, polygon_name_chr, n_polygons, sort=TRUE, name="n_device_days")


# Travel distance and time -----------------------------------------------
travel_distance_path<-"Data/intermediate/travel_distance_results.csv"
dir.create(dirname(travel_distance_path), recursive=TRUE, showWarnings=FALSE)

if(file.exists(travel_distance_path)) { # Checking to see if travel distance and time results exist, if so loads data, if not runs model
  
  distance_results<-read_csv(
    travel_distance_path,
    col_types=cols(
      .default=col_guess(),
      FEATUREID=col_character(),
      CENSUS_BLOCK_GROUP_ID=col_character()
    )
  )
  
} else {

  options(tigris_use_cache=TRUE)
  options(osrm.server="http://127.0.0.1:5000/")
  options(osrm.profile="driving")

  pairs<-dfs |>
    st_drop_geometry() |>
    mutate(
      FEATUREID=as.character(FEATUREID),
      CENSUS_BLOCK_GROUP_ID=as.character(CENSUS_BLOCK_GROUP_ID)
    ) |>
    filter(
      !is.na(FEATUREID),
      !is.na(CENSUS_BLOCK_GROUP_ID),
      nchar(CENSUS_BLOCK_GROUP_ID) == 12
    ) |>
    distinct(FEATUREID, CENSUS_BLOCK_GROUP_ID)

  # unique_feature_cbg <- dfs %>%
  #   select(FEATUREID, CENSUS_BLOCK_GROUP_ID, geometry) %>%
  #   distinct(FEATUREID, CENSUS_BLOCK_GROUP_ID, .keep_all=TRUE)

  dfst<-st_as_sf(dfst, sf_column_name="geom")

  feature_ids<-unique(as.character(dfst$FEATUREID)) # FEATUREID geometry extraction
  feature_ids<-feature_ids[!is.na(feature_ids)]

  feature_idx<-match(feature_ids, as.character(dfst$FEATUREID))

  feature_geoms<-dfst[feature_idx, c("FEATUREID", "geom")] %>%
    mutate(FEATUREID=as.character(FEATUREID))

  feature_pts<-feature_geoms %>%
    st_make_valid() %>%
    st_transform(4326) %>%
    st_point_on_surface() %>%
    mutate(
      feature_lon=st_coordinates(.)[,1],
      feature_lat=st_coordinates(.)[,2]
    ) %>%
    st_drop_geometry()

  rm(feature_ids, feature_idx, feature_geoms)

  state_fips<-pairs %>%
    mutate(state_fips=substr(CENSUS_BLOCK_GROUP_ID, 1, 2)) %>%
    distinct(state_fips) %>%
    pull(state_fips)

  cbg_poly<-map_dfr(state_fips, function(s) { # Download census block group polygons
    block_groups(
      state=s,
      year=2023,
      cb=TRUE,
      class="sf"
    )
  }) %>%
    st_transform(4326) %>%
    mutate(GEOID=as.character(GEOID)) %>%
    filter(GEOID %in% pairs$CENSUS_BLOCK_GROUP_ID)

  missing_cbgs_initial<-pairs %>% # Check for CBGs missing from 2023 tigris pull
    distinct(CENSUS_BLOCK_GROUP_ID) %>%
    anti_join(
      cbg_poly %>%
        st_drop_geometry() %>%
        transmute(CENSUS_BLOCK_GROUP_ID=GEOID),
      by="CENSUS_BLOCK_GROUP_ID"
    )

  # missing_cbg_diag<-missing_cbgs_initial %>% # Missing CBG diagnostics
  #   mutate(
  #     state_fips=substr(CENSUS_BLOCK_GROUP_ID, 1, 2),
  #     county_fips=substr(CENSUS_BLOCK_GROUP_ID, 1, 5),
  #     tract=substr(CENSUS_BLOCK_GROUP_ID, 6, 11),
  #     block_group=substr(CENSUS_BLOCK_GROUP_ID, 12, 12),
  #     is_990000_tract=tract == "990000",
  #     is_bg_zero=block_group == "0"
  #   )

  # missing_cbg_diag %>%
  #   count(state_fips, is_990000_tract, is_bg_zero, sort=TRUE)

  # missing_cbg_diag %>%
  #   count(state_fips, county_fips, sort=TRUE) %>%
  #   print(n=100)

  missing_ct<-missing_cbgs_initial %>% # Connecticut recovery for old county-based CBG IDs
    filter(substr(CENSUS_BLOCK_GROUP_ID, 1, 2) == "09") %>%
    mutate(ct_tract_bg=substr(CENSUS_BLOCK_GROUP_ID, 6, 12))

  if(nrow(missing_ct) > 0) {
    
    ct_cbg_2023<-block_groups(
      state="09",
      year=2023,
      cb=TRUE,
      class="sf"
    ) %>%
      st_transform(4326) %>%
      mutate(
        GEOID=as.character(GEOID),
        ct_tract_bg=substr(GEOID, 6, 12)
      )
    
    ct_lookup<-missing_ct %>%
      select(CENSUS_BLOCK_GROUP_ID, ct_tract_bg) %>%
      left_join(
        ct_cbg_2023 %>%
          st_drop_geometry() %>%
          select(GEOID_2023=GEOID, ct_tract_bg),
        by="ct_tract_bg"
      )
    
    ct_match_diag<-ct_lookup %>%
      count(CENSUS_BLOCK_GROUP_ID, name="n_matches") %>%
      count(n_matches, sort=TRUE)
    
    print(ct_match_diag)
    
    ct_cbg_recovered<-ct_cbg_2023 %>%
      inner_join(
        ct_lookup %>%
          select(GEOID_2023, CENSUS_BLOCK_GROUP_ID),
        by=c("GEOID"="GEOID_2023")
      ) %>%
      mutate(GEOID=CENSUS_BLOCK_GROUP_ID) %>%
      select(-CENSUS_BLOCK_GROUP_ID, -ct_tract_bg)
    
    cbg_poly_combined<-bind_rows(
      cbg_poly,
      ct_cbg_recovered
    ) %>%
      distinct(GEOID, .keep_all=TRUE)
    
  } else {
    
    cbg_poly_combined<-cbg_poly
    
  }

  # missing_cbgs_after_ct<-pairs %>% # Re-check CBG polygon coverage
  #   distinct(CENSUS_BLOCK_GROUP_ID) %>%
  #   anti_join(
  #     cbg_poly_combined %>%
  #       st_drop_geometry() %>%
  #       transmute(CENSUS_BLOCK_GROUP_ID=GEOID),
  #     by="CENSUS_BLOCK_GROUP_ID"
  #   )

  # nrow(missing_cbgs_initial)
  # nrow(missing_cbgs_after_ct)

  # missing_cbgs_after_ct %>%
  #   mutate(
  #     state_fips=substr(CENSUS_BLOCK_GROUP_ID, 1, 2),
  #     county_fips=substr(CENSUS_BLOCK_GROUP_ID, 1, 5),
  #     tract=substr(CENSUS_BLOCK_GROUP_ID, 6, 11),
  #     block_group=substr(CENSUS_BLOCK_GROUP_ID, 12, 12),
  #     is_990000_tract=tract == "990000",
  #     is_bg_zero=block_group == "0"
  #   ) %>%
  #   count(state_fips, is_990000_tract, is_bg_zero, sort=TRUE) %>%
  #   print(n=100)

  cbg_pts<-cbg_poly_combined %>% # CBG polygon surface points
    select(CENSUS_BLOCK_GROUP_ID=GEOID, geometry) %>%
    st_make_valid() %>%
    st_point_on_surface() %>%
    mutate(
      cbg_lon=st_coordinates(.)[,1],
      cbg_lat=st_coordinates(.)[,2]
    ) %>%
    st_drop_geometry()

  rm(cbg_poly, missing_cbgs_initial, missing_ct)

  if(exists("ct_cbg_2023")) rm(ct_cbg_2023)
  if(exists("ct_lookup")) rm(ct_lookup)
  if(exists("ct_cbg_recovered")) rm(ct_cbg_recovered)

  pair_pts<-pairs %>% # Pair table with origin and destination coordinates
    left_join(feature_pts, by="FEATUREID") %>%
    left_join(cbg_pts, by="CENSUS_BLOCK_GROUP_ID")

  # missing_features<-pair_pts %>% # Missing coordinate checks
  #   filter(is.na(feature_lon) | is.na(feature_lat)) %>%
  #   distinct(FEATUREID)

  # missing_cbgs<-pair_pts %>%
  #   filter(is.na(cbg_lon) | is.na(cbg_lat)) %>%
  #   distinct(CENSUS_BLOCK_GROUP_ID)

  pair_pts<-pair_pts %>%
    filter(
      !is.na(feature_lon),
      !is.na(feature_lat),
      !is.na(cbg_lon),
      !is.na(cbg_lat)
    )

  # cbgs_per_feature<-pair_pts %>% # Pair density diagnostics
  #   count(FEATUREID, name="n_cbgs") %>%
  #   arrange(desc(n_cbgs))

  # summary(cbgs_per_feature$n_cbgs)
  # max(cbgs_per_feature$n_cbgs)

  get_feature_distances<-function(x, max_destinations=5000) { # OSRM routing function
    
    feature_id<-unique(x$FEATUREID)
    
    src<-x %>%
      slice(1) %>%
      transmute(
        id=as.character(FEATUREID),
        lon=feature_lon,
        lat=feature_lat
      ) %>%
      as.data.frame() %>%
      st_as_sf(coords=c("lon", "lat"), crs=4326)
    
    dst_all_df<-x %>%
      distinct(CENSUS_BLOCK_GROUP_ID, cbg_lon, cbg_lat) %>%
      transmute(
        id=as.character(CENSUS_BLOCK_GROUP_ID),
        lon=cbg_lon,
        lat=cbg_lat
      )
    
    dst_batches<-split(
      dst_all_df,
      ceiling(seq_len(nrow(dst_all_df)) / max_destinations)
    )
    
    map_dfr(dst_batches, function(dst_df) {
      
      dst<-dst_df %>%
        as.data.frame() %>%
        st_as_sf(coords=c("lon", "lat"), crs=4326)
      
      out<-tryCatch(
        {
          tab<-osrmTable(
            src=src,
            dst=dst,
            measure=c("duration", "distance")
          )
          
          tibble(
            FEATUREID=feature_id,
            CENSUS_BLOCK_GROUP_ID=dst_df$id,
            distance_m=as.numeric(tab$distances[1, seq_along(dst_df$id)]),
            duration_min=as.numeric(tab$durations[1, seq_along(dst_df$id)]),
            osrm_success=TRUE
          )
        },
        error=function(e) {
          tibble(
            FEATUREID=feature_id,
            CENSUS_BLOCK_GROUP_ID=dst_df$id,
            distance_m=NA_real_,
            duration_min=NA_real_,
            osrm_success=FALSE,
            osrm_error=conditionMessage(e)
          )
        }
      )
      
      out
    })
  }

  # test_feature_ids<-pair_pts %>% # Small test run
  #   distinct(FEATUREID) %>%
  #   slice_head(n=2) %>%
  #   pull(FEATUREID)

  # distance_test<-pair_pts %>%
  #   filter(FEATUREID %in% test_feature_ids) %>%
  #   group_split(FEATUREID) %>%
  #   map_dfr(~get_feature_distances(.x, max_destinations=5000))

  # distance_test

  system.time(distance_results<-pair_pts %>% # Full run
    group_split(FEATUREID) %>%
    map_dfr(~get_feature_distances(.x, max_destinations=5000)))
    
  distance_results<-distance_results %>%
    mutate(
      FEATUREID=as.character(FEATUREID),
      CENSUS_BLOCK_GROUP_ID=as.character(CENSUS_BLOCK_GROUP_ID)
    )

  write_csv(distance_results, travel_distance_path)
}

dfst<-dfst %>%
  mutate(
    FEATUREID=as.character(FEATUREID),
    CENSUS_BLOCK_GROUP_ID=as.character(CENSUS_BLOCK_GROUP_ID)
  ) %>%
  left_join(
    distance_results %>%
      mutate(
        FEATUREID=as.character(FEATUREID),
        CENSUS_BLOCK_GROUP_ID=as.character(CENSUS_BLOCK_GROUP_ID)
      ),
    by=c("FEATUREID", "CENSUS_BLOCK_GROUP_ID")
  )

rm(distance_results,dfs,dds_path,travel_distance_path,pad_cbg)

# GIS data ---------------------------------------------------------------

