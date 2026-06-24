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


# Script options ---------------------------------------------------------
options(tigris_use_cache=TRUE)
options(osrm.server="http://127.0.0.1:5000/") # Run in powershell to start OSRM server: docker run --rm -t -i -p 5000:5000 -v osrm_us_data:/data osrm/osrm-backend osrm-routed --max-table-size 10000 /data/us-latest.osrm
options(osrm.profile="driving")


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
  mutate(CENSUS_BLOCK_GROUP_ID=as.character(CENSUS_BLOCK_GROUP_ID)) |>
  filter( # Filtering out null and water / pseudo / invalid CBG identifiers 
    !is.na(CENSUS_BLOCK_GROUP_ID),
    CENSUS_BLOCK_GROUP_ID != "NULL",
    nchar(CENSUS_BLOCK_GROUP_ID) == 12,
    substr(CENSUS_BLOCK_GROUP_ID, 12, 12) != "0",
    substr(CENSUS_BLOCK_GROUP_ID, 6, 11) != "990000"
  )


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

  pairs<-tidyr::crossing(
    FEATUREID=dfst |>
      st_drop_geometry() |>
      mutate(FEATUREID=as.character(FEATUREID)) |>
      filter(!is.na(FEATUREID)) |>
      distinct(FEATUREID) |>
      pull(FEATUREID),
    CENSUS_BLOCK_GROUP_ID=dfst |>
      st_drop_geometry() |>
      mutate(CENSUS_BLOCK_GROUP_ID=as.character(CENSUS_BLOCK_GROUP_ID)) |>
      filter(
        !is.na(CENSUS_BLOCK_GROUP_ID),
        CENSUS_BLOCK_GROUP_ID != "NULL",
        nchar(CENSUS_BLOCK_GROUP_ID) == 12,
        substr(CENSUS_BLOCK_GROUP_ID, 12, 12) != "0",
        substr(CENSUS_BLOCK_GROUP_ID, 6, 11) != "990000"
      ) |>
      distinct(CENSUS_BLOCK_GROUP_ID) |>
      pull(CENSUS_BLOCK_GROUP_ID)
  )

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
    left_join(cbg_pts, by="CENSUS_BLOCK_GROUP_ID") %>%
    filter(
      !is.na(feature_lon),
      !is.na(feature_lat),
      !is.na(cbg_lon),
      !is.na(cbg_lat)
    )

  # missing_features<-pair_pts %>% # Missing coordinate checks
  #   filter(is.na(feature_lon) | is.na(feature_lat)) %>%
  #   distinct(FEATUREID)

  # missing_cbgs<-pair_pts %>%
  #   filter(is.na(cbg_lon) | is.na(cbg_lat)) %>%
  #   distinct(CENSUS_BLOCK_GROUP_ID)
  
  print(
    pair_pts %>%
      summarise(
        n_pairs=n(),
        n_features=n_distinct(FEATUREID),
        n_cbgs=n_distinct(CENSUS_BLOCK_GROUP_ID)
      )
  )

  # cbgs_per_feature<-pair_pts %>% # Pair density diagnostics
  #   count(FEATUREID, name="n_cbgs") %>%
  #   arrange(desc(n_cbgs))

  # summary(cbgs_per_feature$n_cbgs)
  # max(cbgs_per_feature$n_cbgs)

  get_distance_batch<-function(cbg_batch, feature_pts) { # OSRM table function: many CBGs x many FEATUREIDs
    
    src<-cbg_batch %>%
      transmute(
        id=as.character(CENSUS_BLOCK_GROUP_ID),
        lon=cbg_lon,
        lat=cbg_lat
      ) %>%
      as.data.frame() %>%
      st_as_sf(coords=c("lon", "lat"), crs=4326)
    
    dst<-feature_pts %>%
      transmute(
        id=as.character(FEATUREID),
        lon=feature_lon,
        lat=feature_lat
      ) %>%
      as.data.frame() %>%
      st_as_sf(coords=c("lon", "lat"), crs=4326)
    
    tryCatch(
      {
        outbound_tab<-osrmTable(
          src=src,
          dst=dst,
          measure=c("duration", "distance")
        )
        
        return_tab<-osrmTable(
          src=dst,
          dst=src,
          measure=c("duration", "distance")
        )
        
        tidyr::expand_grid(
          CENSUS_BLOCK_GROUP_ID=cbg_batch$CENSUS_BLOCK_GROUP_ID,
          FEATUREID=feature_pts$FEATUREID
        ) %>%
          mutate(
            outbound_distance_m=as.vector(t(outbound_tab$distances)),
            outbound_duration_min=as.vector(t(outbound_tab$durations)),
            return_distance_m=as.vector(return_tab$distances),
            return_duration_min=as.vector(return_tab$durations),
            distance_m=outbound_distance_m + return_distance_m,
            duration_min=outbound_duration_min + return_duration_min,
            osrm_success=TRUE,
            osrm_error=NA_character_
          )
      },
      error=function(e) {
        tidyr::expand_grid(
          CENSUS_BLOCK_GROUP_ID=cbg_batch$CENSUS_BLOCK_GROUP_ID,
          FEATUREID=feature_pts$FEATUREID
        ) %>%
          mutate(
            outbound_distance_m=NA_real_,
            outbound_duration_min=NA_real_,
            return_distance_m=NA_real_,
            return_duration_min=NA_real_,
            distance_m=NA_real_,
            duration_min=NA_real_,
            osrm_success=FALSE,
            osrm_error=conditionMessage(e)
          )
      }
    )
  }

  cbg_batches<-cbg_pts %>% # CBG batches; each batch gets routed to all FEATUREIDs
    filter(
      CENSUS_BLOCK_GROUP_ID %in% unique(pair_pts$CENSUS_BLOCK_GROUP_ID),
      !is.na(cbg_lon),
      !is.na(cbg_lat),
      is.finite(cbg_lon),
      is.finite(cbg_lat)
    ) %>%
    arrange(CENSUS_BLOCK_GROUP_ID) %>%
    split(ceiling(seq_len(nrow(.)) / 500))

  feature_pts<-feature_pts %>% # Make sure site points are clean and ordered
    filter(
      FEATUREID %in% unique(pair_pts$FEATUREID),
      !is.na(feature_lon),
      !is.na(feature_lat),
      is.finite(feature_lon),
      is.finite(feature_lat)
    ) %>%
    arrange(FEATUREID)

  print(tibble(
    n_cbg_batches=length(cbg_batches),
    n_cbgs=sum(map_int(cbg_batches, nrow)),
    n_features=nrow(feature_pts),
    n_pairs=sum(map_int(cbg_batches, nrow)) * nrow(feature_pts)
  ))

  # test_batch<-get_distance_batch(cbg_batches[[1]] %>% slice_head(n=10), feature_pts %>% slice_head(n=5))
  # test_batch %>% print(n=50)

  travel_distance_dir<-"Data/intermediate/travel_distance_results"
  dir.create(travel_distance_dir, recursive=TRUE, showWarnings=FALSE)

  system.time({
    walk(seq_along(cbg_batches), function(i) {
      
      out_path<-file.path(travel_distance_dir, paste0("cbg_batch_", stringr::str_pad(i, width=5, pad="0"), ".parquet"))
      
      if(!file.exists(out_path)) {
        get_distance_batch(cbg_batches[[i]], feature_pts) %>%
          write_parquet(out_path)
      }
      
      message("Finished CBG batch ", i, " of ", length(cbg_batches))
    })
  })

  distance_results<-open_dataset(travel_distance_dir) %>%
    collect() %>%
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

rm(dfs,dds_path,travel_distance_path,pad_cbg)

# GIS data ---------------------------------------------------------------

