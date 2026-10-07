# Setup -------------------------------------------------------------------
#rm(list=ls()) # Clears workspace

# # Install/call libraries
# install.packages("renv") # Run if you have cloned repository and don't already have renv installed
# renv::restore() # Run once after cloning repository
# renv::install("package") # Run to install new packages
# renv::snapshot() # Run after installing new packages
# renv::init() # Only run when the repository is first created, don't run on cloning an existing repository

# Data for this repository is at https://drive.google.com/drive/folders/1syX_y2lMbo-ETNBXAo24m2FWUK1q60Ux?usp=sharing

pkgs<-c("tidyverse","arrow","survival")

missing<-pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly=TRUE)]
if (length(missing)>0) {
  stop("Missing packages: ", paste(missing, collapse=", "),
       "\nRun renv::restore()")
}

invisible(lapply(pkgs, library, character.only=TRUE))
rm(pkgs, missing)


# Load data --------------------------------------------------------------
source("Airport_calibration.R")


# Travel cost model ------------------------------------------------------
travel_cost_params<-list(
  auto_cost_per_mile=0.2503, # 2024 AAA marginal driving cost
  annual_work_hours=2080,    # 40 hours/week * 52 weeks
  vot_fraction=0.33          # value of travel time as share of hourly income
)

rum_full_path <- "Data/intermediate/rum_full.parquet"
dir.create(dirname(rum_full_path), recursive = TRUE, showWarnings = FALSE)

if(file.exists(rum_full_path)) {

  # Open the cached RUM dataset without loading all rows into memory
  rum_full<-open_dataset(rum_full_path)

} else {

  # Create the set of available destination alternatives
  alts <- dfst %>%
    st_drop_geometry() %>%
    mutate(FEATUREID = as.character(FEATUREID)) %>%
    filter(!is.na(FEATUREID)) %>%
    distinct(FEATUREID, Name, Jurisdiction) %>%
    arrange(FEATUREID)

  # Select one observed destination choice per device-day
  choices <- dfst %>%
    st_drop_geometry() %>%
    dplyr::select(
      DEVICEID,
      DAY_IN_FEATURE,
      FEATUREID,
      CENSUS_BLOCK_GROUP_ID,
      timespan_min
    ) %>%
    filter(
      !is.na(DEVICEID),
      !is.na(DAY_IN_FEATURE),
      !is.na(FEATUREID),
      !is.na(CENSUS_BLOCK_GROUP_ID)
    ) %>%
    mutate(
      FEATUREID = as.character(FEATUREID),
      CENSUS_BLOCK_GROUP_ID = as.character(CENSUS_BLOCK_GROUP_ID)
    ) %>%
    group_by(DEVICEID, DAY_IN_FEATURE) %>%
    slice_max(timespan_min, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    mutate(choice_id = row_number()) %>%
    dplyr::select(
      choice_id,
      DEVICEID,
      DAY_IN_FEATURE,
      CENSUS_BLOCK_GROUP_ID,
      chosen_FEATUREID = FEATUREID
    )

  # Report the dimensions of the full choice-by-alternative dataset
  tibble(
    n_choices = nrow(choices),
    n_alts = nrow(alts),
    n_rum_rows = nrow(choices) * nrow(alts)
  )

  # Prepare origin-destination travel attributes
  travel_costs <- distance_results %>%
    filter(
      CENSUS_BLOCK_GROUP_ID %in% unique(choices$CENSUS_BLOCK_GROUP_ID),
      FEATUREID %in% alts$FEATUREID
    ) %>%
    dplyr::select(
      CENSUS_BLOCK_GROUP_ID,
      FEATUREID,
      distance_m,
      duration_min,
      med_hh_income,
      osrm_success
    ) %>%
    collect() %>%
    mutate(
      CENSUS_BLOCK_GROUP_ID = as.character(CENSUS_BLOCK_GROUP_ID),
      FEATUREID = as.character(FEATUREID),
      travel_key = paste(CENSUS_BLOCK_GROUP_ID, FEATUREID, sep = "|"),
      distance_km = distance_m / 1000,
      duration_hr = duration_min / 60
    ) %>%
    filter(
      osrm_success,
      !is.na(distance_km),
      !is.na(duration_hr)
    ) %>%
    dplyr::select(
      travel_key,
      distance_km,
      duration_hr,
      med_hh_income
    )

  n_choices <- nrow(choices)
  n_alts <- nrow(alts)

  # Construct indices for the full choice-by-alternative Cartesian product
  choice_i <- rep(seq_len(n_choices), each = n_alts)
  alt_i <- rep(seq_len(n_alts), times = n_choices)

  # Match each choice-alternative combination to its travel attributes
  travel_i <- match(
    paste(
      choices$CENSUS_BLOCK_GROUP_ID[choice_i],
      alts$FEATUREID[alt_i],
      sep = "|"
    ),
    travel_costs$travel_key
  )

  print(system.time({

    # Construct the long-format random utility model dataset
    rum_full <- tibble(
      choice_id = choices$choice_id[choice_i],
      DEVICEID = choices$DEVICEID[choice_i],
      DAY_IN_FEATURE = choices$DAY_IN_FEATURE[choice_i],
      CENSUS_BLOCK_GROUP_ID = choices$CENSUS_BLOCK_GROUP_ID[choice_i],
      FEATUREID = alts$FEATUREID[alt_i],
      chosen = as.integer(
        alts$FEATUREID[alt_i] ==
          choices$chosen_FEATUREID[choice_i]
      ),
      distance_km = travel_costs$distance_km[travel_i],
      duration_hr = travel_costs$duration_hr[travel_i],
      med_hh_income = travel_costs$med_hh_income[travel_i]
    ) %>%
      filter(
        !is.na(distance_km),
        !is.na(duration_hr)
      ) %>%
      mutate(
        travel_cost_dollars =
          (distance_km / 1.609344) *
            travel_cost_params$auto_cost_per_mile +
          duration_hr *
            (
              travel_cost_params$vot_fraction *
                med_hh_income /
                travel_cost_params$annual_work_hours
            )
      )

  }))

  # Cache the full RUM dataset in compressed Parquet format
  write_parquet(
    rum_full,
    rum_full_path,
    compression = "zstd"
  )

  # Release large temporary objects from memory
  rm(rum_full, choice_i, alt_i, travel_i)
  gc()

  # Open the cached dataset for lazy Arrow processing
  rum_full <- open_dataset(rum_full_path)
}

# rum_full %>% # Percentage of census block groups missing income data
#   distinct(CENSUS_BLOCK_GROUP_ID, med_hh_income) %>%
#   summarise(
#     n_origin_cbgs = n(),
#     n_missing_income = sum(is.na(med_hh_income)),
#     pct_missing_income = 100 * mean(is.na(med_hh_income))
#   ) %>%
#   collect()

# Identify choice sets with the variables required for model estimation
choice_ids<-rum_full %>%
  filter(!is.na(med_hh_income)) %>%
  dplyr::select(choice_id) %>%
  distinct() %>%
  collect()

sample_choice_ids<-choice_ids %>%
  slice_sample(
    n = min(50000, nrow(choice_ids))
  ) %>%
  pull(choice_id)

# Load only the sampled choice sets and variables needed for modeling
rum_model_df<-rum_full %>%
  filter(choice_id %in% sample_choice_ids) %>%
  dplyr::select(
    choice_id,
    DEVICEID,
    FEATUREID,
    chosen,
    distance_km,
    duration_hr,
    med_hh_income
  ) %>%
  collect() %>%
  mutate(
    travel_cost_dollars =
      (distance_km / 1.609344) *
        travel_cost_params$auto_cost_per_mile +
      duration_hr *
        (
          travel_cost_params$vot_fraction *
            med_hh_income /
            travel_cost_params$annual_work_hours
        )
  ) %>%
  filter(
    !is.na(travel_cost_dollars),
    is.finite(travel_cost_dollars)
  )

# rum_model_df %>% # Summary stats/diagnostics
#   summarise(
#     n=n(),
#     n_choices=n_distinct(choice_id),
#     n_devices=n_distinct(DEVICEID),
#     n_sites=n_distinct(FEATUREID),
#     chosen_share=mean(chosen),
#     missing_cost=sum(is.na(travel_cost_dollars)),
#     median_cost=median(travel_cost_dollars),
#     p95_cost=quantile(travel_cost_dollars, 0.95)
#   )

m1<-clogit(
  chosen ~ travel_cost_dollars +
    strata(choice_id) + cluster(DEVICEID),
  data=rum_model_df,
  method="efron"
)

summary(m1)

coef_m1<-coef(m1)["travel_cost_dollars"]

tibble(
  beta_per_dollar=coef_m1,
  odds_ratio_per_dollar=exp(coef_m1),
  pct_change_odds_per_dollar=100 * (exp(coef_m1) - 1),
  odds_ratio_per_10_dollars=exp(10 * coef_m1),
  pct_change_odds_per_10_dollars=100 * (exp(10 * coef_m1) - 1)
)

system.time(m2<-clogit(
  chosen ~ travel_cost_dollars + factor(FEATUREID) +
    strata(choice_id) + cluster(DEVICEID),
  data=rum_model_df,
  method="efron"
))

summary(m2)

