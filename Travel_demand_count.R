# Setup -------------------------------------------------------------------
#rm(list=ls()) # Clears workspace

# # Install/call libraries
# install.packages("renv") # Run if you have cloned repository and don't already have renv installed
# renv::restore() # Run once after cloning repository
# renv::install("package") # Run to install new packages
# renv::snapshot() # Run after installing new packages
# renv::init() # Only run when the repository is first created, don't run on cloning an existing repository

# Data for this repository is at https://drive.google.com/drive/folders/1syX_y2lMbo-ETNBXAo24m2FWUK1q60Ux?usp=sharing

pkgs<-c("tidyverse","arrow","sf","fixest")

missing<-pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly=TRUE)]
if (length(missing)>0) {
  stop("Missing packages: ", paste(missing, collapse=", "),
       "\nRun renv::restore()")
}

invisible(lapply(pkgs, library, character.only=TRUE))
rm(pkgs, missing)


# Load data --------------------------------------------------------------
source("Airport_calibration.R")
travel_cost_params <- list(
  auto_cost_per_mile = 0.2503,
  annual_work_hours = 2080,
  vot_fraction = 0.33
)

analysis_year <- 2024L  # Set to the calendar year of the mobile-device visits.
cache_dir <- "Data/intermediate"
counts_path <- file.path(cache_dir, paste0("cbg_site_visits_", analysis_year, ".parquet"))
site_model_path <- file.path(cache_dir, paste0("cbg_site_model_", analysis_year, ".parquet"))
rebuild_counts <- FALSE
rebuild_site_data <- FALSE
fit_origin_sample <- NULL  # NULL = all origins; e.g. 1000 for diagnostic fitting.
include_area_control <- TRUE
calculate_site_welfare <- FALSE  # Optional finite site-level mangrove changes.
n_threads <- max(1L, parallel::detectCores(logical = FALSE) - 2L)


# Observed visitor-days ----------------------------------------------------
if (!file.exists(counts_path) || rebuild_counts) { # Rebuild when visit definitions, the observation year, or calibration changes.
  if (!exists("dfst")) stop("Object 'dfst' is required to rebuild visit counts.")
  needed <- c("DEVICEID", "DAY_IN_FEATURE", "FEATUREID",
              "CENSUS_BLOCK_GROUP_ID", "timespan_min", "calibrated_visits")
  if (!all(needed %in% names(dfst))) {
    stop("Missing dfst columns: ", paste(setdiff(needed, names(dfst)), collapse = ", "))
  }

  cbg_site_visits <- dfst %>%
    sf::st_drop_geometry() %>%
    dplyr::select(dplyr::all_of(needed)) %>%
    mutate(
      DAY_IN_FEATURE = as.Date(DAY_IN_FEATURE),
      FEATUREID = as.character(FEATUREID),
      CENSUS_BLOCK_GROUP_ID = as.character(CENSUS_BLOCK_GROUP_ID)
    ) %>%
    filter(
      as.integer(format(DAY_IN_FEATURE, "%Y")) == analysis_year,
      !is.na(DEVICEID), !is.na(DAY_IN_FEATURE), !is.na(FEATUREID),
      !is.na(CENSUS_BLOCK_GROUP_ID), !is.na(timespan_min),
      is.finite(calibrated_visits), calibrated_visits > 0
    ) %>%
    group_by(DEVICEID, DAY_IN_FEATURE) %>%
    slice_max(timespan_min, n = 1, with_ties = FALSE) %>% # Select one destination per device-day based on longest duration
    ungroup() %>%
    group_by(CENSUS_BLOCK_GROUP_ID, FEATUREID) %>%
    summarise(
      device_visitor_days = n(),
      calibrated_visitor_days = sum(calibrated_visits),
      .groups = "drop"
    )

  dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  if (nrow(cbg_site_visits) == 0) stop("No visits match analysis_year.")
  arrow::write_parquet(cbg_site_visits, counts_path, compression = "zstd")
  rm(cbg_site_visits)
}

cbg_site_visits <- arrow::read_parquet(counts_path) %>%
  mutate(
    CENSUS_BLOCK_GROUP_ID = as.character(CENSUS_BLOCK_GROUP_ID),
    FEATUREID = as.character(FEATUREID)
  )


# Site attributes ----------------------------------------------------------
df <- df %>%
  dplyr::select(
    -dplyr::any_of(c(
      "UNIT_NAME", "Category", "UNIT_CODE", "ForestName",
      "MultiPart", "NVUM_year", "siteid"
    ))
  ) %>%
  mutate(
    id = as.character(id),
    mangrove_presence = as.numeric(as.character(mangrove_presence)),
    park_area_m2 = as.numeric(park_area_m2),
    log_park_area = if_else(
      is.finite(park_area_m2) & park_area_m2 >= 0,
      log1p(park_area_m2), NA_real_
    )
  )

if (anyNA(df$id) || anyDuplicated(df$id)) {
  stop("df$id must contain unique, nonmissing site identifiers.")
}

if (nrow(
  cbg_site_visits %>%
    distinct(FEATUREID) %>%
    anti_join(
      df %>% sf::st_drop_geometry() %>% dplyr::select(id),
      by = c("FEATUREID" = "id")
    )
) > 0) {
  stop("Some chosen sites have no matched df$id.")
}

site_controls <- character(0)
if (include_area_control) {
  if (anyNA(df$log_park_area)) {
    stop("Cannot include log_park_area: missing or invalid park areas. ",
         "Set include_area_control=FALSE to omit.")
  }
  site_controls <- "log_park_area"
}

# Travel-cost and site-choice data ----------------------------------------
if (!file.exists(site_model_path) || rebuild_site_data) {
  if (!exists("distance_results")) {
    distance_file <- file.path(cache_dir, "travel_distance_results.csv")
    if (!file.exists(distance_file)) stop("Missing distance_results and its CSV cache.")
    distance_results <- readr::read_csv(
      distance_file,
      col_types = readr::cols(
        CENSUS_BLOCK_GROUP_ID = readr::col_character(),
        FEATUREID = readr::col_character(), .default = readr::col_guess()
      ), show_col_types = FALSE
    )
  }
  needed_distance <- c("CENSUS_BLOCK_GROUP_ID", "FEATUREID", "distance_m",
                       "duration_min", "med_hh_income", "osrm_success")
  if (!all(needed_distance %in% names(distance_results))) {
    stop("Missing distance_results columns: ",
         paste(setdiff(needed_distance, names(distance_results)), collapse = ", "))
  }

  eligible_origins <- cbg_site_visits %>%
    distinct(CENSUS_BLOCK_GROUP_ID) %>%
    inner_join(cbg_population, by = "CENSUS_BLOCK_GROUP_ID")

   cbg_site_model <- distance_results %>%
    dplyr::select(dplyr::all_of(needed_distance)) %>%
    {if (inherits(., c("Dataset", "arrow_dplyr_query"))) dplyr::collect(.) else .} %>%
    mutate(
      CENSUS_BLOCK_GROUP_ID = as.character(CENSUS_BLOCK_GROUP_ID),
      FEATUREID = as.character(FEATUREID)
    ) %>%
    semi_join(eligible_origins, by = "CENSUS_BLOCK_GROUP_ID") %>%
    inner_join(
      df %>%
        sf::st_drop_geometry() %>%
        dplyr::select(id, mangrove_presence, log_park_area),
      by = c("FEATUREID" = "id")
    ) %>%
    filter(
      osrm_success, is.finite(distance_m), is.finite(duration_min),
      is.finite(med_hh_income), med_hh_income > 0,
      distance_m >= 0, duration_min >= 0
    ) %>%
    mutate(
      travel_cost_dollars = (
        (distance_m / 1000 / 1.609344) * travel_cost_params$auto_cost_per_mile +
          (duration_min / 60) * travel_cost_params$vot_fraction *
          med_hh_income / travel_cost_params$annual_work_hours
      )
    ) %>%
    dplyr::select(
      CENSUS_BLOCK_GROUP_ID, FEATUREID, travel_cost_dollars,
      med_hh_income, mangrove_presence, log_park_area
    )

  if (anyDuplicated(cbg_site_model[c("CENSUS_BLOCK_GROUP_ID", "FEATUREID")])) {
    stop("Duplicate CBG-site travel pairs in distance_results.")
  }

  chosen_unmatched <- cbg_site_visits %>%
    semi_join(cbg_site_model %>% distinct(CENSUS_BLOCK_GROUP_ID),
              by = "CENSUS_BLOCK_GROUP_ID") %>%
    anti_join(cbg_site_model, by = c("CENSUS_BLOCK_GROUP_ID", "FEATUREID"))
  if (nrow(chosen_unmatched) > 0) {
    print(dplyr::slice_head(chosen_unmatched, n = 10))
    stop(nrow(chosen_unmatched), " observed CBG-site combinations have no valid travel-cost alternative.")
  }

  cbg_site_model <- cbg_site_model %>%
    left_join(
      cbg_site_visits %>%
        dplyr::select(CENSUS_BLOCK_GROUP_ID, FEATUREID,
                      device_visitor_days, calibrated_visitor_days),
      by = c("CENSUS_BLOCK_GROUP_ID", "FEATUREID")
    ) %>%
    mutate(
      device_visitor_days = replace_na(device_visitor_days, 0L),
      calibrated_visitor_days = replace_na(calibrated_visitor_days, 0)
    )

  n_sites <- nrow(df)
  coverage <- cbg_site_model %>%
    count(CENSUS_BLOCK_GROUP_ID, name = "available_sites") %>%
    summarise(
      origins = n(),
      origins_with_full_choices = sum(available_sites == n_sites),
      min_available_sites = min(available_sites),
      median_available_sites = median(available_sites)
    )
  print(coverage)
  if (any(coverage$min_available_sites < n_sites)) {
    warning("Some origin-site routes are missing.")
  }

  arrow::write_parquet(cbg_site_model, site_model_path, compression = "zstd")
  rm(cbg_site_model)
}

cbg_site_model <- arrow::read_parquet(site_model_path) %>%
  mutate(
    CENSUS_BLOCK_GROUP_ID = as.character(CENSUS_BLOCK_GROUP_ID),
    FEATUREID = as.character(FEATUREID)
  )

if (!is.null(fit_origin_sample)) { # Allow a small-scale model fit before estimating the full dataset
  all_origins <- unique(cbg_site_model$CENSUS_BLOCK_GROUP_ID)
  sample_origins <- sample(all_origins, min(fit_origin_sample, length(all_origins)))
  cbg_site_model <- cbg_site_model %>%
    filter(CENSUS_BLOCK_GROUP_ID %in% sample_origins)
  rm(all_origins, sample_origins)
}

# Stage 1: conditional site-choice model ----------------------------------
# Conditional-logit/Poisson equivalence with origin-specific normalization.
# All zero-count feasible alternatives must remain in the estimation data.
choice_rhs <- c("travel_cost_dollars", "mangrove_presence", site_controls)
choice_formula <- stats::as.formula(paste(
  "device_visitor_days ~", paste(choice_rhs, collapse = " + "),
  "| CENSUS_BLOCK_GROUP_ID"
))

system.time(m_site <- fixest::fepois(
  choice_formula,
  data = cbg_site_model,
  vcov = ~ CENSUS_BLOCK_GROUP_ID + FEATUREID,
  nthreads = n_threads,
  lean = FALSE
))

print(summary(m_site))

beta <- coef(m_site)
if (!all(choice_rhs %in% names(beta))) {
  stop("A required coefficient was omitted (collinearity or non-identification): ",
       paste(setdiff(choice_rhs, names(beta)), collapse = ", "))
}
if (!is.finite(beta[["travel_cost_dollars"]]) ||
    beta[["travel_cost_dollars"]] >= 0) {
  stop("Travel-cost coefficient must be negative for money-metric welfare.")
}

# Stage 1: monetary inclusive value --------------------------------------
# Excludes origin fixed effects, which are choice-set normalizers and must
# not be included in the inclusive value used by the trip-frequency model.
cbg_site_model <- cbg_site_model %>%
  mutate(
    V = beta[["travel_cost_dollars"]] * travel_cost_dollars +
      beta[["mangrove_presence"]] * mangrove_presence
  )
if ("log_park_area" %in% site_controls) {
  cbg_site_model <- cbg_site_model %>%
    mutate(V = V + beta[["log_park_area"]] * log_park_area)
}

cbg_inclusive_value <- cbg_site_model %>%
  group_by(CENSUS_BLOCK_GROUP_ID) %>%
  summarise(
    max_V = max(V),
    IV = max_V + log(sum(exp(V - max_V))),
    .groups = "drop"
  ) %>%
  mutate(monetary_inclusive_value = IV / -beta[["travel_cost_dollars"]]) %>%
  dplyr::select(-max_V)

# Stage 2: annual population trip-frequency model ------------------------
# PPML on calibrated annual visitor-day totals with Census population offset.
# The target is the mean annual visitor-days per CBG resident.
cbg_annual <- cbg_site_model %>%
  group_by(CENSUS_BLOCK_GROUP_ID) %>%
  summarise(
    device_visitor_days = sum(device_visitor_days),
    calibrated_visitor_days = sum(calibrated_visitor_days),
    med_hh_income = first(med_hh_income),
    .groups = "drop"
  ) %>%
  inner_join(cbg_population, by = "CENSUS_BLOCK_GROUP_ID") %>%
  inner_join(cbg_inclusive_value, by = "CENSUS_BLOCK_GROUP_ID") %>%
  mutate(
    log_income = log(med_hh_income),
    state_fips = substr(CENSUS_BLOCK_GROUP_ID, 1, 2)
  )

stopifnot(all(is.finite(cbg_annual$calibrated_visitor_days)),
          all(cbg_annual$cbg_population > 0))

# Main mean specification. State fixed effects are a robustness model,
# not CBG fixed effects; the cross-origin accessibility effect is identified
# from origins that differ in the monetary inclusive value.
m_freq <- fixest::fepois(
  calibrated_visitor_days ~ monetary_inclusive_value + log_income,
  offset = ~ log(cbg_population),
  data = cbg_annual,
  vcov = ~ state_fips,
  nthreads = n_threads
)
print(summary(m_freq))

m_freq_state <- fixest::fepois(
  calibrated_visitor_days ~ monetary_inclusive_value + log_income | state_fips,
  offset = ~ log(cbg_population),
  data = cbg_annual,
  vcov = ~ state_fips,
  nthreads = n_threads
)
print(summary(m_freq_state))

# Baseline estimates -------------------------------------------------------
cbg_annual <- cbg_annual %>%
  mutate(
    predicted_annual_visitor_days = as.numeric(predict(m_freq, type = "response")),
    observed_trips_per_resident = calibrated_visitor_days / cbg_population,
    predicted_trips_per_resident = predicted_annual_visitor_days / cbg_population
  )

beta_m <- beta[["mangrove_presence"]]
beta_c <- beta[["travel_cost_dollars"]]
delta <- unname(coef(m_freq)[["monetary_inclusive_value"]])

marginal_results <- tibble(
  measure = c("mangrove_logit_utility_coefficient",
              "travel_cost_coefficient_per_dollar",
              "money_equivalent_of_site_utility_shift_dollars",
              "frequency_response_per_dollar_of_accessibility"),
  estimate = c(beta_m, beta_c, beta_m / -beta_c, delta)
)
print(marginal_results)

# Binary mangrove site-level finite changes -------------------------------
# Computes adding mangrove presence to currently absent sites, or removing
# it from currently present sites, one site at a time. Site values are NOT
# additive across sites because diversion/substitution changes across scenarios.
if (calculate_site_welfare) {
  if (!is.finite(delta) || delta <= 0) {
    warning("Frequency response is not positive. Skipping participation-inclusive welfare.")
  } else {
    site_welfare <- cbg_site_model %>%
      inner_join(
        cbg_inclusive_value %>%
          dplyr::select(CENSUS_BLOCK_GROUP_ID, IV),
        by = "CENSUS_BLOCK_GROUP_ID"
      ) %>%
      inner_join(
        cbg_annual %>%
          transmute(CENSUS_BLOCK_GROUP_ID,
                    annual_calibrated_visitor_days = calibrated_visitor_days,
                    predicted_annual_visitor_days),
        by = "CENSUS_BLOCK_GROUP_ID"
      ) %>%
      mutate(
        baseline_choice_probability = exp(V - IV),
        utility_change = if_else(mangrove_presence == 1, -beta_m, beta_m),
        delta_welfare_per_trip = log1p(
          baseline_choice_probability * expm1(utility_change)
        ) / -beta_c,
        conditional_welfare_change =
          annual_calibrated_visitor_days * delta_welfare_per_trip,
        frequency_inclusive_welfare_change =
          predicted_annual_visitor_days *
          expm1(delta * delta_welfare_per_trip) / delta,
        change_in_annual_trips =
          predicted_annual_visitor_days * expm1(delta * delta_welfare_per_trip)
      ) %>%
      group_by(FEATUREID, mangrove_presence) %>%
      summarise(
        conditional_welfare_change = sum(conditional_welfare_change),
        frequency_inclusive_welfare_change = sum(frequency_inclusive_welfare_change),
        change_in_annual_trips = sum(change_in_annual_trips),
        .groups = "drop"
      ) %>%
      mutate(
        attribute_change = if_else(mangrove_presence == 1,
                                   "remove_mangroves", "add_mangroves")
      )
    print(dplyr::slice_head(site_welfare, n = 10))
  }
}

# Report model coverage and save compact results --------------------------
model_coverage <- cbg_annual %>%
  summarise(
    origins = n(),
    represented_population = sum(cbg_population),
    observed_device_visitor_days = sum(device_visitor_days),
    calibrated_visitor_days = sum(calibrated_visitor_days),
    predicted_visitor_days = sum(predicted_annual_visitor_days),
    visitor_days_per_resident = sum(calibrated_visitor_days) / sum(cbg_population)
  )
print(model_coverage)
