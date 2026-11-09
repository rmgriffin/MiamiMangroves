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

analysis_year <- 2024L
fit_origin_sample <- NULL  # NULL uses all eligible origins.
calculate_site_marginals <- TRUE
n_threads <- max(1L, parallel::detectCores(logical = FALSE) - 2L)
cache_dir <- "Data/intermediate"
counts_path <- file.path(cache_dir, paste0("cbg_site_visits_", analysis_year, ".parquet"))
device_sample_path <- file.path(cache_dir, paste0("dfst_model_source_", analysis_year, ".parquet"))
dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)


# Eligible destinations -------------------------------------------------
needed_site <- c(
  "id", "Name", "island", "mangrove_presence", "mangrove_m2",
  "beach_area_ha", "tree_canopy_pct", "impervious_pct", "sports_facilities",
  "marine_waterfront", "park_area_m2"
)
if (!exists("df") || !all(needed_site %in% names(df))) {
  stop("Missing site attributes in df: ",
       paste(setdiff(needed_site, if (exists("df")) names(df) else character()),
             collapse = ", "))
}

site_attributes <- df %>%
  sf::st_drop_geometry() %>%
  transmute(
    FEATUREID = as.character(id),
    site_name = as.character(Name),
    island = as.integer(island),
    mangrove_presence = as.integer(mangrove_presence),
    mangrove_ha = as.numeric(mangrove_m2) / 10000,
    log_mangrove_ha = log1p(mangrove_ha),
    beach_area_ha = as.numeric(beach_area_ha),
    log_beach_ha = log1p(beach_area_ha),
    tree_canopy_pct = as.numeric(tree_canopy_pct),
    impervious_pct = as.numeric(impervious_pct),
    sports_facilities = as.integer(sports_facilities),
    marine_waterfront = as.integer(marine_waterfront),
    park_ha = as.numeric(park_area_m2) / 10000,
    log_park_area = log(park_ha)
  ) |> 
  filter(island == 0L)

if (anyNA(site_attributes$FEATUREID) || anyDuplicated(site_attributes$FEATUREID)) {
  stop("Every site must have a unique, nonmissing id.")
}
if (anyNA(site_attributes$island) ||
    !all(site_attributes$island %in% c(0L, 1L))) {
  stop("island must be coded 0 or 1 for every site.")
}
site_checks <- tibble(
  all_sites = nrow(site_attributes),
  island_sites_excluded = sum(site_attributes$island == 1L),
  mainland_sites = sum(site_attributes$island == 0L)
)

site_controls <- c("log_beach_ha", "tree_canopy_pct", "impervious_pct", "log_park_area", "sports_facilities", "marine_waterfront")
site_terms <- c("mangrove_presence", "log_mangrove_ha", site_controls)
if (!all(site_attributes$mangrove_presence %in% c(0L, 1L)) ||
    !all(site_attributes$sports_facilities %in% c(0L, 1L)) ||
    !all(site_attributes$marine_waterfront %in% c(0L, 1L)) ||
    any(!is.finite(as.matrix(site_attributes[site_terms]))) ||
    any(!is.finite(site_attributes$park_ha) | site_attributes$park_ha <= 0) ||
    any(site_attributes$mangrove_ha < 0) ||
    any(site_attributes$beach_area_ha < 0) ||
    any(site_attributes$tree_canopy_pct < 0 | site_attributes$tree_canopy_pct > 100) ||
    any(site_attributes$impervious_pct < 0 | site_attributes$impervious_pct > 100)) {
  stop("Site covariates are missing or invalid; inspect df.")
}

# Device-day analysis sample --------------------------------------------
# Select the longest eligible park visit per device-day, retaining source records.
if (file.exists(device_sample_path)) {
  dfst_selected<-arrow::read_parquet(device_sample_path)
  if ("geom" %in% names(dfst_selected) && inherits(dfst$geom,"sfc")) {
    dfst_selected$geom<-sf::st_as_sfc(
      structure(lapply(dfst_selected$geom,as.raw),class="WKB"),
      crs=sf::st_crs(dfst$geom)
    )
  }
} else {
  dfst_selected<-local({
    if (!exists("dfst",inherits=TRUE)) stop("dfst is required to construct the analysis sample.")
    needed_visits<-c("DEVICEID","DAY_IN_FEATURE","FEATUREID",
                     "CENSUS_BLOCK_GROUP_ID","timespan_min","calibrated_visits")
    if (!all(needed_visits %in% names(dfst))) {
      stop("Missing dfst columns: ",
           paste(setdiff(needed_visits,names(dfst)),collapse=", "))
    }

    rows<-dfst |>
      sf::st_drop_geometry() |>
      dplyr::select(dplyr::all_of(needed_visits)) |>
      mutate(
        .source_row=row_number(),
        DAY_IN_FEATURE=as.Date(DAY_IN_FEATURE),
        FEATUREID=as.character(FEATUREID),
        CENSUS_BLOCK_GROUP_ID=as.character(CENSUS_BLOCK_GROUP_ID)
      ) |>
      filter(
        !is.na(DAY_IN_FEATURE),
        as.integer(format(DAY_IN_FEATURE,"%Y"))==analysis_year,
        !is.na(DEVICEID),!is.na(FEATUREID),!is.na(CENSUS_BLOCK_GROUP_ID),
        is.finite(timespan_min),is.finite(calibrated_visits),calibrated_visits>0
      ) |>
      semi_join(site_attributes |> dplyr::select(FEATUREID),by="FEATUREID") |>
      group_by(DEVICEID,DAY_IN_FEATURE) |>
      slice_max(timespan_min,n=1,with_ties=FALSE) |>
      ungroup() |>
      pull(.source_row)

    dfst[rows,] |>
      mutate(
        DAY_IN_FEATURE=as.Date(DAY_IN_FEATURE),
        FEATUREID=as.character(FEATUREID),
        CENSUS_BLOCK_GROUP_ID=as.character(CENSUS_BLOCK_GROUP_ID)
      )
  })
  if (nrow(dfst_selected)==0L) stop("No visitor-days in the analysis year.")
  local({
    cache<-as.data.frame(dfst_selected)
    if ("geom" %in% names(cache) && inherits(cache$geom,"sfc")) {
      cache$geom<-sf::st_as_binary(cache$geom)
      class(cache$geom)<-c("arrow_binary","vctrs_vctr",class(cache$geom),"list")
    }
    arrow::write_parquet(cache,device_sample_path,compression="zstd")
  })
}

# Aggregate selected visitor-days to origin-site counts.
if (!file.exists(counts_path)) {
  cbg_site_visits<-dfst_selected |>
    sf::st_drop_geometry() |>
    group_by(CENSUS_BLOCK_GROUP_ID,FEATUREID) |>
    summarise(
      device_visitor_days=n(),
      calibrated_visitor_days=sum(calibrated_visits),
      .groups="drop"
    )
  arrow::write_parquet(cbg_site_visits,counts_path,compression="zstd")
}
cbg_site_visits<-arrow::read_parquet(counts_path) |>
  mutate(
    CENSUS_BLOCK_GROUP_ID=as.character(CENSUS_BLOCK_GROUP_ID),
    FEATUREID=as.character(FEATUREID)
  )
if (nrow(cbg_site_visits)==0L) stop("No visitor-days in the analysis year.")
if (nrow(dfst_selected)!=sum(cbg_site_visits$device_visitor_days) ||
    !isTRUE(all.equal(sum(dfst_selected$calibrated_visits),
                      sum(cbg_site_visits$calibrated_visitor_days),
                      tolerance=1e-8))) {
  stop("Cached visit counts do not match selected device records; delete ",
       counts_path," and rerun.")
}


# Construct origin-site model data -------------------------------------------------
cbg_site_model <- local({

  if (!exists("distance_results", inherits=TRUE)) {
    distance_file <- file.path(cache_dir, "travel_distance_results.csv")
    if (!file.exists(distance_file)) stop("Missing distance_results and CSV cache.")
    distance_results <- readr::read_csv(
      distance_file,
      col_types=readr::cols(
        CENSUS_BLOCK_GROUP_ID=readr::col_character(),
        FEATUREID=readr::col_character(),
        .default=readr::col_guess()
      ),
      show_col_types=FALSE
    )
  }

  needed_distance <- c("CENSUS_BLOCK_GROUP_ID", "FEATUREID", "distance_m",
                       "duration_min", "med_hh_income", "osrm_success")
  if (!all(needed_distance %in% names(distance_results))) {
    stop("Missing distance_results columns: ",
         paste(setdiff(needed_distance, names(distance_results)), collapse=", "))
  }

  eligible_origins <- cbg_site_visits %>%
    distinct(CENSUS_BLOCK_GROUP_ID) %>%
    inner_join(
      cbg_population %>%
        filter(is.finite(cbg_population), cbg_population > 0),
      by="CENSUS_BLOCK_GROUP_ID"
    )

  dist_input <- distance_results %>%
    dplyr::select(dplyr::all_of(needed_distance))
  if (inherits(dist_input, c("Dataset", "arrow_dplyr_query"))) {
    dist_input <- dplyr::collect(dist_input)
  }

  model <- dist_input %>%
    mutate(
      CENSUS_BLOCK_GROUP_ID=as.character(CENSUS_BLOCK_GROUP_ID),
      FEATUREID=as.character(FEATUREID)
    ) %>%
    semi_join(eligible_origins, by="CENSUS_BLOCK_GROUP_ID") %>%
    inner_join(
      site_attributes %>%
        dplyr::select(FEATUREID, mangrove_ha, park_ha, dplyr::all_of(site_terms)),
      by="FEATUREID"
    ) %>%
    filter(
      osrm_success,
      is.finite(distance_m), is.finite(duration_min),
      is.finite(med_hh_income), med_hh_income > 0,
      distance_m >= 0, duration_min >= 0
    ) %>%
    mutate(
      # Generalized round-trip travel cost in dollars.
      travel_cost_dollars=
        (distance_m / 1000 / 1.609344) * travel_cost_params$auto_cost_per_mile +
        (duration_min / 60) * travel_cost_params$vot_fraction *
        med_hh_income / travel_cost_params$annual_work_hours
    ) %>%
    dplyr::select(
      CENSUS_BLOCK_GROUP_ID, FEATUREID, travel_cost_dollars,
      med_hh_income, mangrove_ha, park_ha, dplyr::all_of(site_terms)
    )

  if (anyDuplicated(model[c("CENSUS_BLOCK_GROUP_ID", "FEATUREID")])) {
    stop("Duplicate origin-site travel pairs.")
  }

  chosen_unmatched <- cbg_site_visits %>%
    semi_join(eligible_origins, by="CENSUS_BLOCK_GROUP_ID") %>%
    anti_join(model, by=c("CENSUS_BLOCK_GROUP_ID", "FEATUREID"))

  if (nrow(chosen_unmatched) > 0) {
    warning(
      nrow(chosen_unmatched),
      " observed CBG-site pairs excluded due to invalid travel routes (",
      sum(chosen_unmatched$device_visitor_days),
      " device visitor-days; ",
      round(
        100 * sum(chosen_unmatched$device_visitor_days) /
          sum(cbg_site_visits$device_visitor_days),
        4
      ),
      "% of observed device visitor-days)."
    )
  }

  model <- model %>%
    left_join(
      cbg_site_visits %>%
        dplyr::select(CENSUS_BLOCK_GROUP_ID, FEATUREID,
                      device_visitor_days, calibrated_visitor_days),
      by=c("CENSUS_BLOCK_GROUP_ID", "FEATUREID")
    ) %>%
    mutate(
      device_visitor_days=replace_na(device_visitor_days, 0L),
      calibrated_visitor_days=replace_na(calibrated_visitor_days, 0)
    ) %>%
    group_by(CENSUS_BLOCK_GROUP_ID) %>%
    filter(any(device_visitor_days > 0)) %>%
    ungroup()

  # Select origin choice sets for estimation.
  if (!is.null(fit_origin_sample)) {
    set.seed(20261020)
    sample_origins <- model %>%
      distinct(CENSUS_BLOCK_GROUP_ID) %>%
      pull(CENSUS_BLOCK_GROUP_ID)

    model <- model %>%
      filter(CENSUS_BLOCK_GROUP_ID %in% sample(
        sample_origins, min(fit_origin_sample, length(sample_origins))
      ))
  }

  model
})

# Retain the device records represented by the final origin-site choice sets.
dfst_model<-dfst_selected |>
  inner_join(
    cbg_site_model |>
      filter(device_visitor_days>0) |>
      dplyr::select(
        CENSUS_BLOCK_GROUP_ID,FEATUREID,
        rum_travel_cost_dollars=travel_cost_dollars
      ),
    by=c("CENSUS_BLOCK_GROUP_ID","FEATUREID")
  )

rm(dfst_selected)

# Verify device-level and model-level visitation totals.
stopifnot(
  nrow(dfst_model)==sum(cbg_site_model$device_visitor_days),
  isTRUE(all.equal(
    sum(dfst_model$calibrated_visits),
    sum(cbg_site_model$calibrated_visitor_days),
    tolerance=1e-8
  ))
)

# Check destination coverage
choice_set_coverage <- cbg_site_model %>%
  count(CENSUS_BLOCK_GROUP_ID, name = "available_sites") %>%
  summarise(
    origins = n(),
    eligible_sites = nrow(site_attributes),
    complete_origins = sum(available_sites == nrow(site_attributes)),
    minimum_sites = min(available_sites),
    median_sites = median(available_sites),
    .groups = "drop"
  )

if (choice_set_coverage$complete_origins < choice_set_coverage$origins) {
  warning("Some CBGs have incomplete OSRM choice sets; investigate before interpreting welfare.")
}

# Stage 1: conditional destination choice -------------------------------
# Nested specifications assess coefficient sensitivity to site controls.
fit_site_choice <- function(mangrove_term, controls = character(), data = cbg_site_model) {
  rhs <- c("travel_cost_dollars", mangrove_term, controls)

  formula <- stats::as.formula(paste(
    "device_visitor_days ~", paste(rhs, collapse = " + "),
    "| CENSUS_BLOCK_GROUP_ID"
  ))

  fit <- fixest::fepois(
    formula,
    data = data,
    vcov = ~ CENSUS_BLOCK_GROUP_ID + FEATUREID,
    nthreads = n_threads,
    lean = TRUE
  )

  b <- stats::coef(fit)

  if (!all(rhs %in% names(b))) {
    stop("Omitted site-choice coefficients: ",
         paste(setdiff(rhs, names(b)), collapse = ", "))
  }

  if (!is.finite(b[["travel_cost_dollars"]]) ||
      b[["travel_cost_dollars"]] >= 0) {
    stop("Travel-cost coefficient must be negative for money-metric welfare.")
  }

  fit
}

# Full destination-choice specifications
m_site_presence <- fit_site_choice("mangrove_presence", site_controls)
m_site_extent <- fit_site_choice("log_mangrove_ha", site_controls)

# Park-area functional form sensitivity
m_site_extent_quadratic <- fixest::fepois(
  device_visitor_days ~ travel_cost_dollars + log_mangrove_ha +
    log_beach_ha + tree_canopy_pct + impervious_pct +
    log_park_area + I(log_park_area^2) +
    sports_facilities + marine_waterfront |
    CENSUS_BLOCK_GROUP_ID,
  data = cbg_site_model,
  vcov = ~ CENSUS_BLOCK_GROUP_ID + FEATUREID,
  nthreads = n_threads,
  lean = TRUE
)

# Nested specification comparisons
m_site_progression <- list(
  Presence = list(
    "Travel cost" = fit_site_choice("mangrove_presence"),
    "Park area" = fit_site_choice(
      "mangrove_presence",
      "log_park_area"
    ),
    "Full model" = m_site_presence
  ),
  Extent = list(
    "Travel cost" = fit_site_choice("log_mangrove_ha"),
    "Park area" = fit_site_choice(
      "log_mangrove_ha",
      "log_park_area"
    ),
    "Full model" = m_site_extent
  )
)

bp <- stats::coef(m_site_presence)
be <- stats::coef(m_site_extent)

# Stage 1: inclusive values, excluding origin nuisance fixed effects ----
cbg_site_model <- cbg_site_model %>%
  mutate(
    V_presence = unname(bp[["travel_cost_dollars"]]) * travel_cost_dollars +
      unname(bp[["mangrove_presence"]]) * mangrove_presence,
    V_extent = unname(be[["travel_cost_dollars"]]) * travel_cost_dollars +
      unname(be[["log_mangrove_ha"]]) * log_mangrove_ha
  )
for (term in site_controls) {
  cbg_site_model[["V_presence"]] <- cbg_site_model[["V_presence"]] +
    unname(bp[[term]]) * cbg_site_model[[term]]
  cbg_site_model[["V_extent"]] <- cbg_site_model[["V_extent"]] +
    unname(be[[term]]) * cbg_site_model[[term]]
}
iv_data <- cbg_site_model %>%
  group_by(CENSUS_BLOCK_GROUP_ID) %>%
  summarise(
    max_presence = max(V_presence),
    IV_presence = max_presence + log(sum(exp(V_presence - max_presence))),
    max_extent = max(V_extent),
    IV_extent = max_extent + log(sum(exp(V_extent - max_extent))),
    .groups = "drop"
  ) %>%
  mutate(
    monetary_iv_presence = IV_presence / -unname(bp[["travel_cost_dollars"]]),
    monetary_iv_extent = IV_extent / -unname(be[["travel_cost_dollars"]])
  ) %>%
  dplyr::select(CENSUS_BLOCK_GROUP_ID, IV_presence, IV_extent,
                monetary_iv_presence, monetary_iv_extent)

# Stage 2: annual trip frequency ---------------------------------------
cbg_annual <- cbg_site_model %>%
  group_by(CENSUS_BLOCK_GROUP_ID) %>%
  summarise(
    device_visitor_days = sum(device_visitor_days),
    calibrated_visitor_days = sum(calibrated_visitor_days),
    med_hh_income = first(med_hh_income),
    .groups = "drop"
  ) %>%
  inner_join(cbg_population, by = "CENSUS_BLOCK_GROUP_ID") %>%
  inner_join(iv_data, by = "CENSUS_BLOCK_GROUP_ID") %>%
  mutate(
    log_income = log(med_hh_income),
    state_fips = substr(CENSUS_BLOCK_GROUP_ID, 1, 2)
  )
if (!all(is.finite(cbg_annual$calibrated_visitor_days)) ||
    !all(is.finite(cbg_annual$log_income)) ||
    !all(cbg_annual$cbg_population > 0)) {
  stop("CBG visitor-days, income, or Census population are invalid.")
}
fit_frequency <- function(iv_term, state_fe = FALSE, data = cbg_annual) {
  formula <- stats::as.formula(paste0(
    "calibrated_visitor_days ~ ", iv_term, " + log_income",
    if (state_fe) " | state_fips" else ""
  ))
  fixest::fepois(
    formula,
    offset = ~ log(cbg_population),
    data = data,
    vcov = "hetero",
    nthreads = n_threads,
    glm.iter = 100,
    lean = FALSE
  )
}
m_freq_presence <- fit_frequency("monetary_iv_presence")
m_freq_extent <- fit_frequency("monetary_iv_extent")
state_models <- NULL

cbg_annual <- cbg_annual %>%
  mutate(
    predicted_visits_presence = as.numeric(predict(m_freq_presence, type = "response")),
    predicted_visits_extent = as.numeric(predict(m_freq_extent, type = "response")),
    observed_visitor_days_per_resident = calibrated_visitor_days / cbg_population,
    predicted_rate_presence = predicted_visits_presence / cbg_population,
    predicted_rate_extent = predicted_visits_extent / cbg_population
  )
if (any(c(stats::coef(m_freq_presence)[["monetary_iv_presence"]],
          stats::coef(m_freq_extent)[["monetary_iv_extent"]]) <= 0)) {
  warning("A frequency model has a nonpositive accessibility response; review welfare interpretation.")
}

# Optional site-by-site marginal annual welfare -------------------------
# Restoration converts existing non-canopy park land to mature mangrove canopy.
# Park area, beach area, impervious cover, and other attributes stay fixed.
site_marginals<-if (calculate_site_marginals) local({
  gamma<-unname(stats::coef(m_freq_extent)[["monetary_iv_extent"]])
  cost_coef<--unname(be[["travel_cost_dollars"]])

  cbg_site_model |>
    inner_join(
      cbg_annual |>
        dplyr::select(
          CENSUS_BLOCK_GROUP_ID,
          calibrated_visitor_days,
          predicted_visits_extent
        ),
      by="CENSUS_BLOCK_GROUP_ID",
      suffix=c("","_annual")
    ) |>
    mutate(
      destination_probability=exp(
        V_extent-iv_data$IV_extent[
          match(CENSUS_BLOCK_GROUP_ID,iv_data$CENSUS_BLOCK_GROUP_ID)
        ]
      ),
      restoration_capacity_ha=pmax(
        0,
        pmin(
          park_ha*(100-tree_canopy_pct)/100,
          park_ha-mangrove_ha
        )
      ),
      one_ha_feasible=restoration_capacity_ha>=1 & marine_waterfront==1L,
      joint_utility_per_ha=
        unname(be[["log_mangrove_ha"]])/(1+mangrove_ha)+
        unname(be[["tree_canopy_pct"]])*100/park_ha,
      marginal_dollars_per_trip_ha=
        destination_probability*joint_utility_per_ha/cost_coef,
      conditional_dollars_per_ha=
        calibrated_visitor_days_annual*marginal_dollars_per_trip_ha,
      linked_dollars_per_ha=
        predicted_visits_extent*marginal_dollars_per_trip_ha,
      utility_change_1ha=if_else(
        one_ha_feasible,
        unname(be[["log_mangrove_ha"]])*
          (log1p(mangrove_ha+1)-log_mangrove_ha)+
          unname(be[["tree_canopy_pct"]])*100/park_ha,
        NA_real_
      ),
      iv_change_dollars_1ha=log1p(
        destination_probability*expm1(utility_change_1ha)
      )/cost_coef,
      conditional_dollars_1ha=
        calibrated_visitor_days_annual*iv_change_dollars_1ha,
      predicted_visit_change_1ha=
        predicted_visits_extent*expm1(gamma*iv_change_dollars_1ha),
      linked_dollars_1ha=if (abs(gamma)<1e-10) {
        predicted_visits_extent*iv_change_dollars_1ha
      } else {
        predicted_visits_extent*
          expm1(gamma*iv_change_dollars_1ha)/gamma
      }
    ) |>
    group_by(FEATUREID) |>
    summarise(
      restoration_capacity_ha=first(restoration_capacity_ha),
      one_ha_feasible=first(one_ha_feasible),
      conditional_annual_dollars_per_ha=
        if (first(restoration_capacity_ha)>0) {
          sum(conditional_dollars_per_ha)
        } else NA_real_,
      linked_annual_dollars_per_ha=
        if (first(restoration_capacity_ha)>0) {
          sum(linked_dollars_per_ha)
        } else NA_real_,
      conditional_annual_dollars_1ha=
        if (first(one_ha_feasible)) {
          sum(conditional_dollars_1ha)
        } else NA_real_,
      linked_annual_dollars_1ha=
        if (first(one_ha_feasible)) {
          sum(linked_dollars_1ha)
        } else NA_real_,
      predicted_annual_visit_change_1ha=
        if (first(one_ha_feasible)) {
          sum(predicted_visit_change_1ha)
        } else NA_real_,
      .groups="drop"
    ) |>
    left_join(
      site_attributes |>
        dplyr::select(
          FEATUREID,site_name,mangrove_presence,
          mangrove_ha,park_ha,tree_canopy_pct
        ),
      by="FEATUREID"
    ) |>
    filter(one_ha_feasible) |>
    arrange(desc(linked_annual_dollars_1ha))
}) else NULL

rum_results <- local({
  # Comparable coefficient and interpretation tables -----------------------------------
  extract_coefficients <- function(fit, model, stage) {
    ct <- fixest::coeftable(fit)
    tibble(
      stage = stage, model = model, term = rownames(ct),
      estimate = as.numeric(ct[, 1]), se = as.numeric(ct[, 2]),
      p_value = as.numeric(ct[, 4])
    )
  }
  coefficients <- bind_rows(
    extract_coefficients(m_site_presence, "Presence", "Site choice"),
    extract_coefficients(m_site_extent, "Extent", "Site choice"),
    extract_coefficients(m_freq_presence, "Presence", "Annual frequency"),
    extract_coefficients(m_freq_extent, "Extent", "Annual frequency")
  )
  if (!is.null(state_models)) {
    coefficients <- bind_rows(
      coefficients,
      extract_coefficients(state_models$presence, "Presence + state FE", "Annual frequency"),
      extract_coefficients(state_models$extent, "Extent + state FE", "Annual frequency")
    )
  }

  # Attribute changes expressed in their original units.
  choice_effects <- bind_rows(lapply(
    list(Presence = m_site_presence, Extent = m_site_extent),
    function(fit) {
      b <- stats::coef(fit)
      ct <- fixest::coeftable(fit)
      model <- if ("mangrove_presence" %in% names(b)) "Presence" else "Extent"
      increments <- c(
        "Travel cost: +$10" = 10,
        "Beach: 0 to 1 ha" = log(2),
        "Tree canopy: +1 percentage point" = 1,
        "Impervious: +1 percentage point" = 1,
        "Sports facilities: absent to present" = 1,
        "Marine waterfront: absent to present" = 1
      )
      term_map <- c(
        "Travel cost: +$10" = "travel_cost_dollars",
        "Beach: 0 to 1 ha" = "log_beach_ha",
        "Tree canopy: +1 percentage point" = "tree_canopy_pct",
        "Impervious: +1 percentage point" = "impervious_pct",
        "Sports facilities: absent to present" = "sports_facilities",
        "Marine waterfront: absent to present" = "marine_waterfront"
      )
      increments <- c(increments, "Park area: doubled" = log(2))
      term_map <- c(term_map, "Park area: doubled" = "log_park_area")
      mangrove_label <- if (model == "Presence")
        "Mangroves: absent to present" else "Mangroves: 0 to 1 ha"
      increments <- c(increments, stats::setNames(
        if (model == "Presence") 1 else log(2), mangrove_label
      ))
      term_map <- c(term_map, stats::setNames(
        if (model == "Presence") "mangrove_presence" else "log_mangrove_ha",
        mangrove_label
      ))
      tibble(
        model = model,
        comparison = names(increments),
        coefficient = unname(term_map),
        increment = unname(increments)
      ) %>%
        mutate(
          log_odds_change = unname(b[coefficient]) * increment,
          log_odds_se = unname(ct[coefficient, "Std. Error"]) * abs(increment),
          odds_ratio = exp(log_odds_change),
          odds_ratio_low = exp(log_odds_change - 1.96 * log_odds_se),
          odds_ratio_high = exp(log_odds_change + 1.96 * log_odds_se),
          percent_change_odds = 100 * (odds_ratio - 1)
        ) %>%
        dplyr::select(model, comparison, odds_ratio, odds_ratio_low,
                      odds_ratio_high, percent_change_odds)
    }
  ))

  mangrove_money <- bind_rows(lapply(
    list(Presence = m_site_presence, Extent = m_site_extent),
    function(fit) {
      b <- stats::coef(fit)
      model <- if ("mangrove_presence" %in% names(b)) "Presence" else "Extent"
      attr <- if (model == "Presence") "mangrove_presence" else "log_mangrove_ha"
      step <- if (model == "Presence") 1 else log(2)
      vcv <- stats::vcov(fit)
      bm <- unname(b[[attr]])
      bc <- unname(b[["travel_cost_dollars"]])
      grad_m <- -step / bc
      grad_c <- step * bm / bc^2
      variance <- grad_m^2 * vcv[attr, attr] +
        grad_c^2 * vcv["travel_cost_dollars", "travel_cost_dollars"] +
        2 * grad_m * grad_c * vcv[attr, "travel_cost_dollars"]
      est <- -step * bm / bc
      se <- sqrt(max(variance, 0))
      tibble(
        model = model,
        mangrove_change = if (model == "Presence")
          "Absent to present" else "0 to 1 hectare",
        money_equivalent_dollars = est,
        ci_low = est - 1.96 * se,
        ci_high = est + 1.96 * se
      )
    }
  ))

  coverage <- cbg_annual %>%
    summarise(
      origin_cbgs = n(),
      mainland_sites = nrow(site_attributes),
      represented_population = sum(cbg_population),
      observed_device_visitor_days = sum(device_visitor_days),
      calibrated_visitor_days = sum(calibrated_visitor_days),
      predicted_visitor_days_presence = sum(predicted_visits_presence),
      predicted_visitor_days_extent = sum(predicted_visits_extent),
      annual_visitor_days_per_resident =
        sum(calibrated_visitor_days) / sum(cbg_population),
      .groups = "drop"
    )

  frequency_effects <- tibble(
    model = c("Presence", "Extent"),
    coefficient_per_dollar = c(
      unname(stats::coef(m_freq_presence)[["monetary_iv_presence"]]),
      unname(stats::coef(m_freq_extent)[["monetary_iv_extent"]])
    )
  ) %>%
    mutate(percent_change_visits_per_dollar =
             100 * (exp(coefficient_per_dollar) - 1))

  model_fit <- purrr::imap_dfr(
    list(
      "Site: presence" = m_site_presence,
      "Site: extent" = m_site_extent,
      "Frequency: presence" = m_freq_presence,
      "Frequency: extent" = m_freq_extent
    ),
    ~ tibble(
      model = .y,
      observations = as.numeric(stats::nobs(.x)),
      aic = as.numeric(stats::AIC(.x))
    )
  )

  # Compact synthesis figures --------------------------------------------
  figures <- list(
    site_choice_odds = ggplot(choice_effects, aes(x = comparison, y = odds_ratio)) +
      geom_hline(yintercept = 1, linetype = "dashed") +
      geom_pointrange(aes(ymin = odds_ratio_low, ymax = odds_ratio_high)) +
      coord_flip() +
      facet_wrap(~ model) +
      labs(title = "Destination-choice effects",
           subtitle = "Odds ratios for specified attribute changes; 95% clustered intervals",
           x = NULL, y = "Choice odds ratio") +
      theme_minimal(),
    frequency_fit = cbg_annual %>%
      mutate(accessibility_decile = ntile(monetary_iv_extent, 10)) %>%
      group_by(accessibility_decile) %>%
      summarise(
        observed = sum(calibrated_visitor_days) / sum(cbg_population),
        predicted = sum(predicted_visits_extent) / sum(cbg_population),
        .groups = "drop"
      ) %>%
      pivot_longer(c(observed, predicted), names_to = "series", values_to = "rate") %>%
      ggplot(aes(x = accessibility_decile, y = rate, linetype = series)) +
      geom_line() + geom_point() +
      scale_x_continuous(breaks = 1:10) +
      labs(title = "Annual visitation and recreation accessibility",
           subtitle = "Population-weighted visitor-day rates by monetary-accessibility decile",
           x = "Accessibility decile (extent model)",
           y = "Annual visitor-days per resident", linetype = "Series") +
      theme_minimal()
  )
  if (!is.null(site_marginals)) {
    figures$site_marginals <- site_marginals %>%
      slice_max(order_by = abs(linked_annual_dollars_per_ha),
                n = 15, with_ties = FALSE) %>%
      ggplot(aes(x = reorder(site_name, linked_annual_dollars_per_ha),
                 y = linked_annual_dollars_per_ha)) +
      geom_col() + coord_flip() +
      labs(title = "Largest site-level marginal mangrove values",
           subtitle = "Joint mangrove and canopy effect; local derivative at baseline trips",
           x = NULL, y = "Annual marginal welfare ($/additional ha)") +
      theme_minimal()
  }

  list(
    models = list(
      site_presence = m_site_presence, site_extent = m_site_extent,
      frequency_presence = m_freq_presence, frequency_extent = m_freq_extent,
      frequency_state = state_models
    ),
    tables = list(
      inventory = site_checks, choice_set_coverage = choice_set_coverage,
      population_coverage = coverage, model_fit = model_fit,
      coefficients = coefficients, site_choice_effects = choice_effects,
      mangrove_money = mangrove_money, frequency_effects = frequency_effects,
      site_marginals = site_marginals
    ),
    figures = figures,
    data = list(cbg_annual = cbg_annual, site_attributes = site_attributes)
  )
})

# Geographic sensitivity: Florida-origin visitors ----------------------
florida <- local({
  model <- cbg_site_model %>%
    filter(substr(CENSUS_BLOCK_GROUP_ID, 1, 2) == "12")
  if (nrow(model) == 0L) stop("No Florida-origin observations.")

  site_presence <- fit_site_choice("mangrove_presence", site_controls, data = model)
  site_extent <- fit_site_choice("log_mangrove_ha", site_controls, data = model)
  bp_fl <- stats::coef(site_presence)
  be_fl <- stats::coef(site_extent)

  # Utilities use the Florida-estimated destination-choice coefficients.
  model <- model %>%
    mutate(
      V_presence = unname(bp_fl[["travel_cost_dollars"]]) * travel_cost_dollars +
        unname(bp_fl[["mangrove_presence"]]) * mangrove_presence,
      V_extent = unname(be_fl[["travel_cost_dollars"]]) * travel_cost_dollars +
        unname(be_fl[["log_mangrove_ha"]]) * log_mangrove_ha
    )
  for (term in site_controls) {
    model[["V_presence"]] <- model[["V_presence"]] +
      unname(bp_fl[[term]]) * model[[term]]
    model[["V_extent"]] <- model[["V_extent"]] +
      unname(be_fl[[term]]) * model[[term]]
  }

  iv_fl <- model %>%
    group_by(CENSUS_BLOCK_GROUP_ID) %>%
    summarise(
      max_presence = max(V_presence),
      IV_presence = max_presence + log(sum(exp(V_presence - max_presence))),
      max_extent = max(V_extent),
      IV_extent = max_extent + log(sum(exp(V_extent - max_extent))),
      .groups = "drop"
    ) %>%
    mutate(
      monetary_iv_presence = IV_presence / -unname(bp_fl[["travel_cost_dollars"]]),
      monetary_iv_extent = IV_extent / -unname(be_fl[["travel_cost_dollars"]])
    ) %>%
    dplyr::select(CENSUS_BLOCK_GROUP_ID, monetary_iv_presence, monetary_iv_extent)

  annual_fl <- model %>%
    group_by(CENSUS_BLOCK_GROUP_ID) %>%
    summarise(
      device_visitor_days = sum(device_visitor_days),
      calibrated_visitor_days = sum(calibrated_visitor_days),
      med_hh_income = first(med_hh_income),
      .groups = "drop"
    ) %>%
    inner_join(cbg_population, by = "CENSUS_BLOCK_GROUP_ID") %>%
    inner_join(iv_fl, by = "CENSUS_BLOCK_GROUP_ID") %>%
    mutate(log_income = log(med_hh_income))

  list(
    models = list(
      site_presence = site_presence,
      site_extent = site_extent,
      frequency_presence = fit_frequency("monetary_iv_presence", data = annual_fl),
      frequency_extent = fit_frequency("monetary_iv_extent", data = annual_fl)
    ),
    coverage = tibble(
      origin_cbgs = nrow(annual_fl),
      represented_population = sum(annual_fl$cbg_population),
      device_visitor_days = sum(annual_fl$device_visitor_days),
      calibrated_visitor_days = sum(annual_fl$calibrated_visitor_days),
      share_calibrated_visitor_days =
        sum(annual_fl$calibrated_visitor_days) / sum(cbg_annual$calibrated_visitor_days)
    )
  )
})
rum_results$models$florida <- florida$models
rum_results$models$site_extent_quadratic <- m_site_extent_quadratic
rum_results$tables$florida_coverage <- florida$coverage
rm(florida)

rum_results$tables$coefficients <- bind_rows(
  rum_results$tables$coefficients,
  purrr::imap_dfr(
    rum_results$models$florida,
    function(fit, model) {
      ct <- fixest::coeftable(fit)
      tibble(
        stage = if (startsWith(model, "site_")) "Site choice" else "Annual frequency",
        model = paste("Florida", if (endsWith(model, "presence")) "Presence" else "Extent"),
        term = rownames(ct),
        estimate = as.numeric(ct[, 1]),
        se = as.numeric(ct[, 2]),
        p_value = as.numeric(ct[, 4])
      )
    }
  )
)

rum_results$tables$site_sensitivity <- purrr::imap_dfr(
  list(
    "All origins: presence" = m_site_presence,
    "Florida: presence" = rum_results$models$florida$site_presence,
    "All origins: extent" = m_site_extent,
    "Florida: extent" = rum_results$models$florida$site_extent,
    "All origins: extent, quadratic park size" = m_site_extent_quadratic
  ),
  function(fit, specification) {
    b <- stats::coef(fit)
    term <- if ("mangrove_presence" %in% names(b))
      "mangrove_presence" else "log_mangrove_ha"
    increment <- if (term == "mangrove_presence") 1 else log(2)
    ct <- fixest::coeftable(fit)
    tibble(
      specification = specification,
      observations = as.numeric(stats::nobs(fit)),
      mangrove_coefficient = unname(b[[term]]),
      se = unname(ct[term, "Std. Error"]),
      p_value = unname(ct[term, 4]),
      odds_ratio = exp(unname(b[[term]]) * increment),
      monetary_equivalent = unname(b[[term]]) * increment /
        -unname(b[["travel_cost_dollars"]])
    )
  }
)

# Mangrove coefficient sensitivity --------------------------------------
rum_results$tables$mangrove_progression <- purrr::imap_dfr(
  m_site_progression,
  function(models, measure) {

    purrr::imap_dfr(
      models,
      function(fit, specification) {

        term <- if (measure == "Presence") {
          "mangrove_presence"
        } else {
          "log_mangrove_ha"
        }

        increment <- if (measure == "Presence") 1 else log(2)

        ct <- fixest::coeftable(fit)

        tibble(
          measure = measure,
          specification = specification,
          coefficient = unname(ct[term, 1]),
          std_error = unname(ct[term, 2]),
          p_value = unname(ct[term, 4]),
          odds_ratio = exp(coefficient * increment),
          ci_low = exp((coefficient - 1.96 * std_error) * increment),
          ci_high = exp((coefficient + 1.96 * std_error) * increment),
          monetary_equivalent = coefficient * increment /
            -unname(stats::coef(fit)[["travel_cost_dollars"]])
        )
      }
    )
  }
)

rum_results$models$site_progression<-m_site_progression

rum_results$tables$site_marginals<-local({ # Mangrove and canopy welfare decomposition
  bm<-unname(coef(rum_results$models$site_extent)[["log_mangrove_ha"]])
  bc<-unname(coef(rum_results$models$site_extent)[["tree_canopy_pct"]])
  bt<-unname(coef(rum_results$models$site_extent)[["travel_cost_dollars"]])
  gamma<-unname(coef(rum_results$models$frequency_extent)[["monetary_iv_extent"]])

  d<-cbg_site_model |>
    semi_join(
      rum_results$tables$site_marginals |>
        slice_max(order_by=abs(linked_annual_dollars_1ha),n=15,with_ties=FALSE) |>
        dplyr::select(FEATUREID),
      by="FEATUREID"
    ) |>
    inner_join(
      cbg_annual |>
        dplyr::select(CENSUS_BLOCK_GROUP_ID,IV_extent,predicted_visits_extent),
      by="CENSUS_BLOCK_GROUP_ID"
    ) |>
    mutate(
      p=exp(V_extent-IV_extent),
      delta_m=bm*(log1p(mangrove_ha+1)-log1p(mangrove_ha)),
      delta_c=bc*100/park_ha,
      iv_m=log1p(p*expm1(delta_m))/-bt,
      iv_c=log1p(p*expm1(delta_c))/-bt,
      iv_both=log1p(p*expm1(delta_m+delta_c))/-bt,
      w_m=predicted_visits_extent*expm1(gamma*iv_m)/gamma,
      w_c=predicted_visits_extent*expm1(gamma*iv_c)/gamma,
      w_both=predicted_visits_extent*expm1(gamma*iv_both)/gamma
    ) |>
    group_by(FEATUREID) |>
    summarise(
      mangrove_welfare_1ha=sum((w_m+w_both-w_c)/2),
      canopy_welfare_1ha=sum((w_c+w_both-w_m)/2),
      calibrated_site_visitor_days=sum(calibrated_visitor_days),
      .groups="drop"
    )

  rum_results$tables$site_marginals |>
    left_join(d,by="FEATUREID")
})


rum_results$tables$site_responses<-local({ # Welfare response decomposition
  bm<-unname(coef(rum_results$models$site_extent)[["log_mangrove_ha"]])
  bc<-unname(coef(rum_results$models$site_extent)[["tree_canopy_pct"]])
  bt<-unname(coef(rum_results$models$site_extent)[["travel_cost_dollars"]])
  gamma<-unname(coef(rum_results$models$frequency_extent)[["monetary_iv_extent"]])

  cbg_site_model |>
    semi_join(
      rum_results$tables$site_marginals |>
        filter(
          !is.na(mangrove_welfare_1ha),
          calibrated_site_visitor_days>0
        ) |>
        dplyr::select(FEATUREID),
      by="FEATUREID"
    ) |>
    inner_join(
      cbg_annual |>
        dplyr::select(
          CENSUS_BLOCK_GROUP_ID,
          IV_extent,
          predicted_visits_extent
        ),
      by="CENSUS_BLOCK_GROUP_ID"
    ) |>
    mutate(
      p0=exp(V_extent-IV_extent),
      delta_v=bm*(log1p(mangrove_ha+1)-log1p(mangrove_ha))+
        bc*100/park_ha,
      delta_iv=log1p(p0*expm1(delta_v))/(-bt),
      direct_welfare=predicted_visits_extent*p0*delta_v/(-bt),
      substitution_welfare=predicted_visits_extent*
        (delta_iv-p0*delta_v/(-bt)),
      frequency_adjustment=predicted_visits_extent*
        (expm1(gamma*delta_iv)/gamma-delta_iv)
    ) |>
    group_by(FEATUREID) |>
    summarise(
      direct_welfare=sum(direct_welfare),
      substitution_welfare=sum(substitution_welfare),
      frequency_adjustment=sum(frequency_adjustment),
      .groups="drop"
    ) |>
    left_join(
      rum_results$tables$site_marginals |>
        dplyr::select(
          FEATUREID,
          site_name,
          linked_annual_dollars_1ha,
          calibrated_site_visitor_days
        ),
      by="FEATUREID"
    ) |>
    mutate(
      net_welfare=direct_welfare+
        substitution_welfare+frequency_adjustment
    )
})



# Consolidated output -----------------------------------------------------
# Model summaries and synthesis figures.
cat("\n================ MIAMI MANGROVE RECREATION RUM ================\n")
cat("Year:", analysis_year,
    "| Origins:", if (is.null(fit_origin_sample)) "all" else fit_origin_sample,
    "| Travel cost: round-trip inputs (not doubled)\n")
cat("\nSITE INVENTORY AND CHOICE-SET COVERAGE\n")
print(rum_results$tables$inventory, width = Inf)
print(rum_results$tables$choice_set_coverage, width = Inf)
cat("\nDESTINATION CHOICE: PRESENCE VS. EXTENT\n")
print(fixest::etable(rum_results$models$site_presence,
                    rum_results$models$site_extent, digits = 3))
cat("\nANNUAL TRIP FREQUENCY: PRESENCE VS. EXTENT\n")
print(fixest::etable(rum_results$models$frequency_presence,
                    rum_results$models$frequency_extent, digits = 3))
if (!is.null(rum_results$models$frequency_state)) {
  cat("\nTRIP FREQUENCY WITH STATE FIXED EFFECTS (ROBUSTNESS)\n")
  print(fixest::etable(rum_results$models$frequency_state$presence,
                      rum_results$models$frequency_state$extent, digits = 3))
}
cat("\nSITE-ATTRIBUTE EFFECTS: CHOICE ODDS RATIOS (95% CI)\n")
print(rum_results$tables$site_choice_effects, n = Inf, width = Inf)
cat("\nMANGROVE ATTRIBUTE: MONETARY EQUIVALENT FOR A CHOSEN SITE (95% CI)\n")
print(rum_results$tables$mangrove_money, n = Inf, width = Inf)
cat("\nFREQUENCY RESPONSE TO RECREATION ACCESSIBILITY\n")
print(rum_results$tables$frequency_effects, n = Inf, width = Inf)
cat("\nMODEL FIT AND REPRESENTED POPULATION\n")
print(rum_results$tables$model_fit, n = Inf, width = Inf)
print(rum_results$tables$population_coverage, width = Inf)
if (!is.null(rum_results$tables$site_marginals)) {
  cat("\nTOP 15 SITES BY ABSOLUTE LINKED MARGINAL ANNUAL MANGROVE VALUE\n")
  print(rum_results$tables$site_marginals %>%
          slice_max(order_by = abs(linked_annual_dollars_per_ha),
                    n = 15, with_ties = FALSE), n = Inf, width = Inf)
}
cat("\nFIGURES\n")
print(rum_results$figures$site_choice_odds)
print(rum_results$figures$frequency_fit)
if (!is.null(rum_results$figures$site_marginals)) {
  print(rum_results$figures$site_marginals)
}
cat("\nFLORIDA ORIGIN SAMPLE\n")
print(rum_results$tables$florida_coverage, width = Inf)
cat("\nGEOGRAPHIC SENSITIVITY: DESTINATION CHOICE\n")
cat("Columns: All origins presence | Florida presence | All origins extent | Florida extent\n")
print(fixest::etable(
  rum_results$models$site_presence,
  rum_results$models$florida$site_presence,
  rum_results$models$site_extent,
  rum_results$models$florida$site_extent, digits = 3
))
cat("\nGEOGRAPHIC SENSITIVITY: ANNUAL FREQUENCY\n")
cat("Columns: All origins presence | Florida presence | All origins extent | Florida extent\n")
print(fixest::etable(
  rum_results$models$frequency_presence,
  rum_results$models$florida$frequency_presence,
  rum_results$models$frequency_extent,
  rum_results$models$florida$frequency_extent, digits = 3
))
cat("\nPARK-AREA FUNCTIONAL FORM SENSITIVITY\n")
print(fixest::etable(
  rum_results$models$site_extent,
  rum_results$models$site_extent_quadratic, digits = 3
))
print(rum_results$tables$site_sensitivity, n = Inf, width = Inf)
cat("\nResults available in rum_results$models, $tables, $figures, and $data.\n")

writeClipboard(paste(capture.output({ print(fixest::etable(rum_results$models$site_presence, rum_results$models$site_extent)); print(fixest::etable(rum_results$models$frequency_presence, rum_results$models$frequency_extent)); print(rum_results$tables) }), collapse="\r\n"))

rm(
  cbg_site_model, cbg_site_visits, cbg_annual, site_attributes,
  site_checks, site_controls, site_terms, needed_site, choice_set_coverage,
  iv_data, site_marginals, m_site_presence, m_site_extent,
  m_site_extent_quadratic, m_site_progression, m_freq_presence,
  m_freq_extent, bp, be, state_models, fit_site_choice, fit_frequency,
  travel_cost_params, analysis_year, fit_origin_sample,
  calculate_site_marginals, n_threads, cache_dir, device_sample_path
)