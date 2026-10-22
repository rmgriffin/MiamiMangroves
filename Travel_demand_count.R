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
site_model_path <- file.path(cache_dir, paste0("cbg_site_model_", analysis_year, ".parquet"))
dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)


# Eligible destinations -------------------------------------------------
needed_site <- c(
  "id", "Name", "island", "mangrove_presence", "mangrove_m2",
  "beach_area_ha", "tree_canopy_pct", "impervious_pct"
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
    beach_area_ha = as.numeric(beach_area_ha),
    tree_canopy_pct = as.numeric(tree_canopy_pct),
    impervious_pct = as.numeric(impervious_pct),
    log_park_area = as.numeric(log(1+park_area_m2))
  )
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
site_attributes <- site_attributes %>%
  filter(island == 0L) %>%
  mutate(
    log_mangrove_ha = log1p(mangrove_ha),
    log_beach_ha = log1p(beach_area_ha)
  )
site_controls <- c("log_beach_ha", "tree_canopy_pct", "impervious_pct", "log_park_area")
site_terms <- c("mangrove_presence", "log_mangrove_ha", site_controls)
if (!all(site_attributes$mangrove_presence %in% c(0L, 1L)) ||
    any(!is.finite(as.matrix(site_attributes[site_terms]))) ||
    any(site_attributes$mangrove_ha < 0) ||
    any(site_attributes$beach_area_ha < 0) ||
    any(site_attributes$tree_canopy_pct < 0 | site_attributes$tree_canopy_pct > 100) ||
    any(site_attributes$impervious_pct < 0 | site_attributes$impervious_pct > 100)) {
  stop("Non-island site covariates are missing or invalid; inspect df.")
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
    inner_join(cbg_population, by="CENSUS_BLOCK_GROUP_ID")
  if (nrow(eligible_origins) == 0L) stop("No eligible origins with population data.")

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
        dplyr::select(FEATUREID, mangrove_ha, dplyr::all_of(site_terms)),
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
      med_hh_income, mangrove_ha, dplyr::all_of(site_terms)
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
    )

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
fit_site_choice <- function(mangrove_term, controls = character()) {
  rhs <- c("travel_cost_dollars", mangrove_term, controls)

  formula <- stats::as.formula(paste(
    "device_visitor_days ~", paste(rhs, collapse = " + "),
    "| CENSUS_BLOCK_GROUP_ID"
  ))

  fit <- fixest::fepois(
    formula,
    data = cbg_site_model,
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
fit_frequency <- function(iv_term, state_fe = FALSE) {
  formula <- stats::as.formula(paste0(
    "calibrated_visitor_days ~ ", iv_term, " + log_income",
    if (state_fe) " | state_fips" else ""
  ))
  fixest::fepois(
    formula,
    offset = ~ log(cbg_population),
    data = cbg_annual,
    vcov = "hetero",
    nthreads = n_threads,
    glm.iter = 100,
    lean = FALSE
  )
}
m_freq_presence <- fit_frequency("monetary_iv_presence")
m_freq_extent <- fit_frequency("monetary_iv_extent")
state_models <- if (dplyr::n_distinct(cbg_annual$state_fips) >= 2L) {
  list(
    presence = fit_frequency("monetary_iv_presence", state_fe = TRUE),
    extent = fit_frequency("monetary_iv_extent", state_fe = TRUE)
  )
} else NULL

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
# Marginal annual welfare for an additional hectare at each site.
site_marginals <- if (calculate_site_marginals) {
  cbg_site_model %>%
    inner_join(
      cbg_annual %>%
        dplyr::select(CENSUS_BLOCK_GROUP_ID, calibrated_visitor_days,
                      predicted_visits_extent),
      by = "CENSUS_BLOCK_GROUP_ID",
      suffix = c("", "_annual")
    ) %>%
    mutate(
      destination_probability = exp(V_extent -
        iv_data$IV_extent[match(CENSUS_BLOCK_GROUP_ID,
                                iv_data$CENSUS_BLOCK_GROUP_ID)]),
      marginal_dollars_per_trip_ha = destination_probability *
        unname(be[["log_mangrove_ha"]]) /
        (-unname(be[["travel_cost_dollars"]]) * (1 + mangrove_ha)),
      conditional_dollars_per_ha = calibrated_visitor_days_annual *
        marginal_dollars_per_trip_ha,
      linked_dollars_per_ha = predicted_visits_extent *
        marginal_dollars_per_trip_ha
    ) %>%
    group_by(FEATUREID) %>%
    summarise(
      conditional_annual_dollars_per_ha = sum(conditional_dollars_per_ha),
      linked_annual_dollars_per_ha = sum(linked_dollars_per_ha),
      .groups = "drop"
    ) %>%
    left_join(
      site_attributes %>%
        dplyr::select(FEATUREID, site_name, mangrove_presence, mangrove_ha),
      by = "FEATUREID"
    ) %>%
    arrange(desc(linked_annual_dollars_per_ha))
} else NULL


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
        "Impervious: +1 percentage point" = 1
      )
      term_map <- c(
        "Travel cost: +$10" = "travel_cost_dollars",
        "Beach: 0 to 1 ha" = "log_beach_ha",
        "Tree canopy: +1 percentage point" = "tree_canopy_pct",
        "Impervious: +1 percentage point" = "impervious_pct"
      )
      increments <- c(increments, "Park area: +1 log-area unit" = 1)
      term_map <- c(term_map, "Park area: +1 log-area unit" = "log_park_area")
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
           subtitle = "Local annual $/ha derivative using predicted baseline trips",
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
cat("\nResults available in rum_results$models, $tables, $figures, and $data.\n")
