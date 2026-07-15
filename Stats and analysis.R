# Setup -------------------------------------------------------------------
rm(list=ls()) # Clears workspace

# # Install/call libraries
# install.packages("renv") # Run if you have cloned repository and don't already have renv installed
# renv::restore() # Run once after cloning repository
# renv::install("package") # Run to install new packages
# renv::snapshot() # Run after installing new packages
# renv::init() # Only run when the repository is first created, don't run on cloning an existing repository

# Data for this repository is at https://drive.google.com/drive/folders/1syX_y2lMbo-ETNBXAo24m2FWUK1q60Ux?usp=sharing

pkgs<-c("tidyverse","arrow","sf","shiny","survival")

missing<-pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly=TRUE)]
if (length(missing)>0) {
  stop("Missing packages: ", paste(missing, collapse=", "),
       "\nRun renv::restore()")
}

invisible(lapply(pkgs, library, character.only=TRUE))
rm(pkgs, missing)


# Load data --------------------------------------------------------------
source("Data_collation.R")


# Device stats -----------------------------------------------------------
length(unique(dfst$DEVICEID)) # Unique device ids 

dfst |> # Frequencies of repeat visits by device id
  st_drop_geometry() |>
  filter(!is.na(DEVICEID)) |>
  count(DEVICEID, name="n_visits") |>
  mutate(freq_bin=cut(n_visits, breaks=c(1,2,3,5,10,25,50,100,250,Inf), right=FALSE,
                      labels=c("1","2","3-4","5-9","10-24","25-49","50-99","100-249","250+"))) |>
  count(freq_bin, name="n_devices") |>
  mutate(share_devices=n_devices / sum(n_devices)) |>
  ggplot(aes(freq_bin, share_devices)) +
  geom_col() +
  geom_text(aes(label=scales::percent(share_devices, accuracy=0.1)), vjust=-0.3, size=3) +
  scale_y_continuous(labels=scales::percent, expand=expansion(mult=c(0, 0.1))) +
  labs(x="Visits per DEVICEID", y="Share of DEVICEIDs")


# CBG stats --------------------------------------------------------------
dfst |> # Frequencies of repeat visits by census block group
  st_drop_geometry() |>
  filter(!is.na(CENSUS_BLOCK_GROUP_ID)) |>
  count(CENSUS_BLOCK_GROUP_ID, name="n_visits") |>
  mutate(freq_bin=cut(n_visits, breaks=c(1,2,3,5,10,25,50,100,250,500,1000,2500,5000,Inf), right=FALSE,
                      labels=c("1","2","3-4","5-9","10-24","25-49","50-99","100-249","250-499","500-999","1000-2499","2500-4999","5000+"))) |>
  count(freq_bin, name="n_cbgs") |>
  mutate(share_cbgs=n_cbgs / sum(n_cbgs)) |>
  ggplot(aes(freq_bin, share_cbgs)) +
  geom_col() +
  geom_text(aes(label=scales::percent(share_cbgs, accuracy=0.1)), vjust=-0.3, size=3) +
  scale_y_continuous(labels=scales::percent, expand=expansion(mult=c(0, 0.1))) +
  labs(x="Observations per census block group", y="Share of CBGs")

# Multiple site visits on the same day -----------------------------------
df_dups<-dfst %>% # Sites visited together on same day (raw data)
  add_count(DEVICEID, DAY_IN_FEATURE, name="n_obs_device_day") %>%
  filter(n_obs_device_day > 1) |> 
  arrange(DEVICEID)

poly_dup_compare<-dfst %>% # Comparison of polygon visits that are part of multi-site visits versus the only site visited per day
  st_drop_geometry() %>%
  count(FEATUREID, Name, Jurisdiction, name="n_total_visits") %>%
  left_join(
    df_dups %>%
      st_drop_geometry() %>%
      count(FEATUREID, Name, Jurisdiction, name="n_dup_context_visits"),
    by=c("FEATUREID", "Name", "Jurisdiction")
  ) %>%
  mutate(
    n_dup_context_visits=replace_na(n_dup_context_visits, 0L),
    pct_dup_context=100 * n_dup_context_visits / n_total_visits
  ) %>%
  arrange(desc(pct_dup_context), desc(n_dup_context_visits))

poly_sets<-dfst %>% # Sites visited together on the same day (summary)
  sf::st_drop_geometry() %>% # remove this line if dfst is not an sf object
  filter(!is.na(DEVICEID), !is.na(DAY_IN_FEATURE), !is.na(FEATUREID)) %>%
  mutate(
    polygon_name=if_else(is.na(Name), paste0("FEATUREID ", FEATUREID), Name)
  ) %>%
  distinct(DEVICEID, DAY_IN_FEATURE, FEATUREID, polygon_name) %>%
  arrange(DEVICEID, DAY_IN_FEATURE, FEATUREID) %>%
  group_by(DEVICEID, DAY_IN_FEATURE) %>%
  summarise(
    n_polygons=n_distinct(FEATUREID),
    polygon_set_chr=paste(FEATUREID[!duplicated(FEATUREID)], collapse=" | "),
    polygon_name_chr=paste(polygon_name[!duplicated(FEATUREID)], collapse=" | "),
    .groups="drop"
  ) %>%
  filter(n_polygons > 1) %>%
  count(polygon_set_chr, polygon_name_chr, n_polygons, sort=TRUE, name="n_device_days")


# Travel stats -----------------------------------------------------------
cdf_df<-dfst |>
  st_drop_geometry() |>
  mutate(FEATUREID=as.character(FEATUREID),
         Name=if_else(is.na(Name) | Name=="", "Unnamed site", Name),
         site_label=paste0(Name, " [", FEATUREID, "]"),
         distance_km=as.numeric(distance_m)/1000,
         duration_min=as.numeric(duration_min)) |>
  select(FEATUREID, Name, site_label, distance_km, duration_min) |>
  filter(!is.na(FEATUREID), is.finite(distance_km), is.finite(duration_min))

site_choices<-cdf_df |>
  distinct(FEATUREID, site_label) |>
  arrange(site_label) |>
  select(site_label, FEATUREID) |>
  deframe()


ui<-fluidPage(
  titlePanel("CDF viewer by site"),
  sidebarLayout(
    sidebarPanel(
      selectizeInput("features", "Select site(s)", choices=site_choices, selected=head(site_choices, 1),
                     multiple=TRUE, options=list(placeholder="Search by name or FEATUREID", maxItems=20)),
      checkboxInput("overlay", "Overlay selected sites", value=TRUE),
      checkboxInput("log_x", "Log-scale x-axis", value=FALSE),
      sliderInput("min_n", "Minimum observations per site", min=1, max=1000, value=20, step=1)
    ),
    mainPanel(tabsetPanel(
      tabPanel("Distance CDF", plotOutput("distance_plot", height="700px")),
      tabPanel("Duration CDF", plotOutput("duration_plot", height="700px")),
      tabPanel("Site summary", tableOutput("summary_table"))
    ))
  )
)

server<-function(input, output, session) {
  
  filtered_df<-reactive({
    req(input$features)
    cdf_df |> filter(FEATUREID %in% input$features) |> group_by(FEATUREID) |> filter(n() >= input$min_n) |> ungroup()
  })
  
  make_cdf_plot<-function(df, var, x_lab) {
    p<-ggplot(df, aes(x=.data[[var]], color=site_label)) +
      stat_ecdf(linewidth=0.8) + labs(x=x_lab, y="Cumulative proportion", color="Site") + theme_bw()
    if(!input$overlay) p<-p + facet_wrap(~site_label, scales="free_x") + theme(legend.position="none")
    if(input$log_x) p<-p + scale_x_log10()
    p
  }
  
  output$distance_plot<-renderPlot({
    make_cdf_plot(filtered_df() |> filter(distance_km > 0), "distance_km", "Distance (km)")
  })
  
  output$duration_plot<-renderPlot({
    make_cdf_plot(filtered_df() |> filter(duration_min > 0), "duration_min", "Duration (minutes)")
  })
  
  output$summary_table<-renderTable({
    filtered_df() |>
      group_by(FEATUREID, Name, site_label) |>
      summarise(n=n(),
                distance_p25_km=quantile(distance_km, 0.25, na.rm=TRUE),
                distance_median_km=median(distance_km, na.rm=TRUE),
                distance_p75_km=quantile(distance_km, 0.75, na.rm=TRUE),
                duration_p25=quantile(duration_min, 0.25, na.rm=TRUE),
                duration_median=median(duration_min, na.rm=TRUE),
                duration_p75=quantile(duration_min, 0.75, na.rm=TRUE),
                .groups="drop") |>
      arrange(site_label)
  })
}

shinyApp(ui, server)


# Travel cost model ------------------------------------------------------
rum_full_path<-"Data/intermediate/rum_full.parquet"
dir.create(dirname(rum_full_path), recursive=TRUE, showWarnings=FALSE)

if(file.exists(rum_full_path)) {

  rum_full<-read_parquet(rum_full_path)

} else {

  alts<-dfst %>%
    st_drop_geometry() %>%
    mutate(FEATUREID=as.character(FEATUREID)) %>%
    filter(!is.na(FEATUREID)) %>%
    distinct(FEATUREID, Name, Jurisdiction) %>%
    arrange(FEATUREID)

  choices <- dfst %>% # Selecting one visit per day based on timespan length over which the device is seen
    st_drop_geometry() %>%
    select(DEVICEID, DAY_IN_FEATURE, FEATUREID, CENSUS_BLOCK_GROUP_ID, timespan_min) %>%
    filter(!is.na(DEVICEID), !is.na(DAY_IN_FEATURE), !is.na(FEATUREID), !is.na(CENSUS_BLOCK_GROUP_ID)) %>%
    mutate(
      FEATUREID=as.character(FEATUREID),
      CENSUS_BLOCK_GROUP_ID=as.character(CENSUS_BLOCK_GROUP_ID)
    ) %>%
    group_by(DEVICEID, DAY_IN_FEATURE) %>%
    slice_max(timespan_min, n=1, with_ties=FALSE) %>%
    ungroup() %>%
    mutate(choice_id=row_number()) %>%
    select(choice_id, DEVICEID, DAY_IN_FEATURE, CENSUS_BLOCK_GROUP_ID, chosen_FEATUREID=FEATUREID)

  tibble(
    n_choices=nrow(choices),
    n_alts=nrow(alts),
    n_rum_rows=nrow(choices) * nrow(alts)
  )

  travel_costs<-distance_results %>%
    filter(
      CENSUS_BLOCK_GROUP_ID %in% unique(choices$CENSUS_BLOCK_GROUP_ID),
      FEATUREID %in% alts$FEATUREID
    ) %>%
    select(
      CENSUS_BLOCK_GROUP_ID,
      FEATUREID,
      distance_m,
      duration_min,
      med_hh_income,
      osrm_success
    ) %>%
    collect() %>%
    mutate(
      CENSUS_BLOCK_GROUP_ID=as.character(CENSUS_BLOCK_GROUP_ID),
      FEATUREID=as.character(FEATUREID),
      travel_key=paste(CENSUS_BLOCK_GROUP_ID, FEATUREID, sep="|"),
      distance_km=distance_m/1000,
      duration_hr=duration_min/60
    ) %>%
    filter(osrm_success, !is.na(distance_km), !is.na(duration_hr)) %>%
    select(travel_key, distance_km, duration_hr, med_hh_income)

  n_choices<-nrow(choices)
  n_alts<-nrow(alts)

  choice_i<-rep(seq_len(n_choices), each=n_alts)
  alt_i<-rep(seq_len(n_alts), times=n_choices)

  travel_i<-match(
    paste(choices$CENSUS_BLOCK_GROUP_ID[choice_i], alts$FEATUREID[alt_i], sep="|"),
    travel_costs$travel_key
  )

  print(system.time({
    rum_full<-tibble(
      choice_id=choices$choice_id[choice_i],
      DEVICEID=choices$DEVICEID[choice_i],
      DAY_IN_FEATURE=choices$DAY_IN_FEATURE[choice_i],
      CENSUS_BLOCK_GROUP_ID=choices$CENSUS_BLOCK_GROUP_ID[choice_i],
      FEATUREID=alts$FEATUREID[alt_i],
      chosen=as.integer(
        alts$FEATUREID[alt_i] ==
          choices$chosen_FEATUREID[choice_i]
      ),
      distance_km=travel_costs$distance_km[travel_i],
      duration_hr=travel_costs$duration_hr[travel_i],
      med_hh_income=travel_costs$med_hh_income[travel_i]
    ) %>%
      filter(
        !is.na(distance_km),
        !is.na(duration_hr)
      )
  }))

  write_parquet(
    rum_full,
    rum_full_path,
    compression="zstd")
  
  rm(choice_i, alt_i, travel_i)
  gc()
}

# n_choices<-n_distinct(rum_full$choice_id)
# n_alts<-n_distinct(rum_full$FEATUREID)

# tibble(
#   n_choices=n_choices,
#   n_alts=n_alts,
#   n_rum_rows=nrow(rum_full)
# )

# rum_full %>% # Check to see if filtering failed routes removed chosen alternatives or created incomplete choice sets
#   group_by(choice_id) %>%
#   summarise(
#     n_available=n(),
#     n_chosen=sum(chosen),
#     .groups="drop"
#   ) %>%
#   summarise(
#     n_choice_sets=n(),
#     missing_chosen=sum(n_chosen != 1),
#     incomplete_sets=sum(n_available != n_alts)
#   )

travel_cost_params<-list(
  auto_cost_per_mile=0.2503, # 2024 AAA marginal driving cost
  annual_work_hours=2080,    # 40 hours/week * 52 weeks
  vot_fraction=0.33          # value of travel time as share of hourly income
)

rum_full<-rum_full %>% # Creating total travel cost
  mutate(travel_cost_dollars= (distance_km / 1.609344) * travel_cost_params$auto_cost_per_mile +
      duration_hr * (travel_cost_params$vot_fraction * med_hh_income / travel_cost_params$annual_work_hours))

# rum_full %>% # Percentage of census block groups missing income data
#   distinct(CENSUS_BLOCK_GROUP_ID, med_hh_income) %>%
#   summarise(
#     n_origin_cbgs=n(),
#     n_missing_income=sum(is.na(med_hh_income)),
#     pct_missing_income=100 * mean(is.na(med_hh_income))
#   )

rum_model_base<-rum_full %>%
  select(choice_id, DEVICEID, FEATUREID, chosen, travel_cost_dollars) %>% # Only essential variables
  filter(!is.na(travel_cost_dollars), is.finite(travel_cost_dollars)) # Filtering out trips attached to a census block group with no household income info

sample_rum_choices<-function(rum_df, n_choices_sample, seed=1) { # Function for subsetting full_rum dataframe
  
  if(!is.null(seed)) set.seed(seed)
  
  choice_ids<-rum_df %>%
    distinct(choice_id)
  
  sample_choice_ids<-choice_ids %>%
    slice_sample(n=min(n_choices_sample, nrow(choice_ids))) %>%
    pull(choice_id)
  
  rum_df %>%
    filter(choice_id %in% sample_choice_ids)
}

rum_model_df<-sample_rum_choices(
  rum_df=rum_model_base,
  n_choices_sample=50000,
  seed=1
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


