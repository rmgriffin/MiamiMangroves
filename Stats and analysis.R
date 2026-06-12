# Setup -------------------------------------------------------------------
rm(list=ls()) # Clears workspace

# # Install/call libraries
# install.packages("renv") # Run if you have cloned repository and don't already have renv installed
# renv::restore() # Run once after cloning repository
# renv::install("package") # Run to install new packages
# renv::snapshot() # Run after installing new packages
# renv::init() # Only run when the repository is first created, don't run on cloning an existing repository

# Data for this repository is at https://drive.google.com/drive/folders/1syX_y2lMbo-ETNBXAo24m2FWUK1q60Ux?usp=sharing

pkgs<-c("tidyverse","arrow","sf","shiny")

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
