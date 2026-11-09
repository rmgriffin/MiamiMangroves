# Setup -------------------------------------------------------------------
#rm(list=ls()) # Clears workspace

# # Install/call libraries
# install.packages("renv") # Run if you have cloned repository and don't already have renv installed
# renv::restore() # Run once after cloning repository
# renv::install("package") # Run to install new packages
# renv::snapshot() # Run after installing new packages
# renv::init() # Only run when the repository is first created, don't run on cloning an existing repository

# Data for this repository is at https://drive.google.com/drive/folders/1syX_y2lMbo-ETNBXAo24m2FWUK1q60Ux?usp=sharing

pkgs<-c("tidyverse","arrow","sf","shiny","mapgl","gt")

missing<-pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly=TRUE)]
if (length(missing)>0) {
  stop("Missing packages: ", paste(missing, collapse=", "),
       "\nRun renv::restore()")
}

invisible(lapply(pkgs, library, character.only=TRUE))
rm(pkgs, missing)


# Load data --------------------------------------------------------------
source("Travel_demand_count.R")

# Park summaries ---------------------------------------------------------
site_summary<-rum_results$data$site_attributes |>
  left_join(
    dfst_model |>
      sf::st_drop_geometry() |>
      group_by(FEATUREID) |>
      summarise(
        device_visitor_days=n(),
        calibrated_visitor_days=sum(calibrated_visits),
        .groups="drop"
      ),
    by="FEATUREID"
  ) |>
  mutate(
    park_ha=expm1(log_park_area)/10000,
    device_visitor_days=replace_na(device_visitor_days,0),
    calibrated_visitor_days=replace_na(calibrated_visitor_days,0),
    mangrove_group=if_else(mangrove_presence==1L,"Mangroves","No mangroves")
  )


# Site stats -------------------------------------------------------------
local({ # Park characteristics and visitation
  fmt<-function(x,accuracy=.1) {
    if (!is.finite(x)) return("NA")
    scales::comma(x,accuracy=accuracy)
  }

  mean_sd<-function(x,accuracy=.1) {
    paste0(fmt(mean(x,na.rm=TRUE),accuracy),
           " (",fmt(sd(x,na.rm=TRUE),accuracy),")")
  }

  d<-site_summary |>
    left_join(
      dfst_model |>
        st_drop_geometry() |>
        group_by(FEATUREID) |>
        summarise(
          site_device_pairs=n_distinct(DEVICEID,na.rm=TRUE),
          median_distance_km=median(outbound_distance_m/1000,na.rm=TRUE),
          median_duration_min=median(outbound_duration_min,na.rm=TRUE),
          .groups="drop"
        ),
      by="FEATUREID"
    )

  site_descriptives<-function(x,group) {
    tibble(
      characteristic=c(
        "Total parks",
        "Calibrated annual visitor-days",
        "Observed device-days",
        "Unique device IDs",
        "Unique device–park combinations",
        "Park area (ha)",
        "Mangrove area (ha)",
        "Beach area (ha)",
        "Tree canopy (%)",
        "Impervious cover (%)",
        "Marine waterfront (%)",
        "Sports facilities (%)",
        "Annual visitor-days per park",
        "One-way travel distance (km)",
        "One-way travel time (min)"
      ),
      value=c(
        scales::comma(nrow(x)),
        scales::comma(sum(x$calibrated_visitor_days)),
        scales::comma(sum(x$device_visitor_days)),
        scales::comma(n_distinct(
          dfst_model$DEVICEID[
            dfst_model$FEATUREID %in% x$FEATUREID
          ],
          na.rm=TRUE
        )),
        scales::comma(sum(x$site_device_pairs,na.rm=TRUE)),
        mean_sd(x$park_ha),
        mean_sd(x$mangrove_ha),
        mean_sd(x$beach_area_ha),
        mean_sd(x$tree_canopy_pct),
        mean_sd(x$impervious_pct),
        fmt(100*mean(x$marine_waterfront,na.rm=TRUE)),
        fmt(100*mean(x$sports_facilities,na.rm=TRUE)),
        mean_sd(x$calibrated_visitor_days,accuracy=1),
        mean_sd(x$median_distance_km),
        mean_sd(x$median_duration_min)
      ),
      group=group
    )
  }

  d<-list(
    "All parks"=d,
    "No mangroves"=filter(d,mangrove_presence==0L),
    "Mangroves"=filter(d,mangrove_presence==1L)
  ) |>
    purrr::imap_dfr(site_descriptives) |>
    pivot_wider(names_from=group,values_from=value) |>
    dplyr::select(characteristic,`All parks`,`No mangroves`,Mangroves) |>
    gt::gt() |>
    gt::tab_row_group(
      label="Devices and visits",
      rows=characteristic %in% c(
        "Calibrated annual visitor-days",
        "Observed device-days",
        "Unique device IDs",
        "Unique device–park combinations",
        "Annual visitor-days per park",
        "One-way travel distance (km)",
        "One-way travel time (min)"
      )
    ) |>
    gt::tab_row_group(
      label="Park attributes",
      rows=characteristic %in% c(
        "Total parks",
        "Park area (ha)",
        "Mangrove area (ha)",
        "Beach area (ha)",
        "Tree canopy (%)",
        "Impervious cover (%)",
        "Marine waterfront (%)",
        "Sports facilities (%)"
      )
    ) |>
    gt::row_group_order(
      groups=c("Park attributes","Devices and visits")
    ) |>
    gt::cols_label(
      characteristic="Characteristic"
    ) |>
    gt::cols_align(
      align="right",
      columns=c(`All parks`,`No mangroves`,Mangroves)
    ) |>
    project_table(
      title="Park characteristics and recreational visitation, 2024",
      note="Continuous variables are reported as mean (standard deviation), and binary attributes as percentages. Travel statistics summarize park-level median travel distances and times for visited parks. Unique device IDs are not additive across groups."
    )

  print(d)
})


df |> # Map of annual visitation for the RUM estimation sample
  mutate(FEATUREID=as.character(id)) |>
  semi_join(site_summary |> dplyr::select(FEATUREID),by="FEATUREID") |>
  left_join(
    site_summary |>
      dplyr::select(FEATUREID,annual_visitor_days=calibrated_visitor_days),
    by="FEATUREID"
  ) |>
  mutate(
    annual_visitor_days=replace_na(annual_visitor_days,0),
    visitor_days_class=as.character(cut(
      annual_visitor_days,
      breaks=c(-Inf,2500,10000,50000,250000,1000000,Inf),
      right=FALSE,
      labels=c("< 2,500","2,500 - 9,999","10,000 - 49,999",
               "50,000 - 249,999","250,000 - 999,999","1,000,000+")
    ))
  ) |>
  st_transform(4326) |>
  (\(x) mapgl::maplibre(
    style=mapgl::openfreemap_style("positron"),
    bounds=x
  ) |>
    mapgl::add_source(id="parks",data=x) |>
    mapgl::add_fill_layer(
      id="visitor-days",
      source="parks",
      fill_color=mapgl::match_expr(
        column="visitor_days_class",
        values=c("< 2,500","2,500 - 9,999","10,000 - 49,999",
                 "50,000 - 249,999","250,000 - 999,999","1,000,000+"),
        stops=c("#7FC8E3","#42A5D5","#1F78B4","#155A96","#103E73","#08264F"),
        default="#bdbdbd"
      ),
      fill_opacity=.9,
      fill_outline_color="white",
      tooltip="Name"
    ) |>
    mapgl::add_legend(
      "Calibrated visitor-days, 2024 (RUM sample)",
      values=c("< 2,500","2,500 - 9,999","10,000 - 49,999",
               "50,000 - 249,999","250,000 - 999,999","1,000,000+"),
      colors=c("#7FC8E3","#42A5D5","#1F78B4","#155A96","#103E73","#08264F"),
      type="categorical",
      position="bottom-left"
    )
  )()

local({ # Park size and visitation
  d<-site_summary |>
    filter(park_ha>0) |>
    ggplot(aes(park_ha,calibrated_visitor_days+1,color=mangrove_group)) +
    geom_point(alpha=.75,size=1.9) +
    scale_x_log10(labels=scales::comma) +
    scale_y_log10(labels=scales::comma) +
    labs(
      title="Park area and recreational visitation",
      x="Park area (ha, log scale)",
      y="Calibrated annual visitor-days + 1 (log scale)",
      color=NULL
    ) +
    theme_minimal(base_size=11) +
    theme(
      plot.title=element_text(face="bold",size=12),
      panel.grid.minor=element_blank(),
      legend.position="bottom"
    )

  print(d)
})

site_summary |> # Calibrated visitor-days by park for high and low visitation parks
  mutate(
    annual_visitor_days=calibrated_visitor_days
  ) |>
  arrange(desc(annual_visitor_days)) |>
  mutate(rank=row_number()) |>
  (\(x) bind_rows(
    x |>
      slice_max(annual_visitor_days,n=30,with_ties=FALSE) |>
      mutate(panel="A. Highest visitation parks"),
    x |>
      filter(mangrove_presence==1L) |>
      mutate(panel="B. Parks with mangroves")
  ))() |>
  mutate(
    panel=factor(panel,levels=c("A. Highest visitation parks","B. Parks with mangroves")),
    site_label=paste0(str_to_title(site_name)," [rank ",rank,"]")
  ) |>
  ggplot(aes(annual_visitor_days,reorder(site_label,annual_visitor_days))) +
  geom_col() +
  geom_text(
    aes(label=scales::comma(round(annual_visitor_days))),
    hjust=-0.1,
    size=3
  ) +
  facet_wrap(~panel,scales="free_y",ncol=1,drop=FALSE) +
  scale_x_continuous(
    labels=scales::comma,
    expand=expansion(mult=c(0,0.18))
  ) +
  coord_cartesian(clip="off") +
  labs(
    x="Visitor-days",
    y=NULL,
    title="Calibrated visitor-days by site, 2024",
    subtitle="Nationwide RUM estimation sample; site rank shown in brackets"
  ) +
  theme_minimal() +
  theme(plot.margin=margin(5.5,35,5.5,5.5))

dfst_model |> # Observed visitor-days per device ID, 2024
  st_drop_geometry() |>
  filter(!is.na(DEVICEID)) |>
  group_by(DEVICEID) |>
  summarise(
    n_visits=n(),
    state_fips=first(substr(CENSUS_BLOCK_GROUP_ID[!is.na(CENSUS_BLOCK_GROUP_ID)], 1, 2),
                     default=NA_character_),
    .groups="drop"
  ) |>
  filter(!is.na(state_fips)) |>
  mutate(
    location=factor(if_else(state_fips=="12", "Florida", "Outside Florida"),
                    levels=c("Florida", "Outside Florida")),
    freq_bin=cut(n_visits, breaks=c(1,2,3,5,10,25,50,100,250,Inf), right=FALSE,
                 labels=c("1","2","3-4","5-9","10-24","25-49","50-99","100-249","250+"))
  ) |>
  count(freq_bin, location, name="n_devices") |>
  mutate(share_devices=n_devices/sum(n_devices)) |>
  ggplot(aes(freq_bin, share_devices, fill=location)) +
  geom_col(color="black", linewidth=.25) +
  scale_fill_manual(
    values=c("Florida"="white", "Outside Florida"="grey55")) +
  geom_text(
    aes(label=if_else(share_devices>=.005,
                      scales::percent(share_devices, accuracy=.1), "")),
    position=position_stack(vjust=.5), size=3
  ) +
  scale_y_continuous(labels=scales::percent, expand=expansion(mult=c(0,.1))) +
  labs(
    x="Park visitor-days per device ID",
    y="Share of device IDs",
    fill="Home location",
    title="Park visits per device ID, 2024"
  ) +
  theme_minimal()

dfst |> # Observed visitor-days per device ID, 2024
  st_drop_geometry() |>
  mutate(year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y"))) |>
  filter(year==2024, !is.na(DEVICEID)) |>
  group_by(DEVICEID) |>
  summarise(
    n_visits=n(),
    state_fips=first(substr(CENSUS_BLOCK_GROUP_ID[!is.na(CENSUS_BLOCK_GROUP_ID)], 1, 2),
                     default=NA_character_),
    .groups="drop"
  ) |>
  filter(!is.na(state_fips)) |>
  mutate(
    location=factor(if_else(state_fips=="12", "Florida", "Outside Florida"),
                    levels=c("Florida", "Outside Florida")),
    freq_bin=cut(n_visits, breaks=c(1,2,3,5,10,25,50,100,250,Inf), right=FALSE,
                 labels=c("1","2","3-4","5-9","10-24","25-49","50-99","100-249","250+"))
  ) |>
  count(freq_bin, location, name="n_devices") |>
  mutate(share_devices=n_devices/sum(n_devices)) |>
  ggplot(aes(freq_bin, share_devices, fill=location)) +
  geom_col(color="black", linewidth=.25) +
  scale_fill_manual(
    values=c("Florida"="white", "Outside Florida"="grey55")) +
  geom_text(
    aes(label=if_else(share_devices>=.005,
                      scales::percent(share_devices, accuracy=.1), "")),
    position=position_stack(vjust=.5), size=3
  ) +
  scale_y_continuous(labels=scales::percent, expand=expansion(mult=c(0,.1))) +
  labs(
    x="Park visitor-days per device ID",
    y="Share of device IDs",
    fill="Home location",
    title="Park visitor-days per device ID, 2024"
  ) +
  theme_minimal()

dfst |> # Number of parks visited per device-day
  st_drop_geometry() |>
  mutate(year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y"))) |>
  filter(year==2024, !is.na(DEVICEID), !is.na(DAY_IN_FEATURE), !is.na(FEATUREID)) |>
  distinct(DEVICEID, DAY_IN_FEATURE, FEATUREID) |>
  count(DEVICEID, DAY_IN_FEATURE, name="n_parks") |>
  mutate(n_parks=factor(if_else(n_parks>=6, "6+", as.character(n_parks)),
                        levels=c("1","2","3","4","5","6+"))) |>
  count(n_parks, name="n_device_days") |>
  mutate(share_device_days=n_device_days/sum(n_device_days)) |>
  ggplot(aes(n_parks, share_device_days)) +
  geom_col() +
  geom_text(aes(label=scales::percent(share_device_days, accuracy=.1)),
            vjust=-.3, size=3) +
  scale_y_continuous(labels=scales::percent, expand=expansion(mult=c(0,.1))) +
  labs(
    x="Distinct parks visited in one day",
    y="Share of device-days",
    title="Number of parks visited per device-day, 2024"
  ) +
  theme_minimal()

dfst |> # Parks most associated with multi-park device-days
  st_drop_geometry() |>
  mutate(
    year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
    FEATUREID=trimws(as.character(FEATUREID)),
    Name=if_else(is.na(Name) | Name=="", paste0("FEATUREID ", FEATUREID), Name)
  ) |>
  filter(year==2024, !is.na(DEVICEID), !is.na(DAY_IN_FEATURE), !is.na(FEATUREID)) |>
  distinct(DEVICEID, DAY_IN_FEATURE, FEATUREID, Name, Jurisdiction) |>
  add_count(DEVICEID, DAY_IN_FEATURE, name="n_parks") |>
  group_by(FEATUREID, Name, Jurisdiction) |>
  summarise(
    total_visits=n(),
    multi_park_visits=sum(n_parks>1),
    share_multi_park=mean(n_parks>1),
    .groups="drop"
  ) |>
  filter(total_visits>=100) |>
  slice_max(share_multi_park, n=20, with_ties=FALSE) |>
  mutate(site_label=paste0(Name, " [n=", scales::comma(total_visits), "]")) |>
  ggplot(aes(share_multi_park, reorder(site_label, share_multi_park))) +
  geom_col() +
  geom_text(aes(label=scales::percent(share_multi_park, accuracy=.1)),
            hjust=-.1, size=3) +
  scale_x_continuous(labels=scales::percent, expand=expansion(mult=c(0,.18))) +
  coord_cartesian(clip="off") +
  labs(
    x="Share of visits occurring on multi-park visitor-days",
    y=NULL,
    title="Parks most often included in multi-park visitor-days, 2024",
    subtitle="Parks with at least 100 observed visits; visitor-day count shown in brackets"
  ) +
  theme_minimal() +
  theme(plot.margin=margin(5.5,35,5.5,5.5))

dfst |> # Most common same-day park pairs
  st_drop_geometry() |>
  mutate(
    year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
    FEATUREID=trimws(as.character(FEATUREID)),
    park_name=if_else(is.na(Name) | Name=="", paste0("FEATUREID ", FEATUREID), Name)
  ) |>
  filter(year==2024, !is.na(DEVICEID), !is.na(DAY_IN_FEATURE), !is.na(FEATUREID)) |>
  distinct(DEVICEID, DAY_IN_FEATURE, FEATUREID, park_name) |>
  group_by(DEVICEID, DAY_IN_FEATURE) |>
  filter(n()>1) |>
  summarise(parks=list(sort(unique(park_name))), .groups="drop") |>
  mutate(pair=map(parks, \(x) combn(x, 2, FUN=\(z) paste(z, collapse=" + ")))) |>
  select(-parks) |>
  unnest_longer(pair) |>
  count(pair, sort=TRUE, name="n_device_days") |>
  slice_head(n=20) |>
  ggplot(aes(n_device_days, reorder(pair, n_device_days))) +
  geom_col() +
  geom_text(aes(label=scales::comma(n_device_days)), hjust=-.1, size=3) +
  scale_x_continuous(labels=scales::comma, expand=expansion(mult=c(0,.15))) +
  coord_cartesian(clip="off") +
  labs(
    x="Visitor-days",
    y=NULL,
    title="Most common same-day park pairs, 2024",
    subtitle="Top 20 pairs visited by the same device on the same day"
  ) +
  theme_minimal() +
  theme(plot.margin=margin(5.5,35,5.5,5.5))


# Calibration ------------------------------------------------------------
local({ # Visitor calibration
  d<-Oleta |>
    transmute(
      month, Month=month_name,
      park_visitors=counts,
      park_devices=device_visits
    ) |>
    full_join(
      mia_enplanements_monthly |>
        transmute(
          month, Month_mia=month_name,
          airport_visitors=enplanements,
          airport_devices=device_enplanements
        ),
      by="month"
    ) |>
    arrange(month) |>
    mutate(
      Month=coalesce(Month,Month_mia),
      park_ratio=park_visitors/park_devices,
      airport_ratio=airport_visitors/airport_devices
    ) |>
    select(-month,-Month_mia) |>
    (\(x) bind_rows(x,tibble(
      Month="Annual total",
      park_visitors=sum(x$park_visitors,na.rm=TRUE),
      park_devices=sum(x$park_devices,na.rm=TRUE),
      airport_visitors=sum(x$airport_visitors,na.rm=TRUE),
      airport_devices=sum(x$airport_devices,na.rm=TRUE),
      park_ratio=park_visitors/park_devices,
      airport_ratio=airport_visitors/airport_devices
    )))() |>
    gt::gt() |>
    gt::tab_spanner(
      label="Oleta River State Park",
      columns=c(park_visitors,park_devices,park_ratio)
    ) |>
    gt::tab_spanner(
      label="Miami International Airport",
      columns=c(airport_visitors,airport_devices,airport_ratio)
    ) |>
    gt::cols_label(
      Month="Month",
      park_visitors="Visitors",
      park_devices="Device visits",
      park_ratio="Visitors/device",
      airport_visitors="Enplanements",
      airport_devices="Device enplanements",
      airport_ratio="Passengers/device"
    ) |>
    gt::cols_move_to_end(columns=c(airport_visitors,airport_devices,airport_ratio)) |>
    gt::fmt_number(
      columns=c(park_visitors,park_devices,airport_visitors,airport_devices),
      decimals=0
    ) |>
    gt::fmt_number(
      columns=c(park_ratio,airport_ratio),
      decimals=1
    ) |>
    gt::cols_align(
      align="right",
      columns=c(park_visitors,park_devices,park_ratio,
                airport_visitors,airport_devices,airport_ratio)
    ) |>
    gt::tab_style(
      style=gt::cell_text(weight="bold"),
      locations=gt::cells_body(rows=Month=="Annual total")
    ) |>
    gt::tab_style(
      style=gt::cell_borders(sides="top",color="black",weight=gt::px(1)),
      locations=gt::cells_body(rows=Month=="Annual total")
    ) |>
    project_table(
      title="Visitor calibration at Oleta River State Park and Miami International Airport, 2024",
      note="Calibration factors are calculated as visitor counts divided by observed device visits at Oleta River State Park and passenger enplanements divided by observed device enplanements at Miami International Airport. Annual factors are calculated from annual totals."
    )

  print(d)
})

ggplot( # Share of devices classified as enplaned by distance
  enplanement_by_dist, aes(x=dist_median_km, y=p_enplaned)) +
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
    y="Share classified as enplaned",
    title="Share of devices classified as enplaned by distance from MIA",
    #subtitle="2024",
    #caption=paste("Note: Passengers per observed device equals passenger enplanements", "divided by observed device enplanements.")
  )

ggplot( # Median net difference in driving vs device time by distance from MIA
  conus_dist_margin, aes(x=dist_median_km, y=median_drive_time_margin_hr)) +
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
    y="Median drive-time margin, hours",
    title="Median net difference in driving vs device time by distance from MIA",
    #subtitle="2024",
    #caption=paste("Note: Passengers per observed device equals passenger enplanements", "divided by observed device enplanements.")
  )


# Home location stats --------------------------------------------------------------
dfst_model |> # Origin CBG representation in park-visit device data
  st_drop_geometry() |>
  mutate(
    year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y"))
  ) |> 
  filter(!is.na(CENSUS_BLOCK_GROUP_ID), year == 2024) |>
  count(CENSUS_BLOCK_GROUP_ID, name="n_visits") |>
  mutate(freq_bin=cut(n_visits, breaks=c(1,2,3,5,10,25,50,100,250,500,1000,2500,5000,Inf), right=FALSE,
                      labels=c("1","2","3-4","5-9","10-24","25-49","50-99","100-249","250-499","500-999","1000-2499","2500-4999","5000+"))) |>
  count(freq_bin, name="n_cbgs") |>
  mutate(share_cbgs=n_cbgs / sum(n_cbgs)) |>
  ggplot(aes(freq_bin, share_cbgs)) +
  geom_col() +
  geom_text(aes(label=scales::percent(share_cbgs, accuracy=0.1)), vjust=-0.3, size=3) +
  scale_y_continuous(labels=scales::percent, expand=expansion(mult=c(0, 0.1))) +
  labs(x="Frequency of visitor-days across all parks by CBG", y="Share of CBGs",
       title = "Frequency of visitor-days to all parks by census block group, 2024" )


dfst_model |> # Share of visitors by distance band
  st_drop_geometry() |>
  mutate(
    year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
    distance_km=outbound_distance_m / 1000,
    distance_band=cut(
      distance_km,
      breaks=c(0, 5, 10, 25, 50, 100, 250, 500, 1000, Inf),
      right=FALSE,
      labels=c("0-5 km", "5-10 km", "10-25 km", "25-50 km", "50-100 km",
               "100-250 km", "250-500 km", "500-1000 km","1000+ km")
    )
  ) |>
  filter(!is.na(distance_band), year == 2024) |>
  group_by(distance_band) |>
  summarise(
    annual_visitor_days=sum(calibrated_visits, na.rm=TRUE),
    observed_device_days=n(),
    unique_devices=n_distinct(DEVICEID),
    unique_sites=n_distinct(FEATUREID),
    .groups="drop"
  ) |>
  mutate(share_visitor_days=annual_visitor_days / sum(annual_visitor_days)) |>
  ggplot(aes(distance_band, share_visitor_days)) +
  geom_col() +
  geom_text(aes(label=scales::percent(share_visitor_days, accuracy=0.1)), vjust=-0.25, size=3) +
  scale_y_continuous(labels=scales::percent, expand=expansion(mult=c(0, 0.12))) +
  labs(
    x="One-way travel distance band",
    y="Share of annual visitor-days",
    title="Estimated visitor-days by one-way travel-distance band"
  ) +
  theme_minimal()

dfst_model |> # Home location by state and Miami metro
  st_drop_geometry() |>
  mutate(
    year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
    state_fips=substr(CENSUS_BLOCK_GROUP_ID, 1, 2),
    county_fips=substr(CENSUS_BLOCK_GROUP_ID, 1, 5)
  ) |>
  filter(year == 2024, !is.na(state_fips)) |>
  left_join(
    tibble(
      state_fips=c("01","02","04","05","06","08","09","10","11","12","13","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","44","45","46","47","48","49","50","51","53","54","55","56","72"),
      state=c("AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","PR")
    ),
    by="state_fips"
  ) |>
  filter(!is.na(state)) |>
  mutate(
    origin=case_when(
      state=="FL" & county_fips %in% c("12011","12086","12099") ~ "Miami metro",
      state=="FL" ~ "Rest of Florida",
      TRUE ~ state
    )
  ) |>
  group_by(origin) |>
  summarise(
    annual_visitor_days=sum(calibrated_visits, na.rm=TRUE),
    observed_device_days=n(),
    unique_devices=n_distinct(DEVICEID),
    unique_sites=n_distinct(FEATUREID),
    .groups="drop"
  ) |>
  mutate(share_visitor_days=annual_visitor_days / sum(annual_visitor_days)) |>
  arrange(desc(annual_visitor_days)) |>
  mutate(other_rank=cumsum(!origin %in% c("Miami metro","Rest of Florida"))) |>
  filter(origin %in% c("Miami metro","Rest of Florida") | other_rank<=8) |>
  ggplot(aes(annual_visitor_days, reorder(origin, annual_visitor_days))) +
  geom_col() +
  geom_text(aes(label=scales::percent(share_visitor_days, accuracy=0.1)), hjust=-0.1, size=3) +
  scale_x_continuous(labels=scales::comma, expand=expansion(mult=c(0, 0.18))) +
  coord_cartesian(clip="off") +
  labs(
    x="Annual visitor-days",
    y=NULL,
    title="Estimated visitor-days by origin",
    subtitle="Miami metro, rest of Florida, and top 8 other states"
  ) +
  theme_minimal() +
  theme(plot.margin=margin(5.5, 35, 5.5, 5.5))

dfst_model |> # One-way travel-distance distributions for highest- and lowest-visitation parks
  st_drop_geometry() |>
  mutate(
    year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
    FEATUREID=trimws(as.character(FEATUREID)),
    distance_km=outbound_distance_m / 1000
  ) |>
  filter(year == 2024, !is.na(distance_km), is.finite(distance_km), distance_km >= 0) |>
  inner_join(
    dfst_model |>
      st_drop_geometry() |>
      mutate(
        year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
        FEATUREID=trimws(as.character(FEATUREID))
      ) |>
      filter(year == 2024) |>
      group_by(FEATUREID, Name) |>
      summarise(
        annual_visitor_days=sum(calibrated_visits, na.rm=TRUE),
        .groups="drop"
      ) |>
      arrange(desc(annual_visitor_days)) |>
      mutate(visitation_rank=row_number()) |>
      (\(x) bind_rows(
        x |>
          slice_max(annual_visitor_days, n=15, with_ties=FALSE) |>
          mutate(panel="A. Highest visitation parks"),
        x |>
          slice_min(annual_visitor_days, n=15, with_ties=FALSE) |>
          mutate(panel="B. Lowest visitation parks")
      ))() |>
      select(FEATUREID, panel, annual_visitor_days, visitation_rank),
    by="FEATUREID"
  ) |>
  group_by(panel, FEATUREID, Name, annual_visitor_days, visitation_rank) |>
  summarise(
    p10_distance_km=quantile(distance_km, 0.10, na.rm=TRUE),
    p25_distance_km=quantile(distance_km, 0.25, na.rm=TRUE),
    median_distance_km=median(distance_km, na.rm=TRUE),
    p75_distance_km=quantile(distance_km, 0.75, na.rm=TRUE),
    p90_distance_km=quantile(distance_km, 0.90, na.rm=TRUE),
    .groups="drop"
  ) |>
  group_by(panel) |>
  arrange(annual_visitor_days, .by_group=TRUE) |>
  mutate(
    site_label=paste0(Name, " [rank ", visitation_rank, "]"),
    site_label_panel=factor(
      paste(panel, site_label, sep="___"),
      levels=unique(paste(panel, site_label, sep="___"))
    )
  ) |>
  ungroup() |>
  ggplot(aes(median_distance_km, site_label_panel)) +
  geom_segment(
    aes(x=p10_distance_km, xend=p90_distance_km, y=site_label_panel, yend=site_label_panel),
    alpha=0.25,
    linewidth=1
  ) +
  geom_segment(
    aes(x=p25_distance_km, xend=p75_distance_km, y=site_label_panel, yend=site_label_panel),
    linewidth=1.6
  ) +
  geom_point(size=2) +
  facet_wrap(~panel, scales="free_y", ncol=1) +
  scale_y_discrete(labels=\(x) sub(".*___", "", x)) +
  scale_x_continuous(labels=scales::comma, expand=expansion(mult=c(0.02, 0.06))) +
  labs(
    x="Travel distance, km",
    y=NULL,
    title="One-way travel-distance distributions for highest- and lowest-visitation parks",
    subtitle="Point is median; dark line is IQR; light line is 10th to 90th percentile"
  ) +
  theme_minimal()

dfst_model |> # One-way travel-distance distributions for shortest- and furthest-travel-distance parks
  st_drop_geometry() |>
  mutate(year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
         FEATUREID=trimws(as.character(FEATUREID)),
         distance_km=outbound_distance_m/1000) |>
  filter(year==2024, !is.na(distance_km), is.finite(distance_km), distance_km>=0) |>
  inner_join(
    dfst_model |>
      st_drop_geometry() |>
      mutate(year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
             FEATUREID=trimws(as.character(FEATUREID)),
             distance_km=outbound_distance_m/1000) |>
      filter(year==2024, !is.na(distance_km), is.finite(distance_km), distance_km>=0) |>
      group_by(FEATUREID, Name) |>
      summarise(
        median_distance_km=median(distance_km, na.rm=TRUE),
        annual_visitor_days=sum(calibrated_visits, na.rm=TRUE),
        .groups="drop"
      ) |>
      arrange(desc(annual_visitor_days)) |>
      mutate(visitation_rank=row_number()) |>
      (\(x) bind_rows(
        slice_min(x, median_distance_km, n=15, with_ties=FALSE) |>
          mutate(panel="A. Shortest-distance parks"),
        slice_max(x, median_distance_km, n=15, with_ties=FALSE) |>
          mutate(panel="B. Furthest-distance parks")
      ))() |>
      select(FEATUREID, panel, visitation_rank),
    by="FEATUREID"
  ) |>
  group_by(panel, FEATUREID, Name, visitation_rank) |>
  summarise(
    p10_distance_km=quantile(distance_km, .10, na.rm=TRUE),
    p25_distance_km=quantile(distance_km, .25, na.rm=TRUE),
    median_distance_km=median(distance_km, na.rm=TRUE),
    p75_distance_km=quantile(distance_km, .75, na.rm=TRUE),
    p90_distance_km=quantile(distance_km, .90, na.rm=TRUE),
    .groups="drop"
  ) |>
  group_by(panel) |>
  arrange(median_distance_km, .by_group=TRUE) |>
  mutate(
    site_label=paste0(Name, " [rank ", visitation_rank, "]"),
    site_label_panel=factor(
      paste(panel, site_label, sep="___"),
      levels=unique(paste(panel, site_label, sep="___"))
    )
  ) |>
  ungroup() |>
  ggplot(aes(median_distance_km, site_label_panel)) +
  geom_segment(aes(x=p10_distance_km, xend=p90_distance_km,
                   y=site_label_panel, yend=site_label_panel),
               alpha=.25, linewidth=1) +
  geom_segment(aes(x=p25_distance_km, xend=p75_distance_km,
                   y=site_label_panel, yend=site_label_panel),
               linewidth=1.6) +
  geom_point(size=2) +
  facet_wrap(~panel, scales="free_y", ncol=1) +
  scale_y_discrete(labels=\(x) sub(".*___", "", x)) +
  scale_x_continuous(labels=scales::comma, expand=expansion(mult=c(.02,.06))) +
  labs(
    x="Travel distance, km",
    y=NULL,
    title="One-way travel-distance distributions for shortest- and furthest-travel-distance parks",
    subtitle="Point is median; dark line is IQR; light line is 10th to 90th percentile; visitation rank in brackets"
  ) +
  theme_minimal()


# Travel cost summary stats ----------------------------------------------
local({ # Distribution of estimated travel costs
  d <- dfst_model |>
    st_drop_geometry() |>
    mutate(
      year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
      travel_cost=rum_travel_cost_dollars
    ) |>
    filter(year == 2024, !is.na(travel_cost), is.finite(travel_cost), travel_cost >= 0)

  qs <- quantile(d$travel_cost, probs=c(0.25, 0.50, 0.75), na.rm=TRUE)

  ggplot(d, aes(travel_cost, weight=calibrated_visits)) +
    geom_histogram(bins=40) +
    geom_vline(xintercept=qs[1], linetype="dashed") +
    geom_vline(xintercept=qs[2], linewidth=0.7) +
    geom_vline(xintercept=qs[3], linetype="dashed") +
    scale_x_continuous(labels=scales::dollar) +
    scale_y_continuous(labels=scales::comma) +
    labs(
      x="Travel cost",
      y="Estimated visitor-days",
      title="Distribution of estimated travel costs",
      subtitle="Solid line is median; dashed lines are 25th and 75th percentiles"
    ) +
    theme_minimal()
})

dfst_model |> # Travel cost distribution for highest and lowest parks
  st_drop_geometry() |>
  mutate(
    year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
    FEATUREID=trimws(as.character(FEATUREID)),
    travel_cost=rum_travel_cost_dollars
  ) |>
  filter(year == 2024, !is.na(travel_cost), is.finite(travel_cost), travel_cost >= 0) |>
  inner_join(
    dfst_model |>
      st_drop_geometry() |>
      mutate(
        year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
        FEATUREID=trimws(as.character(FEATUREID)),
        travel_cost=rum_travel_cost_dollars
      ) |>
      filter(year == 2024, !is.na(travel_cost), is.finite(travel_cost), travel_cost >= 0) |>
      group_by(FEATUREID, Name) |>
      summarise(
        median_travel_cost=median(travel_cost, na.rm=TRUE),
        .groups="drop"
      ) |>
      (\(x) bind_rows(
        x |>
          slice_min(median_travel_cost, n=15, with_ties=FALSE) |>
          mutate(panel="A. Lowest-cost parks"),
        x |>
          slice_max(median_travel_cost, n=15, with_ties=FALSE) |>
          mutate(panel="B. Highest-cost parks")
      ))() |>
      select(FEATUREID, panel),
    by="FEATUREID"
  ) |>
  group_by(panel, FEATUREID, Name) |>
  summarise(
    p10_travel_cost=quantile(travel_cost, 0.10, na.rm=TRUE),
    p25_travel_cost=quantile(travel_cost, 0.25, na.rm=TRUE),
    median_travel_cost=median(travel_cost, na.rm=TRUE),
    p75_travel_cost=quantile(travel_cost, 0.75, na.rm=TRUE),
    p90_travel_cost=quantile(travel_cost, 0.90, na.rm=TRUE),
    .groups="drop"
  ) |>
  group_by(panel) |>
  arrange(median_travel_cost, .by_group=TRUE) |>
  mutate(
    site_label=Name,
    site_label_panel=factor(
      paste(panel, site_label, sep="___"),
      levels=rev(paste(panel, site_label, sep="___"))
    )
  ) |>
  ungroup() |>
  ggplot(aes(median_travel_cost, site_label_panel)) +
  geom_errorbarh(aes(xmin=p10_travel_cost, xmax=p90_travel_cost), height=0, alpha=0.25) +
  geom_errorbarh(aes(xmin=p25_travel_cost, xmax=p75_travel_cost), height=0.18) +
  geom_point(size=1.8) +
  facet_wrap(~panel, scales="free_y", ncol=1) +
  scale_y_discrete(labels=\(x) sub(".*___", "", x)) +
  scale_x_continuous(labels=scales::dollar) +
  labs(
    x="Travel cost",
    y=NULL,
    title="Travel-cost distributions for lowest- and highest-cost parks",
    subtitle="Point is median; dark line is IQR; light line is 10th to 90th percentile"
  ) +
  theme_minimal()

local({ # Income data sources
  d<-readr::read_csv(
    "Data/intermediate/cbg_income_acs5_2024.csv",
    col_types=readr::cols(
      income_source=readr::col_character(),
      .default=readr::col_skip()
    ),
    show_col_types=FALSE
  ) |>
    count(income_source,name="n_cbgs") |>
    mutate(
      pct_cbgs=100*n_cbgs/sum(n_cbgs),
      income_source=recode(
        income_source,
        block_group="Block group",
        tract="Census tract",
        county="County",
        unavailable="Unavailable"
      ),
      income_source=factor(
        income_source,
        levels=c("Block group","Census tract","County","Unavailable")
      )
    ) |>
    arrange(income_source) |>
    gt::gt() |>
    gt::cols_label(
      income_source="Income data source",
      n_cbgs="CBGs",
      pct_cbgs="Share (%)"
    ) |>
    gt::fmt_number(columns=n_cbgs,decimals=0) |>
    gt::fmt_number(columns=pct_cbgs,decimals=1) |>
    gt::cols_align(align="right",columns=c(n_cbgs,pct_cbgs)) |>
    project_table(
      title="Geographic sources of median household income estimates, 2024",
      note="Income estimates are taken from census block groups where available, with census tract and county estimates used when block-group estimates are unavailable."
    )

  print(d)
})


# RUM tables and figures -------------------------------------------------
local({ # Destination-choice regression table
  models<-list(
    "all_travel"=rum_results$models$site_progression$Extent[["Travel cost"]],
    "all_area"=rum_results$models$site_progression$Extent[["Park area"]],
    "all_extent"=rum_results$models$site_extent,
    "all_presence"=rum_results$models$site_presence,
    "florida_extent"=rum_results$models$florida$site_extent
  )

  labels<-c(
    travel_cost_dollars="Round-trip travel cost ($)",
    mangrove_presence="Mangroves present",
    log_mangrove_ha="Log(1 + mangrove ha)",
    log_beach_ha="Log(1 + beach ha)",
    tree_canopy_pct="Tree canopy (%)",
    impervious_pct="Impervious cover (%)",
    log_park_area="Log(park ha)",
    sports_facilities="Sports facilities present",
    marine_waterfront="Marine waterfront"
  )

  fmt<-function(x) {
    ifelse(abs(x)<.01 & x!=0,sprintf("%.5f",x),
           ifelse(abs(x)<.1 & x!=0,sprintf("%.4f",x),sprintf("%.3f",x)))
  }

  d<-purrr::imap_dfr(models,\(fit,model) {
    ct<-fixest::coeftable(fit)
    tibble(
      term=rownames(ct),
      estimate=as.numeric(ct[,1]),
      se=as.numeric(ct[,2]),
      p_value=as.numeric(ct[,4]),
      model=model
    )
  }) |>
    mutate(
      stars=case_when(
        p_value<.001~"***",
        p_value<.01~"**",
        p_value<.05~"*",
        TRUE~""
      ),
      cell=paste0(fmt(estimate),stars," (",fmt(se),")")
    ) |>
    dplyr::select(term,model,cell) |>
    pivot_wider(names_from=model,values_from=cell)

  d<-tibble(term=names(labels)) |>
    left_join(d,by="term")

  obs<-tibble(term=c("Observations","AIC"))
  for (model in names(models)) {
    obs[[model]]<-scales::comma(c(
      stats::nobs(models[[model]]),
      stats::AIC(models[[model]])
    ),accuracy=1)
  }

  d<-bind_rows(d,obs) |>
    mutate(term=coalesce(unname(labels[term]),term)) |>
    rename(Characteristic=term) |>
    gt::gt() |>
    gt::tab_spanner(
      label="All origins",
      columns=c(all_travel,all_area,all_extent,all_presence)
    ) |>
    gt::tab_spanner(
      label="Florida origins",
      columns=florida_extent
    ) |>
    gt::cols_label(
      all_travel="(1) Travel cost",
      all_area="(2) + Park area",
      all_extent="(3) Full extent",
      all_presence="(4) Full presence",
      florida_extent="(5) Full extent"
    ) |>
    gt::sub_missing(missing_text="") |>
    gt::cols_align("right",columns=-Characteristic) |>
    project_table(
      title="Destination-choice models",
      note="All models use Psuedo-Poisson Maximum Likelihood with origin CBG fixed effects and standard errors clustered by origin CBG and park. Lower AIC indicates better relative fit among models estimated on the same sample. * p < 0.05; ** p < 0.01; *** p < 0.001."
    )

  print(d)
})

local({ # Trip-frequency regression table
  models<-list(
    "all_origins"=rum_results$models$frequency_extent,
    "florida_origins"=rum_results$models$florida$frequency_extent
  )

  labels<-c(
    monetary_iv_extent="Monetary inclusive value ($)",
    log_income="Log median household income",
    "(Intercept)"="Intercept"
  )

  fmt<-function(x) {
    ifelse(abs(x)<.01 & x!=0,sprintf("%.5f",x),
           ifelse(abs(x)<.1 & x!=0,sprintf("%.4f",x),sprintf("%.3f",x)))
  }

  d<-purrr::imap_dfr(models,\(fit,model) {
    ct<-fixest::coeftable(fit)
    tibble(
      term=rownames(ct),
      estimate=as.numeric(ct[,1]),
      se=as.numeric(ct[,2]),
      p_value=as.numeric(ct[,4]),
      model=model
    )
  }) |>
    mutate(
      stars=case_when(
        p_value<.001~"***",
        p_value<.01~"**",
        p_value<.05~"*",
        TRUE~""
      ),
      cell=paste0(fmt(estimate),stars," (",fmt(se),")")
    ) |>
    dplyr::select(term,model,cell) |>
    pivot_wider(names_from=model,values_from=cell)

  d<-tibble(term=names(labels)) |>
    left_join(d,by="term")

  obs<-tibble(
    term=c("Observations (CBGs)","Population represented"),
    all_origins=scales::comma(c(
      stats::nobs(models$all_origins),
      sum(rum_results$data$cbg_annual$cbg_population)
    ),accuracy=1),
    florida_origins=scales::comma(c(
      stats::nobs(models$florida_origins),
      rum_results$tables$florida_coverage$represented_population
    ),accuracy=1)
  )

  d<-bind_rows(d,obs) |>
    mutate(term=coalesce(unname(labels[term]),term)) |>
    rename(Characteristic=term) |>
    gt::gt() |>
    gt::cols_label(
      all_origins="All origins",
      florida_origins="Florida origins"
    ) |>
    gt::sub_missing(missing_text="") |>
    gt::cols_align("right",columns=-Characteristic) |>
    project_table(
      title="Annual trip-frequency models",
      note="Dependent variable: calibrated annual visitor-days per CBG. PPML with log CBG population as an offset and heteroskedasticity-robust standard errors. Population includes nonvisiting residents, so aggregate per-capita demand reflects both participation (extensive margin) and visit frequency among participants (intensive margin), without estimating these margins separately. Monetary inclusive values are derived from the corresponding mangrove-extent destination-choice models, re-estimated separately for Florida. * p < 0.05; ** p < 0.01; *** p < 0.001."
    )

  print(d)
})

local({ # Appendix - Mangrove coefficient sensitivity
  d<-rum_results$tables$site_sensitivity |>
    mutate(
      mangrove_measure=if_else(
        grepl("presence",specification),"Presence","Extent"
      ),
      increment=if_else(mangrove_measure=="Presence",1,log(2)),
      ci_low=exp((mangrove_coefficient-1.96*se)*increment),
      ci_high=exp((mangrove_coefficient+1.96*se)*increment),
      origin_sample=if_else(
        grepl("Florida",specification),"Florida origins","All origins"
      ),
      model=case_when(
        grepl("quadratic",specification)~"Extent (quadratic park area)",
        mangrove_measure=="Presence"~"Presence",
        TRUE~"Extent"
      ),
      n=scales::comma(observations),
      p=if_else(p_value<.001,"<0.001",sprintf("%.3f",p_value))
    ) |>
    dplyr::select(
      origin_sample,model,n,mangrove_coefficient,se,p,odds_ratio,
      ci_low,ci_high,monetary_equivalent
    ) |>
    gt::gt() |>
    gt::cols_label(
      origin_sample="Origin sample",
      model="Specification",
      n="N",
      mangrove_coefficient="Coefficient",
      se="SE",
      p="p-value",
      odds_ratio="Odds ratio",
      ci_low="95% CI low",
      ci_high="95% CI high",
      monetary_equivalent="Dollar equivalent"
    ) |>
    gt::fmt_number(
      columns=c(mangrove_coefficient,se,odds_ratio,ci_low,ci_high),
      decimals=3
    ) |>
    gt::fmt_number(columns=monetary_equivalent,decimals=2) |>
    gt::cols_align(
      "right",
      columns=c(n,mangrove_coefficient,se,p,odds_ratio,
                ci_low,ci_high,monetary_equivalent)
    ) |>
    project_table(
      title="Mangrove coefficient sensitivity",
      note="Presence: absent versus present. Extent: 0 versus 1 ha. Confidence intervals use park- and origin-clustered standard errors. Dollar equivalents represent conditional destination utility, not aggregate annual welfare."
    )

  print(d)
})

# local({ # Conditional monetary equivalents
#   d<-rum_results$tables$mangrove_money |>
#     dplyr::select(
#       model,mangrove_change,money_equivalent_dollars,ci_low,ci_high
#     ) |>
#     gt::gt() |>
#     gt::cols_label(
#       model="Model",
#       mangrove_change="Mangrove change",
#       money_equivalent_dollars="Dollar equivalent",
#       ci_low="95% CI low",
#       ci_high="95% CI high"
#     ) |>
#     gt::fmt_number(
#       columns=c(money_equivalent_dollars,ci_low,ci_high),
#       decimals=2
#     ) |>
#     gt::cols_align(
#       "right",
#       columns=c(money_equivalent_dollars,ci_low,ci_high)
#     ) |>
#     project_table(
#       title="Conditional monetary equivalents of mangrove attributes",
#       note="Dollar equivalents are derived from the destination-choice utility and travel-cost coefficients. They are not aggregate annual welfare estimates."
#     )

#   print(d)
# })

# local({ # Destination-choice effects
#   print(rum_results$figures$site_choice_odds)
# })

local({ # Frequency model fit - ################## revisit this with alt specifications of the trip frequency model
  print(rum_results$figures$frequency_fit)
})




local({ # One-hectare mangrove restoration welfare
  cap<-20

  d<-rum_results$tables$site_marginals |>
    filter(
      !is.na(mangrove_welfare_1ha),
      calibrated_site_visitor_days>0
    ) |>
    arrange(desc(abs(linked_annual_dollars_1ha))) |>
    mutate(
      site_label=paste0(
        stringr::str_to_title(site_name),
        " (",scales::comma(calibrated_site_visitor_days,accuracy=1),")"
      ),
      site=factor(site_label,levels=rev(site_label)),
      welfare_per_visit=linked_annual_dollars_1ha/calibrated_site_visitor_days
    )

  components<-d |>
    dplyr::select(site,mangrove_welfare_1ha,canopy_welfare_1ha) |>
    pivot_longer(
      cols=c(mangrove_welfare_1ha,canopy_welfare_1ha),
      names_to="component",
      values_to="value"
    ) |>
    mutate(
      component=recode(
        component,
        mangrove_welfare_1ha="Mangroves",
        canopy_welfare_1ha="Tree canopy"
      ),
      measure="A. Annual welfare ($)"
    )

  net<-bind_rows(
    d |>
      transmute(
        site,
        measure="A. Annual welfare ($)",
        value=linked_annual_dollars_1ha,
        offscale=FALSE
      ),
    d |>
      transmute(
        site,
        measure="B. Welfare per calibrated park visitor-day ($)",
        value=pmax(-cap,pmin(cap,welfare_per_visit)),
        offscale=abs(welfare_per_visit)>cap
      )
  )

  offscale_labels<-d |>
    filter(abs(welfare_per_visit)>cap) |>
    transmute(
      label=paste0(
        stringr::str_to_title(site_name),
        " (",scales::dollar(welfare_per_visit,accuracy=.01),")"
      )
    ) |>
    pull(label)

  d<-ggplot() +
    geom_hline(yintercept=0,linetype="dashed",color="grey60") +
    geom_col(
      data=components,
      aes(x=site,y=value,fill=component),
      width=0.7
    ) +
    geom_point(
      data=net |> filter(!offscale),
      aes(x=site,y=value),
      size=2,
      color="black"
    ) +
    geom_point(
      data=net |> filter(offscale),
      aes(x=site,y=value),
      shape=18,
      size=3,
      color="black"
    ) +
    facet_wrap(~measure,nrow=1,scales="free") +
    coord_flip() +
    scale_x_discrete(drop=FALSE) +
    scale_fill_manual(
      values=c("Mangroves"="#B46B55","Tree canopy"="#399980")
    ) +
    scale_y_continuous(
      labels=scales::label_dollar(
        accuracy=0.01,
        scale_cut=scales::cut_short_scale()
      )
    ) +
    labs(
      title="Recreational welfare effects of mangrove restoration",
      subtitle="One additional hectare at each park; parentheses show calibrated annual visitor-days",
      x=NULL,
      y="Change in recreational welfare",
      fill=NULL,
      caption=paste0(
        "Panel B capped at ±$20 for display. Diamonds indicate off-scale values",
        if (length(offscale_labels)>0) {
          paste0(": ",paste(offscale_labels,collapse="; "))
        } else {
          ""
        },
        ".\nAnnual welfare incorporates destination substitution and trip-frequency responses for top 15 affected parks. Per-visit values use baseline calibrated visitor-days at the affected park. Point estimates; no confidence intervals shown."
      )
    ) +
    theme_minimal(base_size=11) +
    theme(
      plot.title=element_text(face="bold",size=12),
      strip.text=element_text(face="bold"),
      panel.grid.minor=element_blank(),
      legend.position="bottom",
      plot.caption=element_text(hjust=0)
    )

  print(d)
})


local({ # Welfare decomposition of mangrove restoration
  d<-rum_results$tables$site_responses |>
    arrange(desc(abs(linked_annual_dollars_1ha))) |>
    mutate(
      site_label=paste0(
        stringr::str_to_title(site_name),
        " (",scales::comma(calibrated_site_visitor_days,accuracy=1),")"
      ),
      site=factor(site_label,levels=rev(site_label))
    )

  components<-d |>
    dplyr::select(
      site,
      direct_welfare,
      substitution_welfare,
      frequency_adjustment
    ) |>
    pivot_longer(
      cols=-site,
      names_to="component",
      values_to="value"
    ) |>
    mutate(
      component=recode(
        component,
        direct_welfare="Direct destination effect",
        substitution_welfare="Destination substitution",
        frequency_adjustment="Trip-frequency adjustment"
      ),
      component=factor(
        component,
        levels=c(
          "Direct destination effect",
          "Destination substitution",
          "Trip-frequency adjustment"
        )
      )
    )

  d<-ggplot() +
    geom_hline(
      yintercept=0,
      linetype="dashed",
      color="grey60"
    ) +
    geom_col(
      data=components,
      aes(x=site,y=value,fill=component),
      width=0.7
    ) +
    geom_point(
      data=d,
      aes(x=site,y=net_welfare),
      size=2,
      color="black"
    ) +
    coord_flip() +
    scale_fill_manual(
      values=c(
        "Direct destination effect"="#B46B55",
        "Destination substitution"="#399980",
        "Trip-frequency adjustment"="#769BB7"
      )
    ) +
    scale_y_continuous(
      labels=scales::label_dollar(
        accuracy=0.01,
        scale_cut=scales::cut_short_scale()
      )
    ) +
    labs(
      title="Welfare decomposition of mangrove restoration",
      subtitle="One additional hectare at each park; black points indicate net annual welfare",
      x=NULL,
      y="Annual welfare change ($)",
      fill=NULL,
      caption=paste0(
        "Direct effect holds baseline destination choices fixed. ",
        "Substitution measures the welfare recovered through destination adjustment.\n",
        "The trip-frequency adjustment accounts for changes in total recreation demand. ",
        "All components use model-predicted calibrated visitor-days.\n",
        "Parentheses show baseline observed calibrated visitor-days at each park. ",
        "Point estimates; no confidence intervals shown."
      )
    ) +
    theme_minimal(base_size=11) +
    theme(
      plot.title=element_text(face="bold",size=12),
      panel.grid.minor=element_blank(),
      legend.position="bottom",
      plot.caption=element_text(hjust=0)
    )

  print(d)
})













# Travel stats viewer -----------------------------------------------------------
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

