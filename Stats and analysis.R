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
source("Airport_calibration.R")


# Site stats -------------------------------------------------------------
local({ # Park stats
  d<-dfst |>
    st_drop_geometry() |>
    mutate(year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y"))) |>
    filter(year==2024) |>
    mutate(total_unique_devices=n_distinct(DEVICEID, na.rm=TRUE)) |>
    group_by(FEATUREID, Name) |>
    summarise(
      annual_visitor_days=sum(calibrated_visits, na.rm=TRUE),
      observed_device_days=n(),
      site_device_pairs=n_distinct(DEVICEID, na.rm=TRUE),
      total_unique_devices=first(total_unique_devices),
      median_distance_km=median(outbound_distance_m/1000, na.rm=TRUE),
      median_duration_min=median(outbound_duration_min, na.rm=TRUE),
      .groups="drop"
    ) |>
    summarise(
      parks=n_distinct(FEATUREID),
      annual_visitor_days=sum(annual_visitor_days),
      observed_device_days=sum(observed_device_days),
      unique_device_ids=first(total_unique_devices),
      site_device_pairs=sum(site_device_pairs),
      median_park_travel_distance_km=median(median_distance_km, na.rm=TRUE),
      median_park_travel_time_min=median(median_duration_min, na.rm=TRUE)
    ) |>
    pivot_longer(everything(), names_to="metric", values_to="value") |>
    mutate(
      metric=recode(metric,
        parks="Parks", annual_visitor_days="Calibrated annual visitor-days",
        observed_device_days="Observed device-days", unique_device_ids="Unique device IDs",
        site_device_pairs="Site-device pairs",
        median_park_travel_distance_km="Median park one-way travel distance",
        median_park_travel_time_min="Median park one-way travel time"),
      value=case_when(
        str_detect(metric, "distance") ~ paste0(round(value, 1), " km"),
        str_detect(metric, "time") ~ paste0(round(value, 1), " min"),
        TRUE ~ scales::comma(round(value))),
      y=rev(row_number()), shade=row_number()%%2==0
    )

  ggplot(d) +
    geom_rect(data=filter(d, shade), aes(ymin=y-.5, ymax=y+.5),
              xmin=-.05, xmax=2.75, fill="grey95", inherit.aes=FALSE) +
    geom_text(aes(0, y, label=metric), hjust=0, size=3.8) +
    geom_text(aes(2.7, y, label=value), hjust=1, size=3.8) +
    annotate("text", x=c(0,2.7), y=max(d$y)+1, label=c("Metric","Value"),
             hjust=c(0,1), fontface="bold", size=3.8) +
    geom_hline(yintercept=max(d$y)+1.55, linewidth=.7) +
    geom_hline(yintercept=max(d$y)+.5, linewidth=.4) +
    geom_hline(yintercept=.5, linewidth=.7) +
    coord_cartesian(xlim=c(-.05,2.75), ylim=c(.3,max(d$y)+1.7), clip="off") +
    labs(title="Park sample summary, 2024", 
         # subtitle="2024",
         caption="Note: Travel distance and time are one-way park-level medians based on outbound routing.") +
    theme_void() +
    theme(plot.title=element_text(size=13),
          plot.subtitle=element_text(size=10),
          plot.caption=element_text(hjust=0, size=8),
          plot.margin=margin(10,20,10,10))
})

dfst |> # Map 
  select(FEATUREID, Name, Jurisdiction, geom) |>
  distinct(FEATUREID, .keep_all=TRUE) |>
  st_as_sf(sf_column_name="geom") |>
  left_join(
    dfst |>
      st_drop_geometry() |>
      mutate(year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y"))) |>
      filter(year == 2024) |>
      group_by(FEATUREID) |>
      summarise(
        annual_visitor_days=sum(calibrated_visits, na.rm=TRUE),
        observed_device_days=n(),
        unique_devices=n_distinct(DEVICEID),
        .groups="drop"
      ),
    by="FEATUREID"
  ) |>
  ggplot() +
  geom_sf(aes(fill=annual_visitor_days), color="white", linewidth=0.1) +
  scale_fill_viridis_c(
    trans="log10",
    labels=scales::comma,
    na.value="grey90"
  ) +
  labs(
    fill="Annual visitor-days",
    title="Estimated annual visitor-days by site"
  ) +
  theme_minimal()

selected_ids <- c("93","357","358","68","24","97","353","57","130","100","14","355","122","98","30","96","135","102","134","136","356","1")  # Mangrove FEATUREIDs

dfst |> # Calibrated visitor days by park for high and low visitation parks
  st_drop_geometry() |>
  mutate(
    year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
    FEATUREID=trimws(as.character(FEATUREID))
  ) |>
  filter(year == 2024) |>
  group_by(FEATUREID, Name, Jurisdiction) |>
  summarise(
    annual_visitor_days=sum(calibrated_visits, na.rm=TRUE),
    observed_device_days=n(),
    unique_devices=n_distinct(DEVICEID),
    .groups="drop"
  ) |>
  arrange(desc(annual_visitor_days)) |>
  mutate(rank=row_number()) |>
  (\(x) bind_rows(
    x |>
      slice_max(annual_visitor_days, n=30, with_ties=FALSE) |>
      mutate(panel="A. Highest visitation parks"),
    x |>
      filter(FEATUREID %in% trimws(as.character(selected_ids))) |>
      mutate(panel="B. Parks with mangroves")
  ))() |>
  mutate(
    panel=factor(panel, levels=c("A. Highest visitation parks", "B. Parks with mangroves")),
    site_label=paste0(Name, " [rank ", rank, "]")
  ) |>
  ggplot(aes(annual_visitor_days, reorder(site_label, annual_visitor_days))) +
  geom_col() +
  geom_text(
    aes(label=scales::comma(round(annual_visitor_days))),
    hjust=-0.1,
    size=3
  ) +
  facet_wrap(~panel, scales="free_y", ncol=1, drop=FALSE) +
  scale_x_continuous(
    labels=scales::comma,
    expand=expansion(mult=c(0, 0.18))
  ) +
  coord_cartesian(clip="off") +
  labs(
    x="Visitor-days",
    y=NULL,
    title="Calibrated visitor-days by site, 2024",
    subtitle="Site rank shown in brackets"
  ) +
  theme_minimal() +
  theme(
    plot.margin=margin(5.5, 35, 5.5, 5.5)
  )

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
local({ # Oleta State Park visitors and device stats
  d<-Oleta |>
    arrange(month) |>
    transmute(Month=month_name, Visitors=counts, `Observed device visits`=device_visits,
              `Visitors per device`=counts/device_visits) |>
    (\(x) bind_rows(x, tibble(
      Month="Annual total", Visitors=sum(x$Visitors, na.rm=TRUE),
      `Observed device visits`=sum(x$`Observed device visits`, na.rm=TRUE),
      `Visitors per device`=sum(x$Visitors, na.rm=TRUE)/sum(x$`Observed device visits`, na.rm=TRUE))))() |>
    mutate(y=rev(row_number()), total=Month=="Annual total", shade=row_number()%%2==0,
           visitors_label=scales::comma(Visitors, accuracy=1),
           devices_label=scales::comma(`Observed device visits`, accuracy=1),
           ratio_label=scales::number(`Visitors per device`, accuracy=.1))

  ggplot(d) +
    geom_rect(data=filter(d, shade, !total), aes(ymin=y-.5, ymax=y+.5),
              xmin=-.1, xmax=3.2, fill="grey95", inherit.aes=FALSE) +
    geom_text(aes(0, y, label=Month, fontface=if_else(total, "bold", "plain")), hjust=0, size=3.7) +
    geom_text(aes(1.2, y, label=visitors_label, fontface=if_else(total, "bold", "plain")), hjust=1, size=3.7) +
    geom_text(aes(2.25, y, label=devices_label, fontface=if_else(total, "bold", "plain")), hjust=1, size=3.7) +
    geom_text(aes(3.15, y, label=ratio_label, fontface=if_else(total, "bold", "plain")), hjust=1, size=3.7) +
    annotate("text", x=c(0,1.2,2.25,3.15), y=max(d$y)+1,
             label=c("Month","Visitor\ncount","Observed visitor\ndays","Visitors per\nobserved device"),
             hjust=c(0,1,1,1), fontface="bold", size=3.7) +
    geom_hline(yintercept=max(d$y)+1.55, linewidth=.7) +
    geom_hline(yintercept=max(d$y)+.5, linewidth=.4) +
    geom_hline(yintercept=1.5, linewidth=.4) +
    geom_hline(yintercept=.5, linewidth=.7) +
    coord_cartesian(xlim=c(-.1,3.2), ylim=c(.3,max(d$y)+1.7), clip="off") +
    labs(title="Oleta River State Park visitor counts and device calibration, 2024", 
         #subtitle="2024",
         caption="Note: Visitors per observed device equals visitor count divided by observed visitor days.") +
    theme_void() +
    theme(plot.title=element_text(size=13), plot.subtitle=element_text(size=10),
          plot.caption=element_text(hjust=0, size=8), plot.margin=margin(10,15,10,10))
})

local({ # Miami enplanements and devices
  d<-mia_enplanements_monthly |>
    arrange(month) |>
    transmute(
      Month=month_name,
      Enplanements=enplanements,
      `Observed devices`=device_enplanements,
      `Enplanements per device`=enplanements / device_enplanements
    ) |>
    (\(x) bind_rows(
      x,
      tibble(
        Month="Annual total",
        Enplanements=sum(x$Enplanements, na.rm=TRUE),
        `Observed devices`=sum(x$`Observed devices`, na.rm=TRUE),
        `Enplanements per device`=
          sum(x$Enplanements, na.rm=TRUE) /
          sum(x$`Observed devices`, na.rm=TRUE)
      )
    ))() |>
    mutate(
      y=rev(row_number()),
      total=Month == "Annual total",
      shade=row_number() %% 2 == 0,
      enplanements_label=scales::comma(Enplanements, accuracy=1),
      devices_label=scales::comma(`Observed devices`, accuracy=1),
      ratio_label=scales::number(`Enplanements per device`, accuracy=0.1)
    )

  p<-ggplot(d) +
    geom_rect(
      data=filter(d, shade, !total),
      aes(ymin=y - 0.5, ymax=y + 0.5),
      xmin=-0.1,
      xmax=3.25,
      fill="grey95",
      inherit.aes=FALSE
    ) +
    geom_text(
      aes(0, y, label=Month, fontface=if_else(total, "bold", "plain")),
      hjust=0,
      size=3.7
    ) +
    geom_text(
      aes(1.25, y, label=enplanements_label,
          fontface=if_else(total, "bold", "plain")),
      hjust=1,
      size=3.7
    ) +
    geom_text(
      aes(2.25, y, label=devices_label,
          fontface=if_else(total, "bold", "plain")),
      hjust=1,
      size=3.7
    ) +
    geom_text(
      aes(3.2, y, label=ratio_label,
          fontface=if_else(total, "bold", "plain")),
      hjust=1,
      size=3.7
    ) +
    annotate(
      "text",
      x=c(0, 1.25, 2.25, 3.2),
      y=max(d$y) + 1,
      label=c(
        "Month",
        "Passenger\nenplanements",
        "Observed device\nenplanements",
        "Passengers per\nobserved device"
      ),
      hjust=c(0, 1, 1, 1),
      fontface="bold",
      size=3.7
    ) +
    geom_hline(
      yintercept=c(max(d$y) + 1.55, max(d$y) + 0.5),
      linewidth=c(0.7, 0.4)
    ) +
    geom_hline(
      yintercept=c(1.5, 0.5),
      linewidth=c(0.4, 0.7)
    ) +
    coord_cartesian(
      xlim=c(-0.1, 3.25),
      ylim=c(0.3, max(d$y) + 1.7),
      clip="off"
    ) +
    labs(
      title="MIA passenger enplanements and device calibration, 2024",
      #subtitle="2024",
      caption=paste(
        "Note: Passengers per observed device equals passenger enplanements",
        "divided by observed device enplanements."
      )
    ) +
    theme_void() +
    theme(
      plot.title=element_text(size=13),
      plot.subtitle=element_text(size=10),
      plot.caption=element_text(hjust=0, size=8),
      plot.margin=margin(10, 15, 10, 10)
    )

  print(p)

  # Uncomment to export:
  # dir.create("Figures", showWarnings=FALSE)
  # ggsave(
  #   "Figures/mia_enplanements_table.pdf",
  #   plot=p,
  #   width=7.5,
  #   height=6.5
  # )
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
dfst |> # Origin CBG representation in park-visit device data
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
  labs(x="Frequency of visitor-days across all parks by CBG", y="Share of CBGs",
       title = "Frequency of visitor-days to all parks by census block group, 2024" )

dfst |> # Share of visitors by distance band
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
  filter(!is.na(distance_band)) |>
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

dfst |> # Home location by state
  st_drop_geometry() |>
  mutate(
    year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
    state_fips=substr(CENSUS_BLOCK_GROUP_ID, 1, 2)
  ) |>
  filter(year == 2024, !is.na(state_fips)) |>
  left_join(
    tibble(
      state_fips=c("01","02","04","05","06","08","09","10","11","12","13","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","44","45","46","47","48","49","50","51","53","54","55","56","72"),
      state=c("AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","PR")
    ),
    by="state_fips"
  ) |>
  group_by(state) |>
  summarise(
    annual_visitor_days=sum(calibrated_visits, na.rm=TRUE),
    observed_device_days=n(),
    unique_devices=n_distinct(DEVICEID),
    unique_sites=n_distinct(FEATUREID),
    .groups="drop"
  ) |>
  mutate(share_visitor_days=annual_visitor_days / sum(annual_visitor_days)) |>
  slice_max(annual_visitor_days, n=20) |>
  ggplot(aes(annual_visitor_days, reorder(state, annual_visitor_days))) +
  geom_col() +
  geom_text(aes(label=scales::percent(share_visitor_days, accuracy=0.1)), hjust=-0.1, size=3) +
  scale_x_continuous(labels=scales::comma, expand=expansion(mult=c(0, 0.18))) +
  coord_cartesian(clip="off") +
  labs(
    x="Annual visitor-days",
    y=NULL,
    title="Estimated visitor-days by origin state",
    subtitle="Top 20 states by annual visitor-days"
  ) +
  theme_minimal() +
  theme(plot.margin=margin(5.5, 35, 5.5, 5.5))

dfst |> # One-way travel-distance distributions for highest- and lowest-visitation parks
  st_drop_geometry() |>
  mutate(
    year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
    FEATUREID=trimws(as.character(FEATUREID)),
    distance_km=outbound_distance_m / 1000
  ) |>
  filter(year == 2024, !is.na(distance_km), is.finite(distance_km), distance_km >= 0) |>
  inner_join(
    dfst |>
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

dfst |> # One-way travel-distance distributions for shortest- and furthest-travel-distance parks
  st_drop_geometry() |>
  mutate(year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
         FEATUREID=trimws(as.character(FEATUREID)),
         distance_km=outbound_distance_m/1000) |>
  filter(year==2024, !is.na(distance_km), is.finite(distance_km), distance_km>=0) |>
  inner_join(
    dfst |>
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
travel_cost_params<-list(
  auto_cost_per_mile=0.2503, # 2024 AAA marginal driving cost
  annual_work_hours=2080,    # 40 hours/week * 52 weeks
  vot_fraction=0.33          # value of travel time as share of hourly income
)

local({
  d <- dfst |>
    st_drop_geometry() |>
    mutate(
      year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
      distance_miles=distance_m / 1609.344,
      duration_hours=duration_min / 60,
      hourly_income=med_hh_income / travel_cost_params$annual_work_hours,
      travel_cost=
        travel_cost_params$auto_cost_per_mile * distance_miles +
        travel_cost_params$vot_fraction * hourly_income * duration_hours
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

dfst |>
  st_drop_geometry() |>
  mutate(
    year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
    FEATUREID=trimws(as.character(FEATUREID)),
    distance_miles=distance_m / 1609.344,
    duration_hours=duration_min / 60,
    hourly_income=med_hh_income / travel_cost_params$annual_work_hours,
    travel_cost=
      travel_cost_params$auto_cost_per_mile * distance_miles +
      travel_cost_params$vot_fraction * hourly_income * duration_hours
  ) |>
  filter(year == 2024, !is.na(travel_cost), is.finite(travel_cost), travel_cost >= 0) |>
  inner_join(
    dfst |>
      st_drop_geometry() |>
      mutate(
        year=as.integer(format(as.Date(DAY_IN_FEATURE), "%Y")),
        FEATUREID=trimws(as.character(FEATUREID)),
        distance_miles=distance_m / 1609.344,
        duration_hours=duration_min / 60,
        hourly_income=med_hh_income / travel_cost_params$annual_work_hours,
        travel_cost=
          travel_cost_params$auto_cost_per_mile * distance_miles +
          travel_cost_params$vot_fraction * hourly_income * duration_hours
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

rum_full<-rum_full %>% # Creating total round trip travel cost
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


