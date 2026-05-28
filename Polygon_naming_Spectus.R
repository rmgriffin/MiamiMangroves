# Setup -------------------------------------------------------------------
rm(list=ls()) # Clears workspace

# # Install/call libraries
# install.packages("renv") # Run if you have cloned repository and don't already have renv installed
# renv::restore() # Run once after cloning repository
# renv::install("package") # Run to install new packages
# renv::snapshot() # Run after installing new packages
# renv::init() # Only run when the repository is first created, don't run on cloning an existing repository

# Data for this repository is at https://drive.google.com/drive/folders/1syX_y2lMbo-ETNBXAo24m2FWUK1q60Ux?usp=sharing

pkgs<-c("tidyverse","googledrive","sf")

missing<-pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly=TRUE)]
if (length(missing)>0) {
  stop("Missing packages: ", paste(missing, collapse=", "),
       "\nRun renv::restore()")
}

invisible(lapply(pkgs, library, character.only=TRUE))
rm(pkgs, missing)

options(scipen = 999) # Prevent scientific notation
`%ni%`<- Negate(`%in%`) # Useful function


# Load data --------------------------------------------------------------
# dir.create(file.path('Data'), recursive = TRUE)
# folder_url<-"https://drive.google.com/open?id=1syX_y2lMbo-ETNBXAo24m2FWUK1q60Ux"
# folder<-drive_get(as_id(folder_url))
# files<-drive_ls(folder)
# dl<-function(files){
#   walk(files, ~ drive_download(as_id(.x), overwrite = TRUE))
# }
# setwd("./Data")
# system.time(map(files$id,dl))
# system.time(unzip("Data.zip", exdir = "."))
# file.remove("Data.zip")
# setwd("..")
# rm(files, folder, folder_url, dl)

target_path<-"Data/GIS_data/Spectus_rename/cope_polygons_miami_only.shp"

lookup_paths<-c(
  NatlStateParkPreserve="Data/GIS_data/Miami_jurisdictional_layers/NatlStateParkPreserve_gdb_2532132946686411322.gpkg",
  MunicipalParkBoundary="Data/GIS_data/Miami_jurisdictional_layers/MunicipalParkBoundary_gdb_-2044306313321623328.gpkg",
  CountyParkBoundaries_PROS="Data/GIS_data/Miami_jurisdictional_layers/CountyParkBoundaries_PROS_View_for_ALWAYS_ON_-4911464144142080635.gpkg",
  Beach_poly="Data/GIS_data/Miami_jurisdictional_layers/Beachpoly_gdb_-3435592677776368442.gpkg"
)

out_path<-"Data/GIS_data/County_State_National_Municipal_v3.gpkg"

target<-st_read(target_path, quiet=TRUE) |> 
  mutate(target_id=row_number())

target_crs<-st_crs(target)

read_lookup<-function(path, source_name) {
  
  x<-st_read(path, quiet=TRUE)
  
  name_col<-names(x)[str_to_lower(names(x))=="name"]
  
  if(length(name_col)!=1) {
    stop(
      "Could not identify exactly one Name/name field in: ",
      source_name,
      ". Found: ",
      paste(name_col, collapse=", ")
    )
  }
  
  x<-x %>%
    st_make_valid() %>%
    st_transform(target_crs) %>%
    mutate(
      lookup_source=source_name,
      lookup_name=as.character(.data[[name_col]])
    )
  
  out<-x %>%
    st_drop_geometry() %>%
    select(lookup_source, lookup_name)
  
  st_as_sf(
    bind_cols(out, geometry=st_geometry(x)),
    sf_column_name="geometry",
    crs=st_crs(x)
  )
}

lookup<-imap_dfr(
  lookup_paths,
  ~read_lookup(.x, .y)
)

target_match_geom<-target %>%
  select(target_id) %>%
  st_make_valid()

matches<-target_match_geom %>%
  st_join(
    lookup,
    join=st_intersects,
    left=FALSE
  ) %>%
  st_drop_geometry() %>%
  distinct(target_id, lookup_source, lookup_name)

multiple_match_ids<-matches %>%
  count(target_id, name="n_overlap_matches") %>%
  filter(n_overlap_matches>1) %>%
  arrange(target_id)

if(nrow(multiple_match_ids)>0) {
  
  message(
    "Spatial join produced more than one lookup match for the following target_id values: ",
    paste(multiple_match_ids$target_id, collapse=", ")
  )
  
  print(
    matches %>%
      semi_join(multiple_match_ids, by="target_id") %>%
      arrange(target_id, lookup_source, lookup_name)
  )
  
} else {
  
  message("Spatial join produced no target polygons with more than one lookup match.")
  
}

manual_name_overrides<-tribble(
  ~target_id, ~overlap_name, ~overlap_source,
1,"Oleta River State Park","NatlStateParkPreserve",
3,"Bill Baggs Cape State Park","NatlStateParkPreserve",
4,"Biscayne National Park","NatlStateParkPreserve",
8,"Dinner Key Auditorium & Marina","MunicipalParkBoundary",
10,"Ludovici Park","MunicipalParkBoundary",
12,"Newport Fishing Pier","MunicipalParkBoundary",
16,"St. Agnes Athletic Field","MunicipalParkBoundary",
21,"South Pointe Park","MunicipalParkBoundary",
29,"North Shore Open Space Park","MunicipalParkBoundary",
36,"Lummus Park - Miami Beach","MunicipalParkBoundary",
46,"North Miami Interama Property","MunicipalParkBoundary",
47,"Surfside Community Center","MunicipalParkBoundary",
52,"Samson Oceanfront Park","MunicipalParkBoundary",
55,"Indian Beach Park","MunicipalParkBoundary",
65,"Allison Park","MunicipalParkBoundary",
77,"Mangrove Preserve","CountyParkBoundaries_PROS",
78,"R. Hardy Matheson Preserve","CountyParkBoundaries_PROS",
81,"Fairchild Tropical Botanic Garden","CountyParkBoundaries_PROS",
82,"Deering Estate","CountyParkBoundaries_PROS",
83,"Lakes By the Bay Park","CountyParkBoundaries_PROS",
84,"Matheson Hammock Park","CountyParkBoundaries_PROS",
86,"Crandon Park","CountyParkBoundaries_PROS",
87,"Homestead Bayfront Park","CountyParkBoundaries_PROS",
88,"Sunny Isles Beach","CountyParkBoundaries_PROS",
89,"Miami Beach","CountyParkBoundaries_PROS",
90,"Surfside Beach","CountyParkBoundaries_PROS",
91,"Black Point Park and Marina","CountyParkBoundaries_PROS",
92,"Bal Harbour Beach","CountyParkBoundaries_PROS",
93,"Virginia Key","CountyParkBoundaries_PROS",
94,"Haulover Beach","CountyParkBoundaries_PROS",
95,"Hobie Beach","CountyParkBoundaries_PROS",
96,"Willis Island","MunicipalParkBoundary", # Manually added https://www.miamidade.gov/govaction/legistarfiles/Matters/Y2024/242025.pdf
97,"Dinner Key Island A","MunicipalParkBoundary", # Manually added
98,"Morningside Island","MunicipalParkBoundary", # Manually added
100,"Mangrove Island","MunicipalParkBoundary", # Manually added
101,"Frigate Island","NatlStateParkPreserve", # Manually added (state)
103,"Bird Key","NatlStateParkPreserve", # Manually added (private)
104,"Sandpiper Island","NatlStateParkPreserve", # Manually added (state)
105,"Tern Island","NatlStateParkPreserve", # Manually added (state)
106,"Quayside Island","NatlStateParkPreserve", # Manually added (state)
107,"Helkers Island","MunicipalParkBoundary", # Manually added
108,"Crescent Islands","MunicipalParkBoundary", # Manually added
109,"Little Sandspur Island","MunicipalParkBoundary", # Manually added
110,"Golden Beach","Beach_poly",
112,"Crandon Park","CountyParkBoundaries_PROS", # Merge with 86
113,"Lake Park + East Enid Linear Park + Ocean Beach Park","MunicipalParkBoundary",
115,"Rickenbacker Beach + Miami Seaquarium","CountyParkBoundaries_PROS",
116,"Bayshore Municipal Golf Course Par 3 + Scott Rakow Youth Center","MunicipalParkBoundary",
117,"Intracoastal Park + Oceania Park","MunicipalParkBoundary",
120,"Arthur I. Snyder Memorial Park - CRC + Aventura Community Recreation Center","MunicipalParkBoundary",
121,"Virginia Key Park","MunicipalParkBoundary",
150,"Normandy Shores","MunicipalParkBoundary",
151,"Fairway Park & Rec Center","MunicipalParkBoundary",
168,"Ed Abdella Field House/Athletics","MunicipalParkBoundary",
194,"Pinecrest","MunicipalParkBoundary",
231,"Patricia A. Mishcon Athletic Field","MunicipalParkBoundary",
232,"Harry Cohen Complex / Challenger Park","MunicipalParkBoundary",
233,"Snake Creek Linear Park","MunicipalParkBoundary",
299,"Snake River Trail","MunicipalParkBoundary",
346,"Seminole Wayside Park","CountyParkBoundaries_PROS",
354,"Icon Bay Park","MunicipalParkBoundary", # Added (new park City of Miami)
355,"Mast Academy Field","MunicipalParkBoundary",
357,"Manatee Bend Park","MunicipalParkBoundary", # Added (new park City of Miami)
358,"Virginia Key North","MunicipalParkBoundary",
359,"Historic Virginia Key Park","MunicipalParkBoundary")

multiple_match_ids<-matches %>%
  count(target_id, name="n_raw_overlap_matches") %>%
  filter(n_raw_overlap_matches>1) %>%
  arrange(target_id)

if(nrow(multiple_match_ids)>0) {
  message(
    "Spatial join produced more than one lookup match for the following target_id values: ",
    paste(multiple_match_ids$target_id, collapse=", ")
  )
  
  print(
    matches %>%
      semi_join(multiple_match_ids, by="target_id") %>%
      arrange(target_id, lookup_source, lookup_name)
  )
} else {
  message("Spatial join produced no target polygons with more than one lookup match.")
}

duplicate_manual_overrides<-manual_name_overrides %>%
  count(target_id, name="n_manual_overrides") %>%
  filter(n_manual_overrides>1)

if(nrow(duplicate_manual_overrides)>0) {
  stop(
    "manual_name_overrides has more than one row for these target_id values: ",
    paste(duplicate_manual_overrides$target_id, collapse=", ")
  )
}

bad_manual_sources<-manual_name_overrides %>%
  filter(!overlap_source %in% names(lookup_paths))

if(nrow(bad_manual_sources)>0) {
  message("Some manual overlap_source values do not match names in lookup_paths. Check spelling.")
  print(bad_manual_sources)
}

missing_manual_target_ids<-manual_name_overrides %>%
  anti_join(
    target %>%
      st_drop_geometry() %>%
      select(target_id),
    by="target_id"
  )

if(nrow(missing_manual_target_ids)>0) {
  stop(
    "manual_name_overrides includes target_id values not present in target: ",
    paste(missing_manual_target_ids$target_id, collapse=", ")
  )
}

raw_match_summary<-target %>%
  st_drop_geometry() %>%
  select(target_id) %>%
  left_join(
    matches %>%
      group_by(target_id) %>%
      summarise(
        n_raw_overlap_matches=n(),
        all_overlap_sources=paste(unique(lookup_source), collapse="; "),
        all_overlap_names=paste(unique(lookup_name), collapse="; "),
        .groups="drop"
      ),
    by="target_id"
  ) %>%
  mutate(
    n_raw_overlap_matches=coalesce(n_raw_overlap_matches, 0L)
  )

auto_single_matches<-matches %>%
  group_by(target_id) %>%
  filter(n()==1) %>%
  ungroup() %>%
  transmute(
    target_id,
    overlap_name=lookup_name,
    overlap_source=lookup_source,
    resolution_method="automatic_single_match"
  )

manual_matches<-manual_name_overrides %>%
  mutate(
    resolution_method="manual_override"
  )

final_matches<-auto_single_matches %>%
  anti_join(manual_matches, by="target_id") %>%
  bind_rows(manual_matches) %>%
  distinct(target_id, .keep_all=TRUE)

unresolved_multiple_matches<-matches %>%
  semi_join(multiple_match_ids, by="target_id") %>%
  anti_join(manual_matches, by="target_id") %>%
  arrange(target_id, lookup_source, lookup_name)

if(nrow(unresolved_multiple_matches)>0) {
  unresolved_ids<-unresolved_multiple_matches %>%
    distinct(target_id) %>%
    pull(target_id)
  
  message(
    "After manual overrides, the following target_id values still have unresolved multiple lookup matches: ",
    paste(unresolved_ids, collapse=", ")
  )
  
  print(unresolved_multiple_matches)
} else {
  message("After manual overrides, no target polygons have unresolved multiple lookup matches.")
}

final_match_summary<-final_matches %>%
  select(
    target_id,
    overlap_source,
    overlap_name,
    resolution_method
  ) %>%
  mutate(
    n_overlap_matches=1L,
    multiple_overlap_matches=FALSE
  )

match_summary<-raw_match_summary %>%
  left_join(final_match_summary, by="target_id") %>%
  mutate(
    n_overlap_matches=case_when(
      !is.na(overlap_name)~1L,
      TRUE~n_raw_overlap_matches
    ),
    multiple_overlap_matches=case_when(
      !is.na(overlap_name)~FALSE,
      n_raw_overlap_matches>1~TRUE,
      TRUE~FALSE
    )
  ) %>%
  select(
    target_id,
    n_overlap_matches,
    overlap_source,
    overlap_name,
    resolution_method,
    all_overlap_sources,
    all_overlap_names,
    multiple_overlap_matches
  )

target_out<-target %>%
  left_join(match_summary, by="target_id") %>%
  mutate(
    n_overlap_matches=coalesce(n_overlap_matches, 0L),
    has_overlap=n_overlap_matches>0,
    multiple_overlap_matches=coalesce(multiple_overlap_matches, FALSE)
  )

match_count_summary<-target_out %>%
  st_drop_geometry() %>%
  count(n_overlap_matches, name="n_target_polygons") %>%
  arrange(n_overlap_matches)

print(match_count_summary)

print(
  target_out %>%
    st_drop_geometry() %>%
    count(resolution_method, name="n_target_polygons") %>%
    arrange(resolution_method)
)

target_out<-target_out |> 
  mutate(Jurisdiction = case_when(
    overlap_source == "NatlStateParkPreserve" ~ "State",
    overlap_source == "MunicipalParkBoundary" ~ "City",
    overlap_source == "CountyParkBoundaries_PROS" ~ "County",
    overlap_source == "Beach_poly" ~ "Unknown",
    TRUE~NA_character_
    )) |> 
  mutate(Jurisdiction = case_when(
    overlap_name == "Biscayne National Park" ~ "Federal",
    TRUE~Jurisdiction
  ))

ids_to_merge<-c(86, 112)

merged_geom<-target_out %>%
  filter(target_id %in% ids_to_merge) %>%
  st_geometry() %>%
  st_union() %>%
  st_sfc(crs=st_crs(target_out))

merged_polygon<-target_out %>%
  filter(target_id==min(ids_to_merge)) %>%
  slice(1) %>%
  mutate(
    target_id=min(ids_to_merge),
    n_overlap_matches=1L,
    overlap_source="CountyParkBoundaries_PROS",
    overlap_name="Crandon Park",
    resolution_method="manual_spatial_merge",
    multiple_overlap_matches=FALSE,
    has_overlap=TRUE
  )

st_geometry(merged_polygon)<-merged_geom

target_out<-target_out %>%
  filter(!target_id %in% ids_to_merge) %>%
  bind_rows(merged_polygon) %>%
  arrange(target_id)

if(file.exists(out_path)) {
  unlink(out_path)
}

if(file.exists(out_path)) {
  stop("Could not delete existing GeoPackage. Close it in QGIS/ArcGIS/Windows Explorer or write to a new filename.")
}

target_out$id<-seq(1,nrow(target_out),1)

st_write(
  target_out |> rename(Name = overlap_name) |> dplyr::select(UNIT_NAME,Category,UNIT_CODE,ForestName,MultiPart,NVUM_year,siteid,Jurisdiction,id,Name),
  out_path,
  delete_dsn=TRUE,
  quiet=TRUE
)
