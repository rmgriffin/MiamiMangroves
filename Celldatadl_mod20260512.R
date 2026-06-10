# Setup -------------------------------------------------------------------
rm(list=ls()) # Clears workspace

# # Install/call libraries
# install.packages("renv") # Run if you have cloned repository and don't already have renv installed
# renv::restore() # Run once after cloning repository
# renv::install("package") # Run to install new packages
# renv::snapshot() # Run after installing new packages
# renv::init() # Only run when the repository is first created, don't run on cloning an existing repository

# Data for this repository is at https://drive.google.com/drive/folders/1syX_y2lMbo-ETNBXAo24m2FWUK1q60Ux?usp=sharing

pkgs<-c("sf","tidyverse","httpuv","httr","jsonlite","geojsonsf","furrr","arrow")

missing<-pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly=TRUE)]
if (length(missing)>0) {
  stop("Missing packages: ", paste(missing, collapse=", "),
       "\nRun renv::restore()")
}

invisible(lapply(pkgs, library, character.only=TRUE))
rm(pkgs, missing)

options(scipen = 999) # Prevent scientific notation
`%ni%`<- Negate(`%in%`) # Useful function

# # Download data -----------------------------------------------------------
### Not currently up to date, need to fix if redownloading mobile device data
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


# Load data into workspace ---------------------------------------------------
df<-st_transform(st_read("Data/County_State_National_Municipal_v2.gpkg"), crs = 4326)
#df$id<-seq(1,nrow(df),1) 

# API preparation ---------------------------------------------------------
api_key<-read.csv(file = "APIkey.csv", header = FALSE)
api_key<-api_key$V1
headers<-add_headers(Authorization = api_key, `Content-Type` = "application/json") # Set up the headers including the API key
url<-"https://api.gravyanalytics.com/v1.1/areas/tradeareas" # API URL to query
if (!dir.exists("tData")) { # Create cell data directory if it doesn't exist
  dir.create("tData", recursive = TRUE)
}


# Trade areas api query ---------------------------------------------------
tradeapi<-function(dft,s,e,fpath,fname_prefix = "batch_"){ # Function converts sf object to json, passes to api, gets returned data, and merges back with sf object
  
  dft_name<-deparse(substitute(dft)) # Extract the name of the object passed to the function for naming
  dft$startDateTimeEpochMS<-s # These work as query variables
  dft$endDateTimeEpochMS<-e
  dft$excludeFlags<-25216 # Corresponds to guidance for visitation from venntel
  # dft$startDateTimeEpochMS<-as.numeric(as.POSIXct("2023-06-15 00:00:00.000", tz = "America/New_York")) * 1000
  # dft$endDateTimeEpochMS<-as.numeric(as.POSIXct("2023-08-15 23:59:59.999", tz = "America/New_York")) * 1000
  #dft<-dft %>% select(-PUD_YR_AVG) # Need more than the geometry column to create a feature collection using sf_geojson. Also, there is a limit of 20 features per request (even if it doesn't return results for 20 features).
  dftj<-sf_geojson(dft,atomise = FALSE) # Convert sf object to GeoJSON
  
  dftj<-fromJSON(dftj) # Doesn't seem to like geojson formatting, switching to json
  dftj<-toJSON(dftj, auto_unbox = TRUE, digits = 15)
  g1<-st_geometry(dft)
  g2<-st_geometry(st_read(dftj, quiet = TRUE))
  suppressWarnings(st_crs(g2) <- st_crs(g1))
  
  if (all(lengths(st_equals(g1, g2))>0)) {
    message("Geometry conversion to JSON consistent")
  } else {
    warning("Geometry conversion to JSON inconsistent")
  }
  
  # Export query (asynchronous)
  response<-POST(url, headers, body = dftj, encode = "json", query = list(
    # includeHeaders = FALSE, # Remove headers - potentially useful for batching
    # returnDeviceCountByGeoHash = TRUE, # "If true, the geoHashDeviceCount and geoHashWidthHeights fields are populated per feature" - don't see this. It does return "searchobjectid" in the response psv that corresponds to a given "id" in the json properties
    #decisionLocationTypes = list(c("LATLNG","CBG")),
    decisionLocationTypes = "CBG",
    includeAdditionalCbgInfo = TRUE,
    startTimeOfDay = "09:00:00Z", # Need to adjust for timezone when binning "daily visits" - this adjusts UTC ("Z") to Eastern Time
    endTimeOfDay = "23:59:59Z",
    #includeGeometryWithCbgInfo = TRUE, # Geometry of CBG for GIS
    exportSchema = "EVENING_COMMON_CLUSTERS",
    compressOutputFiles = FALSE, # Compressed outputs?
    responseType = "EXPORT"  # Requesting an export response
  ))
  
  requestID <- response$headers$requestid; if (!is.null(requestID)) cat("Request ID:", requestID, "\n")
  
  if (status_code(response) != 200) {
    if (nrow(dft) > 1) {
      # Batch failed: break into individual calls
      message("Batch failed with status ", status_code(response), ". Retrying rows individually...")
      for (i in seq_len(nrow(dft))) {
        single_row<-dft[i, , drop = FALSE]
        tradeapi(single_row, s, e, fpath = fpath, fname_prefix = paste0("retry_", fname_prefix))
      }
    } else {
      # Individual row failed: log and skip
      message(dft$id, " failed with status ", status_code(response), "and request id ", response$headers$requestid, ". Skipping.")
      tryCatch({ # If failing, check to see if input sf object is valid, if so check to see if derived JSON is valid
        if (!st_is_valid(dft)) {
          message("Geometry is invalid for id ", dft$id, " from original sf object")
        } else {
          tryCatch({
            sf_from_json <- st_read(dftj, quiet = TRUE)
            if (!st_is_valid(sf_from_json)) {
              message("Geometry is invalid for id ", dft$id, " after GeoJSON conversion")
            }
          }, error = function(e) {
            # Silent fail
          })
        }
      })
    }
    return(invisible(NULL))
  }
  
  status_url<-paste0("https://api.gravyanalytics.com/v1.1/requestStatus/", requestID)
  export_complete<-FALSE
  
  # Function that pings the API to see if the export request is done every 1 seconds and returns either of {files ready, still waiting}
  while (!export_complete) {
    Sys.sleep(10)  # Wait for 10 seconds before polling again
    status_response <- GET(status_url, add_headers(Authorization = api_key))
    status_content <- content(status_response, "parsed")
    
    if (status_content$status == "DONE") {
      export_complete <- TRUE
      if (!is.null(status_content$message) && status_content$message == "No files were exported") {
        message(dft$id," API query completed (status ",status_code(response), ") but no files were exported")
        return(invisible(NULL))
      }
      aws_s3_link <- as.character(status_content$presignedUrlsByDataType$tradeAreas)
      base::cat("Your files are ready.\n")} 
    
    else {
      base::cat("Export is still in progress. Status:",
                round(status_content$requestDurationSeconds / 60, 2), "m\n")
    }
  }
  
  # Loading export results into workspace -----------------------------------
  file_name<-sub("\\?.*", "", basename(aws_s3_link)) # Extracting the file name
  
  downloaded_files<-lapply(seq_along(aws_s3_link), function(i) { # Batch downloading all links returned by the API call. Mode = "wb" is important.
    file_path<-file.path(fpath, file_name[i]) # Construct full path
    download.file(aws_s3_link[i], destfile = file_path, mode = "wb") # Download file
    return(file_path) # Return the file path
  })
  downloaded_files<-unlist(downloaded_files)
  
  xp<-do.call(rbind, # Row bind files into a dataframe
              lapply( # Apply over all elements in a list
                file.path(fpath, sub("\\.gz$", "", file_name)), # Elements in a list that are named based on the API call
                function(file) {read.csv(file, sep = "|", header = TRUE)})) # Reading files in
  
  invisible(unlink(downloaded_files)) # Deleting downloaded psv files
  
  if (is.null(xp) || nrow(xp) == 0) { # Handles no data situations where there are no observations in the provided polygon(s)
    warning("No data returned from API for this batch. Skipping...")
    return(NULL)  # Return NULL to avoid stopping execution
  }
  
  #xp<-merge(xp,dft, by.x = "FEATUREID", by.y = "id") # %>% dplyr::select(FEATUREID,DEVICEID,DAY_IN_FEATURE,EARLIEST_OBSERVATION_OF_DAY,LATEST_OBSERVATION_OF_DAY,LATITUDE,LONGITUDE,CENSUS_BLOCK_GROUP_ID,startDateTimeEpochMS,endDateTimeEpochMS,DEVICES_WITH_DECISION_IN_CBG_COUNT,TOTAL_POPULATION)
  xp<-xp %>% dplyr::select(FEATUREID,DEVICEID,DAY_IN_FEATURE,EARLIEST_OBSERVATION_OF_DAY,LATEST_OBSERVATION_OF_DAY,CENSUS_BLOCK_GROUP_ID)
  
  fname<-paste0(fname_prefix, dft_name, "_", paste0(unique(dft$id), collapse = "_"))
  write_parquet(xp, paste0(fpath,fname,".parquet")) # Write to parquet file to save space, versus csv
}

# tradeapi(dft = df[14,],s = as.numeric(as.POSIXct("2023-01-01 00:00:00.000", tz = "America/New_York")) * 1000,
#          e = as.numeric(as.POSIXct("2023-12-31 23:59:59.999", tz = "America/New_York")) * 1000, fpath = "tData/",fname_prefix = "test")

split_df<-split(df, ceiling(seq_len(nrow(df))/20)) # api returns 404 error if even one polygon in the batch has a problem

plan(sequential)
plan(multisession, workers = 2) # Initializing parallel processing, API can only handle two concurrent connections
set.seed(12)
 
system.time(future_imap(
  split_df, # split_df[3:length(split_df)]
  function(data, index) {
    cat("Processing index:", index, "\n")
    tradeapi(
      data,
      s = as.numeric(as.POSIXct("2024-01-01 00:00:00.000", tz = "America/New_York")) * 1000,
      e = as.numeric(as.POSIXct("2024-12-31 23:59:59.999", tz = "America/New_York")) * 1000,
      fpath = "tData/", # Filepath of output
      fname_prefix = 2024
    )
  },
  .options = furrr_options(
    packages = c("R.utils", "httr", "tidyverse", "jsonlite", "sf", "geojsonsf", "lwgeom", "furrr", "arrow"),
    seed = TRUE
  ),
  .progress = TRUE
))
