#' @title assign_zone
#' @description Assign discard zones to data.
#' @param marfis.df A dataframe of MARFIS data with latitude and longitude columns.
#' @param isdb.df A dataframe of ISDB data for the relevant year. Default is "isdb" as supplied by the function isdbpull.R
#' @param y Year
#' @param lat.field Latitude field, in decimal degrees. Default is "lat".
#' @param lon.field Longitude field, in decimal degrees. Default in "lon".
#' @param dir Directory with locally stored data, which can be generated with the function localcopies.R. Default is "C:/LocalDataDump/Fleets"
#' @param extract Choose whether or not to force extract all local copies of required MARFIS and ISDB tables (T/F). Default is "F", which extracts only missing tables and does not update existing ones.
#' @return a list of two dataframes: one with an additional column for Zone, with all Zones assigned; another with an additional column for Zone, with no Zones assigned.
#' @examples \dontrun{
#' example1 <- assign_zone(marfis)
#' }
#' @export

assign_zone <- function(marfis.df = NULL, isdb.df = NULL, y = year, lat.field = "lat", lon.field = "lon", dir = "C:/LocalDataDump/Fleets", extract = "F") {
  # This confirms that the necessary files are there and downloads the files that aren't
  if (extract == "F") {
    Mar.fleets::enable_local(data.dir = "C:/LocalDataDump/Fleets", oracle.username = oracle.username, oracle.password = oracle.password, oracle.dsn = oracle.dsn, usepkg = "roracle")
  } else if (extract == "T") {
    Mar.fleets::enable_local(data.dir = "C:/LocalDataDump/Fleets", usepkg = "roracle", force.extract = T, oracle.username = oracle.username, oracle.password = oracle.password, oracle.dsn = oracle.dsn)
  } else {
    print("Assign T or F to 'extract' parameter")
    stop()
  }

  # Assign zone using shapefile of Georges Bank discard zones
  marfis.df1 <- Mar.utils::identify_area(df = marfis.df |> as.data.frame(), lat.field = lat.field, lon.field = lon.field, agg.poly.shp = Mar.data::GeorgesBankDiscardZones_sf, agg.poly.field = "Id") |>
    dplyr::rename(zone = Id)

  ### Correct zone if missing or 0-------------------

  ## Create two dataframes, one that is correct and one that needs corrections
  zone_correct <- marfis.df1 |>
    dplyr::filter(zone >= 1)
  zone_corrections <- marfis.df1 |>
    dplyr::filter(!zone >= 1)

  ## Correct observed trips with missing zones
  observed <- zone_corrections |>
    dplyr::rename(trip=trip.marfis) |>
    dplyr::filter(!is.na(trip)) |>
    dplyr::left_join(isdb.df |>
      dplyr::select(trip, lat, lon), by = "trip") |>
    dplyr::mutate(lon.y = lon.y * -1)

  ## Try the lat lons from the ISDB instead of MARFIS

  if (nrow(observed) > 1) {
    observed <- Mar.utils::identify_area(df = observed |> as.data.frame(), lat.field = "lat.y", lon.field = "lon.y", agg.poly.shp = Mar.data::GeorgesBankDiscardZones_sf, agg.poly.field = "Id") |>
      select(-zone) |>
      dplyr::rename(zone = Id, lat = lat.x, lon = lon.x)
  } else {
    observed <- observed
  }

  ## Correct unobserved trips with missing zones or observed trips that are still outside known areas

  unobserved <- zone_corrections |>
    dplyr::filter(!log_efrt_std_info_id %in% observed$log_efrt_std_info_id) |>
    dplyr::mutate(landed_date = lubridate::as_date(landed_date), date_fished = lubridate::dmy(date_fished))

  observed_stillmissing <- observed |> dplyr::filter(!zone>0)

  if ("881" %in% unobserved$sector | "885" %in% unobserved$sector | "311" %in% unobserved$sector) {
    requireNamespace("Mar.fleets")
    mobile_5Z <- Mar.fleets::fishin_CHPs(type = "MOBILE", stock = "5Z", dateStart = "2024-01-01", dateEnd = "2024-12-31", useLocal = T, data.dir = "C:/LocalDataDump/Fleets", socks = T, usepkg = "roracle")
    mobileVMSRaw <- Mar.utils::VMS_from_MARFIS(df = mobile_5Z$marf$MARF_TRIPS, VR_field = "VR_NUMBER_FISHING", usepkg = "roracle", make_segments = F, LANDED_field = "T_DATE2")
    mobileVMSRaw <- mobileVMSRaw[["marf_VMS"]]
  }

  if ("90" %in% unobserved$sector | "89" %in% unobserved$sector | "884" %in% unobserved$sector | "886" %in% unobserved$sector) {
    requireNamespace("Mar.fleets")
    fixed_5Z <- Mar.fleets::fishin_CHPs(type = "FIXED", stock = "5Z", dateStart = "2024-01-01", dateEnd = "2024-12-31", useLocal = T, data.dir = "C:/LocalDataDump/Fleets", socks = T, usepkg = "roracle")
    fixedVMSRaw <- Mar.utils::VMS_from_MARFIS(df = fixed_5Z$marf$MARF_TRIPS, VR_field = "VR_NUMBER_FISHING", usepkg = "roracle", make_segments = F, LANDED_field = "T_DATE2") # Depending on the license conditions, there may only be VMS data for unobserved trips of mobile gear
    fixedVMSRaw <- fixedVMSRaw[["marf_VMS"]]
  }

  if (exists("mobileVMSRaw") == TRUE & exists("fixedVMSRaw") == TRUE) {
    chpVMS <- rbind(mobileVMSRaw, fixedVMSRaw)
  } else if (exists("mobileVMSRaw") == TRUE & exists("fixedVMSRaw") == FALSE) {
    chpVMS <- mobileVMSRaw
  } else if (exists("mobileVMSRaw") == FALSE & exists("fixedVMSRaw") == TRUE) {
    chpVMS <- fixedVMSRaw
  }


  #For unobserved data

  # Filter existing VMS data using VR_NUMBER and LANDED_DATE from unobserved trips missing zone numbers
  chpVMS_filter <- chpVMS |>
    janitor::clean_names() |>
    dplyr::mutate(date = as.Date(position_utc_date, format = "%Y/%m/%d")) |>
    dplyr::filter(vr_number %in% unobserved$vr_number_fishing & date %in% unobserved$date_fished)

  # Assign zones to VMS data and remove points outside of zones (assumed to be transiting)
  chpVMS_filter <- Mar.utils::identify_area(
    df = chpVMS_filter, lat.field = "latitude", lon.field = "longitude",
    agg.poly.shp = Mar.data::GeorgesBankDiscardZones_sf, agg.poly.field = "Id"
  )
  chpVMS_filter <- subset(chpVMS_filter, Id > 0)
  chpVMS_filter <- chpVMS_filter |>
    dplyr::group_by(vr_number, date) |>
    dplyr::summarize(meanlat = mean(latitude, na.rm = TRUE), meanlon = mean(longitude, na.rm = TRUE)) # Available VMS data for unobserved trips

  if (nrow(unobserved) > 0) {
    unobserved <- dplyr::left_join(unobserved, as.data.frame(chpVMS_filter), by = (c("vr_number_fishing" = "vr_number", "date_fished" = "date")))
    unobserved <- Mar.utils::identify_area(
      df = unobserved, lat.field = "meanlat", lon.field = "meanlon",
      agg.poly.shp = Mar.data::GeorgesBankDiscardZones_sf, agg.poly.field = "Id"
    ) |>
      dplyr::mutate(zone = Id, lon = meanlat, lat = meanlon) |>
      dplyr::select(-meanlat, -meanlon)
  } else {
    unobserved <- unobserved
  }

  #For still missing observed data


  # Filter existing VMS data using VR_NUMBER and LANDED_DATE from observed trips still missing zone numbers
  chpVMS_filter <- chpVMS |>
    janitor::clean_names() |>
    dplyr::mutate(date = as.Date(position_utc_date, format = "%Y/%m/%d")) |>
    dplyr::filter(vr_number %in% observed_stillmissing$vr_number_fishing & date %in% lubridate::dmy(observed_stillmissing$date_fished))

  # Assign zones to VMS data and remove points outside of zones (assumed to be transiting)
  chpVMS_filter <- Mar.utils::identify_area(
    df = chpVMS_filter, lat.field = "latitude", lon.field = "longitude",
    agg.poly.shp = Mar.data::GeorgesBankDiscardZones_sf, agg.poly.field = "Id"
  )
  chpVMS_filter <- subset(chpVMS_filter, Id > 0)
  chpVMS_filter <- chpVMS_filter |>
    dplyr::group_by(vr_number, date) |>
    dplyr::summarize(meanlat = mean(latitude, na.rm = TRUE), meanlon = mean(longitude, na.rm = TRUE)) # Available VMS data

  if (nrow(observed_stillmissing) > 0) {
    observed_stillmissing <- dplyr::left_join(observed_stillmissing |> mutate(date_fished=lubridate::dmy(date_fished)), as.data.frame(chpVMS_filter), by = (c("vr_number_fishing" = "vr_number", "date_fished" = "date")))
    observed_stillmissing <- Mar.utils::identify_area(
      df = observed_stillmissing, lat.field = "meanlat", lon.field = "meanlon",
      agg.poly.shp = Mar.data::GeorgesBankDiscardZones_sf, agg.poly.field = "Id"
    ) |>
      dplyr::mutate(zone = Id, lon = meanlat, lat = meanlon) |>
      dplyr::select(-meanlat, -meanlon)
  } else {
    observed_stillmissing <- observed_stillmissing
  }

  # Bind observed and unobserved dataframes together and remove trips missing Zone

  temp <- dplyr::bind_rows(zone_correct |> dplyr::mutate(date_fished=lubridate::dmy(date_fished)) |> dplyr::rename(trip=trip.marfis),
                           observed |> dplyr::mutate(date_fished=lubridate::dmy(date_fished)) |> dplyr::select(c(1:22), zone) |> dplyr::filter(zone>0),
                   observed_stillmissing |> dplyr::mutate(zone=as.character(zone)) |> dplyr::select(c(1:22, zone)),
                   unobserved |> dplyr::mutate(landed_date=as.character(landed_date),
                                               zone = as.character(zone)) |>
                     dplyr::rename(trip=trip.marfis) |> dplyr::select(c(1:22, zone)))

  marfis <- temp |> dplyr::filter(zone >= 1)
  noZone <- temp |>
    dplyr::filter(!zone >= 1) |>
    dplyr::mutate(comment = "no zone")

  df.list <- list(marfis, noZone)

  Id <- date_fished <- landed_date <- lat <- lat.x <- latitude <- log_efrt_std_info_id <- lon <- lon.x <- lon.y <- longitude <- meanlat <- meanlon <- oracle.dsn <- oracle.password <- oracle.username <- position_utc_date <- setNames <- trip <- vr_number <- year <- zone <- trip.marfis <- NULL

  print(df.list)
}
