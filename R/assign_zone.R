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
#' @examples
#' example1 <- assign_zone(marfis)
#' @export

assign_zone <- function(marfis.df = NULL, isdb.df = NULL, y = year, lat.field = "lat", lon.field = "lon", dir = "C:/LocalDataDump/Fleets", extract = "F") {

  #This confirms that the necessary files are there and downloads the files that aren't
  if(extract=="F"){
    Mar.fleets::enable_local(data.dir = "C:/LocalDataDump/Fleets", oracle.username=oracle.username, oracle.password=oracle.password, oracle.dsn=oracle.dsn, usepkg="roracle")
  }else if(extract=="T"){
    Mar.fleets::enable_local(data.dir = "C:/LocalDataDump/Fleets", usepkg = 'roracle', force.extract = T, oracle.username = oracle.username, oracle.password = oracle.password, oracle.dsn = oracle.dsn)
  } else {
    print("Assign T or F to 'extract' parameter")
    stop()
  }

  #Assign zone using shapefile of Georges Bank discard zones
  marfis.df1 <- Mar.utils::identify_area(df = marfis.df |> as.data.frame(), lat.field = lat.field, lon.field = lon.field, agg.poly.shp = Mar.data::GeorgesBankDiscardZones_sf, agg.poly.field = "Id") |>
    dplyr::rename(zone=Id)

### Correct zone if missing or 0-------------------

  ## Create two dataframes, one that is correct and one that needs corrections
  zone_correct <- marfis.df1 |>
    dplyr::filter(zone >= 1)
  zone_corrections <- marfis.df1 |>
    dplyr::filter(!zone >= 1)

  ## Correct observed trips with missing zones
  observed <- zone_corrections |>
    dplyr::filter(!is.na(trip)) |>
    dplyr::left_join(isdb.df |>
    dplyr::select(trip,lat,lon), by = "trip") |>
    dplyr::mutate(lon.y=lon.y*-1)

  ##Try the lat lons from the ISDB instead of MARFIS

  if (nrow(observed) > 1) {

    observed <- Mar.utils::identify_area(df = observed |>  as.data.frame(), lat.field = "lat.y", lon.field = "lon.y", agg.poly.shp = Mar.data::GeorgesBankDiscardZones_sf, agg.poly.field = "Id") |>
      select(-zone) |>
      dplyr::rename(zone=Id,lat=lat.x,lon=lon.x)

  } else {

    observed <- observed

  }

  ## Correct unobserved trips with missing zones

  unobserved <- zone_corrections |>
    dplyr::filter(!log_efrt_std_info_id%in%observed$log_efrt_std_info_id) |>
    dplyr::mutate(landed_date = lubridate::as_date(landed_date), date_fished = lubridate::dmy(date_fished))

  if('881'%in%unobserved$sector | '885'%in%unobserved$sector | '311'%in%unobserved$sector){

    library(Mar.fleets)
    mobile_5Z <- Mar.fleets::fishin_CHPs(type="MOBILE", stock = "5Z", dateStart = "2024-01-01", dateEnd= "2024-12-31", useLocal = T, data.dir='C:/LocalDataDump/Fleets', socks=T, usepkg = "roracle")
    mobileVMSRaw <- Mar.utils::VMS_from_MARFIS(df=mobile_5Z$marf$MARF_TRIPS, VR_field = "VR_NUMBER_FISHING", usepkg = "roracle", make_segments = F, LANDED_field = "T_DATE2" )
    mobileVMSRaw <- mobileVMSRaw[["marf_VMS"]]

  }

  if ('90'%in%unobserved$sector | '89'%in%unobserved$sector | '884'%in%unobserved$sector | '886'%in%unobserved$sector){

    library(Mar.fleets)
    fixed_5Z <- Mar.fleets::fishin_CHPs(type="FIXED", stock = "5Z", dateStart = "2024-01-01", dateEnd= "2024-12-31", useLocal = T, data.dir='C:/LocalDataDump/Fleets', socks=T, usepkg = "roracle")
    fixedVMSRaw <- Mar.utils::VMS_from_MARFIS(df=fixed_5Z$marf$MARF_TRIPS, VR_field = "VR_NUMBER_FISHING", usepkg = "roracle", make_segments = F, LANDED_field = "T_DATE2" ) #Depending on the license conditions, there may only be VMS data for unobserved trips of mobile gear
    fixedVMSRaw <- fixedVMSRaw[["marf_VMS"]]

  }

  if (exists('mobileVMSRaw')==TRUE & exists('fixedVMSRaw')==TRUE){

    chpVMS <- rbind(mobileVMSRaw,fixedVMSRaw)

  } else if (exists('mobileVMSRaw')==TRUE & exists('fixedVMSRaw')==FALSE){

    chpVMS <- mobileVMSRaw

  } else if (exists('mobileVMSRaw')==FALSE & exists('fixedVMSRaw')==TRUE){

    chpVMS <- fixedVMSRaw

  }


  # Filter existing VMS data using VR_NUMBER and LANDED_DATE from unobserved trips missing zone numbers
  chpVMS_filter <- chpVMS |>
    janitor::clean_names() |>
    dplyr::mutate(date = as.Date(position_utc_date, format = "%Y/%m/%d")) |>
    dplyr::filter(vr_number %in% unobserved$vr_number_fishing & date %in% unobserved$date_fished)

  # Assign zones to VMS data and remove points outside of zones (assumed to be transiting)
  chpVMS_filter <- Mar.utils::identify_area(
    df = chpVMS_filter, lat.field = "latitude", lon.field = "longitude",
    agg.poly.shp = Mar.data::GeorgesBankDiscardZones_sf, agg.poly.field = "Id")
  chpVMS_filter <- subset(chpVMS_filter,Id>0)
  chpVMS_filter <- chpVMS_filter |>
    dplyr::group_by(vr_number,date) |>
    dplyr::summarize(meanlat = mean(latitude, na.rm = TRUE), meanlon = mean(longitude, na.rm = TRUE)) # Available VMS data for unobserved trips

  if (nrow(unobserved) > 1) {

    unobserved <- dplyr::left_join(unobserved, as.data.frame(chpVMS_filter), by = (c("vr_number_fishing" = "vr_number", "date_fished" = "date")))
    unobserved <- Mar.utils::identify_area(
      df = unobserved, lat.field = "meanlat", lon.field = "meanlon",
      agg.poly.shp = Mar.data::GeorgesBankDiscardZones_sf, agg.poly.field = "Id") |>
      dplyr::mutate(zone=Id, lon=meanlat, lat=meanlon) |>
      dplyr::select(-meanlat, -meanlon)

  } else {

    unobserved <- unobserved
  }

  # Bind observed and unobserved dataframes together and remove trips missing Zone

  temp <- rbind(zone_correct, setNames(observed |> dplyr::select(c(1:23)), names(zone_correct)), setNames(unobserved |> dplyr::select(1:23), names(zone_correct)))

  marfis <- temp |>  dplyr::filter(zone >= 1)
  noZone <- temp |>
    dplyr::filter(!zone >= 1) |>
    dplyr::mutate(comment = "no zone")

  df.list <- list(marfis, noZone)

  print(df.list)

}
