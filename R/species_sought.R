#' @title species_sought
#' @description Remove trips that sought species other than haddock, and then remove
#' @param marfis.df A dataframe of MARFIS data with TRIP codes.
#' @param y Year
#' @param username Oracle username. Default is the value oracle.username stored in .Rprofile.
#' @param password Oracle password. Default is the value oracle.password stored in .Rprofile.
#' @param dsn Oracle dsn. Default is the value oracle.dsn stored in .Rprofile.
#' @return a list of two data.frames: one with trips that sought haddock to keep; another with trips that sought other species to remove.
#' @examples \dontrun{
#' example <- species_sought(marfis.df = data)
#' }
#' @export

species_sought <- function(marfis.df = NULL, username = oracle.username, password = oracle.password, dsn = oracle.dsn, y = year) {
  # Identify trips that recorded that they sought species other than cod or haddock
  channel <- ROracle::dbConnect(DBI::dbDriver("Oracle"), username = username, password = password, dsn)

  sought <- ROracle::dbGetQuery(channel, paste("select a.CFV, a.VESSEL_NAME, b.TRIP, c.GEARCD_ID, b.LANDING_DATE, d.SPECSCD_ID, d.SET_NO, e.SETDATE
from isdb.isvessels a, isdb.istrips b, isdb.isgears c, isdb.isfishsets d, isdb.issetprofile e
where b.VESS_ID = a.VESS_ID
and e.FISHSET_ID = d.FISHSET_ID
and b.TRIP_ID = d.TRIP_ID
and d.GEAR_ID = c.GEAR_ID
and TO_CHAR(b.LANDING_DATE,'yyyy')=", y, "
and d.NAFAREA_ID like '5Z%'
and e.PNTCD_ID in (1,2)
and d.SETCD_ID=1
group by a.CFV, a.VESSEL_NAME, b.TRIP, c.GEARCD_ID, b.LANDING_DATE, d.SPECSCD_ID, d.SET_NO, e.SETDATE", sep = " ")) |>
    dplyr::rename("VR_NUMBER_FISHING" = "CFV") |>
    dplyr::mutate(VR_NUMBER_FISHING = as.numeric(VR_NUMBER_FISHING)) |>
    dplyr::filter(!SPECSCD_ID %in% c(10, 11, 7001)) |>
    dplyr::select(VR_NUMBER_FISHING, TRIP, SPECSCD_ID) |>
    dplyr::distinct(VR_NUMBER_FISHING, TRIP, SPECSCD_ID, .keep_all = TRUE) |>
    janitor::clean_names() |>
    dplyr::filter(trip %in% marfis.df$trip)

  noncodhad <- marfis.df |>
    dplyr::filter(trip %in% sought$trip) |>
    dplyr::mutate(comment = "Recorded species sought not cod (10), haddock (11), or combined groundfish (7001).")

  # Identify trips where cod:had ratio for all sets in the trip are greater than 0.8 (i.e. sought cod for the whole trip)

  test1 <- marfis.df |>
    dplyr::group_by(year, q, vr_number_fishing, vessel_name, tc, lc, trip_id, landed_date, log_efrt_std_info_id, date_fished, gear_code, area, lon, lat, licence_id, trip, sector, zone) |>
    # dplyr::summarise(cod=sum(cod),had=sum(had),pol=sum(pol)) |>
    tidyr::replace_na(list(cod = 0, had = 0, pol = 0)) |>
    dplyr::mutate(
      test = (cod / had) < 0.8,
      test = ifelse(test == TRUE | is.na(test), "not cod directed", "cod directed"),
      test2 = ifelse(test == "not cod directed", 0, 1)
    )

  #Trips with all sets cod directed should have a mean of 1
  trips_cod_directed <- test1 |>
    dplyr::group_by(trip_id) |>
    dplyr::summarise(mean_cod=mean(test2)) |>
    dplyr::filter(mean_cod==1)

  cod_directed1 <- marfis.df |>
    dplyr::filter(trip_id%in%trips_cod_directed$trip_id) |>
    dplyr::mutate(comment = "All sets on the trip were cod directed (>0.8).")

  # Identify trips where sets at the end of the trip had a cod:had ratio greater than 0.8 (i.e. sought cod at the end of the trip). Sets with ratio >0.8 are cod directed on these trips.

  #Trips with some sets cod directed should have a mean between 0 and 1
  some_sets_cod_directed <- test1 |>
    dplyr::group_by(trip_id) |>
    dplyr::summarise(mean_cod=mean(test2)) |>
    dplyr::filter(mean_cod>0 & mean_cod<1)

  last_set_cod_directed <- test1 |>
    dplyr::filter(trip_id%in%some_sets_cod_directed$trip_id) |>  # select only trips with both cod directed and not directed sets
    dplyr::group_by(trip_id) |>
    dplyr::slice_max(log_efrt_std_info_id) |> # filter for last set per trip
    dplyr::filter(test == "cod directed") # filter for cod directed sets

  cod_directed2 <- marfis.df |>
    dplyr::filter(log_efrt_std_info_id%in%last_set_cod_directed$log_efrt_std_info_id) |>
    dplyr::mutate(comment = "Last set(s) on trip were cod directed (>0.8).")

  #Pollock directed

  # Identify sets where pollock landings are greater than combined cod and haddock landings (i.e. sought pollock)

  pollock_directed <- marfis.df |>
    tidyr::replace_na(list(cod = 0, had = 0, pol = 0)) |>
    dplyr::mutate(pol_ratio = pol > (cod + had)) |>
    dplyr::filter(pol_ratio == TRUE) |>
    dplyr::mutate(comment = "Set was pollock directed (pol>(cod+had)).") |>
    dplyr::ungroup()

  # Create two dataframes, one with removed records and one with kept records

  directed <- dplyr::bind_rows(tibble::lst(noncodhad, cod_directed1, cod_directed2, pollock_directed), .id = "id") |>
    dplyr::select(1:23) |>
    dplyr::distinct_at(dplyr::vars(-1), .keep_all = TRUE)

  marfis <- marfis.df |> dplyr::filter(!log_efrt_std_info_id %in% directed$log_efrt_std_info_id)

  df.list <- list(marfis, directed)

  SPECSCD_ID <- TRIP <- VR_NUMBER_FISHING <- area <- cod <- date_fished <- gear_code <- had <- landed_date <- lat <- lc <- licence_id <- log_efrt_std_info_id <- lon <- oracle.dsn <- oracle.password <- oracle.username <- pol <- pol_ratio <- sector <- tc <- test <- trip <- trip_id <- vessel_name <- vr_number_fishing <- year <- zone <- NULL

  print(df.list)
}
