#' @title egb_qaqc
#' @description A function to QAQC MARFIS data using the ISDB database.
#' @param marfis.df The name of the MARFIS dataframe previously pulled using the marfispull function. Default is "marfis"
#' @param isdb.df The name of the ISDB dataframe previously pulled using the isdbpull function. Default is "isdb"
#' @param directory Directory where output files will be saved. Default is the working directory.
#' @param savedoutput Specify whether or not you could like the output files saved in the working directory for review or action (see below for the descriptions of these files). Default is "T" (true).
#' @returns Returns a data frame of QAQC's data ready for further cleaning, and five Excel or .csv files for review or action: Discards_ISDB_check to send to the observer companies, Discards_MARFIS_missing to send to CDD, an observer coverage summary (Discards_ObsCoverageSummary), removed records (Discards_RemovedRecords), aggregated data (Discards_MARFISXtab_Aggregated) and data for grouping (Discards_DataforGrouping).
#' @examples \dontrun{
#' example1 <- egb_qaqc(marfis.df=marfis, isdb.df=isdb)
#' }
#' @export

egb_qaqc <- function(year = as.numeric(marfis.df=marfis, isdb.df=isdb, savedoutput = "T", directory = getwd())) {

  #Create a dataframes of records that match and do not match based on only VR_NUMBER_FISHING and LANDED_DATE
  temp <- marfis.df |>
    janitor::clean_names() |>
    dplyr::rename(trip.marfis=trip) |>
    dplyr::left_join(isdb.df |>
                       janitor::clean_names() |>
                       dplyr::select(!c(lat, lon, vessel_name)) |>
                       dplyr::distinct(vr_number_fishing, landed_date, .keep_all=TRUE) |>
                       dplyr::rename(trip.isdb=trip))
  join_match <- temp |> dplyr::filter(!is.na(trip.isdb)) #match
  join_nomatch <- temp |> dplyr::filter(is.na(trip.isdb)) #no match

  #For MARFIS data that did match ISDB based on VRN and landed date, match trip numbers in joined MARFIS and ISDB records, and replace missing/mistake MARFIS trip numbers with ISDB trip numbers.

  join_good1 <- join_match |>
    dplyr::filter(trip.marfis == trip.isdb) |>
    dplyr::full_join(join_match |> dplyr::filter(is.na(trip.marfis) & is.na(trip.isdb))) #MARFIS and ISDB trip numbers match, alphanumeric or NA=NA

  join_good2 <- join_match |>
    dplyr::filter(trip.marfis != trip.isdb) |>
    dplyr::mutate(trip.marfis=dplyr::case_when(trip.marfis=="" ~ trip.isdb,
                                               trip.marfis !="" ~ trip.marfis)) #If MARFIS trip number is missing, replace it with the ISDB trip number

  #For MARFIS data that did not match ISDB based on VRN and landed date, match MARFIS data to ISDB data +/- 2 days. Do not replace MARFIS dates with ISDB dates.

  join2 <- fuzzyjoin::fuzzy_left_join(
    join_nomatch |>  dplyr::select(c(1:21)) |>
      dplyr::mutate(landed_date=lubridate::dmy(landed_date)), isdb.df |>  janitor::clean_names() |> dplyr::mutate(start=lubridate::dmy(landed_date)-2,end=lubridate::dmy(landed_date)+2) |> dplyr::rename(trip.isdb=trip),
    by = c(
      "vr_number_fishing" = "vr_number_fishing",
      "landed_date" = "start",
      "landed_date" = "end"
    ),
    match_fun = list(`==`, `>=`, `<=`)
  )

  join2_match <- join2 |>  dplyr::filter(!is.na(tripcd_id)) #Records that match
  join2_nomatch <- join2 |>  dplyr::filter(is.na(tripcd_id)) #Records that do not match

  #For matched dataframe, match trip numbers in joined MARFIS and ISDB records, and replace missing/mistake MARFIS trip numbers with ISDB trip numbers
  join_good3 <- join2_match |> dplyr::filter(trip.marfis == trip.isdb)

  join_good4 <- join2_match |>
    dplyr::filter(trip.marfis != trip.isdb) |>
    dplyr::mutate(trip.marfis=dplyr::case_when(trip.marfis=="" ~ trip.isdb,
                                               trip.marfis!="" ~ trip.marfis))


  #MARFIS records with no match to ISDB based on VRN and date, fall into two categories: observer records in MARFIS that have not yet been entered into the ISDB (will have a TRIP#) and non-observed trips (no TRIP#)

  join_good5 <- join2_nomatch |>  dplyr::filter(is.na(trip.marfis)) #These are the unobserved trips
  join_good6 <- join2_nomatch |>  dplyr::filter(!is.na(trip.marfis)) #These are the observed trips not entered in the ISDB (aka ISDB errors)

  # Select MARFIS columns and bind the QAQC'd parts of the MARFIS dataframe back together

  colnames(join_good6) <- colnames(join_good5) <- colnames(join_good4) <- colnames(join_good3) <- colnames(join_good2) <- colnames(join_good1)

  marfis_qaqc <- dplyr::bind_rows(join_good1 |> dplyr::select(1:21),
                                  join_good2 |> dplyr::select(1:21),
                                  join_good3 |> dplyr::select(1:21) |> dplyr::mutate(landed_date=as.character(landed_date)),
                                  join_good4 |> dplyr::select(1:21) |> dplyr::mutate(landed_date=as.character(landed_date)),
                                  join_good5 |> dplyr::select(1:21) |> dplyr::mutate(landed_date=as.character(landed_date)),
                                  join_good6 |> dplyr::select(1:21) |> dplyr::mutate(landed_date=as.character(landed_date)))

  #MARFIS and ISDB errors to report to CDD and Observer Program

  marfis_error1 <- join_match |>
    dplyr::filter(trip.marfis != trip.isdb) |>
    dplyr::mutate(COMMENT=dplyr::case_when(trip.marfis=="" ~ "MARFIS trip number missing", trip.marfis!="" ~ "MARFIS trip number incorrect as compared to ISDB")) |>
    dplyr::select(c(1:18), trip.isdb) |>
    dplyr::mutate(landed_date=lubridate::dmy(landed_date))

  marfis_error2 <- join2_match |>
    dplyr::filter(trip.marfis != trip.isdb) |>
    dplyr::mutate(comment=dplyr::case_when(trip.marfis=="" ~ "MARFIS trip missing",trip.marfis!="" ~ "MARFIS trip incorrect as compared to ISDB")) |>
    dplyr::select(c(1:18), trip.isdb)

  marfis_errors <- rbind(marfis_error1, setNames(marfis_error2, names(marfis_error1))) #Observed trips entered in MARFIS that do not match the ISDB.

  marfis_errors_missing <- marfis_errors |>
    dplyr::select(vr_number_fishing, trip_id, landed_date, trip.marfis, trip.isdb) |>
    dplyr::filter(trip.marfis == "") |>
    dplyr::distinct(vr_number_fishing, trip_id, landed_date, trip.marfis, trip.isdb)

  marfis_errors_incorrect <- marfis_errors |>
    dplyr::select(vr_number_fishing, trip_id, landed_date, trip.marfis, trip.isdb) |>
    dplyr::filter(trip.marfis!= "") |>
    dplyr::distinct()

  isdb_check <- isdb.df |>
    janitor::clean_names() |>
    dplyr::filter(trip %in% marfis_errors_incorrect$trip.isdb) |>
    dplyr::select(vr_number_fishing, trip, landed_date) |>
    dplyr::ungroup() |>
    dplyr::distinct()

  ##ISDB errors
  isdb_errors <- join_good6 |>
    dplyr::select(1:18, trip.isdb) |>
    dplyr::mutate(comment = "trip entered in marfis but not isdb") |>
    dplyr::ungroup() |>
    dplyr::distinct(vr_number_fishing, trip_id, landed_date, trip.marfis)#Observed trip number entered in MARFIS not entered equal to that in the ISDB - this may not be an ISDB error, but these should also be checked.


  if (savedoutput == "T") {
    write.csv(marfis_errors_missing, here("data/Discards_MARFIS_missing.csv")) #file sent to cdd to correct missing marfis trip numbers
    write.csv(marfis_errors_incorrect, here("data/Discards_MARFIS_incorrect.csv")) #File sent to CDD to correct MARFIS trip numbers that don't match ISDB, after the observer program confirms theirs are correct (in isdb_check)
    write.csv(isdb_check, here("data/Discards_ISDB_check.csv")) #File sent to observer program to check if VRN, date landed, and trip number are correct. If they are correct in the ISDB, then they are wrong in MARFIS.
    write.csv(isdb_errors, here("data/Discards_ISDB_missing.csv")) #File sent to observer program as outstanding data not yet entered
  }

  return(marfis_qaqc)

}
