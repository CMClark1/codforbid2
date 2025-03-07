#' @title isdbpull
#' @description Pull observed trips for the groundfish fleet from the ISDB database.
#' @param year The year required.
#' @param username Oracle username. Default is the value oracle.username stored in .Rprofile.
#' @param password Oracle password. Default is the value oracle.password stored in .Rprofile.
#' @param dsn Oracle dsn. Default is the value oracle.dsn stored in .Rprofile.
#' @return a data.frame of observed trips
#' @examples \dontrun{
#' example1 <- isdbpull(year = 2024)
#' }
#' @export

isdbpull <- function(year = NULL, username = oracle.username, password = oracle.password, dsn = oracle.dsn) {
  channel <- ROracle::dbConnect(DBI::dbDriver("Oracle"), username = username, password = password, dsn)

  isdb <- ROracle::dbGetQuery(channel, paste("select CAST(a.CFV AS INT) VR_NUMBER_FISHING, a.VESSEL_NAME, b.TRIP, b.TRIPCD_ID, b.MARFIS_CONF_NUMBER, SUBSTR(b.landing_date, 1, 10) AS LANDED_DATE, Min(c.SETDATE) as MINDATE, Max(c.SETDATE) as MAXDATE, d.GEARCD_ID, e.SETCD_ID, Min(c.LATITUDE) AS LAT, Min(c.LONGITUDE) AS LON from isdb.isvessels a, isdb.istrips b, isdb.issetprofile c, isdb.isgears d, isdb.isfishsets e where TO_CHAR(c.SETDATE,'yyyy')=", year, "and e.NAFAREA_ID like '5Z%' and b.VESS_ID = a.VESS_ID and c.FISHSET_ID = e.FISHSET_ID and b.TRIP_ID = e.TRIP_ID and e.GEAR_ID = d.GEAR_ID group by a.CFV, a.VESSEL_NAME, b.TRIP, b.TRIPCD_ID, b.MARFIS_CONF_NUMBER, b.LANDING_DATE, d.GEARCD_ID, e.SETCD_ID having d.GEARCD_ID not in (71, 62) order by b.TRIP", sep = " "))

  oracle.username <- oracle.password <- oracle.dsn <- NULL

  print(isdb)
}
