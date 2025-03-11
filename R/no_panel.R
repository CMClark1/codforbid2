#'@title no_panel
#'@description Remove trips that did not use a separator panel
#'@param marfis.df A dataframe of MARFIS data with TRIP codes.
#'@param y Year
#'@param username Oracle username. Default is the value oracle.username stored in .Rprofile.
#'@param password Oracle password. Default is the value oracle.password stored in .Rprofile.
#'@param dsn Oracle dsn. Default is the value oracle.dsn stored in .Rprofile.
#'@return a list of two dataframes: one with trips that used a separator panel to keep; another with trips that did not use a separator panel to remove
#'@examples \dontrun{
#'example1 <- nopanel(marfis.df=marfis)
#'}
#'@export


nopanel <- function(marfis.df=NULL, username=oracle.username,password=oracle.password,dsn=oracle.dsn, y=year) {

  channel <- ROracle::dbConnect(DBI::dbDriver("Oracle"), username=username, password=password, dsn)

  separator <- ROracle::dbGetQuery(channel, paste("select a.CFV, a.VESSEL_NAME, b.TRIP, b.LANDING_DATE, c.GEARCD_ID, d.GEARFCD_ID, Count(e.SET_NO)
from isdb.isvessels a, isdb.istrips b, isdb.isgears c, isdb.isgearfeatures d, isdb.isfishsets e, isdb.issetprofile f
where b.VESS_ID = a.VESS_ID
and f.FISHSET_ID = e.FISHSET_ID
and b.TRIP_ID = e.TRIP_ID
and e.GEAR_ID = c.GEAR_ID
and c.GEAR_ID = d.GEAR_ID
and TO_CHAR(b.LANDING_DATE,'yyyy')=",y,"
and e.NAFAREA_ID like '5Z%'
and f.PNTCD_ID=2
and c.GEARCD_ID=12
and d.GEARFCD_ID in (89, 90)
group by a.CFV, a.VESSEL_NAME, b.TRIP, b.LANDING_DATE, c.GEARCD_ID, d.GEARFCD_ID
",sep=" ")) |>
    janitor::clean_names()

  marfis <- dplyr::left_join(marfis.df, separator |>
                               dplyr::select(trip, gearfcd_id)) |>
    dplyr::filter(is.na(gearfcd_id) | gearfcd_id != 89)

  nopaneltrips <- dplyr::left_join(marfis.df, separator |>
                                     dplyr::select(trip, gearfcd_id)) |>
    dplyr::filter(gearfcd_id == 89) |>
    dplyr::mutate(comment="did not use separator panel")

  gearfcd_id <- oracle.dsn <- oracle.password <- oracle.username <- trip <- year <- NULL

  df.list <- list(marfis,nopaneltrips)

  print(df.list)

}
