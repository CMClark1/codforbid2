#' @title marfispull
#' @description Pull landings data for the groundfish fleet from DFO's Maritime Fishery Information System (MARFIS) database.
#' @param year The year required.
#' @param username Oracle username. Default is the value oracle.username stored in .Rprofile.
#' @param password Oracle password. Default is the value oracle.password stored in .Rprofile.
#' @param dsn Oracle dsn. Default is the value oracle.dsn stored in .Rprofile.
#' @return a data.frame of landings
#' @examples \dontrun{
#' example <- marfispull(year = 2024)
#' }
#' @export


marfispull <- function(year = NULL, username = oracle.username, password = oracle.password, dsn = oracle.dsn) {
  channel <- ROracle::dbConnect(DBI::dbDriver("Oracle"), username = username, password = password, dsn)
  marfis1 <- ROracle::dbGetQuery(channel, paste("

select EXTRACT(YEAR FROM a.LANDED_DATE) YEAR,
  TO_CHAR(a.date_fished,'Q')  Q,
  a.vr_number_fishing,
  b.vessel_name,
  COALESCE(CASE WHEN b.GROSS_TONNAGE>=2000 THEN 7 ELSE NULL END,CASE WHEN b.GROSS_TONNAGE>=1000 THEN 6 ELSE NULL END,CASE WHEN b.GROSS_TONNAGE>=500 THEN 5 ELSE NULL END,CASE WHEN b.GROSS_TONNAGE>=150 THEN 4 ELSE NULL END,CASE WHEN b.GROSS_TONNAGE>=50 THEN 3 ELSE NULL END,CASE WHEN b.GROSS_TONNAGE>=25 THEN 2 ELSE NULL END,CASE WHEN b.GROSS_TONNAGE>=1 THEN 1 ELSE 0 END) AS TC,
  COALESCE(CASE WHEN b.LOA>=125 THEN 6 ELSE NULL END,CASE WHEN b.LOA>=100 THEN 5 ELSE NULL END,CASE WHEN b.LOA>=65 THEN 4 ELSE NULL END,CASE WHEN b.LOA>=45 THEN 3 ELSE NULL END,CASE WHEN b.LOA>=35 THEN 2 ELSE NULL END,CASE WHEN b.LOA>=1 THEN 1 ELSE 0 END) AS LC,
  a.trip_id,
  SUBSTR(a.landed_date, 1, 10) AS LANDED_DATE,
  a.log_efrt_std_info_id,
  SUBSTR(a.date_fished, 1, 10) AS DATE_FISHED,
  a.gear_code,
  c.area,
(CASE WHEN a.LATITUDE>0 THEN ROUND (TRUNC (a.latitude / 10000) + MOD (a.latitude, 10000) / 6000.,5) ELSE NULL END) AS LAT,
(CASE WHEN a.LONGITUDE>0 THEN ROUND (TRUNC (a.longitude / 10000) + MOD (a.longitude, 10000) / 6000.,5) * -1.0 ELSE NULL END) AS LON,
  a.licence_id,
  d.sflt_desc_id,
  e.desc_eng,
  a.species_code,
  a.rnd_weight_kgs,
  a.mon_doc_id

from marfissci.pro_spc_info a,
marfissci.vessels b,
marfissci.areas c,
marfissci.sflt_quotas d,
marfissci.sflt_descs e,
marfissci.lic_quotas g,
marfissci.fleet_quotas h

where a.vr_number_fishing=b.vr_number
        and a.nafo_unit_area_id=c.area_id
        and a.lic_quota_id=g.lic_quota_id
        and g.sflt_quota_id=d.sflt_quota_id
        and d.sflt_desc_id=e.sflt_desc_id
        and d.fleet_quota_id=h.fleet_quota_id
        and c.area like '5Z%'
        and a.species_code in (100,110,170)
        and EXTRACT(YEAR FROM a.LANDED_DATE)=", year, "
        and TO_CHAR(a.date_fished,'Q') in (1,2,3,4)
", sep = " "))

  obs_trip <- ROracle::dbGetQuery(channel, "select a.mon_doc_id, a.column_defn_id, a.data_value TRIP from marfissci.mon_doc_entrd_dets a where a.column_defn_id=741")

  oracle.username <- oracle.password <- oracle.dsn <- RND_WEIGHT_KGS <- MON_DOC_ID <- COLUMN_DEFN_ID <- SPECIES_CODE <- SumOfRND_WEIGHT_KGS <- NULL

  marfis <- marfis1 |>
    dplyr::left_join(obs_trip) |>
    dplyr::summarise(SumOfRND_WEIGHT_KGS = sum(RND_WEIGHT_KGS), .by = c(-MON_DOC_ID, -COLUMN_DEFN_ID, -RND_WEIGHT_KGS)) |>
    dplyr::mutate(SPECIES_CODE = dplyr::case_when(
      SPECIES_CODE == 100 ~ "cod",
      SPECIES_CODE == 110 ~ "had",
      SPECIES_CODE == 170 ~ "pol"
    )) |>
    tidyr::pivot_wider(names_from = SPECIES_CODE, values_from = SumOfRND_WEIGHT_KGS) |>
    janitor::clean_names()

  print(marfis)
}
