#' @title assign_sector
#' @description Assign fleet sectors to data.
#' @param data A dataframe with a fleet code column (sflt_desc_id)
#' @return a data.frame with a new column for fleet/sector
#' @examples \dontrun{
#' example <- assign_sector(df)
#' }
#' @export

assign_sector <- function(data = NULL) {
  data |>
    dplyr::mutate(
      sector = dplyr::case_when(
        sflt_desc_id %in% c(90:95, 8976, 8977, 9318, 9319, 10865, 10866, 12305, 12195) ~ 90 # New Fleet Desc 90 (FG<45’)
        , sflt_desc_id %in% c(89, 9317) ~ 89 # New Fleet Desc 89 (HL) (Gear = 41 so not handline)
        , sflt_desc_id %in% c(881) ~ 881 # New Fleet Desc 881 (MG<65’ – SF ITQ)
        , sflt_desc_id %in% c(884, 6875) ~ 884 # New Fleet Desc 884 (FG 45’ to 64’)
        , sflt_desc_id %in% c(885, 1472) ~ 885 # New Fleet Desc 885 (MG>100’)
        , sflt_desc_id %in% c(886, 1817) ~ 886 # New Fleet Desc 886 (FG 65’ to 100’)
        , sflt_desc_id %in% c(311, 1523) ~ 311 # New Fleet Desc 311(MG 65’ to 100’)
        , sflt_desc_id %in% c(3328) ~ 3328
      )
    )
}
