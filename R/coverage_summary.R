#' @title coverage_summary
#' @description Export an observer coverage summary Excel document.
#' @param marfis.df A dataframe of MARFIS data with observer TRIP ID codes.
#' @param y Year
#' @return An Excel document (Discards_ObsCoverageSummary_2024.xlsx) of observer coverage in the working directory.
#' @examples \dontrun{
#' example <- coveragesummary(marfis.df = marfis, y = 2023)
#' }
#' @export


coverage_summary <- function(marfis.df = NULL, y = year) {
  # Write a coverage summary and export it as an Excel document

  trip_level <- marfis.df |>
    dplyr::group_by(vr_number_fishing, vessel_name, licence_id, tc, lc, trip_id, landed_date, gear_code, q, trip, zone, sector) |>
    dplyr::summarize(cod = sum(cod, na.rm = TRUE), had = sum(had, na.rm = TRUE), pol = sum(pol, na.rm = TRUE)) |>
    mutate(obs = ifelse(is.na(trip), "N", "Y"))

  fleet_summary <- trip_level |>
    dplyr::group_by(sector, obs) |>
    dplyr::tally() |>
    tidyr::pivot_wider(id_cols = c(sector), names_from = obs, values_from = n) |>
    tidyr::replace_na(list(Y = 0, N = 0)) |>
    dplyr::mutate(coverage = Y / sum(N + Y))

  total_coverage <- sum(fleet_summary$Y) / (sum(fleet_summary$N) + sum(fleet_summary$Y))

  mean_coverage <- mean(fleet_summary$coverage)

  fleet_allobs <- fleet_summary |> dplyr::filter(coverage == 1)

  fleetqz_summary <- trip_level |>
    dplyr::group_by(sector, zone, q, obs) |>
    dplyr::tally() |>
    tidyr::pivot_wider(id_cols = c(sector, zone, q), names_from = obs, values_from = n) |>
    tidyr::replace_na(list(Y = 0, N = 0)) |>
    dplyr::mutate(coverage = Y / sum(N + Y))

  fleetqz_allobs <- fleetqz_summary |> dplyr::filter(coverage == 1)

  N <- Y <- cod <- coverage <- gear_code <- had <- landed_date <- lc <- licence_id <- n <- obs <- pol <- sector <- tc <- trip <- trip_id <- vessel_name <- vr_number_fishing <- year <- zone <- NULL

  # Export coverage summary as an excel document

  coverage_list <- list(fleet_summary, total_coverage, fleet_allobs, fleetqz_summary, fleetqz_allobs)
  sheet_names <- c("Fleet Summary", "Total Coverage", "Fleet 100%", "Fleet Zone Quarter Summary", "Fleet Zone Quarter 100%")
  names(coverage_list) <- sheet_names
  openxlsx::write.xlsx(coverage_list, file = here::here(paste("data/Discards_ObsCoverageSummary_", y, ".xlsx", sep = "")))

  print(fleetqz_allobs)

}
