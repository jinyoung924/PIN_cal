# =============================================================================
# 01_data_prep.R — 원본 틱 데이터 전처리
# =============================================================================
#
# 기능:
#   1) load_raw_data()     : 나라별 parquet 파일 로드 (필요 컬럼만 선택)
#   2) prep_daily_bs()     : PIN/AdjPIN용 일별 buys/sells 집계
#   3) prep_vpin_ticks()   : VPIN용 틱 데이터 준비 (LR, Volume 기반)
#
# 입력 데이터 컬럼:
#   Price(float64), Volume(float64), Symbol(str), Date(date32),
#   Time(time64), LR(int8: 1=매수, -1=매도, 0=미분류)
#
# =============================================================================


#' 나라별 원본 parquet 파일 전부 로드
#'
#' @param country 나라 코드 ("KR", "US", "JP" 등)
#' @param columns 읽을 컬럼 벡터 (NULL이면 전체)
#' @return data.frame
load_raw_data <- function(country, columns = NULL) {
  input_dir <- get_input_dir(country)

  if (!dir.exists(input_dir))
    stop(sprintf("입력 디렉토리 없음: %s", input_dir))

  # 재귀적으로 모든 .parquet 파일 탐색
  parquet_files <- list.files(input_dir, pattern = "\\.parquet$",
                              full.names = TRUE, recursive = TRUE)

  if (length(parquet_files) == 0)
    stop(sprintf("parquet 파일 없음: %s", input_dir))

  log_msg(sprintf("[데이터 로드] %s: %d개 parquet 파일 발견", country, length(parquet_files)))

  # arrow dataset으로 효율적 로드
  ds <- arrow::open_dataset(parquet_files)

  if (!is.null(columns)) {
    df <- ds |> dplyr::select(dplyr::all_of(columns)) |> dplyr::collect()
  } else {
    df <- dplyr::collect(ds)
  }

  # 타입 보정
  df$Date   <- as.Date(df$Date)
  df$Symbol <- as.character(df$Symbol)

  if ("LR" %in% names(df))
    df$LR <- as.integer(df$LR)
  if ("Volume" %in% names(df))
    df$Volume <- as.numeric(df$Volume)
  if ("Price" %in% names(df))
    df$Price <- as.numeric(df$Price)

  log_msg(sprintf("  로드 완료: %s행, 종목 %d개, 날짜 %s ~ %s",
                  format(nrow(df), big.mark = ","),
                  length(unique(df$Symbol)),
                  min(df$Date), max(df$Date)))
  df
}


#' PIN/AdjPIN용 일별 매수/매도 건수 집계
#'
#' @param raw_df load_raw_data()의 결과 (Date, Symbol, LR 필수)
#' @return data.frame: date, buys, sells, Symbol
prep_daily_bs <- function(raw_df) {
  log_msg("[전처리] 일별 buys/sells 집계 시작")

  result <- raw_df |>
    dplyr::filter(LR != 0) |>              # 미분류 제거
    dplyr::group_by(Symbol, Date) |>
    dplyr::summarise(
      buys  = sum(LR == 1L, na.rm = TRUE),
      sells = sum(LR == -1L, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::rename(date = Date) |>
    dplyr::arrange(Symbol, date)

  log_msg(sprintf("  집계 완료: %s행, 종목 %d개",
                  format(nrow(result), big.mark = ","),
                  length(unique(result$Symbol))))
  result
}


#' VPIN용 틱 데이터 준비
#'
#' LR 컬럼을 이용해 매수/매도 거래량을 분리한다.
#' LR == 0 (미분류)은 제외한다.
#'
#' @param raw_df load_raw_data()의 결과 (Date, Time, Symbol, Price, Volume, LR 필수)
#' @return data.frame: Symbol, timestamp(POSIXct), Price, Volume, LR
prep_vpin_ticks <- function(raw_df) {
  log_msg("[전처리] VPIN용 틱 데이터 준비 시작")

  result <- raw_df |>
    dplyr::filter(LR != 0) |>              # 미분류 제거
    dplyr::mutate(
      timestamp = as.POSIXct(
        paste(as.character(Date), format(Time, "%H:%M:%S")),
        format = "%Y-%m-%d %H:%M:%S"
      )
    ) |>
    dplyr::filter(!is.na(timestamp), Volume > 0) |>
    dplyr::select(Symbol, timestamp, Price, Volume, LR) |>
    dplyr::arrange(Symbol, timestamp)

  log_msg(sprintf("  준비 완료: %s행, 종목 %d개",
                  format(nrow(result), big.mark = ","),
                  length(unique(result$Symbol))))
  result
}


#' 종목 큐 생성 (알파벳순 정렬)
#'
#' @param df data.frame (Symbol 컬럼 포함)
#' @return character vector (정렬된 고유 종목 코드)
make_symbol_queue <- function(df) {
  sort(unique(df$Symbol))
}
