# =============================================================================
# 01_data_prep.R — 스트리밍 데이터 전처리
# =============================================================================
#
# OOM 방지: 원본 parquet 파일을 한 번에 메모리에 올리지 않는다.
# 파일 1개씩 읽고 → 즉시 집계/필터 → raw 해제 → 다음 파일 반복.
#
# 제공 함수:
#   1) stream_daily_bs()             : PIN/AdjPIN용 — 파일별 3컬럼만 읽고 즉시 일별 B/S 집계
#   2) stream_vpin_ticks_to_disk()   : VPIN용 — 파일별 6컬럼 읽고 종목별 임시 parquet 저장
#   3) load_vpin_sym()               : VPIN용 — 종목 1개 임시파일 로드
#   4) make_symbol_queue()           : 종목 큐 생성 (알파벳순 정렬)
#
# =============================================================================


#' PIN/AdjPIN용 일별 매수/매도 건수 스트리밍 집계
#'
#' parquet 파일을 하나씩 읽어 즉시 집계한다.
#' 메모리 사용량 = 파일 1개의 3컬럼(Date, Symbol, LR) + 누적 집계 결과.
#'
#' @param country 나라 코드
#' @return data.frame: Symbol, date, buys, sells
stream_daily_bs <- function(country, log_file = NULL) {
  input_dir <- get_input_dir(country)

  if (!dir.exists(input_dir))
    stop(sprintf("입력 디렉토리 없음: %s", input_dir))

  parquet_files <- list.files(input_dir, pattern = "\\.parquet$",
                              full.names = TRUE, recursive = TRUE)

  if (length(parquet_files) == 0)
    stop(sprintf("parquet 파일 없음: %s", input_dir))

  log_msg(sprintf("[스트리밍 집계] %s: %d개 parquet 파일 처리 시작",
                  country, length(parquet_files)), log_file)

  chunk_results <- vector("list", length(parquet_files))

  for (i in seq_along(parquet_files)) {
    f <- parquet_files[[i]]
    log_msg(sprintf("  [%d/%d] %s", i, length(parquet_files), basename(f)), log_file)

    chunk <- tryCatch({
      # 3컬럼만 읽기 — 메모리 핵심 절감
      raw <- arrow::read_parquet(f, col_select = c("Date", "Symbol", "LR"))
      raw$Date   <- as.Date(raw$Date)
      raw$Symbol <- as.character(raw$Symbol)
      raw$LR     <- as.integer(raw$LR)

      # 즉시 집계: 틱 → 일별 종목별 B/S
      agg <- raw |>
        dplyr::filter(LR != 0L) |>
        dplyr::group_by(Symbol, Date) |>
        dplyr::summarise(
          buys  = sum(LR == 1L,  na.rm = TRUE),
          sells = sum(LR == -1L, na.rm = TRUE),
          .groups = "drop"
        ) |>
        dplyr::rename(date = Date)

      rm(raw); gc()
      agg
    }, error = function(e) {
      log_msg(sprintf("    [WARN] 파일 읽기 실패: %s", e$message), log_file)
      NULL
    })

    chunk_results[[i]] <- chunk
  }

  # 유효 청크 병합 + 재집계 (동일 Symbol-date가 여러 파일에 걸칠 수 있음)
  valid <- Filter(function(x) !is.null(x) && nrow(x) > 0, chunk_results)

  if (length(valid) == 0)
    stop("모든 파일에서 유효 데이터 없음")

  combined <- do.call(rbind, valid)
  rm(chunk_results, valid); gc()

  result <- combined |>
    dplyr::group_by(Symbol, date) |>
    dplyr::summarise(
      buys  = sum(buys,  na.rm = TRUE),
      sells = sum(sells, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::arrange(Symbol, date)

  rm(combined); gc()

  log_msg(sprintf("[스트리밍 집계 완료] %s행, 종목 %d개, 날짜 %s ~ %s",
                  format(nrow(result), big.mark = ","),
                  length(unique(result$Symbol)),
                  min(result$date), max(result$date)), log_file)
  result
}


#' VPIN용 틱 데이터를 종목별 임시 parquet 파일로 스트리밍 쓰기
#'
#' 전체 틱을 메모리에 올리지 않고, 파일 1개씩 읽어 종목별로 분산 저장.
#' 이후 run_vpin_all_streaming()이 종목별 파일을 하나씩 읽어 처리.
#'
#' @param country 나라 코드
#' @param vpin_tmp_dir 종목별 임시 파일 저장 디렉토리
#' @param log_file 로그 파일 경로
#' @return character vector: 발견된 고유 종목 코드 목록 (정렬됨)
stream_vpin_ticks_to_disk <- function(country, vpin_tmp_dir, log_file = NULL) {
  input_dir <- get_input_dir(country)

  parquet_files <- list.files(input_dir, pattern = "\\.parquet$",
                              full.names = TRUE, recursive = TRUE)

  if (length(parquet_files) == 0)
    stop(sprintf("parquet 파일 없음: %s", input_dir))

  dir.create(vpin_tmp_dir, recursive = TRUE, showWarnings = FALSE)

  log_msg(sprintf("[VPIN 스트리밍] %s: %d개 파일 → %s",
                  country, length(parquet_files), vpin_tmp_dir), log_file)

  all_symbols <- character(0)

  for (i in seq_along(parquet_files)) {
    f <- parquet_files[[i]]
    log_msg(sprintf("  [%d/%d] %s", i, length(parquet_files), basename(f)), log_file)

    tryCatch({
      raw <- arrow::read_parquet(
        f,
        col_select = c("Date", "Time", "Symbol", "Price", "Volume", "LR")
      )
      raw$Date   <- as.Date(raw$Date)
      raw$Symbol <- as.character(raw$Symbol)
      raw$LR     <- as.integer(raw$LR)
      raw$Volume <- as.numeric(raw$Volume)
      raw$Price  <- as.numeric(raw$Price)

      # LR==0 제거 + Volume > 0 필터
      raw <- raw[!is.na(raw$LR) & raw$LR != 0L &
                 !is.na(raw$Volume) & raw$Volume > 0, ]

      if (nrow(raw) == 0) {
        rm(raw); gc()
        next
      }

      # timestamp 생성
      raw$timestamp <- as.POSIXct(
        paste(as.character(raw$Date), format(raw$Time, "%H:%M:%S")),
        format = "%Y-%m-%d %H:%M:%S"
      )
      raw <- raw[!is.na(raw$timestamp), ]
      raw <- raw[, c("Symbol", "timestamp", "Price", "Volume", "LR")]

      # 종목별 분할 → 개별 임시 파일에 append
      syms_in_file <- unique(raw$Symbol)
      all_symbols  <- union(all_symbols, syms_in_file)

      for (sym in syms_in_file) {
        sym_rows <- raw[raw$Symbol == sym, ]
        sym_path <- file.path(vpin_tmp_dir, sprintf("sym_%s.parquet", sym))

        if (file.exists(sym_path)) {
          existing <- arrow::read_parquet(sym_path)
          sym_rows <- rbind(existing, sym_rows)
          rm(existing)
        }
        arrow::write_parquet(sym_rows, sym_path, compression = "zstd")
      }

      rm(raw); gc()
    }, error = function(e) {
      log_msg(sprintf("    [WARN] 파일 처리 실패: %s", e$message), log_file)
    })
  }

  log_msg(sprintf("[VPIN 스트리밍 완료] 종목 %d개 임시 파일 생성",
                  length(all_symbols)), log_file)
  sort(all_symbols)
}


#' VPIN 임시 파일에서 단일 종목 틱 데이터 로드
#'
#' @param sym 종목 코드
#' @param vpin_tmp_dir 임시 파일 디렉토리
#' @return data.frame (정렬됨) 또는 NULL
load_vpin_sym <- function(sym, vpin_tmp_dir) {
  sym_path <- file.path(vpin_tmp_dir, sprintf("sym_%s.parquet", sym))
  if (!file.exists(sym_path)) return(NULL)
  df <- arrow::read_parquet(sym_path)
  if (nrow(df) == 0) return(NULL)
  df[order(df$timestamp), ]
}


#' 종목 큐 생성 (알파벳순 정렬)
#'
#' @param df data.frame (Symbol 컬럼 포함)
#' @return character vector
make_symbol_queue <- function(df) {
  sort(unique(df$Symbol))
}
