###############################################################################
# Rolling 60-day PIN Estimation (Fast)
# - LR 컬럼(1=매수, -1=매도)을 그대로 사용하여 빠른 집계
# - 입력: E:/vpin_project_parquet/processing_data/{COUNTRY}/
#         파일명: {country}_{year}_{chunk}.parquet
# - 출력: E:/vpin_project_parquet/R_output/{COUNTRY}/PIN/
#
# 컬럼 구조: Date(date32), Time(time64), Symbol(str),
#            Price(float64), Volume(float64), LR(int8)
###############################################################################

# === 패키지 ===
pkgs <- c("PINstimation", "arrow", "data.table")
for (p in pkgs) {
  if (!requireNamespace(p, quietly = TRUE)) install.packages(p)
  library(p, character.only = TRUE)
}

###############################################################################
# 설정 — 여기만 수정하세요
###############################################################################

COUNTRY    <- "KOR"
WINDOW     <- 60L
PIN_METHOD <- "EA"         # "EA", "YZ", "GWJ", "BAYES"
FACTORIZE  <- "LK"         # "LK", "E", "EHO", "NONE"
XTRA_CL    <- 4L

BASE_IN    <- "E:/vpin_project_parquet/processing_data"
BASE_OUT   <- "E:/R_pdf_output"

###############################################################################
# 1. 데이터 로드 & 일별 B/S 집계
###############################################################################

load_and_aggregate <- function(country) {

  # country로 시작하는 모든 하위 폴더 찾기
  all_dirs <- list.dirs(BASE_IN, recursive = FALSE)
  matched  <- all_dirs[grepl(paste0("^", country), basename(all_dirs), ignore.case = TRUE)]

  if (length(matched) == 0) stop("매칭 폴더 없음: ", BASE_IN, "/", country, "*")

  cat(sprintf("[1/4] %d개 폴더 매칭: %s\n",
              length(matched),
              paste(basename(matched), collapse = ", ")))

  # 매칭된 모든 폴더에서 parquet 수집
  files <- unlist(lapply(matched, list.files,
                         pattern = "\\.parquet$", full.names = TRUE))

  if (length(files) == 0) stop("parquet 파일 없음")
  cat(sprintf("   총 %d개 parquet 파일 발견\n", length(files)))

  # 필요한 컬럼만 읽기 (메모리 절약 + 속도)
  dt_list <- lapply(files, function(f) {
    tryCatch(
      as.data.table(read_parquet(f, col_select = c("Date", "Symbol", "LR"))),
      error = function(e) {
        # 대소문자 다를 수 있으므로 전체 읽고 선택
        tmp <- as.data.table(read_parquet(f))
        nm  <- tolower(names(tmp))
        names(tmp) <- nm
        tmp[, .(date = date, symbol = symbol, lr = lr)]
      }
    )
  })

  dt <- rbindlist(dt_list, use.names = TRUE, fill = TRUE)
  rm(dt_list); gc()

  # 컬럼명 소문자 통일
  nm <- tolower(names(dt))
  names(dt) <- nm

  cat(sprintf("   총 %s 틱 로드 완료\n", format(nrow(dt), big.mark = ",")))

  # LR: 1 = 매수, -1 = 매도 → 일별 종목별 건수 집계
  cat("[2/4] 일별 B/S 집계 중...\n")

  daily <- dt[, .(B = sum(lr == 1L), S = sum(lr == -1L)),
              keyby = .(symbol, date)]

  rm(dt); gc()

  # 0건인 날 제거 (혹시 모를 빈 행)
  daily <- daily[B + S > 0]

  cat(sprintf("   %d 종목 × %s 일-종목 행\n",
              uniqueN(daily$symbol),
              format(nrow(daily), big.mark = ",")))

  return(daily)
}

###############################################################################
# 2. 단일 종목 롤링 PIN 추정 (벡터화 최적화)
###############################################################################

estimate_pin_one <- function(sym_dt, sym, win, method, fact) {

  setorder(sym_dt, date)
  n <- nrow(sym_dt)
  if (n < win) return(NULL)

  n_win <- n - win + 1L
  dates_end   <- sym_dt$date[(win):n]
  dates_start <- sym_dt$date[1:n_win]

  # 사전 할당
  pin_vec   <- rep(NA_real_, n_win)
  pinG_vec  <- rep(NA_real_, n_win)
  pinB_vec  <- rep(NA_real_, n_win)
  alpha_vec <- rep(NA_real_, n_win)
  delta_vec <- rep(NA_real_, n_win)
  mu_vec    <- rep(NA_real_, n_win)
  eb_vec    <- rep(NA_real_, n_win)
  es_vec    <- rep(NA_real_, n_win)
  lkl_vec   <- rep(NA_real_, n_win)
  ok_vec    <- rep(FALSE, n_win)

  mat_BS <- as.matrix(sym_dt[, .(B, S)])

  for (i in seq_len(n_win)) {

    win_data <- as.data.frame(mat_BS[i:(i + win - 1L), ])

    est <- tryCatch({
      switch(method,
        "EA"    = pin_ea(win_data,  factorization = fact,
                         xtraclusters = XTRA_CL, verbose = FALSE),
        "YZ"    = pin_yz(win_data,  factorization = fact, verbose = FALSE),
        "GWJ"   = pin_gwj(win_data, factorization = fact, verbose = FALSE),
        "BAYES" = pin_bayes(win_data, xtraclusters = XTRA_CL, verbose = FALSE),
        pin_ea(win_data, factorization = fact,
               xtraclusters = XTRA_CL, verbose = FALSE)
      )
    }, error = function(e) NULL)

    if (!is.null(est) && est@success) {
      p          <- est@parameters
      gb         <- est@pin.goodbad
      pin_vec[i]   <- est@pin
      pinG_vec[i]  <- if (!is.null(gb$pinG)) gb$pinG else NA_real_
      pinB_vec[i]  <- if (!is.null(gb$pinB)) gb$pinB else NA_real_
      alpha_vec[i] <- p$alpha
      delta_vec[i] <- p$delta
      mu_vec[i]    <- p$mu
      eb_vec[i]    <- p$eps.b
      es_vec[i]    <- p$eps.s
      lkl_vec[i]   <- est@likelihood
      ok_vec[i]    <- TRUE
    }
  }

  data.table(
    date      = dates_end,
    symbol    = sym,
    win_start = dates_start,
    PIN       = pin_vec,
    pinG      = pinG_vec,
    pinB      = pinB_vec,
    alpha     = alpha_vec,
    delta     = delta_vec,
    mu        = mu_vec,
    eps_b     = eb_vec,
    eps_s     = es_vec,
    loglik    = lkl_vec,
    success   = ok_vec
  )
}

###############################################################################
# 3. 메인 실행
###############################################################################

run <- function(country = COUNTRY) {

  out_dir <- file.path(BASE_OUT, country, "PIN")
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

  cat("================================================================\n")
  cat(sprintf(" Rolling %d-day PIN  |  %s  |  %s-%s\n",
              WINDOW, country, PIN_METHOD, FACTORIZE))
  cat("================================================================\n\n")

  # 로드 & 집계
  daily <- load_and_aggregate(country)

  symbols <- unique(daily$symbol)
  n_sym   <- length(symbols)
  cat(sprintf("\n[3/4] %d 종목 롤링 PIN 추정 시작\n", n_sym))

  all_res <- vector("list", n_sym)
  t0 <- Sys.time()

  for (j in seq_len(n_sym)) {
    sym <- symbols[j]
    sdt <- daily[symbol == sym]

    n_days <- nrow(sdt)
    n_win  <- max(0L, n_days - WINDOW + 1L)

    if (n_win == 0L) {
      if (j <= 20 || j %% 100 == 0)
        cat(sprintf("  [%d/%d] %s  SKIP (%d일 < %d)\n",
                    j, n_sym, sym, n_days, WINDOW))
      next
    }

    tj <- Sys.time()
    res <- estimate_pin_one(sdt, sym, WINDOW, PIN_METHOD, FACTORIZE)
    ej  <- round(difftime(Sys.time(), tj, units = "secs"), 1)

    if (!is.null(res) && nrow(res) > 0) {
      all_res[[j]] <- res

      sr <- round(mean(res$success) * 100, 1)
      mp <- round(median(res$PIN, na.rm = TRUE), 4)

      if (j <= 20 || j %% 50 == 0 || j == n_sym)
        cat(sprintf("  [%d/%d] %s  %d윈도우  %.1fs  성공%.0f%%  PIN중앙=%.4f\n",
                    j, n_sym, sym, nrow(res), ej, sr, mp))
    }
  }

  # 결과 합치기
  cat("\n[4/4] 저장 중...\n")
  final <- rbindlist(Filter(Negate(is.null), all_res))

  if (nrow(final) == 0) { cat("[WARN] 결과 없음\n"); return(invisible(NULL)) }

  # --- parquet 저장 ---
  out_pq <- file.path(out_dir, sprintf("PIN_%s.parquet", country))
  write_parquet(final, out_pq)
  cat(sprintf("  -> %s\n", out_pq))

  # --- 종목별 요약 CSV ---
  smry <- final[success == TRUE, .(
    days       = .N,
    pin_mean   = round(mean(PIN, na.rm = TRUE), 6),
    pin_med    = round(median(PIN, na.rm = TRUE), 6),
    pin_sd     = round(sd(PIN, na.rm = TRUE), 6),
    pin_min    = round(min(PIN, na.rm = TRUE), 6),
    pin_max    = round(max(PIN, na.rm = TRUE), 6),
    pinG_mean  = round(mean(pinG, na.rm = TRUE), 6),
    pinB_mean  = round(mean(pinB, na.rm = TRUE), 6),
    alpha_mean = round(mean(alpha, na.rm = TRUE), 6),
    mu_mean    = round(mean(mu, na.rm = TRUE), 2)
  ), by = symbol]

  out_csv <- file.path(out_dir, sprintf("PIN_summary_%s.csv", country))
  fwrite(smry, out_csv)
  cat(sprintf("  -> %s\n", out_csv))

  elapsed <- round(difftime(Sys.time(), t0, units = "mins"), 1)

  cat("\n================================================================\n")
  cat(sprintf(" 종목: %d  |  추정: %s건  |  성공: %.1f%%\n",
              uniqueN(final$symbol),
              format(nrow(final), big.mark = ","),
              mean(final$success) * 100))
  cat(sprintf(" PIN 평균: %.4f  |  중앙: %.4f  |  [%.4f ~ %.4f]\n",
              mean(final$PIN, na.rm = TRUE),
              median(final$PIN, na.rm = TRUE),
              min(final$PIN, na.rm = TRUE),
              max(final$PIN, na.rm = TRUE)))
  cat(sprintf(" 소요: %.1f분\n", elapsed))
  cat("================================================================\n")

  invisible(final)
}

###############################################################################
# 실행
###############################################################################

result <- run(COUNTRY)

# # 여러 국가 순차 실행
# for (cc in c("KOR", "USA", "JPN")) {
#   cat("\n\n>>>", cc, "<<<\n\n")
#   run(cc)
# }