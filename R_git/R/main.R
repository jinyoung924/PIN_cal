# =============================================================================
# main.R — PINstimation 파이프라인 메인 실행 스크립트
# =============================================================================
#
# 실행 흐름:
#   1. 00_setup.R       → 패키지 로드, 경로 설정, 코어 수 확인
#   2. 01_data_prep.R   → 전처리 함수 정의
#   3. 02_pin_worker.R  → PIN 워커 함수 정의
#   4. 03_adjpin_worker.R → AdjPIN 워커 함수 정의
#   5. 04_vpin_worker.R → VPIN 워커 함수 정의
#   6. 나라별 순회:
#      a) 원본 데이터 로드
#      b) PIN 전처리 + 병렬 추정
#      c) AdjPIN 전처리 + 병렬 추정
#      d) VPIN 전처리 + 병렬 계산
#   7. 완료 로그 기록
#
# 실행:
#   Rscript R/main.R
#
# =============================================================================

cat(sprintf("\n%s\n", strrep("#", 65)))
cat(sprintf("# PINstimation 배치 파이프라인\n"))
cat(sprintf("# 시작: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
cat(sprintf("%s\n\n", strrep("#", 65)))


# =============================================================================
# [1] 모듈 로드
# =============================================================================

# 스크립트 디렉토리 자동 감지
script_dir <- tryCatch({
  # Rscript로 실행 시
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", args, value = TRUE)
  if (length(file_arg) > 0) {
    dirname(normalizePath(sub("^--file=", "", file_arg)))
  } else {
    # RStudio 등에서 source() 시
    getSrcDirectory(function(x) x)
  }
}, error = function(e) {
  "R"  # 기본값: 현재 디렉토리의 R/ 하위
})

# R/ 디렉토리가 아니면 R/ 하위로 이동
if (!grepl("/R$", script_dir) && !grepl("\\\\R$", script_dir)) {
  script_dir <- file.path(script_dir, "R")
}

# 디렉토리 존재 여부 확인, 없으면 현재 작업 디렉토리 사용
if (!dir.exists(script_dir)) {
  script_dir <- if (dir.exists("R")) "R" else "."
}

source(file.path(script_dir, "00_setup.R"))
source(file.path(script_dir, "01_data_prep.R"))
source(file.path(script_dir, "02_pin_worker.R"))
source(file.path(script_dir, "03_adjpin_worker.R"))
source(file.path(script_dir, "04_vpin_worker.R"))


# =============================================================================
# [2] 나라별 처리 루프
# =============================================================================

pipeline_start <- Sys.time()

for (country in COUNTRIES) {

  cat(sprintf("\n%s\n", strrep("=", 65)))
  cat(sprintf("[%s] 파이프라인 시작\n", country))
  cat(sprintf("%s\n", strrep("=", 65)))

  log_file <- get_log_path(country)
  log_msg(sprintf("=== %s 파이프라인 시작 ===", country), log_file)


  # ── 2-a) 데이터 로드 ────────────────────────────────────────────────────
  log_msg(sprintf("[%s] 원본 데이터 로드 중...", country), log_file)

  raw_data <- tryCatch(
    load_raw_data(country, columns = c("Date", "Time", "Symbol", "Price", "Volume", "LR")),
    error = function(e) {
      log_msg(sprintf("[ERROR] %s 데이터 로드 실패: %s", country, e$message), log_file)
      NULL
    }
  )

  if (is.null(raw_data)) {
    log_msg(sprintf("[%s] 건너뜀 (데이터 로드 실패)", country), log_file)
    next
  }


  # ── 2-b) PIN 추정 ──────────────────────────────────────────────────────
  log_msg(sprintf("[%s] PIN 추정 시작...", country), log_file)

  daily_bs <- tryCatch(
    prep_daily_bs(raw_data),
    error = function(e) {
      log_msg(sprintf("[ERROR] PIN 전처리 실패: %s", e$message), log_file)
      NULL
    }
  )

  if (!is.null(daily_bs) && nrow(daily_bs) > 0) {
    run_pin_all(daily_bs, country, log_file)
  }


  # ── 2-c) AdjPIN 추정 ───────────────────────────────────────────────────
  log_msg(sprintf("[%s] AdjPIN 추정 시작...", country), log_file)

  # daily_bs는 PIN과 동일한 전처리 결과를 재사용
  if (!is.null(daily_bs) && nrow(daily_bs) > 0) {
    run_adjpin_all(daily_bs, country, log_file)
  }


  # ── 2-d) VPIN 계산 ─────────────────────────────────────────────────────
  log_msg(sprintf("[%s] VPIN 계산 시작...", country), log_file)

  vpin_ticks <- tryCatch(
    prep_vpin_ticks(raw_data),
    error = function(e) {
      log_msg(sprintf("[ERROR] VPIN 전처리 실패: %s", e$message), log_file)
      NULL
    }
  )

  if (!is.null(vpin_ticks) && nrow(vpin_ticks) > 0) {
    run_vpin_all(vpin_ticks, country, log_file)
  }

  # 메모리 해제
  rm(raw_data, daily_bs, vpin_ticks)
  gc()

  log_msg(sprintf("=== %s 파이프라인 완료 ===", country), log_file)
}


# =============================================================================
# [3] 전체 완료
# =============================================================================

total_elapsed <- as.numeric(difftime(Sys.time(), pipeline_start, units = "mins"))

cat(sprintf("\n%s\n", strrep("#", 65)))
cat(sprintf("# 전체 파이프라인 완료\n"))
cat(sprintf("# 처리 나라: %s\n", paste(COUNTRIES, collapse = ", ")))
cat(sprintf("# 총 소요 시간: %.1f분\n", total_elapsed))
cat(sprintf("# 종료: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
cat(sprintf("%s\n", strrep("#", 65)))
