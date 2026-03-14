# PIN_cal — 주식 시장 정보 비대칭성 분석 도구

한국(KRX) 및 주요 글로벌 주식 시장의 틱 데이터를 기반으로 **PIN**, **APIN**, **VPIN** 세 가지 정보 비대칭성 지표를 추정하는 연구용 파이프라인입니다.

---

## 지표 개요

| 지표 | 논문 | 설명 |
|------|------|------|
| **PIN** | Easley, Kiefer, O'Hara & Paperman (1996) | 정보 거래 확률 — 60일 롤링 윈도우로 일별 추정 |
| **APIN** | Duarte & Young (2009) | 조정된 PIN — 비정보적 비대칭 주문 흐름 분리 |
| **VPIN** | Easley, de Prado & O'Hara (2012) | 거래량 동기화 PIN — 1분봉 기반 버킷 단위 추정 |

---

## 디렉토리 구조

```
PIN_cal/
├── 00_sas_to_parquet.py    # [전처리 0] SAS7BDAT → Parquet 변환
├── 00pin/
│   ├── pin_pre.py          # [PIN 1-2단계] 일별 B/S 집계 + 거래일 정렬
│   └── pin_r.R             # [PIN 3단계]  PIN 추정 (PINstimation)
├── 01apin/
│   ├── apin_pre.py         # [APIN 1-2단계] 일별 B/S 집계 + 거래일 정렬
│   └── apin_r.R            # [APIN 3단계]  APIN 추정 (PINstimation)
└── 02vpin/
    ├── vpin_pre.py         # [VPIN 1단계] 1분봉 집계
    └── vpin_r.R            # [VPIN 2단계] VPIN 추정 (PINstimation)
```

---

## 전체 파이프라인

```
SAS7BDAT 틱 데이터
        │
        ▼
[00_sas_to_parquet.py]  ─────────────────────────
        │ Parquet 변환 완료                       │
        ▼                                         │
  ┌─────────────┬──────────────┐                  │
  ▼             ▼              ▼                  │
[00pin]      [01apin]       [02vpin]               │
pin_pre.py   apin_pre.py   vpin_pre.py            │
     ↓            ↓              ↓                │
  pin_r.R     apin_r.R      vpin_r.R              │
     ↓            ↓              ↓                │
pin_*.parquet apin_*.parquet vpin_*.parquet        │
(일별 PIN 추정) (일별 APIN 추정) (버킷별 VPIN 값) ◄──┘
```

> **참고**: PIN과 APIN은 동일한 중간 캐시(`all_daily_bs.parquet`, `full_daily_bs.parquet`)를 공유합니다.
> PIN 전처리를 먼저 실행하면 APIN 전처리는 캐시를 재사용합니다.

---

## 입력 데이터 구조

### 원본 SAS7BDAT 틱 데이터

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `Symbol` | String | 종목 코드 (예: `"258540"`) |
| `Date` | date32 | 거래일 |
| `Time` | time64[ns] | 거래 시각 (나노초 정밀도) |
| `Price` | Float64 | 체결 가격 |
| `Volume` | Float64 | 체결 수량 |
| `LR` | Int8 | 거래 방향: `1`=매수, `-1`=매도, `null`=미분류 |
| `MidPoint` | Float64 | 중간 호가 |
| `QSpread` | Float64 | 호가 스프레드 |

파일 위치 예시:
```
E:\vpin_project_sas7bdat\KOR_2019\KOR_201901.sas7bdat
E:\vpin_project_sas7bdat\KOR_2019\KOR_201902.sas7bdat
...
```

---

## 출력 데이터 구조

### PIN (`pin_KOR_YYYYMMDD_HHMM.parquet`)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `Symbol` | String | 종목 코드 |
| `Date` | Date | 추정 기준일 (60일 윈도우 마지막 날) |
| `valid_days` | Int | 윈도우 내 유효 거래일 수 (≥30 필요) |
| `a` | Float | α: 정보 이벤트 발생 확률 |
| `d` | Float | δ: 나쁜 뉴스 조건부 확률 |
| `u` | Float | μ: 정보 거래자의 거래 강도 |
| `eb` | Float | ε_b: 매수 측 비정보 거래 강도 |
| `es` | Float | ε_s: 매도 측 비정보 거래 강도 |
| `PIN` | Float | **최종 PIN 값** |

### APIN (`apin_KOR_YYYYMMDD_HHMM.parquet`)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `Symbol` | String | 종목 코드 |
| `Date` | Date | 추정 기준일 |
| `valid_days` | Int | 윈도우 내 유효 거래일 수 |
| `a` | Float | α |
| `d` | Float | δ |
| `t1` | Float | θ |
| `t2` | Float | θ' |
| `ub`, `us` | Float | μ_b, μ_s: 매수·매도 정보 거래 강도 |
| `eb`, `es` | Float | ε_b, ε_s: 비정보 거래 강도 |
| `pb`, `ps` | Float | d_b, d_s |
| `APIN` | Float | **최종 APIN 값** |
| `PSOS` | Float | 비정보 비대칭 주문 흐름 확률 |

### VPIN (`vpin_KOR_YYYYMMDD_HHMM.parquet`)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `Symbol` | String | 종목 코드 |
| `BucketNo` | Int | 버킷 번호 (순번) |
| `Endtime` | Datetime | 버킷 종료 시각 |
| `AggBuyVol` | Float | 버킷 내 매수 추정 거래량 |
| `AggSellVol` | Float | 버킷 내 매도 추정 거래량 |
| `VPIN` | Float | **최종 VPIN 값** (롤링 50버킷 평균) |

---

## 의존성 및 권장 버전

### Python

| 패키지 | 권장 버전 | 용도 |
|--------|----------|------|
| Python | **3.14.3** | 인터프리터 (`.venv` 기준) |
| `polars` | **1.37.0** | 고속 DataFrame 처리 |
| `pyarrow` | **23.0.1** | Parquet 읽기/쓰기 |
| `pandas` | **3.0.0** | 일반 데이터 처리 |
| `pyreadstat` | **1.3.3** | SAS7BDAT 읽기 |

가상 환경 설치:
```bash
# 프로젝트 루트에서
python -m venv .venv
.venv\Scripts\activate

pip install polars==1.37.0 pyarrow==23.0.1 pandas==3.0.0 pyreadstat==1.3.3
```

### R

| 패키지 | 용도 |
|--------|------|
| `PINstimation` | PIN / APIN / VPIN 추정 모델 |
| `arrow` | Parquet 읽기/쓰기 |
| `parallel` | 병렬 처리 (PSOCK 클러스터) |
| `data.table` | 고속 데이터 조작 |
| `dplyr` | 데이터 파이프라인 (VPIN 전용) |
| `R.utils` | 타임아웃 유틸리티 (APIN 전용) |

R 패키지 설치:
```r
install.packages(c("PINstimation", "arrow", "data.table", "dplyr", "R.utils"))
```

> **권장 R 버전**: R 4.2 이상 (PINstimation 최신 버전 기준)

---

## 실행 방법

### 사전 준비: 경로 설정

각 스크립트 상단의 **"★ User Configuration"** 섹션에서 경로를 수정합니다.

| 스크립트 | 수정할 변수 |
|----------|------------|
| `00_sas_to_parquet.py` | `BASE_INPUT_DIR`, `BASE_OUTPUT_DIR` |
| `00pin/pin_pre.py` | `BASE_DIR`, `COUNTRY` |
| `00pin/pin_r.R` | `BASE_DIR`, `COUNTRY` |
| `01apin/apin_pre.py` | `BASE_DIR`, `COUNTRY` |
| `01apin/apin_r.R` | `BASE_DIR`, `COUNTRY` |
| `02vpin/vpin_pre.py` | `BASE_DIR`, `COUNTRY` |
| `02vpin/vpin_r.R` | `BASE_DIR`, `COUNTRY` |

### 1단계: SAS → Parquet 변환 (최초 1회)

```bash
.venv\Scripts\python.exe 00_sas_to_parquet.py
```

### 2단계: PIN 계산

```bash
.venv\Scripts\python.exe 00pin/pin_pre.py
Rscript 00pin/pin_r.R
```

### 3단계: APIN 계산

```bash
.venv\Scripts\python.exe 01apin/apin_pre.py   # 캐시 있으면 자동 재사용
Rscript 01apin/apin_r.R
```

### 4단계: VPIN 계산 (PIN/APIN과 독립적)

```bash
.venv\Scripts\python.exe 02vpin/vpin_pre.py
Rscript 02vpin/vpin_r.R
```

### 병렬 실행 (시간 단축)

PIN과 APIN 전처리가 완료된 후 PIN R 계산과 VPIN 전처리를 동시에 실행할 수 있습니다.

```
터미널 1: Rscript 00pin/pin_r.R
터미널 2: Rscript 01apin/apin_r.R
터미널 3: .venv\Scripts\python.exe 02vpin/vpin_pre.py → Rscript 02vpin/vpin_r.R
```

---

## 주요 파라미터

### PIN / APIN 공통

| 파라미터 | 기본값 | 설명 |
|----------|--------|------|
| `WINDOW_SIZE` | `60` | 롤링 윈도우 크기 (거래일) |
| `MIN_VALID_DAYS` | `30` | 윈도우당 최소 유효 거래일 |
| `NUM_WORKERS` | 전체 코어 수 | 병렬 워커 수 |

### APIN 추가

| 파라미터 | 기본값 | 설명 |
|----------|--------|------|
| `NUM_INIT_FIRST` | `20` | 첫 윈도우 GE 초기점 수 |
| `NUM_INIT_ROLL` | `5` | 롤링 윈도우 추가 초기점 수 |
| `TIMEOUT_SEC` | `600` | 윈도우당 최대 계산 시간 (초) |

### VPIN

| 파라미터 | 기본값 | 설명 |
|----------|--------|------|
| `TIMEBARSIZE_SEC` | `60` | 시간봉 크기 (초, 1분) |
| `BUCKETS_PER_DAY` | `50` | 일별 버킷 수 |
| `SAMPLENGTH` | `50` | 롤링 버킷 윈도우 크기 |
| `TRADINGHOURS` | `7` | 거래 시간 (KRX 기준) |
| `MIN_ROWS` | `500` | 종목당 최소 유효 행 수 |

---

## 체크포인트 & 재실행

모든 파이프라인은 **중단 후 재실행이 안전**합니다.

- **PIN / APIN**: `checkpoints/sym_{Symbol}.parquet` — 완료된 종목 자동 스킵
- **VPIN**: `checkpoints/sym_{Symbol}.rds` — 완료된 종목 자동 스킵
- **전처리 캐시**: `all_daily_bs.parquet`, `full_daily_bs.parquet` — 이미 존재하면 재사용
  - 강제 재생성: `FORCE_REPROCESS = True` 설정

---

## 지원 국가

| 코드 | 국가 | 거래소 |
|------|------|--------|
| `KOR` | 한국 | KRX (거래시간 6.5h) |
| `US` | 미국 | NYSE/NASDAQ (6.5h) |
| `JP` | 일본 | TSE (5h) |
| `CA` | 캐나다 | TSX (6.5h) |
| `HK` | 홍콩 | HKEx (5.5h) |
| `FR` | 프랑스 | Euronext |
| `GR` | 독일 | XETRA |
| `IT` | 이탈리아 | Borsa Italiana |
| `UK` | 영국 | LSE |

> VPIN `TRADINGHOURS` 파라미터를 국가별 거래 시간에 맞게 설정해야 합니다.

---

## 참고 문헌

- Easley, D., Kiefer, N. M., O'Hara, M., & Paperman, J. B. (1996). Liquidity, information, and infrequently traded stocks. *Journal of Finance*, 51(4), 1405–1436.
- Duarte, J., & Young, L. (2009). Why is PIN priced? *Journal of Financial Economics*, 91(2), 119–138.
- Easley, D., López de Prado, M. M., & O'Hara, M. (2012). Flow toxicity and liquidity in a high-frequency world. *Review of Financial Studies*, 25(5), 1457–1493.
- Ersan, O., & Alici, A. (2016). An unbiased computation methodology for estimating the probability of informed trading (PIN). *Journal of International Financial Markets, Institutions and Money*, 43, 74–94.
