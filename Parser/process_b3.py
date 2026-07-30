# -*- coding: utf-8 -*-
"""
신호별 가격 시퀀스 / 변동률 집계
[입력] B1Sheet 폴더 + A1Sheet 폴더  ->  [출력] B3Sheet 폴더

폴더 구조
─────────────────────────────────────────────────────────────────
  {market}/B1Sheet/{기준일 날짜폴더}/{신호}.parquet
      └─ "이 날짜에 어떤 종목에 신호가 났는지" 목록(Name, Symbol, Close)
         B3Sheet 날짜폴더 구조를 B1Sheet 와 평행하게 유지하기 위해 참조.

  {market}/A1Sheet/{Name}({Symbol}).parquet
      └─ 종목별 가격·지표 시계열 (Date, Close, RSI_BullDiv_Sum, ...)
         신호 컬럼 > 0 (또는 < 0) 인 '역대 모든 행'이 신호발생일.

  {market}/B3Sheet/{기준일 날짜폴더}/{신호}.parquet  &  .csv
      └─ B1Sheet 날짜폴더 구조와 1:1 대응.
         그 날짜의 B1Sheet 에 있던 종목들의 역대 신호 이벤트를 모두 담음.
─────────────────────────────────────────────────────────────────

처리 흐름 (클래스별 역할)
  SignalConfig   : 신호 파일명 → (A1Sheet 컬럼명, 발생 조건) 매핑 + 컬럼 이름 생성
  A1SheetLoader  : A1Sheet 종목 파일 로드·캐시, 발생 행 위치 탐색
  B1SheetReader  : B1Sheet 날짜폴더 탐색, 종목 universe 수집
  B3SheetWriter  : B3Sheet 폴더 생성, CSV + Parquet 저장
  SignalAnalyzer : 날짜폴더별 루프, 윈도우 계산, 결과 조립 (메인 오케스트레이터)

의존성: pandas, pyarrow (pip install pandas pyarrow)
"""

import os
import glob
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

# ──────────────────────────────────────────────────────────────────
# 로깅
# ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────
BASE_DIR: str = r"C:\Users\yslee\PycharmProjects\FinanceMLOps\Data"
M: int = 5                      # 신호발생일 이전 거래일 수
N: int = 5                      # 신호발생일 이후 거래일 수
MARKETS: Optional[List[str]] = None   # None → B1Sheet 있는 모든 market 자동 탐색
SAVE_CSV: bool = True
SAVE_PARQUET: bool = True
CSV_ENCODING: str = "utf-8-sig"       # 한글 깨짐 방지 (엑셀 호환)

CLOSE_COL: str = "Close"
NAME_COL:  str = "Name"
SYMBOL_COL: str = "Symbol"
DATE_COL_CANDIDATES: List[str] = [
    "Date", "date", "DATE", "Datetime", "datetime", "기준일"
]

# B1Sheet 신호 파일명(정규화 stem) → (A1Sheet 컬럼명, 발생 조건)
#   "gt0" : 컬럼값 > 0 이면 신호 발생
#   "lt0" : 컬럼값 < 0 이면 신호 발생 (과매도)
#   RSI_Signal_Sum / CCI_Signal_Sum 은 같은 컬럼을 공유하지만
#   over_buy_* = >0(과매수),  over_sell_* = <0(과매도) 로 구분한다.
SIGNAL_DEFS: Dict[str, Tuple[str, str]] = {
    "df_bear":          ("RSI_BearDiv_Sum",        "gt0"),
    "df_bear_hidden":   ("RSI_Hidden_BearDiv_Sum", "gt0"),
    "df_bull":          ("RSI_BullDiv_Sum",         "gt0"),
    "df_bull_hidden":   ("RSI_Hidden_BullDiv_Sum",  "gt0"),
    "df_downtrend":     ("RSI_DownTrend_Sum",       "gt0"),
    "df_over_buy_cci":  ("CCI_Signal_Sum",          "gt0"),
    "df_over_buy_rsi":  ("RSI_Signal_Sum",          "gt0"),
    "df_over_sell_cci": ("CCI_Signal_Sum",          "lt0"),
    "df_over_sell_rsi": ("RSI_Signal_Sum",          "lt0"),
    "df_sell":          ("Sell_Signal",             "gt0"),
    "df_uptrend":       ("RSI_UpTrend_Sum",         "gt0"),
}


# ──────────────────────────────────────────────────────────────────
# SignalConfig : 신호 파일명 매핑 + 출력 컬럼 이름 생성
# ──────────────────────────────────────────────────────────────────
class SignalConfig:
    """신호 파일명 ↔ (A1Sheet 컬럼, 발생 조건) 매핑과 출력 컬럼 이름을 담당한다."""

    def __init__(
        self,
        signal_defs: Dict[str, Tuple[str, str]],
        m: int,
        n: int,
    ) -> None:
        self._defs = signal_defs
        self.m = m
        self.n = n
        self._offsets: List[int] = list(range(-m, n + 1))

    # ── 정규화 ──────────────────────────────────────────────────────
    @staticmethod
    def normalize_key(signal_file: str) -> str:
        """
        파일명을 SIGNAL_DEFS 키 형식으로 정규화한다.
        'df_bull.hidden.parquet' → 'df_bull_hidden'
        'df_bull_hidden.parquet' → 'df_bull_hidden'
        """
        stem = signal_file
        if stem.lower().endswith(".parquet"):
            stem = stem[: -len(".parquet")]
        return stem.replace(".", "_")

    # ── 조회 ────────────────────────────────────────────────────────
    def lookup(self, signal_file: str) -> Optional[Tuple[str, str]]:
        """신호 파일명 → (컬럼명, 조건). 매핑 없으면 None."""
        return self._defs.get(self.normalize_key(signal_file))

    # ── 출력 컬럼 이름 ───────────────────────────────────────────────
    @property
    def offsets(self) -> List[int]:
        return self._offsets

    @staticmethod
    def price_col(o: int) -> str:
        if o < 0:
            return f"Price_{o}"
        if o == 0:
            return "Price_기준일"
        return f"Price_+{o}"

    @staticmethod
    def rate_col(o: int) -> str:
        if o < 0:
            return f"Rate_{o}"
        if o == 0:
            return "Rate_기준일"
        return f"Rate_+{o}"

    def output_columns(self) -> List[str]:
        cols = [NAME_COL, SYMBOL_COL, "Signal_file_name", "기준일(신호발생일)"]
        cols += [self.price_col(o) for o in self._offsets]
        cols += [self.rate_col(o)  for o in self._offsets]
        return cols


# ──────────────────────────────────────────────────────────────────
# A1SheetLoader : 종목 파일 로드·캐시, 발생 위치 탐색
# ──────────────────────────────────────────────────────────────────
class A1SheetLoader:
    """A1Sheet 폴더의 종목별 parquet 파일을 읽고 메모리에 캐시한다."""

    def __init__(self, a1_root: str) -> None:
        self.a1_root = a1_root
        self._cache: Dict[str, Optional[Tuple[pd.DataFrame, str]]] = {}

    # ── 파일 경로 ────────────────────────────────────────────────────
    def file_path(self, name: str, symbol: str) -> str:
        return os.path.join(self.a1_root, f"{name}({symbol}).parquet")

    # ── 로드 (캐시 적중 시 재사용) ───────────────────────────────────
    def load(self, name: str, symbol: str) -> Optional[Tuple[pd.DataFrame, str]]:
        """
        (df, date_col) 반환. 파일 없거나 오류 시 None.
        같은 종목은 두 번 읽지 않는다.
        """
        key = f"{name}({symbol})"
        if key in self._cache:
            return self._cache[key]

        path = self.file_path(name, symbol)
        result = None

        if not os.path.exists(path):
            log.debug("A1Sheet 파일 없음: %s", path)
        else:
            try:
                df = pd.read_parquet(path)
                if df.empty:
                    log.warning("A1Sheet 빈 파일: %s", path)
                else:
                    df, date_col = self._normalize(df, path)
                    result = (df, date_col)
            except Exception as exc:
                log.warning("A1Sheet 읽기 실패 [%s]: %s", path, exc)

        self._cache[key] = result
        return result

    # ── 내부: 날짜 정규화 & 정렬 ────────────────────────────────────
    @staticmethod
    def _normalize(df: pd.DataFrame, path: str) -> Tuple[pd.DataFrame, str]:
        date_col: Optional[str] = None

        # DatetimeIndex → 컬럼으로
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            date_col = df.columns[0]

        # 후보 컬럼 탐색
        if date_col is None:
            for c in DATE_COL_CANDIDATES:
                if c in df.columns:
                    date_col = c
                    break

        # 최후 수단: 첫 번째 컬럼
        if date_col is None:
            date_col = df.columns[0]
            log.warning("날짜 컬럼 추정(첫 컬럼=%s): %s", date_col, path)

        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        before = len(df)
        df = df.dropna(subset=[date_col]).sort_values(date_col).reset_index(drop=True)
        dropped = before - len(df)
        if dropped:
            log.warning("날짜 파싱 실패 %d행 제거: %s", dropped, path)

        return df, date_col

    # ── 신호 발생 위치 탐색 ──────────────────────────────────────────
    @staticmethod
    def find_occurrences(series: pd.Series, cond: str) -> np.ndarray:
        """
        발생 조건을 만족하는 행 위치(0-based int array) 반환.
        NaN → False 처리. cond = 'gt0' | 'lt0'.
        """
        vals = pd.to_numeric(series, errors="coerce").to_numpy(dtype="float64")
        if cond == "gt0":
            return np.flatnonzero(vals > 0)
        if cond == "lt0":
            return np.flatnonzero(vals < 0)
        raise ValueError(f"알 수 없는 발생 조건: {cond!r}  (gt0 또는 lt0 만 허용)")

    # ── 컬럼 탐색 (대소문자 무시) ────────────────────────────────────
    @staticmethod
    def resolve_col(df: pd.DataFrame, col_name: str) -> Optional[str]:
        """정확 일치 우선, 없으면 대소문자 무시 매칭. 없으면 None."""
        if col_name in df.columns:
            return col_name
        lower_map = {c.lower(): c for c in df.columns}
        return lower_map.get(col_name.lower())

    # ── 캐시 초기화 ──────────────────────────────────────────────────
    def clear_cache(self) -> None:
        self._cache.clear()


# ──────────────────────────────────────────────────────────────────
# B1SheetReader : B1Sheet 날짜폴더 탐색, 종목 universe 수집
# ──────────────────────────────────────────────────────────────────
@dataclass
class DateFolderInfo:
    """B1Sheet 날짜폴더 하나에 대한 메타 정보."""
    date_str: str                              # 폴더명 (= 기준일)
    path: str                                  # 절대 경로
    signal_files: List[str] = field(default_factory=list)   # 폴더 안 parquet 파일명 목록


class B1SheetReader:
    """B1Sheet 폴더를 탐색하고 날짜폴더·종목 universe 를 제공한다."""

    def __init__(self, b1_root: str) -> None:
        self.b1_root = b1_root

    # ── 날짜폴더 목록 ────────────────────────────────────────────────
    def iter_date_folders(self) -> List[DateFolderInfo]:
        """B1Sheet 아래 날짜폴더를 날짜 오름차순으로 반환. 비어있는 폴더도 포함."""
        infos: List[DateFolderInfo] = []
        pattern = os.path.join(self.b1_root, "*")
        for entry in sorted(glob.glob(pattern)):
            if not os.path.isdir(entry):
                continue
            date_str = os.path.basename(entry)
            sig_files = sorted(
                os.path.basename(f)
                for f in glob.glob(os.path.join(entry, "*.parquet"))
            )
            infos.append(DateFolderInfo(date_str=date_str,
                                        path=entry,
                                        signal_files=sig_files))
        return infos

    # ── 날짜폴더 안의 단일 신호 파일에서 (Name, Symbol) 집합 수집 ───
    def read_universe(
        self,
        date_folder: DateFolderInfo,
        signal_file: str,
    ) -> Set[Tuple[str, str]]:
        """
        date_folder 안의 signal_file 을 읽어 (Name, Symbol) 집합 반환.
        파일 없음 / 빈 파일 / 컬럼 누락 / 읽기 오류는 모두 빈 집합으로 처리.
        """
        fpath = os.path.join(date_folder.path, signal_file)

        # 파일 없음 (그날 신호 없는 날짜폴더 → 정상)
        if not os.path.exists(fpath):
            return set()

        try:
            sdf = pd.read_parquet(fpath)
        except Exception as exc:
            log.warning("B1Sheet 읽기 실패 [%s]: %s", fpath, exc)
            return set()

        if sdf.empty:
            log.debug("B1Sheet 빈 파일: %s", fpath)
            return set()

        missing = [c for c in (NAME_COL, SYMBOL_COL) if c not in sdf.columns]
        if missing:
            log.warning("B1Sheet 컬럼 누락 %s [%s]", missing, fpath)
            return set()

        result: Set[Tuple[str, str]] = set()
        for _, row in sdf[[NAME_COL, SYMBOL_COL]].dropna().iterrows():
            name   = str(row[NAME_COL]).strip()
            symbol = str(row[SYMBOL_COL]).strip()
            if name and symbol:
                result.add((name, symbol))
        return result

    # ── 전체 날짜폴더에서 signal_file 의 종목 합집합 ─────────────────
    def collect_all_universe(self, signal_file: str) -> Set[Tuple[str, str]]:
        universe: Set[Tuple[str, str]] = set()
        for df_info in self.iter_date_folders():
            universe |= self.read_universe(df_info, signal_file)
        return universe


# ──────────────────────────────────────────────────────────────────
# B3SheetWriter : B3Sheet 저장
# ──────────────────────────────────────────────────────────────────
class B3SheetWriter:
    """B3Sheet 폴더 생성과 CSV / Parquet 저장을 담당한다."""

    def __init__(self, b3_root: str) -> None:
        self.b3_root = b3_root

    def save(
        self,
        df: pd.DataFrame,
        date_str: str,
        signal_file: str,
    ) -> None:
        """
        B3Sheet/{date_str}/{signal_file} 로 저장 (CSV + Parquet).
        폴더가 없으면 자동 생성.
        """
        out_dir = os.path.join(self.b3_root, date_str)
        os.makedirs(out_dir, exist_ok=True)

        stem = (signal_file[: -len(".parquet")]
                if signal_file.lower().endswith(".parquet")
                else signal_file)

        if SAVE_PARQUET:
            pq_path = os.path.join(out_dir, signal_file)
            try:
                df.to_parquet(pq_path, index=False)
            except Exception as exc:
                log.error("Parquet 저장 실패 [%s]: %s", pq_path, exc)

        if SAVE_CSV:
            csv_path = os.path.join(out_dir, stem + ".csv")
            try:
                df.to_csv(csv_path, index=False, encoding=CSV_ENCODING)
            except Exception as exc:
                log.error("CSV 저장 실패 [%s]: %s", csv_path, exc)


# ──────────────────────────────────────────────────────────────────
# SignalAnalyzer : 메인 오케스트레이터
# ──────────────────────────────────────────────────────────────────
class SignalAnalyzer:
    """
    B1Sheet 날짜폴더 구조를 B3Sheet 와 평행하게 유지하면서
    각 날짜폴더·신호 조합에 대한 가격 윈도우·변동률 결과를 생성한다.

    ┌ 날짜폴더 루프 ─────────────────────────────────────────────┐
    │  ┌ 신호 파일 루프 ─────────────────────────────────────┐   │
    │  │  B1Sheet에서 그 날 종목 universe 읽기               │   │
    │  │  A1Sheet에서 역대 신호 발생일 전부 탐색             │   │
    │  │  [-M, +N] 윈도우 Close + 변동률 계산               │   │
    │  │  B3Sheet/{날짜폴더}/{신호}.parquet + .csv 저장      │   │
    │  └─────────────────────────────────────────────────────┘   │
    └────────────────────────────────────────────────────────────┘
    """

    def __init__(
        self,
        market: str,
        base_dir: str = BASE_DIR,
        m: int = M,
        n: int = N,
    ) -> None:
        self.market = market
        self.m = m
        self.n = n

        self.b1_root = os.path.join(base_dir, market, "B1Sheet")
        self.a1_root = os.path.join(base_dir, market, "A1Sheet")
        self.b3_root = os.path.join(base_dir, market, "B3Sheet")

        self.config   = SignalConfig(SIGNAL_DEFS, m, n)
        self.a1loader = A1SheetLoader(self.a1_root)
        self.b1reader = B1SheetReader(self.b1_root)
        self.b3writer = B3SheetWriter(self.b3_root)

    # ── 윈도우 행 생성 ───────────────────────────────────────────────
    def _build_rows(
        self,
        df: pd.DataFrame,
        date_col: str,
        name: str,
        symbol: str,
        signal_file: str,
        occ_positions: np.ndarray,
    ) -> List[dict]:
        rows: List[dict] = []
        closes = df[CLOSE_COL].to_numpy(dtype="float64")
        dates  = df[date_col]
        n_rows = len(df)
        offs   = self.config.offsets

        for raw_pos in occ_positions:
            pos  = int(raw_pos)
            base = closes[pos]
            valid_base = not (np.isnan(base) or base == 0.0)

            row: dict = {
                NAME_COL:          name,
                SYMBOL_COL:        symbol,
                "Signal_file_name": signal_file,
                "기준일(신호발생일)": dates.iloc[pos].strftime("%Y-%m-%d"),
            }
            for o in offs:
                j     = pos + o
                price = closes[j] if 0 <= j < n_rows else np.nan
                row[self.config.price_col(o)] = price
                if valid_base and not np.isnan(price):
                    row[self.config.rate_col(o)] = 100.0 * (price - base) / base
                else:
                    row[self.config.rate_col(o)] = np.nan

            rows.append(row)
        return rows

    # ── 신호 파일 1개 × 날짜폴더 1개 처리 ──────────────────────────
    def _process_one(
        self,
        date_info: DateFolderInfo,
        signal_file: str,
    ) -> Optional[pd.DataFrame]:
        """
        그날 B1Sheet 에 있는 종목들의 역대 신호 이벤트를 모아 DataFrame 반환.
        이벤트 0건이면 None.
        """
        # ① 신호 정의 확인
        sig_def = self.config.lookup(signal_file)
        if sig_def is None:
            log.warning("SIGNAL_DEFS 매핑 없음: %s  (필요하면 매핑 추가)", signal_file)
            return None
        col_name, cond = sig_def

        # ② 그날 종목 universe 읽기 (빈 날짜폴더·빈 파일 → 빈 집합)
        universe = self.b1reader.read_universe(date_info, signal_file)
        if not universe:
            log.debug("[%s] %s 종목 universe 0건, 건너뜀", date_info.date_str, signal_file)
            return None

        # ③ 종목별 역대 신호 이벤트 수집
        all_rows: List[dict] = []
        stats = {"missing_file": 0, "no_close": 0, "no_col": 0, "no_event": 0}

        for name, symbol in sorted(universe):
            loaded = self.a1loader.load(name, symbol)
            if loaded is None:
                stats["missing_file"] += 1
                continue

            a1_df, date_col = loaded

            if CLOSE_COL not in a1_df.columns:
                stats["no_close"] += 1
                continue

            actual_col = self.a1loader.resolve_col(a1_df, col_name)
            if actual_col is None:
                stats["no_col"] += 1
                log.debug("신호 컬럼 없음 [%s]: %s(%s)", col_name, name, symbol)
                continue

            occ = self.a1loader.find_occurrences(a1_df[actual_col], cond)
            if len(occ) == 0:
                stats["no_event"] += 1
                continue

            all_rows.extend(
                self._build_rows(a1_df, date_col, name, symbol, signal_file, occ)
            )

        # ④ 통계 로그 (건수가 있을 때만)
        if any(stats.values()):
            log.debug(
                "[%s][%s] A1Sheet누락=%d, Close없음=%d, 컬럼없음=%d, 이벤트0=%d",
                date_info.date_str, signal_file,
                stats["missing_file"], stats["no_close"],
                stats["no_col"], stats["no_event"],
            )

        if not all_rows:
            return None

        out = pd.DataFrame(all_rows, columns=self.config.output_columns())
        out = out.sort_values([SYMBOL_COL, "기준일(신호발생일)"]).reset_index(drop=True)
        return out

    # ── market 전체 실행 ─────────────────────────────────────────────
    def run(self) -> None:
        if not os.path.isdir(self.b1_root):
            log.error("B1Sheet 폴더 없음: %s", self.b1_root)
            return
        if not os.path.isdir(self.a1_root):
            log.error("A1Sheet 폴더 없음: %s", self.a1_root)
            return

        date_folders = self.b1reader.iter_date_folders()
        if not date_folders:
            log.warning("[%s] B1Sheet 날짜폴더 없음", self.market)
            return

        log.info(
            "[%s] 시작 | 날짜폴더 %d개 | 윈도우 -%d/+%d",
            self.market, len(date_folders), self.m, self.n,
        )

        total_saved = 0
        total_rows  = 0

        for date_info in date_folders:
            if not date_info.signal_files:
                # B1Sheet 날짜폴더가 비어있는 날 → B3Sheet 에도 아무것도 저장하지 않음
                log.debug("[%s] %s 신호 파일 없음(빈 날), 건너뜀",
                          self.market, date_info.date_str)
                continue

            for signal_file in date_info.signal_files:
                out = self._process_one(date_info, signal_file)
                if out is None or out.empty:
                    continue
                self.b3writer.save(out, date_info.date_str, signal_file)
                total_saved += 1
                total_rows  += len(out)
                log.info(
                    "[%s] %s / %s → %d행 저장",
                    self.market, date_info.date_str, signal_file, len(out),
                )

        log.info(
            "[%s] 완료 | 저장 파일 %d개 | 총 %d행",
            self.market, total_saved, total_rows,
        )


# ──────────────────────────────────────────────────────────────────
# 실행 진입점
# ──────────────────────────────────────────────────────────────────
def detect_markets(base_dir: str) -> List[str]:
    """BASE_DIR 아래 B1Sheet 폴더가 있는 market 폴더를 자동 탐색한다."""
    if not os.path.isdir(base_dir):
        log.error("BASE_DIR 없음: %s", base_dir)
        return []
    found = []
    for name in sorted(os.listdir(base_dir)):
        p = os.path.join(base_dir, name)
        if os.path.isdir(p) and os.path.isdir(os.path.join(p, "B1Sheet")):
            found.append(name)
    return found


def main() -> None:
    markets = MARKETS if MARKETS else detect_markets(BASE_DIR)
    if not markets:
        log.error("처리할 market 없음 (BASE_DIR=%s)", BASE_DIR)
        return

    log.info("Markets: %s", markets)
    for market in markets:
        analyzer = SignalAnalyzer(market=market, base_dir=BASE_DIR, m=M, n=N)
        analyzer.run()


if __name__ == "__main__":
    main()