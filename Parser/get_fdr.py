import FinanceDataReader as fdr
import dask.dataframe as dd
from dask import delayed
import pandas as pd
import time
import os
import random


# ---------------------------------------------------------------------------
# 시장별 로컬 CSV 설정 (최신 fdr 스펙 반영)
#   - file       : 종목 리스트 CSV 파일명 (스크립트와 같은 폴더에 위치)
#   - encoding   : 해당 CSV의 인코딩
#   - code_col   : 종목 식별자 컬럼명 (한국: 'Code', 미국: 'Symbol')
#   - pad_zero   : True 면 6자리로 0-padding (한국 주식용)
#   - fdr_prefix : 최신 fdr 규칙에 맞는 시장 접두어 (예: NAVER)
# ---------------------------------------------------------------------------
MARKET_CONFIG = {
    "KOSPI":  {"file": "kospi_list.parquet",  "encoding": "utf-8-sig", "code_col": "Code",   "pad_zero": True,  "fdr_prefix": "NAVER"},
    "KOSDAQ": {"file": "kosdaq_list.parquet", "encoding": "utf-8-sig", "code_col": "Code",   "pad_zero": True,  "fdr_prefix": "NAVER"},
    "NYSE":   {"file": "nyse_list.parquet",   "encoding": "utf-8-sig", "code_col": "Symbol", "pad_zero": False, "fdr_prefix": None},
    "NASDAQ": {"file": "nasdaq_list.parquet", "encoding": "utf-8-sig", "code_col": "Symbol", "pad_zero": False, "fdr_prefix": None},
}


def load_stock_list_from_csv(market_name):
    """
    fdr.StockListing()이 JSONDecodeError를 자주 던지므로,
    로컬 CSV에서 종목 리스트를 읽어 ['Symbol', 'Name'] 형태로 반환한다.
    한국/미국 시장 모두 'Symbol' 컬럼으로 통일하여 다운스트림 코드 일관성 보장.
    인코딩은 utf-8-sig 우선, 실패 시 cp949로 폴백.
    """
    key = market_name.upper()
    if key not in MARKET_CONFIG:
        raise ValueError(f"지원하지 않는 시장입니다: {market_name}. "
                         f"가능한 값: {list(MARKET_CONFIG.keys())}")

    cfg = MARKET_CONFIG[key]
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, cfg["file"])

    if not os.path.exists(file_path):
        target_lower = cfg["file"].lower()
        match = None
        if os.path.isdir(base_dir):
            for fname in os.listdir(base_dir):
                if fname.lower() == target_lower:
                    match = fname
                    break
        if match:
            file_path = os.path.join(base_dir, match)
        else:
            raise FileNotFoundError(f"종목 리스트 파일을 찾을 수 없습니다: {file_path}")

    if file_path.endswith('.parquet'):
        df = pd.read_parquet(file_path)
    else:
        try:
            df = pd.read_csv(file_path, encoding=cfg["encoding"])
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding="utf-8-sig")

    if cfg["code_col"] != "Symbol":
        df = df.rename(columns={cfg["code_col"]: "Symbol"})

    df["Symbol"] = df["Symbol"].astype(str).str.strip()
    df["Name"] = df["Name"].astype(str).str.strip()

    if cfg["pad_zero"]:
        df["Symbol"] = df["Symbol"].str.zfill(6)

    df = df[(df["Symbol"] != "") & (df["Symbol"].str.lower() != "nan")]
    df = df.drop_duplicates(subset=["Symbol"]).reset_index(drop=True)

    return df[["Symbol", "Name"]]


@delayed
def fetch_stock_data(symbol, name, start_date, fdr_prefix=None, sleep_interval=0.1, max_retries=3):
    """
    개별 종목 데이터를 가져오는 지연 실행(Delayed) 함수.
    - Dask 병렬 처리 시 서버 과부하를 막기 위해 내부에서 sleep 처리
    - 연결 끊김(10054 에러 등) 발생 시 설정된 횟수만큼 재시도 후 안전하게 스킵
    """
    query_symbol = f"{fdr_prefix}:{symbol}" if fdr_prefix else symbol
    
    for attempt in range(max_retries):
        try:
            # 실제 요청 전 딜레이 (Dask Worker들이 동시에 요청 쏘는 것을 방지)
            # 약간의 랜덤성을 부여하면 서버의 차단 확률을 더 낮출 수 있습니다.
            time.sleep(sleep_interval + random.uniform(0.01, 0.05))
            
            df = fdr.DataReader(query_symbol, start_date)
 
            if df is None or df.empty:
                return None

            if df.index.name is None or df.index.name.lower() != 'date':
                df.index.name = 'Date'
 
            df['Symbol'] = symbol
            df['Name']   = name
            df = df.reset_index()
            return df
 
        except Exception as e:
            if attempt < max_retries - 1:
                # 에러 발생 시 2초 대기 후 재시도
                time.sleep(2)
            else:
                # 최종 실패 시 스킵 처리 (None 반환)
                print(f"실패 (스킵됨): {name} ({symbol}) - {e}", flush=True)
                return None

def get_kospi200_dask_data(start_date, num_stocks, output_path, market_name, sleep_interval=0.1):
    print("종목 리스트를 불러오는 중...", flush=True)
    stocks = load_stock_list_from_csv(market_name)
    target_stocks = stocks.head(num_stocks)
    
    key = market_name.upper()
    cfg = MARKET_CONFIG.get(key, {"fdr_prefix": None})
    fdr_prefix = cfg.get("fdr_prefix")

    # ===== [증분 수집 (Delta Ingestion) 자동 탐지] =====
    target_dir = os.path.join(output_path, market_name) if not output_path.endswith(market_name) else output_path
    parquet_path = os.path.join(target_dir, "raw_data.parquet")
    tmp_parquet_path = os.path.join(target_dir, "raw_data.parquet.tmp")
    bak_parquet_path = os.path.join(target_dir, "raw_data.parquet.bak")
    
    existing_df = None
    fetch_start_date = start_date

    if os.path.exists(parquet_path):
        try:
            existing_df = pd.read_parquet(parquet_path)
            if not existing_df.empty and 'Date' in existing_df.columns:
                existing_df['Date'] = pd.to_datetime(existing_df['Date'])
                max_date = existing_df['Date'].max()
                if pd.notnull(max_date):
                    # 안전 윈도우 3일 적용 (휴장일/주말/수정주가 보정 반영)
                    safe_start = (max_date - pd.Timedelta(days=3)).strftime('%Y-%m-%d')
                    print(f"[{market_name}] 기존 parquet 파일 감지 (최신 날짜: {max_date.strftime('%Y-%m-%d')}). 증분 수집 시작일: {safe_start}")
                    fetch_start_date = safe_start
        except Exception as e:
            print(f"[{market_name}] 기존 parquet 파일 로드 실패 ({e}). 전체 수집({start_date})으로 폴백합니다.")
            existing_df = None

    delayed_tasks = []
    
    print(f"데이터 수집 작업 생성 중 (대상: {len(target_stocks)} 종목, 시작일: {fetch_start_date})...", flush=True)
    for index, row in target_stocks.iterrows():
        symbol = row['Symbol']
        name = row['Name']
        task = fetch_stock_data(symbol, name, fetch_start_date, fdr_prefix=fdr_prefix, sleep_interval=sleep_interval)
        delayed_tasks.append(task)

    print("Dask 그래프 계산 및 데이터 수집 시작...", flush=True)
    
    import dask
    results = dask.compute(*delayed_tasks)
    valid_results = [r for r in results if r is not None]

    if not valid_results:
        print("신규 수집된 데이터가 없습니다.", flush=True)
        return existing_df, pd.DataFrame()

    new_df = pd.concat(valid_results, ignore_index=True)
    new_df['Date'] = pd.to_datetime(new_df['Date'])

    # ===== [안전 중복 제거 및 병합 (Deduplicated Merge)] =====
    if existing_df is not None and not existing_df.empty:
        merged_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        merged_df = new_df

    merged_df['Date_str'] = merged_df['Date'].dt.strftime('%Y-%m-%d')
    # 동일 날짜 및 동일 종목에 대해 나중에 수집된 신규 데이터로 안전하게 덮어쓰기 (keep='last')
    merged_df = merged_df.drop_duplicates(subset=['Date_str', 'Symbol'], keep='last')
    merged_df = merged_df.drop(columns=['Date_str'])
    merged_df = merged_df.sort_values(by=['Symbol', 'Date']).reset_index(drop=True)

    if not os.path.exists(target_dir):
        os.makedirs(target_dir, exist_ok=True)

    save_df = merged_df.copy()
    save_df['Date'] = save_df['Date'].dt.strftime('%Y-%m-%d')

    # ===== [원자적 파일 교체 (Atomic Write Pattern)] =====
    try:
        # 1. 임시 파일에 쓰기
        save_df.to_parquet(tmp_parquet_path, index=False, engine='pyarrow')
        
        # 2. 기존 원본이 있다면 백업 생성
        if os.path.exists(parquet_path):
            if os.path.exists(bak_parquet_path):
                os.remove(bak_parquet_path)
            os.rename(parquet_path, bak_parquet_path)

        # 3. 임시 파일을 최종 타겟 파일로 원자적 교체
        os.replace(tmp_parquet_path, parquet_path)
        
        # 4. 쓰기 성공 시 백업 제거
        if os.path.exists(bak_parquet_path):
            os.remove(bak_parquet_path)
            
        print(f"[{market_name}] raw_data.parquet 원자적 쓰기 완료! (총 {len(save_df)} 행, 증분 {len(new_df)} 건)", flush=True)
    except Exception as e:
        print(f"[{market_name}] Parquet 저장 중 오류 발생: {e}")
        # 오류 발생 시 임시 파일 정리 및 백업 복구
        if os.path.exists(tmp_parquet_path):
            os.remove(tmp_parquet_path)
        if os.path.exists(bak_parquet_path) and not os.path.exists(parquet_path):
            os.rename(bak_parquet_path, parquet_path)
        raise e

    return save_df, new_df

# 메인 실행부
if __name__ == "__main__":

    CONFIG1 = {
        "start_date": "2000-01-01",
        "num_stocks": 4000,
        "output_path": "C:/Users/yslee/PycharmProjects/FinanceMLOps/Data/KOSPI",
        "market_name": "KOSPI",
        "sleep_interval": 0.05
    }

    CONFIG2 = {
        "start_date": "2000-01-01",
        "num_stocks": 4000,
        "output_path": "C:/Users/yslee/PycharmProjects/FinanceMLOps/Data/KOSDAQ",
        "market_name": "KOSDAQ",
        "sleep_interval": 0.05
    }

    CONFIG3 = {
        "start_date": "2000-01-01",
        "num_stocks": 4000,
        "output_path": "C:/Users/yslee/PycharmProjects/FinanceMLOps/Data/NASDAQ",
        "market_name": "NASDAQ",
        "sleep_interval": 0.05
    }

    CONFIG4 = {
        "start_date": "2000-01-01",
        "num_stocks": 4000,
        "output_path": "C:/Users/yslee/PycharmProjects/FinanceMLOps/Data/NYSE",
        "market_name": "NYSE",
        "sleep_interval": 0.05
    }

    for CONFIG in [CONFIG1, CONFIG2, CONFIG3, CONFIG4]:
        ddf_result = get_kospi200_dask_data(
            start_date=CONFIG["start_date"],
            num_stocks=CONFIG["num_stocks"],
            output_path=CONFIG["output_path"],
            sleep_interval=CONFIG["sleep_interval"],
            market_name=CONFIG["market_name"]
        )