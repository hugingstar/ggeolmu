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
def fetch_stock_data(symbol, name, start_date, fdr_prefix=None, sleep_interval=0.005, max_retries=3):
    """
    개별 종목 데이터를 가져오는 지연 실행(Delayed) 함수.
    - Dask 멀티스레딩 병렬 수집 시 부하 분산을 위한 가벼운 딜레이 적용
    - 연결 끊김 발생 시 설정된 횟수만큼 백오프 재시도 후 안전 스킵
    """
    query_symbol = f"{fdr_prefix}:{symbol}" if fdr_prefix else symbol
    
    for attempt in range(max_retries):
        try:
            if sleep_interval > 0:
                time.sleep(sleep_interval + random.uniform(0.001, 0.005))
            
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
                time.sleep(0.5 * (attempt + 1))
            else:
                print(f"실패 (스킵됨): {name} ({symbol}) - {e}", flush=True)
                return None

def get_db_max_date():
    """PostgreSQL public.raw_stock_data 테이블에서 최신 수집 일자를 안전하게 조회"""
    try:
        from db_manager import DBManager
        db = DBManager()
        if db.conn:
            rows = db.read_query_direct("SELECT MAX(date) FROM public.raw_stock_data;")
            if rows and rows[0] and rows[0][0]:
                return pd.to_datetime(rows[0][0])
    except Exception:
        pass
    return None

def get_kospi200_dask_data(start_date, num_stocks, output_path, market_name, sleep_interval=0.005, save_parquet=False):
    print("종목 리스트를 불러오는 중...", flush=True)
    stocks = load_stock_list_from_csv(market_name)
    target_stocks = stocks.head(num_stocks)
    
    key = market_name.upper()
    cfg = MARKET_CONFIG.get(key, {"fdr_prefix": None})
    fdr_prefix = cfg.get("fdr_prefix")

    # ===== [경로 정속화: Data/{MARKET}] =====
    target_dir = os.path.join(output_path, market_name) if not output_path.endswith(market_name) else output_path
    if not os.path.exists(target_dir):
        os.makedirs(target_dir, exist_ok=True)

    parquet_path = os.path.join(target_dir, "raw_data.parquet")
    tmp_parquet_path = os.path.join(target_dir, "raw_data.parquet.tmp")
    bak_parquet_path = os.path.join(target_dir, "raw_data.parquet.bak")
    
    existing_df = None
    is_delta_mode = False

    # 1. DB 최신 날짜 우선 탐지 (Parquet 파일 의존성 제거)
    max_date = get_db_max_date()
    
    # 2. DB에 기록이 없으면 로컬 Parquet 확인
    if max_date is None and os.path.exists(parquet_path):
        try:
            existing_df = pd.read_parquet(parquet_path)
            if not existing_df.empty and 'Date' in existing_df.columns:
                existing_df['Date'] = pd.to_datetime(existing_df['Date'])
                max_date = existing_df['Date'].max()
        except Exception as e:
            existing_df = None

    if max_date is not None and pd.notnull(max_date):
        days_diff = (pd.Timestamp.now() - max_date).days
        if days_diff <= 14:
            is_delta_mode = True
            print(f"[{market_name}] DB/스토리지 최신 날짜 감지 ({max_date.strftime('%Y-%m-%d')}). 0.5초 초고속 일괄 증분 수집(Bulk Fetch)을 실행합니다.")

    new_df = None

    # ===== [1) 초고속 일괄 증분 수집 (StockListing Bulk Fetch - 0.5초 완료)] =====
    if is_delta_mode:
        try:
            print(f"[{market_name}] StockListing 기반 전 종목 시세 일괄 다운로드 중...", flush=True)
            listing_df = fdr.StockListing(market_name)
            
            if listing_df is not None and not listing_df.empty:
                code_col = cfg.get("code_col", "Symbol")
                if code_col not in listing_df.columns and "Code" in listing_df.columns:
                    code_col = "Code"
                
                rename_dict = {
                    code_col: 'Symbol',
                    'Name': 'Name',
                    'Open': 'Open',
                    'High': 'High',
                    'Low': 'Low',
                    'Close': 'Close',
                    'Volume': 'Volume',
                    'Chg': 'Change',
                    'Changes': 'Change'
                }
                
                bulk_df = listing_df.rename(columns=rename_dict).copy()
                
                if cfg.get("pad_zero", False) and 'Symbol' in bulk_df.columns:
                    bulk_df['Symbol'] = bulk_df['Symbol'].astype(str).str.zfill(6)
                
                today_str = pd.Timestamp.now().strftime('%Y-%m-%d')
                bulk_df['Date'] = pd.to_datetime(today_str)
                
                req_cols = ['Date', 'Symbol', 'Name', 'Open', 'High', 'Low', 'Close', 'Volume', 'Change']
                for col in req_cols:
                    if col not in bulk_df.columns:
                        bulk_df[col] = 0.0 if col in ['Open','High','Low','Close','Volume','Change'] else None
                
                new_df = bulk_df[req_cols].dropna(subset=['Symbol', 'Close'])
                print(f"[{market_name}] 초고속 일괄 증분 수집 성공! (신규 {len(new_df)} 종목 시세 취득 완료)", flush=True)
        except Exception as e:
            print(f"[{market_name}] 일괄 수집 중 오류 발생 ({e}). 개별 Dask 수집으로 자동 폴백합니다.")
            new_df = None

    # ===== [2) 전체 수집 또는 폴백 수집 (Dask 병렬 수집)] =====
    if new_df is None or new_df.empty:
        fetch_start_date = start_date
        delayed_tasks = []
        
        print(f"개별 데이터 수집 작업 생성 중 (대상: {len(target_stocks)} 종목, 시작일: {fetch_start_date})...", flush=True)
        for index, row in target_stocks.iterrows():
            symbol = row['Symbol']
            name = row['Name']
            task = fetch_stock_data(symbol, name, fetch_start_date, fdr_prefix=fdr_prefix, sleep_interval=sleep_interval)
            delayed_tasks.append(task)

        print("Dask 멀티스레드 병렬 계산 및 데이터 수집 시작...", flush=True)
        import dask
        results = dask.compute(*delayed_tasks, scheduler='threads', num_workers=32)
        valid_results = [r for r in results if r is not None]

        if not valid_results:
            print("신규 수집된 데이터가 없습니다.", flush=True)
            return None, pd.DataFrame()

        new_df = pd.concat(valid_results, ignore_index=True)
        new_df['Date'] = pd.to_datetime(new_df['Date'])

    # Parquet 파일 생성이 명시적으로 요청된 경우에만 저장 (기본값 False: DB 직행 적재)
    if save_parquet:
        if existing_df is not None and not existing_df.empty:
            merged_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            merged_df = new_df

        merged_df['Date_str'] = merged_df['Date'].dt.strftime('%Y-%m-%d')
        merged_df = merged_df.drop_duplicates(subset=['Date_str', 'Symbol'], keep='last')
        merged_df = merged_df.drop(columns=['Date_str'])
        merged_df = merged_df.sort_values(by=['Symbol', 'Date']).reset_index(drop=True)

        save_df = merged_df.copy()
        save_df['Date'] = save_df['Date'].dt.strftime('%Y-%m-%d')

        try:
            save_df.to_parquet(tmp_parquet_path, index=False, engine='pyarrow')
            if os.path.exists(parquet_path):
                if os.path.exists(bak_parquet_path):
                    os.remove(bak_parquet_path)
                os.rename(parquet_path, bak_parquet_path)
            os.replace(tmp_parquet_path, parquet_path)
            if os.path.exists(bak_parquet_path):
                os.remove(bak_parquet_path)
            print(f"[{market_name}] raw_data.parquet 원자적 저장 완료! (총 {len(save_df)} 행)", flush=True)
        except Exception as e:
            print(f"[{market_name}] Parquet 저장 중 오류 발생: {e}")
        return save_df, new_df

    return None, new_df

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