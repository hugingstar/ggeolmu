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

    delayed_tasks = []
    
    print(f"데이터 수집 작업 생성 중 (대상: {len(target_stocks)} 종목)...", flush=True)
    for index, row in target_stocks.iterrows():
        symbol = row['Symbol']
        name = row['Name']
        
        # Delayed 작업에 sleep_interval 전달 (for문 안에서의 sleep은 제거됨)
        task = fetch_stock_data(symbol, name, start_date, fdr_prefix=fdr_prefix, sleep_interval=sleep_interval)
        delayed_tasks.append(task)

    print("Dask 그래프 계산 및 데이터 수집 시작...", flush=True)
    
    import dask
    # 병렬 처리 중 과부하를 더 줄이고 싶다면 아래처럼 워커 개수(num_workers)를 제한할 수도 있습니다.
    # results = dask.compute(*delayed_tasks, scheduler='threads', num_workers=4)
    results = dask.compute(*delayed_tasks)
    
    # 에러로 인해 None이 반환된(스킵된) 결과들은 여기서 자동으로 걸러집니다.
    valid_results = [r for r in results if r is not None]

    if not valid_results:
        print("수집된 데이터가 없습니다.", flush=True)
        return None

    ddf = dd.from_pandas(pd.concat(valid_results, ignore_index=True), npartitions=4)

    print(f"데이터 저장 중... 경로: {output_path}", flush=True)
    
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    final_df = ddf.compute()
    
    # 기존 CSV 저장 로직 유지
    final_df.to_csv(f"{output_path}/raw_data.csv", index=False, encoding='utf-8-sig')
    
    # Parquet 저장 로직 추가
    final_df.to_parquet(f"{output_path}/raw_data.parquet", index=False)
    
    print("저장 완료!", flush=True)

    if final_df is not None:
        print(final_df.head())
        print(final_df.shape)
    return ddf

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