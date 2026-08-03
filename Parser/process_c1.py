import pandas as pd
import os
from datetime import datetime

class PandasProcessor:
    """
    KOSPI 데이터를 Pandas를 사용하여 처리하고 일/주/월 단위로 피벗하며,
    종목별 Z-Score 및 통계치(평균, 표준편차)를 산출하는 클래스 (Dask에서 전환됨)
    """
    def __init__(self, start_date, end_date, num_stocks, input_path, output_path, sleep_interval, market_name, exclude_keywords=None):
        self.start_date = start_date
        self.end_date = end_date
        self.num_stocks = num_stocks  # 최대 처리 종목 수 제한 용도로 사용
        self.input_path = input_path
        self.output_path = output_path
        self.sleep_interval = sleep_interval
        self.market_name = market_name
        self.exclude_keywords = exclude_keywords or []  # 필터링할 단어 리스트 추가

        # Parquet 지정
        self.input_file = "raw_data.parquet"
        self.save_dir = os.path.join(self.output_path, self.market_name, "C1Sheet")

    def process_data(self, freq='1d'):
        """
        freq: '1d' (일봉), '1w' (주봉), '1m' (월봉)
        """
        # 1. 출력 디렉토리 생성
        os.makedirs(self.save_dir, exist_ok=True)

        # 2. Pandas 데이터프레임으로 Parquet 읽기
        input_full_path = os.path.join(self.input_path, self.market_name, self.input_file)
        if not os.path.exists(input_full_path):
            input_full_path = os.path.join(self.input_path, self.market_name, "raw_data.csv")
        
        if not os.path.exists(input_full_path):
            print(f"[오류] 데이터 파일을 찾을 수 없습니다: {input_full_path}")
            return None

        # Parquet / CSV 엔진으로 읽기
        if input_full_path.endswith('.parquet'):
            df = pd.read_parquet(input_full_path)
        else:
            df = pd.read_csv(input_full_path)

        # === [추가 기능: 불필요한 단어 필터링] 데이터를 부르고 바로 적용 ===
        if self.exclude_keywords:
            pattern = '|'.join(self.exclude_keywords)
            # Name 컬럼에 필터링 단어가 포함되지 않은(~ 표시) 데이터만 남김
            df = df[~df['Symbol'].str.contains(pattern, case=False, na=False, regex=True)]
        # =====================================================================

        # 3. 날짜 필터링 및 타입 변환
        df['Date'] = pd.to_datetime(df['Date'])
        df = df[df['Date'] >= pd.to_datetime(self.start_date)]

        # 4. 종목 필터링
        # 고유(unique) 값 추출
        all_names = df['Symbol'].unique().tolist()
        print(f"[정보] 데이터에서 {self.market_name} {len(all_names)}개의 고유 종목을 확인했습니다.")

        # 설정된 num_stocks 개수만큼만 처리 (기존 fallback 제한 로직 유지)
        selected_names = all_names[:self.num_stocks]
        print(f"[정보] 최종 처리 대상 종목 수: {len(selected_names)} (최대 {self.num_stocks}개 제한)")

        df = df[df['Symbol'].isin(selected_names)]

        # 5. 데이터 피벗 (X축: Name, Y축: Date)
        df['Symbol'] = df['Symbol'].astype('category').cat.set_categories(selected_names)
        pivoted_df = df.pivot_table(index='Date', columns='Symbol', values='Close', observed=False)

        # 7. 리샘플링 (주봉/월봉 처리)
        freq_map = {'1d': 'D', '1w': 'W-FRI', '1m': 'ME'}
        target_freq = freq_map.get(freq.lower(), 'D')

        if target_freq != 'D':
            final_df = pivoted_df.resample(target_freq).last()
        else:
            final_df = pivoted_df.copy()

        # --- [추가 기능: Z-Score 및 통계치 계산] ---

        # 8. 평균 및 표준편차 계산 (종목별)
        mean_series = final_df.mean()
        std_series = final_df.std()

        stats_df = pd.DataFrame({
            'Mean': mean_series,
            'Std': std_series
        })

        # 9. Z-Score 계산 및 Transpose 추가
        # 기본 계산 결과: Index=Date(시계열), Columns=Name(종목)
        zscore_df = (final_df - mean_series) / std_series

        # 10. DB 직행 (파일 I/O 제거)
        # trans_df 생성 대신, melt하여 DB 포맷으로 변경
        df_melt = zscore_df.reset_index().melt(id_vars=['Date'], var_name='Symbol', value_name='ZScore')
        df_melt = df_melt[df_melt['ZScore'].notnull()]
        
        try:
            # sys.path 수정 후 import 하도록 최상단이나 본문에 db_manager를 연결해야 함.
            from Database.db_manager import DatabaseManager
            db = DatabaseManager()
            db.upsert_zscore_features(df_melt, freq.lower())
        except Exception as e:
            print(f"[ERROR] DB Z-Score 적재 실패: {e}")

        print(f"--- {freq.upper()} 처리 완료 (DB 전송 완료) ---")

        return {"price": final_df, "zscore": zscore_df, "stats": stats_df}

def get_kospi200_dask_data(start_date, end_date, num_stocks, input_path, output_path, sleep_interval, market_name, frequencies=['1d'], exclude_keywords=None):
    """
    설정된 모든 주기에 대해 데이터를 처리하는 함수 (인터페이스 유지를 위해 함수명 유지)
    """
    processor = PandasProcessor(
        start_date=start_date,
        end_date=end_date,
        num_stocks=num_stocks,
        input_path=input_path,
        output_path=output_path,
        sleep_interval=sleep_interval,
        market_name=market_name,
        exclude_keywords=exclude_keywords  # 파라미터 전달 추가
    )

    results = {}
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Pandas 인메모리 시스템 가동... ")

    try:
        for f in frequencies:
            results[f] = processor.process_data(freq=f)
        return results
    except Exception as e:
        print(f"데이터 처리 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return None

# 메인 실행부
if __name__ == "__main__":
    import pytz
    from datetime import datetime, timedelta
    start_date = "2025-01-01"
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.now(kst)
    today_date = now.strftime('%Y-%m-%d')

    # 불필요한 단어 필터링 리스트 예시 (원하는 대로 수정/추가 가능)
    EXCLUDE_WORD_LIST = [# 1. 국내 ETF/ETN (최신 & 과거 브랜드)
    "TIGER", "KODEX", "RISE", "KBSTAR", "TIME", "SOL", "ACE", "KINDEX", 
    "PLUS", "ARIRANG", "KOSEF", "1Q", "KIWOOM", "KoAct", "WON", "HANARO", 
    "KCGI", "FOCUS", "에셋플러스", "DAISHIN", "MIDAS", "TRUSTON", "TREX", "마이티", 
    "UNICORN", "TDF", "ETN", "ETF", 
    
    # 2. 국내 파생/특수/테마
    "스팩", "인버스", "레버리지", "선물", "옵션", "콜", "풋", 
    "특수채", "회사채", "국고채", "채권", "혼합", "인덱스",
    "2차전지양극재", "IBK K-AI", "액티브", "온디바이스AI",
    
    # 3. 미국 파생/특수/스팩
    "Acquisition", "SPAC", "Warrant", "Trust", "Fund", "Unit", "Right", "VIX"]

    CONFIG1 = {
        "start_date": start_date,
        "end_date": today_date,
        "num_stocks": 4000, 
        "input_path": "Data",
        "output_path": "Data",
        "market_name": "KOSPI",
        "sleep_interval": 0.05,
        "frequencies": ["1d", "1w", "1m"],
        "exclude_keywords": EXCLUDE_WORD_LIST  # 추가됨
    }

    CONFIG2 = {
        "start_date": start_date,
        "end_date": today_date,
        "num_stocks": 4000, 
        "input_path": "Data",
        "output_path": "Data",
        "market_name": "KOSDAQ",
        "sleep_interval": 0.05,
        "frequencies": ["1d", "1w", "1m"],
        "exclude_keywords": EXCLUDE_WORD_LIST  # 추가됨
    }

    CONFIG3 = {
        "start_date": start_date,
        "end_date": today_date,
        "num_stocks": 4000, 
        "input_path": "Data",
        "output_path": "Data",
        "market_name": "NASDAQ",
        "sleep_interval": 0.05,
        "frequencies": ["1d", "1w", "1m"],
        "exclude_keywords": EXCLUDE_WORD_LIST   # 추가됨
    }

    CONFIG4 = {
        "start_date": start_date,
        "end_date": today_date,
        "num_stocks": 4000, 
        "input_path": "Data",
        "output_path": "Data",
        "market_name": "NYSE",
        "sleep_interval": 0.05,
        "frequencies": ["1d", "1w", "1m"],
        "exclude_keywords": EXCLUDE_WORD_LIST   # 추가됨
    }

    # Config 반복 실행
    for CONFIG in [CONFIG1, CONFIG2, CONFIG3, CONFIG4]:
        print(f"\n================ [ {CONFIG['market_name']} ] ================")
        results = get_kospi200_dask_data(
            start_date=CONFIG["start_date"],
            end_date=CONFIG["end_date"],
            num_stocks=CONFIG["num_stocks"],
            input_path=CONFIG["input_path"],
            output_path=CONFIG["output_path"],
            sleep_interval=CONFIG["sleep_interval"],
            market_name=CONFIG["market_name"],
            frequencies=CONFIG["frequencies"],
            exclude_keywords=CONFIG.get("exclude_keywords", [])  # 파라미터 전달 추가
        )
            
        if results:
            if "1d" in results and results["1d"] is not None:
                print("[1D Z-Score 상위 3행 확인]")
                # 현재 구조: 행(시계열), 열(종목)
                print(results["1d"]["zscore"].head(3))
