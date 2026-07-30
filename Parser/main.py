import os
import json
from datetime import datetime, timedelta
import pytz
from apscheduler.schedulers.blocking import BlockingScheduler

# ======================================================================
# [핵심 수정 사항] Matplotlib 백엔드 강제 변경 (Tkinter 스레드 에러 방지)
# 주의: 이 코드는 반드시 데이터 처리 모듈들이 import 되기 전에 실행되어야 합니다.
# ======================================================================
import matplotlib
matplotlib.use('Agg')

# ======================================================================
# External Module Pipeline Import
# ======================================================================
import pandas as pd

from get_fdr import get_kospi200_dask_data
from process_a1 import DaskFinanceProcessor
from process_b1 import MakeSheet

from process_m1_cap import run_process
# process_c1의 함수를 이름 충돌 방지를 위해 run_process_c1으로 가져옵니다.
from process_c1 import get_kospi200_dask_data as run_process_c1 
from process_c2 import run_cluster

class FinancePipeline:
    def __init__(self, base_path: str, s3_bucket: str):
        """
        파이프라인 실행에 필요한 공통 환경 및 설정값을 초기화합니다.
        """
        self.base_path = base_path
        self.s3_bucket = s3_bucket
        self.kst = pytz.timezone('Asia/Seoul')
        
        # 클러스터링 제외 단어 리스트
        self.exclude_words = [
            "TIGER", "KODEX", "RISE", "TIME", "SOL", "ACE", "TDF", "ETN", "PLUS", 
            "1Q", "KIWOOM", "KoAct", "WON", "HANARO", "KCGI", "FOCUS", "에셋플러스", 
            "DAISHIN", "MIDAS", "TRUSTON", "스팩", "SPAC", "KOSEF", "Fund", "Unit", 
            "Right", "Rights", "VIX", "국고채", "채권", "혼합", "KBSTAR", "KINDEX", 
            "ARIRANG", "마이티", "TREX", "특수채", "회사채", "UNICORN", "2차전지양극재", 
            "IBK K-AI", "액티브", "온디바이스AI", "인덱스", "Acquisition", "Warrant", 
            "ETF", "Trust", "200"
        ]

    @staticmethod
    def _safe_read_csv(path: str) -> pd.DataFrame:
        """안전하게 CSV 파일을 읽습니다. 파일이 없거나 손상되었을 경우 빈 DataFrame 반환"""
        if not os.path.exists(path):
            return pd.DataFrame()
        try:
            return pd.read_csv(path)
        except Exception as e:
            print(f"[Warning] Failed to read {path}: {e}")
            return pd.DataFrame()

    def _get_target_dates(self, market: str):
        """현재 시점을 기준으로 마켓별 필요 날짜(target_date, today_date, start_date_5d)를 계산합니다."""
        now = datetime.now(self.kst)
        today_date = now.strftime('%Y-%m-%d')
        yesterday_1d = (now - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date_5d = (now - timedelta(days=5)).strftime('%Y-%m-%d')

        if market in ["KOSPI", "KOSDAQ"]:
            target_date = today_date
        elif market in ["NASDAQ", "NYSE"]:
            target_date = yesterday_1d
        else:
            target_date = today_date

        return now, today_date, start_date_5d, target_date

    def execute_data_pipeline(self, market: str):
        """1~4단계: 취득 -> 가공 -> 시트 생성 -> S3/DB 업로드 실행"""
        now, today_date, start_date_5d, _ = self._get_target_dates(market)
        
        print(f"\n{'='*60}\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] {market} 1~4단계 파이프라인 (데이터 가공) 시작\n{'='*60}")
        
        # 1단계: 데이터 취득
        print(f"[*] 1단계: {market} Raw 데이터 수집 중...")
        get_kospi200_dask_data(
            start_date="2000-01-01",
            num_stocks=4000,
            output_path=os.path.join(self.base_path, market),
            sleep_interval=0.05,
            market_name=market
        )
        
        # [신규] 1.5단계: 수집된 Raw Data를 PostgreSQL에 적재
        print(f"[*] 1.5단계: {market} Raw 데이터를 PostgreSQL에 적재 중...")
        try:
            from db_manager import DBManager
            db = DBManager()
            db.initialize_tables()  # 혹시 테이블이 없다면 생성
            
            raw_path = os.path.join(self.base_path, market, "raw_data.parquet")
            if os.path.exists(raw_path):
                # Dask로 읽어서 Pandas로 변환 후 DB에 Bulk Insert (OOM 방지 위해 일부 파티션 단위 권장하나, 여기선 전체 읽음 처리)
                import dask.dataframe as dd
                ddf = dd.read_parquet(raw_path)
                df = ddf.compute()
                db.insert_raw_data(df)
                print(f"[OK] {market} Raw data ({len(df)}건) Database 갱신 완료")
            else:
                print(f"[Warn] {raw_path} 파일을 찾을 수 없어 DB 적재 건너뜀.")
        except Exception as e:
            print(f"[Error] DB 적재 중 오류 발생: {e}")

        # 2단계: 데이터 가공
        print(f"[*] 2단계: {market} 기술적 지표 병렬 가공 시작...")
        config_a1 = {
            "input_path": self.base_path,
            "output_path": self.base_path,
            "market_name": market
        }
        processor = DaskFinanceProcessor(config_a1)
        processor.run()
        # self._upsert_a1(market) # (기존 함수 존재 가정)
        
        # 3단계: 날짜별 시트 생성
        print(f"[*] 3단계: {market} 시그널별 날짜 시트 생성 시작...")
        config_b1 = {
            "start_date": start_date_5d,  # start_date_5d
            "end_date": today_date,       
            "output_path": self.base_path,
            "market_name": market
        }
        app = MakeSheet(config_b1)
        app.run()
        # self._upsert_b1(market, now, start_date_5d)
        
        print(f"[{datetime.now(self.kst).strftime('%Y-%m-%d %H:%M:%S')}] {market} 1~4단계 파이프라인 완료")


    def execute_clustering_pipeline(self, market: str):
        """5~8단계: M1 전처리 -> C1 Z-Score 연산 -> C2 클러스터링 -> S3 업로드 실행"""
        now, today_date, _, target_date = self._get_target_dates(market)
        
        print(f"\n{'='*60}\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] {market} 5~8단계 파이프라인 (클러스터링) 시작\n{'='*60}")
        
        # 5단계: M1 Process (클러스터링 데이터 전처리)
        print(f"[*] 5단계: {market} M1 Process (시가총액 등 클러스터링 기초 데이터 전처리)...")
        run_process(base_path=self.base_path, target_date=target_date, market=market)
        self._upsert_m1(market, target_date)
        
        # 6단계: C1 Process (Z-Score 및 통계 데이터 생성)
        # [수정] start_date를 today_date 기준 365일 전으로 설정
        start_date_365d = (now - timedelta(days=180)).strftime('%Y-%m-%d')
        print(f"[*] 6단계: {market} C1 Process (Z-Score 및 주기별 통계 데이터 분산 처리)... (시작일: {start_date_365d})")
        run_process_c1(
            start_date=start_date_365d,  
            end_date=today_date,
            num_stocks=4000,
            input_path=self.base_path,
            output_path=self.base_path,
            sleep_interval=0.05,
            market_name=market,
            frequencies=["1d", "1w", "1m"],
            exclude_keywords=self.exclude_words
        )
        self._upsert_c1(market)

        # 7단계: C2 Process (시계열 클러스터링 결과)
        print(f"[*] 7단계: {market} C2 Process (시계열 기반 DTw 클러스터링)...")
        config_c2 = {
            'MARKET': market, 
            'TARGET_DATE': target_date,
            'BASE_PATH': self.base_path,
            'CAP_PATH': f"{self.base_path}/{market}/M1Sheet/{target_date}/df_cap.csv",
            'LOAD_PATH': f"{self.base_path}/{market}/C1Sheet/df_zscore_1w.csv",
            'SAVE_PATH': f"{self.base_path}/{market}/C2Sheet/{target_date}",
            
            'CAP_COLUMN': "MarketCap_KRW",
            'TOP_N': 1000,
            'EXCLUDE_WORDS': self.exclude_words,
            
            'TIME': 'Date',
            'SCALER': 'standard',
            'N_CLUSTER_RANGE': [8, 9],
            'METHOD': 'kmeans_softdtw',
            
            'DBSCAN_EPS': 0.5,
            'DBSCAN_MIN_SAMPLES': 2,
            'SOFT_DTW_GAMMA': 1.0,
            
            'MIN_VALID_RATIO': 0.5,
            'DROP_CONSTANT': True
        }

        self._run_clustering(market, config_c2)
        self._upsert_c2(market, target_date)


        # C2Sheet 선별 업로드 제거됨

        print(f"[{datetime.now(self.kst).strftime('%Y-%m-%d %H:%M:%S')}] {market} 5~8단계 파이프라인 완료")


    def _run_clustering(self, market: str, config_c2: dict):
        """내부 메서드: 시가총액 필터링 및 클러스터링 실행 (DB에서 쿼리 조회)"""
        print(f"Loading Market Cap Data from DB...")
        from db_manager import DBManager
        db = DBManager()
        
        # M1 데이터 (시가총액) 조회
        df_cap = db.read_query("select_market_cap", params=(config_c2['TARGET_DATE'],))
        
        if df_cap.empty:
            raise ValueError(f"에러: {config_c2['TARGET_DATE']} 일자의 시가총액 데이터를 DB에서 찾을 수 없습니다.")

        cap_col = config_c2['CAP_COLUMN'].lower() # sql returns lowercase columns
        if cap_col not in df_cap.columns:
            cap_col = 'market_cap_krw'

        symbol_series = df_cap['symbol']

        def is_valid_symbol(sym):
            sym_str = str(sym).upper()
            return not any(word.upper() in sym_str for word in config_c2['EXCLUDE_WORDS'])

        valid_mask = symbol_series.apply(is_valid_symbol)
        df_cap_filtered = df_cap[valid_mask]

        df_cap_sorted = df_cap_filtered.sort_values(by=cap_col, ascending=False).head(config_c2['TOP_N'])
        filtered_symbols = df_cap_sorted['symbol'].tolist()

        print(f"[필터링 결과] 제외 단어 필터링 후 시가총액 상위 {config_c2['TOP_N']} 종목 추출 완료 (실제 크기: {len(filtered_symbols)}개)")

        print(f"Loading Time-Series Data from DB...")
        # C1 데이터 (Z-Score 1w) 조회 (최근 180일)
        target_date_obj = datetime.strptime(config_c2['TARGET_DATE'], '%Y-%m-%d')
        start_dt = target_date_obj - timedelta(days=180)
        
        df_zscore_raw = db.read_query("select_zscore_features", params=('1w', start_dt.strftime('%Y-%m-%d')))
        
        if df_zscore_raw.empty:
            print("DB에서 Z-Score 데이터를 찾지 못하여 클러스터링을 중단합니다.")
            return

        # Pivot the dataframe so that columns are symbols and index is date
        df_zscore_pivot = df_zscore_raw.pivot_table(index='date', columns='symbol', values='zscore')
        
        valid_cols = [col for col in df_zscore_pivot.columns if col in filtered_symbols]
        source_data = df_zscore_pivot[valid_cols]

        print(f"[데이터 준비 완료] 필터링 후 적용될 시계열 데이터 shape: {source_data.shape}")

        print(f"--- Clustering Process Started for {config_c2['MARKET']} ({config_c2['TARGET_DATE']}) ---")
        run_cluster(config=config_c2, source_data=source_data)
        print("--- Clustering Process Finished ---")

    # ==========================================================
    # 스텝별 DB 적재 헬퍼 메서드 모음
    # ==========================================================
    def _upsert_a1(self, market):
        print(f"[*] A1Sheet DB 적재 시작: {market}")
        from db_manager import DBManager
        import os
        db = DBManager()
        a1_path = f"{self.base_path}/{market}/A1Sheet"
        if os.path.exists(a1_path):
            for file in os.listdir(a1_path):
                if file.endswith('.csv'):
                    df = self._safe_read_csv(os.path.join(a1_path, file))
                    if not df.empty:
                        if 'Market' not in df.columns:
                            df['Market'] = market
                        if 'Name' not in df.columns:
                            name_part = file.split('(')[0]
                            df['Name'] = name_part
                        db.upsert_stocks(df)
                        db.upsert_daily_prices(df)
                        db.upsert_technical_indicators(df)

    def _upsert_b1(self, market, now, start_date_5d):
        print(f"[*] B1Sheet DB 적재 시작: {market}")
        from db_manager import DBManager
        import os
        db = DBManager()
        start_dt = now - timedelta(days=5)
        for i in range(6):
            date_str = (start_dt + timedelta(days=i)).strftime('%Y-%m-%d')
            b1_path = f"{self.base_path}/{market}/B1Sheet/{date_str}"
            if os.path.exists(b1_path):
                for file in os.listdir(b1_path):
                    if file.endswith('.csv'):
                        df = self._safe_read_csv(os.path.join(b1_path, file))
                        if not df.empty:
                            db.upsert_trading_signals(df)

    def _upsert_m1(self, market, target_date):
        print(f"[*] M1Sheet DB 적재 시작: {market}")
        from db_manager import DBManager
        import os
        db = DBManager()
        m1_path = f"{self.base_path}/{market}/M1Sheet/{target_date}"
        if os.path.exists(m1_path):
            for file in os.listdir(m1_path):
                if file.endswith('.csv'):
                    df = self._safe_read_csv(os.path.join(m1_path, file))
                    if not df.empty:
                        df['Date'] = target_date
                        db.upsert_market_cap(df)

    def _upsert_c1(self, market):
        print(f"[*] C1Sheet DB 적재 시작: {market}")
        from db_manager import DBManager
        import os
        db = DBManager()
        c1_path = f"{self.base_path}/{market}/C1Sheet"
        if os.path.exists(c1_path):
            for freq in ['1d', '1w', '1m']:
                target_file = os.path.join(c1_path, f"df_trans_{freq}.csv")
                if os.path.exists(target_file):
                    df = self._safe_read_csv(target_file)
                    if not df.empty:
                        # trans_df: index = Symbol, columns = Date. We need to unpivot(melt) it.
                        if 'Symbol' not in df.columns:
                            # if symbol is in index
                            df = df.reset_index().rename(columns={'index': 'Symbol'})
                        df_melt = df.melt(id_vars=['Symbol'], var_name='Date', value_name='ZScore')
                        db.upsert_zscore_features(df_melt, freq)

    def _upsert_c2(self, market, target_date):
        print(f"[*] C2Sheet DB 적재 시작: {market}")
        from db_manager import DBManager
        import os
        db = DBManager()
        c2_path = f"{self.base_path}/{market}/C2Sheet/{target_date}"
        if os.path.exists(c2_path):
            for file in os.listdir(c2_path):
                if 'elbow' not in file and file.endswith('.csv'):
                    df = self._safe_read_csv(os.path.join(c2_path, file))
                    if not df.empty:
                        df['Market'] = market
                        df['TargetDate'] = target_date
                        if 'Symbol' not in df.columns and 'Code' in df.columns:
                            df['Symbol'] = df['Code']
                        if 'Cluster' in df.columns and 'Cluster_ID' not in df.columns:
                            df['Cluster_ID'] = df['Cluster']
                        method_name = file.split('_k')[0] if '_k' in file else 'unknown'
                        df['Method'] = method_name
                        db.upsert_clustering_results(df)

    def _print_waiting_message(self):
        """작업 완료 후 또는 시작 시 스케줄러 대기 상태를 알리는 메시지"""
        print(f"\n{'='*60}")
        print(f"[{datetime.now(self.kst).strftime('%Y-%m-%d %H:%M:%S')}] 스케줄러 대기 중... 다음 작업을 기다립니다.")
        print("- KOSPI, KOSDAQ : 월~금 16:05 (KST)")
        print("- NASDAQ, NYSE  : 화~토 06:40 (KST)")
        print("종료하려면 Ctrl+C를 누르세요.")
        print(f"{'='*60}\n")

    def run_kr_markets(self):
        """국내 시장(KOSPI, KOSDAQ) 자동 태스크 - 병렬 최적화 구조"""
        print("국내 시장(KOSPI, KOSDAQ) 자동 태스크를 기동합니다.")
        markets = ["KOSPI", "KOSDAQ"]
        
        for market in markets:
            self.execute_data_pipeline(market)
            
        for market in markets:
            self.execute_clustering_pipeline(market)
            
        print(f"[{datetime.now(self.kst).strftime('%Y-%m-%d %H:%M:%S')}] 국내 시장 통합 파이프라인 전체 완료")
        self._print_waiting_message()

    def run_us_markets(self):
        """미국 시장(NASDAQ, NYSE) 자동 태스크 - 병렬 최적화 구조"""
        print("미국 시장(NASDAQ, NYSE) 자동 태스크를 기동합니다.")
        markets = ["NASDAQ", "NYSE"]
        
        for market in markets:
            self.execute_data_pipeline(market)
            
        for market in markets:
            self.execute_clustering_pipeline(market)
            
        print(f"[{datetime.now(self.kst).strftime('%Y-%m-%d %H:%M:%S')}] 미국 시장 통합 파이프라인 전체 완료")
        self._print_waiting_message()

    def run_scheduler(self):
        """스케줄러 설정 및 메인 루프 실행"""
        scheduler = BlockingScheduler(timezone=self.kst)

        # 1. 한국시간 기준 월 ~ 금 16:05 -> KOSPI, KOSDAQ 실행
        scheduler.add_job(
            self.run_kr_markets, 
            trigger='cron', 
            day_of_week='mon-fri', 
            hour=16, 
            minute=5
        )

        # 2. 한국시간 기준 화 ~ 토 06:05 -> NASDAQ, NYSE 실행
        scheduler.add_job(
            self.run_us_markets, 
            trigger='cron', 
            day_of_week='tue-sat', 
            hour=6, 
            minute=5
        )


        print(f"[{datetime.now(self.kst).strftime('%Y-%m-%d %H:%M:%S')}] 통합 파이프라인 스케줄러가 정상 가동되었습니다.")
        self._print_waiting_message()
        
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            print("\n스케줄러가 안전하게 종료되었습니다.")


# ======================================================================
# 메인 루프 실행
# ======================================================================
if __name__ == "__main__":
    import os
    # 저장 경로를 ggeolmu/Data로 변경
    BASE_PATH = "/Users/yusunglee/PycharmProjects/ggeolmu/Data"
    S3_BUCKET_NAME = "yslee-s3-bucket"
    
    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH)
    
    pipeline = FinancePipeline(base_path=BASE_PATH, s3_bucket=S3_BUCKET_NAME)
    
    print("==== 🚀 데이터 즉시 수집 및 파이프라인 가동 ====")
    # 스케줄러 없이 즉시 실행되도록 변경
    pipeline.run_kr_markets()
    pipeline.run_us_markets()