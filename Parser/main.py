import os
import sys
import json
import resource
from datetime import datetime, timedelta
import pytz

def increase_file_descriptor_limit():
    """macOS [Errno 24] Too many open files 에러 방지를 위해 파일 디스크립터 한도를 안전하게 최대치로 상향"""
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target_limit = min(hard, 65536) if hard > 0 else 8192
        if soft < target_limit:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target_limit, hard))
            print(f"[System] macOS 파일 디스크립터 한도 상향 완료: {soft} -> {target_limit}")
    except Exception as e:
        print(f"[System] 파일 디스크립터 한도 설정 경고: {e}")

increase_file_descriptor_limit()

# 4-Tier 디렉토리 독립 구조를 위한 sys.path 등록
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
sys.path.append(os.path.join(project_root, "Database"))
sys.path.append(os.path.join(project_root, "Manager"))

# ======================================================================
# External Module Pipeline Import
# ======================================================================
import pandas as pd
from dask.diagnostics import ProgressBar

# 전체 파이프라인에서 Dask 병렬 처리 진행률을 눈으로 볼 수 있게 프로그레스 바를 켭니다.
ProgressBar().register()

from get_fdr import get_kospi200_dask_data
from process_a1 import DaskFinanceProcessor

from process_m1_cap import run_process

# --- Global Initialization ---
from Manager.pipeline_agent import PipelineLifecycleAgent
from Database.db_manager import DBManager

db = DBManager()


# process_c1의 함수를 이름 충돌 방지를 위해 run_process_c1으로 가져옵니다.
from process_c1 import get_kospi200_dask_data as run_process_c1 
from process_c2 import run_cluster

class FinancePipeline:
    def __init__(self, base_path: str):
        """
        파이프라인 실행에 필요한 공통 환경 및 설정값을 초기화합니다.
        """
        self.base_path = base_path
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

    @staticmethod
    def _safe_read_file(path: str) -> pd.DataFrame:
        """.parquet 및 .csv 확장자를 자동 판별하여 안전하게 DataFrame으로 반환"""
        if not os.path.exists(path):
            return pd.DataFrame()
        try:
            if path.endswith('.parquet'):
                df = pd.read_parquet(path)
                if df.index.name or not isinstance(df.index, pd.RangeIndex):
                    df = df.reset_index()
                return df
            else:
                try:
                    return pd.read_csv(path, encoding='utf-8-sig')
                except UnicodeDecodeError:
                    return pd.read_csv(path, encoding='cp949')
        except Exception as e:
            print(f"[Warning] 파일 읽기 실패 ({path}): {e}")
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
        
        # 🤖 Manager Tier: PipelineLifecycleAgent 기동
        agent = PipelineLifecycleAgent()
        exec_id = agent.start_pipeline(market=f"{market}_DATA_PIPELINE")
        
        print(f"\n{'='*60}\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] {market} 1~4단계 파이프라인 (데이터 가공) 시작\n{'='*60}")
        
        try:
            # 1단계: 데이터 취득 (증분 수집 적용)
            print(f"[*] 1단계: {market} Raw 데이터 수집 중... (증분 감지 시 어제/최신 데이터만 빠르게 수집합니다)")
            merged_df, new_df = get_kospi200_dask_data(
                start_date="2000-01-01",
                num_stocks=4000,
                output_path=self.base_path,
                sleep_interval=0.05,
                market_name=market
            )
            agent.log_step(exec_id, market, now, "1단계_Raw데이터수집", "SUCCESS", {"new_count": len(new_df) if new_df is not None else 0})
            
            # 1.5단계: 수집된 증분 Raw Data를 PostgreSQL에 UPSERT 적재
            print(f"[*] 1.5단계: {market} 증분 Raw 데이터를 PostgreSQL에 적재 중...")
            try:
                db.initialize_tables()  # 혹시 테이블이 없다면 생성
                
                target_insert_df = new_df if new_df is not None and not new_df.empty else merged_df
                
                if target_insert_df is not None and not target_insert_df.empty:
                    db.insert_raw_data(target_insert_df)
                    print(f"[OK] {market} Raw data ({len(target_insert_df)}건 증분) Database UPSERT 갱신 완료")
                else:
                    raw_path = os.path.join(self.base_path, market, "raw_data.parquet")
                    if os.path.exists(raw_path):
                        import dask.dataframe as dd
                        ddf = dd.read_parquet(raw_path)
                        df = ddf.compute()
                        db.insert_raw_data(df)
                        print(f"[OK] {market} Raw data ({len(df)}건) Database 갱신 완료")
                    else:
                        print(f"[Info] {market} 갱신할 신규 데이터가 없어 DB 적재를 스킵합니다.")
            except Exception as db_e:
                print(f"[Warning] {market} Database 적재 중 오류 발생: {db_e}")
                agent.log_step(exec_id, market, now, "1.5단계_DB적재", "WARNING", {"error": str(db_e)})
            
            # 2단계: 데이터 가공
            print(f"[*] 2단계: {market} 기술적 지표 병렬 가공 시작... (보조지표 계산 중, 프로그레스 바 표시됨)")
            config_a1 = {
                "input_path": self.base_path,
                "output_path": self.base_path,
                "market_name": market
            }
            processor = DaskFinanceProcessor(config_a1)
            processor.run()
            agent.log_step(exec_id, market, now, "2단계_지표가공", "SUCCESS")
            
            print(f"[{datetime.now(self.kst).strftime('%Y-%m-%d %H:%M:%S')}] {market} 1~4단계 파이프라인 완료")
            agent.finish_pipeline(exec_id, market=f"{market}_DATA_PIPELINE", start_time=now, status="SUCCESS")
        except Exception as e:
            agent.finish_pipeline(exec_id, market=f"{market}_DATA_PIPELINE", start_time=now, status="FAILED", error=e)
            raise e
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
        print(f"[*] 6단계: {market} C1 Process (Z-Score 및 주기별 통계 데이터 분산 처리)... (시작일: {start_date_365d}, 프로그레스 바 표시됨)")
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
            'CAP_PATH': f"{self.base_path}/{market}/M1Sheet/{target_date}/df_cap.parquet" if os.path.exists(f"{self.base_path}/{market}/M1Sheet/{target_date}/df_cap.parquet") else f"{self.base_path}/{market}/M1Sheet/{target_date}/df_cap.csv",
            'LOAD_PATH': f"{self.base_path}/{market}/C1Sheet/df_trans_1w.parquet" if os.path.exists(f"{self.base_path}/{market}/C1Sheet/df_trans_1w.parquet") else f"{self.base_path}/{market}/C1Sheet/df_trans_1w.csv",
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
        """내부 메서드: 시가총액 필터링 및 클러스터링 실행 (DB 조회, 실패 시 로컬 파일로 폴백)"""
        print(f"Loading Market Cap Data...")
        
        # M1 데이터 (시가총액) 조회
        df_cap = db.read_query("select_market_cap", params=(config_c2['TARGET_DATE'],))
        
        if df_cap.empty:
            import os
            cap_path = config_c2.get('CAP_PATH')
            if cap_path and os.path.exists(cap_path):
                print(f"[Fallback] DB가 비어있거나 연결 실패. 로컬 CSV 파일에서 시가총액 데이터를 읽어옵니다: {cap_path}")
                df_cap = pd.read_csv(cap_path)
                df_cap.columns = [c.lower() for c in df_cap.columns]
                if 'symbol' not in df_cap.columns and 'code' in df_cap.columns:
                    df_cap['symbol'] = df_cap['code']
                if 'market_cap_krw' not in df_cap.columns and 'marketcap_krw' in df_cap.columns:
                    df_cap['market_cap_krw'] = df_cap['marketcap_krw']
            else:
                raise ValueError(f"에러: {config_c2['TARGET_DATE']} 일자의 시가총액 데이터를 DB 및 로컬 파일({cap_path}) 모두에서 찾을 수 없습니다.")

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

        print(f"Loading Time-Series Data...")
        # C1 데이터 (Z-Score 1w) 조회 (최근 180일)
        target_date_obj = datetime.strptime(config_c2['TARGET_DATE'], '%Y-%m-%d')
        start_dt = target_date_obj - timedelta(days=180)
        
        df_zscore_raw = db.read_query("select_zscore_features", params=('1w', start_dt.strftime('%Y-%m-%d')))
        
        if df_zscore_raw.empty:
            import os
            zscore_path = config_c2.get('LOAD_PATH')
            if zscore_path and os.path.exists(zscore_path):
                print(f"[Fallback] DB가 비어있거나 연결 실패. 로컬 CSV 파일에서 Z-Score 데이터를 읽어옵니다: {zscore_path}")
                df_zscore_csv = pd.read_csv(zscore_path)
                
                date_col = None
                for col in ['Date', 'date', 'Unnamed: 0']:
                    if col in df_zscore_csv.columns:
                        date_col = col
                        break
                
                if date_col:
                    if date_col != 'Date':
                        df_zscore_csv = df_zscore_csv.rename(columns={date_col: 'Date'})
                else:
                    df_zscore_csv = df_zscore_csv.reset_index().rename(columns={'index': 'Date'})
                
                df_zscore_raw = df_zscore_csv.melt(id_vars=['Date'], var_name='Symbol', value_name='ZScore')
                df_zscore_raw.columns = ['date', 'symbol', 'zscore']
                df_zscore_raw = df_zscore_raw[df_zscore_raw['date'] >= start_dt.strftime('%Y-%m-%d')]
            else:
                print("DB 및 로컬 파일에서 Z-Score 데이터를 찾지 못하여 클러스터링을 중단합니다.")
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
        import sys
        import os
        import re
        db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Database')
        if db_path not in sys.path:
            sys.path.insert(0, db_path)
        a1_path = f"{self.base_path}/{market}/A1Sheet"
        if os.path.exists(a1_path):
            for file in os.listdir(a1_path):
                if file.endswith('.parquet') or file.endswith('.csv'):
                    df = self._safe_read_file(os.path.join(a1_path, file))
                    if not df.empty:
                        if 'Market' not in df.columns:
                            df['Market'] = market
                        if 'Name' not in df.columns:
                            name_part = file.split('(')[0]
                            df['Name'] = name_part
                        if 'Symbol' not in df.columns:
                            # 파일명 "삼성전자(005930).parquet"에서 "005930" 추출
                            match = re.search(r'\((.*?)\)', file)
                            if match:
                                df['Symbol'] = match.group(1)
                            else:
                                df['Symbol'] = file.split('.')[0]
                        # DB 컬럼명 매핑 (BB_Upper -> Bollinger_High, BB_Lower -> Bollinger_Low)
                        if 'BB_Upper' in df.columns:
                            df.rename(columns={'BB_Upper': 'Bollinger_High'}, inplace=True)
                        if 'BB_Lower' in df.columns:
                            df.rename(columns={'BB_Lower': 'Bollinger_Low'}, inplace=True)

                        db.upsert_technical_indicators(df)

                        # 트레이딩 시그널 추출 및 삽입 (A1Sheet에 포함되어 있음)
                        signals_df = pd.DataFrame()
                        
                        if 'Buy_Signal' in df.columns:
                            buy_idx = df['Buy_Signal'] == 1
                            if buy_idx.any():
                                b_df = df[buy_idx].copy()
                                b_df['signal_type'] = 'BUY'
                                b_df['description'] = 'MACD, RSI 상승장 다이버전스/골든크로스 매수 신호'
                                b_df['signal_strength'] = 1.0
                                signals_df = pd.concat([signals_df, b_df])

                        if 'Sell_Signal' in df.columns:
                            sell_idx = df['Sell_Signal'] == 1
                            if sell_idx.any():
                                s_df = df[sell_idx].copy()
                                s_df['signal_type'] = 'SELL'
                                s_df['description'] = 'MACD, RSI 과매도 데드크로스 매도 신호'
                                s_df['signal_strength'] = 1.0
                                signals_df = pd.concat([signals_df, s_df])
                                
                        if not signals_df.empty:
                            db.upsert_trading_signals(signals_df)

    def _upsert_b1(self, market, now, start_date_5d):
        print(f"[*] B1Sheet DB 적재 시작: {market}")
        import os
        start_dt = now - timedelta(days=5)
        for i in range(6):
            date_str = (start_dt + timedelta(days=i)).strftime('%Y-%m-%d')
            b1_path = f"{self.base_path}/{market}/B1Sheet/{date_str}"
            if os.path.exists(b1_path):
                for file in os.listdir(b1_path):
                    if file.endswith('.parquet') or file.endswith('.csv'):
                        df = self._safe_read_file(os.path.join(b1_path, file))
                        if not df.empty:
                            db.upsert_trading_signals(df)

    def _upsert_m1(self, market, target_date):
        print(f"[*] M1Sheet DB 적재 시작: {market}")
        import os
        m1_path = f"{self.base_path}/{market}/M1Sheet/{target_date}"
        if os.path.exists(m1_path):
            for file in os.listdir(m1_path):
                if file.endswith('.parquet') or file.endswith('.csv'):
                    df = self._safe_read_file(os.path.join(m1_path, file))
                    if not df.empty:
                        df['Date'] = target_date
                        db.upsert_market_cap(df)

    def _upsert_c1(self, market):
        print(f"[*] C1Sheet DB 적재 시작: {market}")
        import os
        c1_path = f"{self.base_path}/{market}/C1Sheet"
        if os.path.exists(c1_path):
            for freq in ['1d', '1w', '1m']:
                target_file_pq = os.path.join(c1_path, f"df_trans_{freq}.parquet")
                target_file_csv = os.path.join(c1_path, f"df_trans_{freq}.csv")
                target_file = target_file_pq if os.path.exists(target_file_pq) else target_file_csv
                if os.path.exists(target_file):
                    df = self._safe_read_file(target_file)
                    if not df.empty:
                        # trans_df: index = Symbol (column 0 in CSV).
                        if 'Symbol' not in df.columns:
                            df = df.rename(columns={df.columns[0]: 'Symbol'})
                        df_melt = df.melt(id_vars=['Symbol'], var_name='Date', value_name='ZScore')
                        df_melt = df_melt[df_melt['ZScore'].notnull()]
                        db.upsert_zscore_features(df_melt, freq)

    def _upsert_c2(self, market, target_date):
        print(f"[*] C2Sheet DB 적재 시작: {market}")
        import os
        c2_path = f"{self.base_path}/{market}/C2Sheet/{target_date}"
        if os.path.exists(c2_path):
            for method_dir in os.listdir(c2_path):
                method_path = os.path.join(c2_path, method_dir)
                if os.path.isdir(method_path):
                    for file in os.listdir(method_path):
                        if 'elbow' not in file and file.endswith('.parquet'):
                            df = pd.read_parquet(os.path.join(method_path, file))
                            if not df.empty:
                                df = df.reset_index()
                                df['Market'] = market
                                df['TargetDate'] = target_date
                                if 'symbol' in df.columns:
                                    df['Symbol'] = df['symbol']
                                elif 'Symbol' not in df.columns:
                                    df['Symbol'] = df[df.columns[0]]
                                    
                                if 'clusters' in df.columns:
                                    df['Cluster_ID'] = df['clusters']
                                elif 'Cluster' in df.columns:
                                    df['Cluster_ID'] = df['Cluster']
                                
                                df['Method'] = method_dir
                                db.upsert_clustering_results(df)

    def run_kr_markets(self):
        """국내 시장(KOSPI, KOSDAQ) 자동 태스크 - 병렬 최적화 구조"""
        print("국내 시장(KOSPI, KOSDAQ) 자동 태스크를 기동합니다.")
        markets = ["KOSPI", "KOSDAQ"]
        
        for market in markets:
            try:
                self.execute_data_pipeline(market)
            except Exception as e:
                print(f"[ERROR] {market} 데이터 수집 중 에러 발생: {e}")
            
        for market in markets:
            try:
                self.execute_clustering_pipeline(market)
            except Exception as e:
                print(f"[ERROR] {market} 클러스터링 실행 중 에러 발생: {e}")
            
        print(f"[{datetime.now(self.kst).strftime('%Y-%m-%d %H:%M:%S')}] 국내 시장 통합 파이프라인 전체 완료")

    def run_us_markets(self):
        """미국 시장(NASDAQ, NYSE) 자동 태스크 - 병렬 최적화 구조"""
        print("미국 시장(NASDAQ, NYSE) 자동 태스크를 기동합니다.")
        markets = ["NASDAQ", "NYSE"]
        
        for market in markets:
            try:
                self.execute_data_pipeline(market)
            except Exception as e:
                print(f"[ERROR] {market} 데이터 수집 중 에러 발생: {e}")
            
        for market in markets:
            try:
                self.execute_clustering_pipeline(market)
            except Exception as e:
                print(f"[ERROR] {market} 클러스터링 실행 중 에러 발생: {e}")
            
        print(f"[{datetime.now(self.kst).strftime('%Y-%m-%d %H:%M:%S')}] 미국 시장 통합 파이프라인 전체 완료")


# ======================================================================
# 메인 루프 실행
# ======================================================================
if __name__ == "__main__":
    import os
    
    # 저장 경로를 프로젝트 내부 Data 폴더로 설정 (OS 무관 상대 경로)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    BASE_PATH = os.path.join(project_root, "Data")
    
    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH)
    
    pipeline = FinancePipeline(base_path=BASE_PATH)
    
    print("==== 🚀 데이터 즉시 수집 및 파이프라인 가동 ====")
    pipeline.run_kr_markets()
    pipeline.run_us_markets()
if "db" in locals() and hasattr(db, "conn") and db.conn:
    db.conn.close()
    print("[System] DB 연결 종료 완료")
