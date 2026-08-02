# Database Tier: PostgreSQL DBManager (파라미터화 쿼리 및 재시도 연결 관리)
import psycopg2
from psycopg2.extras import execute_values
import os
import pandas as pd

class DBManager:
    def __init__(self):
        # PostgreSQL 접속 정보 (환경 변수 또는 기본값)
        self.host = os.environ.get("DB_HOST", "localhost")
        self.port = os.environ.get("DB_PORT", "5432")
        self.dbname = os.environ.get("DB_NAME", "postgres")
        self.user = os.environ.get("DB_USER", "postgres")
        self.password = os.environ.get("DB_PASS", "postgres")
        self.conn = None
        
        # 쿼리 파일 캐싱 (Database/queries 디렉토리)
        self.queries = {}
        self.query_dir = os.path.join(os.path.dirname(__file__), "queries")
        
        self.connect()
        self._load_all_queries()

    def connect(self, retries: int = 3, delay: float = 2.0):
        """PostgreSQL 데이터베이스 연결을 수행하며 실패 시 재시도합니다."""
        import time
        for i in range(retries):
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    dbname=self.dbname,
                    user=self.user,
                    password=self.password,
                    connect_timeout=5
                )
                self.conn.autocommit = True
                print(f"[DBManager] PostgreSQL 데이터베이스 연결 성공 ({self.host}:{self.port}/{self.dbname})")
                return
            except Exception as e:
                print(f"[DBManager] DB 연결 시도 {i+1}/{retries} 실패: {e}")
                if i < retries - 1:
                    time.sleep(delay)

    def _load_all_queries(self):
        """Database/queries 및 로컬 queries 디렉토리 안의 모든 sql 스크립트를 통합 로드합니다."""
        curr_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(curr_dir)
        
        target_dirs = [
            os.path.join(curr_dir, "queries"),
            os.path.join(project_root, "Database", "queries"),
            os.path.join(project_root, "Parser", "queries")
        ]
        
        for qdir in target_dirs:
            if os.path.exists(qdir):
                for filename in os.listdir(qdir):
                    if filename.endswith('.sql'):
                        file_path = os.path.join(qdir, filename)
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                self.queries[filename] = f.read()
                        except Exception as e:
                            print(f"[DBManager] Failed to load query {filename}: {e}")

    def get_query(self, query_name: str) -> str:
        """이름으로 저장된 쿼리를 반환합니다."""
        return self.queries.get(query_name, "")

    def initialize_tables(self):
        """기본적인 종목 데이터, 프롬프트 결과 및 파이프라인 로그 저장용 테이블 생성"""
        if not self.conn:
            return
        
        create_query = self.get_query("001_create_tables.sql")
        if create_query:
            try:
                with self.conn.cursor() as cur:
                    cur.execute(create_query)
                    idx_query = self.get_query("011_create_stock_indexes.sql")
                    if idx_query:
                        cur.execute(idx_query)
                    tech_query = self.get_query("012_create_technical_tables.sql")
                    if tech_query:
                        cur.execute(tech_query)
            except Exception as e:
                print(f"[DBManager] Table creation failed (001, 011, 012): {e}")

        log_create_query = self.get_query("015_create_pipeline_logs.sql")
        if log_create_query:
            try:
                with self.conn.cursor() as cur:
                    cur.execute(log_create_query)
            except Exception as e:
                print(f"[DBManager] Table creation failed (015): {e}")

    def upsert_pipeline_log(self, execution_id, market, start_time, end_time, duration_seconds, status, step_details, error_message):
        """016_upsert_pipeline_logs.sql 기반 파이프라인 수집/가공 라이프사이클 로그 적재"""
        query = self.get_query("016_upsert_pipeline_logs.sql")
        if not query:
            query = """
            INSERT INTO public.pipeline_execution_logs (
                execution_id, market, start_time, end_time, duration_seconds, status, step_details, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (execution_id)
            DO UPDATE SET
                end_time = EXCLUDED.end_time,
                duration_seconds = EXCLUDED.duration_seconds,
                status = EXCLUDED.status,
                step_details = EXCLUDED.step_details,
                error_message = EXCLUDED.error_message;
            """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (execution_id, market, start_time, end_time, duration_seconds, status, step_details, error_message))
        except Exception as e:
            print(f"[DBManager] Error upserting pipeline execution log: {e}")

    def insert_raw_data(self, df: pd.DataFrame):
        """
        get_fdr 등에서 가져온 pandas DataFrame을 테이블에 삽입 (UPSERT)
        """
        if not self.conn or df.empty:
            return

        insert_query = self.get_query("002_insert_raw_data.sql")
        if not insert_query:
            print("[DBManager] 오류: 002_insert_raw_data.sql 파일을 찾을 수 없습니다.")
            return
        
        df_clean = df.where(pd.notnull(df), None)
        cols = ['Date', 'Symbol', 'Name', 'Open', 'High', 'Low', 'Close', 'Volume', 'Change']
        for c in cols:
            if c not in df_clean.columns:
                df_clean[c] = None
                
        records = [tuple(x) for x in df_clean[cols].to_numpy()]
        
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, insert_query, records, page_size=1000)
                print(f"[DBManager] {len(records)} 건 데이터 갱신/삽입 완료.")
        except Exception as e:
            print(f"[DBManager] Bulk Insert 실패: {e}")

    def read_query_direct(self, query_name_or_str: str, params: tuple = None) -> list:
        """
        직접 쿼리 문자열을 전달받거나, 쿼리 파일 이름을 전달받아 실행
        """
        if not self.conn:
            return []
        
        query = self.queries.get(query_name_or_str, query_name_or_str)
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()
        except Exception as e:
            print(f"[DBManager] 조회 쿼리 실행 실패: {e}")
            return []

    def write_query(self, query_name_or_str: str, params: tuple = None) -> bool:
        """
        INSERT, UPDATE, DELETE 등 결과 반환이 없는 쓰기 쿼리를 실행합니다.
        """
        if not self.conn:
            return False
        
        query = self.queries.get(query_name_or_str, query_name_or_str)
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                return True
        except Exception as e:
            print(f"[DBManager] 쓰기 쿼리 실행 실패: {e}")
            return False

    def read_query(self, query_name_or_str: str, params: tuple = None) -> pd.DataFrame:
        """
        주어진 쿼리나 이름을 실행하고 결과를 pandas DataFrame으로 반환합니다.
        """
        if not self.conn:
            return pd.DataFrame()
            
        if query_name_or_str == "select_market_cap":
            query = "SELECT symbol, market_cap_krw FROM public.market_cap WHERE date = %s"
        elif query_name_or_str == "select_zscore_features":
            query = "SELECT date, symbol, zscore FROM public.zscore_features WHERE freq = %s AND date >= %s"
        else:
            query = self.queries.get(query_name_or_str, query_name_or_str)
            
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    data = cur.fetchall()
                    return pd.DataFrame(data, columns=columns)
                return pd.DataFrame()
        except Exception as e:
            print(f"[DBManager] read_query 실패: {e}")
            return pd.DataFrame()

    def upsert_market_cap(self, df: pd.DataFrame):
        if not self.conn or df.empty:
            return
            
        df_clean = df.where(pd.notnull(df), None)
        records = []
        for _, row in df_clean.iterrows():
            d = row.get('Date')
            sym = row.get('Symbol', row.get('Code'))
            cap = row.get('MarketCap_KRW', row.get('market_cap_krw'))
            if pd.isna(cap):
                cap = None
            records.append((d, sym, cap))
            
        query = """
        INSERT INTO public.market_cap (date, symbol, market_cap_krw)
        VALUES %s
        ON CONFLICT (date, symbol) DO UPDATE SET
            market_cap_krw = EXCLUDED.market_cap_krw
        """
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, query, records, page_size=1000)
                print(f"[DBManager] 시가총액 {len(records)} 레코드 Upsert 성공.")
        except Exception as e:
            print(f"[DBManager] upsert_market_cap 실패: {e}")

    def upsert_zscore_features(self, df: pd.DataFrame, freq: str):
        if not self.conn or df.empty:
            return
            
        df_clean = df.where(pd.notnull(df), None)
        records = []
        for _, row in df_clean.iterrows():
            d = row.get('Date', row.get('date'))
            sym = row.get('Symbol', row.get('symbol', row.get('Code', row.get('code'))))
            zs = row.get('ZScore', row.get('zscore'))
            if pd.isna(zs) or zs is None or not d or not sym:
                continue
            records.append((str(d), str(sym), str(freq), float(zs)))
            
        if not records:
            print("[DBManager] upsert_zscore_features: 유효한 레코드가 없어 건너뜁니다.")
            return

        query = self.queries.get("009_upsert_zscore_features.sql", """
        INSERT INTO public.zscore_features (date, symbol, freq, zscore)
        VALUES %s
        ON CONFLICT (date, symbol, freq) DO UPDATE SET
            zscore = EXCLUDED.zscore;
        """)
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, query, records, page_size=1000)
                print(f"[DBManager] ZScore 지표 {len(records)} 레코드 ({freq}) Upsert 성공.")
        except Exception as e:
            print(f"[DBManager] upsert_zscore_features 실패: {e}")

    def upsert_clustering_results(self, df: pd.DataFrame):
        if not self.conn or df.empty:
            return
            
        df_clean = df.where(pd.notnull(df), None)
        records = []
        for _, row in df_clean.iterrows():
            td = row.get('TargetDate', row.get('target_date'))
            mkt = row.get('Market', row.get('market'))
            sym = row.get('Symbol', row.get('symbol', row.get('Code', row.get('code'))))
            cid = row.get('Cluster_ID', row.get('cluster_id', row.get('Cluster', row.get('clusters'))))
            method = row.get('Method', row.get('method', 'kmeans_softdtw'))
            if not td or not sym or cid is None or pd.isna(cid):
                continue
            records.append((str(td), str(mkt), str(sym), int(cid), str(method)))
            
        if not records:
            print("[DBManager] upsert_clustering_results: 유효한 레코드가 없어 건너뜁니다.")
            return

        query = self.queries.get("010_upsert_clustering_results.sql", """
        INSERT INTO public.clustering_results (target_date, market, symbol, cluster_id, method)
        VALUES %s
        ON CONFLICT (target_date, symbol, method) DO UPDATE SET
            market = EXCLUDED.market,
            cluster_id = EXCLUDED.cluster_id;
        """)
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, query, records, page_size=1000)
                print(f"[DBManager] 클러스터링 결과 {len(records)} 레코드 Upsert 성공.")
        except Exception as e:
            print(f"[DBManager] upsert_clustering_results 실패: {e}")

    def upsert_technical_indicators(self, df: pd.DataFrame):
        if not self.conn or df.empty:
            return
        records = []
        col_mappings = [
            ('ma1', 'MA1', 'ma1'),
            ('ma2', 'MA2', 'ma2'),
            ('ma3', 'MA3', 'ma3'),
            ('ma4', 'MA4', 'ma4'),
            ('ma5', 'MA5', 'ma5'),
            ('ma6', 'MA6', 'ma6'),
            ('ma7', 'MA7', 'ma7'),
            ('ma8', 'MA8', 'ma8'),
            ('ma9', 'MA9', 'ma9'),
            ('ma20', 'MA20', 'ma20'),
            ('ma50', 'MA50', 'ma50'),
            ('ma60', 'MA60', 'ma60'),
            ('ma90', 'MA90', 'ma90'),
            ('ma120', 'MA120', 'ma120'),
            ('ma200', 'MA200', 'ma200'),
            ('ma224', 'MA224', 'ma224'),
            ('close_diff_first', 'Close_diff_first', 'close_diff_first'),
            ('close_rate_first', 'Close_rate_first', 'close_rate_first'),
            ('currencyvolume', 'CurrencyVolume', 'currencyvolume'),
            ('currencyvolume_ratio_ma20', 'CurrencyVolume_Ratio_MA20', 'currencyvolume_ratio_ma20'),
            ('obv', 'OBV', 'obv'),
            ('mobv', 'MOBV', 'mobv'),
            ('deltamobv', 'DeltaMOBV', 'deltamobv'),
            ('rsi', 'RSI', 'rsi'),
            ('rsi_signal', 'RSI_Signal', 'rsi_signal'),
            ('rsi_bulldiv', 'RSI_BullDiv', 'rsi_bulldiv'),
            ('rsi_beardiv', 'RSI_BearDiv', 'rsi_beardiv'),
            ('rsi_hidden_bulldiv', 'RSI_Hidden_BullDiv', 'rsi_hidden_bulldiv'),
            ('rsi_hidden_beardiv', 'RSI_Hidden_BearDiv', 'rsi_hidden_beardiv'),
            ('rsi_uptrend', 'RSI_UpTrend', 'rsi_uptrend'),
            ('rsi_downtrend', 'RSI_DownTrend', 'rsi_downtrend'),
            ('rsi2', 'RSI2', 'rsi2'),
            ('rsi_signal2', 'RSI_Signal2', 'rsi_signal2'),
            ('rsi_bulldiv2', 'RSI_BullDiv2', 'rsi_bulldiv2'),
            ('rsi_beardiv2', 'RSI_BearDiv2', 'rsi_beardiv2'),
            ('rsi_hidden_bulldiv2', 'RSI_Hidden_BullDiv2', 'rsi_hidden_bulldiv2'),
            ('rsi_hidden_beardiv2', 'RSI_Hidden_BearDiv2', 'rsi_hidden_beardiv2'),
            ('rsi_uptrend2', 'RSI_UpTrend2', 'rsi_uptrend2'),
            ('rsi_downtrend2', 'RSI_DownTrend2', 'rsi_downtrend2'),
            ('rsi3', 'RSI3', 'rsi3'),
            ('rsi_signal3', 'RSI_Signal3', 'rsi_signal3'),
            ('rsi_bulldiv3', 'RSI_BullDiv3', 'rsi_bulldiv3'),
            ('rsi_beardiv3', 'RSI_BearDiv3', 'rsi_beardiv3'),
            ('rsi_hidden_bulldiv3', 'RSI_Hidden_BullDiv3', 'rsi_hidden_bulldiv3'),
            ('rsi_hidden_beardiv3', 'RSI_Hidden_BearDiv3', 'rsi_hidden_beardiv3'),
            ('rsi_uptrend3', 'RSI_UpTrend3', 'rsi_uptrend3'),
            ('rsi_downtrend3', 'RSI_DownTrend3', 'rsi_downtrend3'),
            ('rsi4', 'RSI4', 'rsi4'),
            ('rsi_signal4', 'RSI_Signal4', 'rsi_signal4'),
            ('rsi_bulldiv4', 'RSI_BullDiv4', 'rsi_bulldiv4'),
            ('rsi_beardiv4', 'RSI_BearDiv4', 'rsi_beardiv4'),
            ('rsi_hidden_bulldiv4', 'RSI_Hidden_BullDiv4', 'rsi_hidden_bulldiv4'),
            ('rsi_hidden_beardiv4', 'RSI_Hidden_BearDiv4', 'rsi_hidden_beardiv4'),
            ('rsi_uptrend4', 'RSI_UpTrend4', 'rsi_uptrend4'),
            ('rsi_downtrend4', 'RSI_DownTrend4', 'rsi_downtrend4'),
            ('rsi5', 'RSI5', 'rsi5'),
            ('rsi_signal5', 'RSI_Signal5', 'rsi_signal5'),
            ('rsi_bulldiv5', 'RSI_BullDiv5', 'rsi_bulldiv5'),
            ('rsi_beardiv5', 'RSI_BearDiv5', 'rsi_beardiv5'),
            ('rsi_hidden_bulldiv5', 'RSI_Hidden_BullDiv5', 'rsi_hidden_bulldiv5'),
            ('rsi_hidden_beardiv5', 'RSI_Hidden_BearDiv5', 'rsi_hidden_beardiv5'),
            ('rsi_uptrend5', 'RSI_UpTrend5', 'rsi_uptrend5'),
            ('rsi_downtrend5', 'RSI_DownTrend5', 'rsi_downtrend5'),
            ('rsi6', 'RSI6', 'rsi6'),
            ('rsi_signal6', 'RSI_Signal6', 'rsi_signal6'),
            ('rsi_bulldiv6', 'RSI_BullDiv6', 'rsi_bulldiv6'),
            ('rsi_beardiv6', 'RSI_BearDiv6', 'rsi_beardiv6'),
            ('rsi_hidden_bulldiv6', 'RSI_Hidden_BullDiv6', 'rsi_hidden_bulldiv6'),
            ('rsi_hidden_beardiv6', 'RSI_Hidden_BearDiv6', 'rsi_hidden_beardiv6'),
            ('rsi_uptrend6', 'RSI_UpTrend6', 'rsi_uptrend6'),
            ('rsi_downtrend6', 'RSI_DownTrend6', 'rsi_downtrend6'),
            ('rsi7', 'RSI7', 'rsi7'),
            ('rsi_signal7', 'RSI_Signal7', 'rsi_signal7'),
            ('rsi_bulldiv7', 'RSI_BullDiv7', 'rsi_bulldiv7'),
            ('rsi_beardiv7', 'RSI_BearDiv7', 'rsi_beardiv7'),
            ('rsi_hidden_bulldiv7', 'RSI_Hidden_BullDiv7', 'rsi_hidden_bulldiv7'),
            ('rsi_hidden_beardiv7', 'RSI_Hidden_BearDiv7', 'rsi_hidden_beardiv7'),
            ('rsi_uptrend7', 'RSI_UpTrend7', 'rsi_uptrend7'),
            ('rsi_downtrend7', 'RSI_DownTrend7', 'rsi_downtrend7'),
            ('rsi8', 'RSI8', 'rsi8'),
            ('rsi_signal8', 'RSI_Signal8', 'rsi_signal8'),
            ('rsi_bulldiv8', 'RSI_BullDiv8', 'rsi_bulldiv8'),
            ('rsi_beardiv8', 'RSI_BearDiv8', 'rsi_beardiv8'),
            ('rsi_hidden_bulldiv8', 'RSI_Hidden_BullDiv8', 'rsi_hidden_bulldiv8'),
            ('rsi_hidden_beardiv8', 'RSI_Hidden_BearDiv8', 'rsi_hidden_beardiv8'),
            ('rsi_uptrend8', 'RSI_UpTrend8', 'rsi_uptrend8'),
            ('rsi_downtrend8', 'RSI_DownTrend8', 'rsi_downtrend8'),
            ('rsi9', 'RSI9', 'rsi9'),
            ('rsi_signal9', 'RSI_Signal9', 'rsi_signal9'),
            ('rsi_bulldiv9', 'RSI_BullDiv9', 'rsi_bulldiv9'),
            ('rsi_beardiv9', 'RSI_BearDiv9', 'rsi_beardiv9'),
            ('rsi_hidden_bulldiv9', 'RSI_Hidden_BullDiv9', 'rsi_hidden_bulldiv9'),
            ('rsi_hidden_beardiv9', 'RSI_Hidden_BearDiv9', 'rsi_hidden_beardiv9'),
            ('rsi_uptrend9', 'RSI_UpTrend9', 'rsi_uptrend9'),
            ('rsi_downtrend9', 'RSI_DownTrend9', 'rsi_downtrend9'),
            ('rsi_signal_sum', 'RSI_Signal_Sum', 'rsi_signal_sum'),
            ('rsi_bulldiv_sum', 'RSI_BullDiv_Sum', 'rsi_bulldiv_sum'),
            ('rsi_beardiv_sum', 'RSI_BearDiv_Sum', 'rsi_beardiv_sum'),
            ('rsi_hidden_bulldiv_sum', 'RSI_Hidden_BullDiv_Sum', 'rsi_hidden_bulldiv_sum'),
            ('rsi_hidden_beardiv_sum', 'RSI_Hidden_BearDiv_Sum', 'rsi_hidden_beardiv_sum'),
            ('rsi_uptrend_sum', 'RSI_UpTrend_Sum', 'rsi_uptrend_sum'),
            ('rsi_downtrend_sum', 'RSI_DownTrend_Sum', 'rsi_downtrend_sum'),
            ('std20', 'STD20', 'std20'),
            ('bollinger_high', 'BB_Upper', 'bb_upper'),
            ('bollinger_low', 'BB_Lower', 'bb_lower'),
            ('bb_width', 'BB_width', 'bb_width'),
            ('cci', 'CCI', 'cci'),
            ('cci_signal', 'CCI_Signal', 'cci_signal'),
            ('cci2', 'CCI2', 'cci2'),
            ('cci_signal2', 'CCI_Signal2', 'cci_signal2'),
            ('cci3', 'CCI3', 'cci3'),
            ('cci_signal3', 'CCI_Signal3', 'cci_signal3'),
            ('cci4', 'CCI4', 'cci4'),
            ('cci_signal4', 'CCI_Signal4', 'cci_signal4'),
            ('cci5', 'CCI5', 'cci5'),
            ('cci_signal5', 'CCI_Signal5', 'cci_signal5'),
            ('cci6', 'CCI6', 'cci6'),
            ('cci_signal6', 'CCI_Signal6', 'cci_signal6'),
            ('cci7', 'CCI7', 'cci7'),
            ('cci_signal7', 'CCI_Signal7', 'cci_signal7'),
            ('cci8', 'CCI8', 'cci8'),
            ('cci_signal8', 'CCI_Signal8', 'cci_signal8'),
            ('cci9', 'CCI9', 'cci9'),
            ('cci_signal9', 'CCI_Signal9', 'cci_signal9'),
            ('cci_signal_sum', 'CCI_Signal_Sum', 'cci_signal_sum'),
            ('macd', 'MACD', 'macd'),
            ('macd_base', 'MACD_Base', 'macd_base'),
            ('macd_hist', 'MACD_Hist', 'macd_hist'),
            ('macd_hist_vel', 'MACD_Hist_Vel', 'macd_hist_vel'),
            ('macd_hist_acc_pct', 'MACD_Hist_Acc_Pct', 'macd_hist_acc_pct'),
            ('macd_positive', 'MACD_Positive', 'macd_positive'),
            ('macd_signal', 'MACD_Signal', 'macd_signal'),
            ('pdi', 'PDI', 'pdi'),
            ('mdi', 'MDI', 'mdi'),
            ('adx', 'ADX', 'adx'),
            ('mom10', 'MOM10', 'mom10'),
            ('mom14', 'MOM14', 'mom14'),
            ('mom25', 'MOM25', 'mom25'),
            ('mom28', 'MOM28', 'mom28'),
            ('mom10_signal', 'MOM10_Signal', 'mom10_signal'),
            ('mom14_signal', 'MOM14_Signal', 'mom14_signal'),
            ('mom25_signal', 'MOM25_Signal', 'mom25_signal'),
            ('mom28_signal', 'MOM28_Signal', 'mom28_signal'),
            ('high_watermark', 'High_watermark', 'high_watermark'),
            ('mdd', 'MDD', 'mdd'),
            ('is_pinbar', 'Is_Pinbar', 'is_pinbar'),
            ('bb_fakeout', 'BB_Fakeout', 'bb_fakeout'),
            ('volume_climax', 'Volume_Climax', 'volume_climax'),
        ]

        for _, row in df_clean.iterrows():
            d = row.get('Date', row.get('date'))
            sym = row.get('Symbol', row.get('symbol', row.get('Code', row.get('code'))))
            if not d or not sym:
                continue
            
            name = str(row.get('Name', row.get('name', '')))
            mkt = str(row.get('Market', row.get('market', '')))
            
            val_tuple = [str(d), str(sym), name, mkt]
            for db_c, py_c, py_c_low in col_mappings:
                val = row.get(py_c, row.get(py_c_low))
                
                # Check for NaN/Inf/NaT and replace with None
                if val is not None:
                    if pd.isna(val):
                        val = None
                    elif isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
                        val = None
                
                val_tuple.append(val)
                
            records.append(tuple(val_tuple))

        cols_sql = 'date, symbol, name, market, ma1, ma2, ma3, ma4, ma5, ma6, ma7, ma8, ma9, ma20, ma50, ma60, ma90, ma120, ma200, ma224, close_diff_first, close_rate_first, currencyvolume, currencyvolume_ratio_ma20, obv, mobv, deltamobv, rsi, rsi_signal, rsi_bulldiv, rsi_beardiv, rsi_hidden_bulldiv, rsi_hidden_beardiv, rsi_uptrend, rsi_downtrend, rsi2, rsi_signal2, rsi_bulldiv2, rsi_beardiv2, rsi_hidden_bulldiv2, rsi_hidden_beardiv2, rsi_uptrend2, rsi_downtrend2, rsi3, rsi_signal3, rsi_bulldiv3, rsi_beardiv3, rsi_hidden_bulldiv3, rsi_hidden_beardiv3, rsi_uptrend3, rsi_downtrend3, rsi4, rsi_signal4, rsi_bulldiv4, rsi_beardiv4, rsi_hidden_bulldiv4, rsi_hidden_beardiv4, rsi_uptrend4, rsi_downtrend4, rsi5, rsi_signal5, rsi_bulldiv5, rsi_beardiv5, rsi_hidden_bulldiv5, rsi_hidden_beardiv5, rsi_uptrend5, rsi_downtrend5, rsi6, rsi_signal6, rsi_bulldiv6, rsi_beardiv6, rsi_hidden_bulldiv6, rsi_hidden_beardiv6, rsi_uptrend6, rsi_downtrend6, rsi7, rsi_signal7, rsi_bulldiv7, rsi_beardiv7, rsi_hidden_bulldiv7, rsi_hidden_beardiv7, rsi_uptrend7, rsi_downtrend7, rsi8, rsi_signal8, rsi_bulldiv8, rsi_beardiv8, rsi_hidden_bulldiv8, rsi_hidden_beardiv8, rsi_uptrend8, rsi_downtrend8, rsi9, rsi_signal9, rsi_bulldiv9, rsi_beardiv9, rsi_hidden_bulldiv9, rsi_hidden_beardiv9, rsi_uptrend9, rsi_downtrend9, rsi_signal_sum, rsi_bulldiv_sum, rsi_beardiv_sum, rsi_hidden_bulldiv_sum, rsi_hidden_beardiv_sum, rsi_uptrend_sum, rsi_downtrend_sum, std20, bollinger_high, bollinger_low, bb_width, cci, cci_signal, cci2, cci_signal2, cci3, cci_signal3, cci4, cci_signal4, cci5, cci_signal5, cci6, cci_signal6, cci7, cci_signal7, cci8, cci_signal8, cci9, cci_signal9, cci_signal_sum, macd, macd_base, macd_hist, macd_hist_vel, macd_hist_acc_pct, macd_positive, macd_signal, pdi, mdi, adx, mom10, mom14, mom25, mom28, mom10_signal, mom14_signal, mom25_signal, mom28_signal, high_watermark, mdd, is_pinbar, bb_fakeout, volume_climax'
        query = self.queries.get("013_upsert_technical_indicators.sql", f"""
        INSERT INTO public.technical_indicators ( {cols_sql} )
        VALUES %s
        ON CONFLICT (date, symbol) DO UPDATE SET
        """ + ", ".join([f"{c} = EXCLUDED.{c}" for c in cols_sql.split(", ") if c not in ["date", "symbol"]]) + ", created_at = CURRENT_TIMESTAMP;")
        try:
            with self.conn.cursor() as cur:
                from psycopg2.extras import execute_values
                execute_values(cur, query, records, page_size=1000)
                print(f"[DBManager] 기술분석 지표 {len(records)} 레코드 Upsert 성공.")
        except Exception as e:
            print(f"[DBManager] upsert_technical_indicators 실패: {e}")

    def upsert_trading_signals(self, df: pd.DataFrame):
        if not self.conn or df.empty:
            return
        df_clean = df.where(pd.notnull(df), None)
        records = []
        for _, row in df_clean.iterrows():
            d = row.get('Date', row.get('date'))
            sym = row.get('Symbol', row.get('symbol', row.get('Code', row.get('code'))))
            sig_type = row.get('Signal_Type', row.get('signal_type', row.get('Signal', 'NEUTRAL')))
            if not d or not sym or not sig_type:
                continue
            name = row.get('Name', row.get('name', ''))
            mkt = row.get('Market', row.get('market', ''))
            sig_strength = row.get('Signal_Strength', row.get('signal_strength', 1.0))
            desc = row.get('Description', row.get('description', ''))
            records.append((str(d), str(sym), str(name), str(mkt), str(sig_type), float(sig_strength) if sig_strength else 1.0, str(desc)))

        if not records:
            return

        query = self.queries.get("014_upsert_trading_signals.sql", """
        INSERT INTO public.trading_signals (date, symbol, name, market, signal_type, signal_strength, description)
        VALUES %s
        ON CONFLICT (date, symbol, signal_type) DO UPDATE SET
            name = EXCLUDED.name, market = EXCLUDED.market, signal_strength = EXCLUDED.signal_strength, description = EXCLUDED.description;
        """)
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, query, records, page_size=1000)
                print(f"[DBManager] 트레이딩 시그널 {len(records)} 레코드 Upsert 성공.")
        except Exception as e:
            print(f"[DBManager] upsert_trading_signals 실패: {e}")
