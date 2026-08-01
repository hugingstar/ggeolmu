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
        """queries/ 디렉토리 안의 모든 sql 스크립트를 로드합니다."""
        if not os.path.exists(self.query_dir):
            return
        
        for filename in os.listdir(self.query_dir):
            if filename.endswith('.sql'):
                file_path = os.path.join(self.query_dir, filename)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        self.queries[filename] = f.read()
                except Exception as e:
                    print(f"[DBManager] 쿼리 파일 로드 실패 {filename}: {e}")

    def get_query(self, query_name: str) -> str:
        """이름으로 저장된 쿼리를 반환합니다."""
        return self.queries.get(query_name, "")

    def initialize_tables(self):
        """기본적인 종목 데이터 및 프롬프트 결과 저장용 테이블 생성"""
        if not self.conn:
            return
        
        create_query = self.get_query("001_create_tables.sql")
        if not create_query:
            print("[DBManager] 오류: 001_create_tables.sql 파일을 찾을 수 없습니다.")
            return

        try:
            with self.conn.cursor() as cur:
                cur.execute(create_query)
                idx_query = self.get_query("011_create_stock_indexes.sql")
                if idx_query:
                    cur.execute(idx_query)
                tech_query = self.get_query("012_create_technical_tables.sql")
                if tech_query:
                    cur.execute(tech_query)
                print("[DBManager] 테이블 데이터베이스, B-Tree 인덱스 및 기술분석 스키마 초기화 성공.")
        except Exception as e:
            print(f"[DBManager] 테이블 생성 실패: {e}")

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
        df_clean = df.where(pd.notnull(df), None)
        records = []
        for _, row in df_clean.iterrows():
            d = row.get('Date', row.get('date'))
            sym = row.get('Symbol', row.get('symbol', row.get('Code', row.get('code'))))
            if not d or not sym:
                continue
            name = row.get('Name', row.get('name', ''))
            mkt = row.get('Market', row.get('market', ''))
            ma5 = row.get('MA5', row.get('ma5'))
            ma20 = row.get('MA20', row.get('ma20'))
            ma60 = row.get('MA60', row.get('ma60'))
            ma120 = row.get('MA120', row.get('ma120'))
            ma200 = row.get('MA200', row.get('ma200'))
            rsi = row.get('RSI', row.get('rsi'))
            macd = row.get('MACD', row.get('macd'))
            macd_sig = row.get('MACD_Signal', row.get('macd_signal'))
            adx = row.get('ADX', row.get('adx'))
            boll_h = row.get('Bollinger_High', row.get('bollinger_high'))
            boll_l = row.get('Bollinger_Low', row.get('bollinger_low'))
            records.append((str(d), str(sym), str(name), str(mkt), ma5, ma20, ma60, ma120, ma200, rsi, macd, macd_sig, adx, boll_h, boll_l))

        if not records:
            return

        query = self.queries.get("013_upsert_technical_indicators.sql", """
        INSERT INTO public.technical_indicators (date, symbol, name, market, ma5, ma20, ma60, ma120, ma200, rsi, macd, macd_signal, adx, bollinger_high, bollinger_low)
        VALUES %s
        ON CONFLICT (date, symbol) DO UPDATE SET
            name = EXCLUDED.name, market = EXCLUDED.market, ma5 = EXCLUDED.ma5, ma20 = EXCLUDED.ma20, ma60 = EXCLUDED.ma60, ma120 = EXCLUDED.ma120, ma200 = EXCLUDED.ma200, rsi = EXCLUDED.rsi, macd = EXCLUDED.macd, macd_signal = EXCLUDED.macd_signal, adx = EXCLUDED.adx, bollinger_high = EXCLUDED.bollinger_high, bollinger_low = EXCLUDED.bollinger_low;
        """)
        try:
            with self.conn.cursor() as cur:
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
