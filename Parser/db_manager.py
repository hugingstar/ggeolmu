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
        
        # 쿼리 파일 캐싱
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
            os.path.join(project_root, "Database", "queries"),
            os.path.join(curr_dir, "queries"),
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
        """기본적인 종목 데이터 및 프롬프트 결과 저장용 테이블 생성"""
        if not self.conn:
            return
        
        create_query = self.get_query("001_create_tables.sql")
        if not create_query:
            print("[DBManager] Error: 001_create_tables.sql not found.")
            return

        try:
            with self.conn.cursor() as cur:
                cur.execute(create_query)
                print("[DBManager] Tables initialized successfully.")
        except Exception as e:
            print(f"[DBManager] Table creation failed: {e}")

    def insert_raw_data(self, df: pd.DataFrame):
        """
        get_fdr 등에서 가져온 pandas DataFrame을 테이블에 삽입 (UPSERT)
        """
        if not self.conn or df.empty:
            return

        insert_query = self.get_query("002_insert_raw_data.sql")
        if not insert_query:
            print("[DBManager] Error: 002_insert_raw_data.sql not found.")
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
                print(f"[DBManager] Inserted/Updated {len(records)} records.")
        except Exception as e:
            print(f"[DBManager] Insert failed: {e}")

    def read_query_direct(self, query_name_or_str: str, params: tuple = None) -> list:
        """
        직접 쿼리 문자열을 전달받거나, 쿼리 파일 이름을 전달받아 실행
        """
        if not self.conn:
            return []
        
        # 만약 .sql 로 끝나는 파일 이름이면 queries 딕셔너리에서 로드
        query = self.queries.get(query_name_or_str, query_name_or_str)
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()
        except Exception as e:
            print(f"[DBManager] Read query failed: {e}")
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
            print(f"[DBManager] Write query failed: {e}")
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
            print(f"[DBManager] read_query failed: {e}")
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
                print(f"[DBManager] Upserted {len(records)} market_cap records.")
        except Exception as e:
            print(f"[DBManager] upsert_market_cap failed: {e}")

    def upsert_zscore_features(self, df: pd.DataFrame, freq: str):
        if not self.conn or df.empty:
            return
            
        df_clean = df.where(pd.notnull(df), None)
        records = []
        for _, row in df_clean.iterrows():
            d = row.get('Date')
            sym = row.get('Symbol', row.get('Code'))
            zs = row.get('ZScore', row.get('zscore'))
            if pd.isna(zs):
                zs = None
            records.append((d, sym, freq, zs))
            
        query = """
        INSERT INTO public.zscore_features (date, symbol, freq, zscore)
        VALUES %s
        ON CONFLICT (date, symbol, freq) DO UPDATE SET
            zscore = EXCLUDED.zscore
        """
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, query, records, page_size=1000)
                print(f"[DBManager] Upserted {len(records)} zscore_features records for {freq}.")
        except Exception as e:
            print(f"[DBManager] upsert_zscore_features failed: {e}")

    def upsert_clustering_results(self, df: pd.DataFrame):
        if not self.conn or df.empty:
            return
            
        df_clean = df.where(pd.notnull(df), None)
        records = []
        for _, row in df_clean.iterrows():
            td = row.get('TargetDate')
            mkt = row.get('Market')
            sym = row.get('Symbol', row.get('Code'))
            cid = row.get('Cluster_ID', row.get('Cluster'))
            method = row.get('Method')
            records.append((td, mkt, sym, cid, method))
            
        query = """
        INSERT INTO public.clustering_results (target_date, market, symbol, cluster_id, method)
        VALUES %s
        ON CONFLICT (target_date, symbol, method) DO UPDATE SET
            market = EXCLUDED.market,
            cluster_id = EXCLUDED.cluster_id
        """
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, query, records, page_size=1000)
                print(f"[DBManager] Upserted {len(records)} clustering_results records.")
        except Exception as e:
            print(f"[DBManager] upsert_clustering_results failed: {e}")


