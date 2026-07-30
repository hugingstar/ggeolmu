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

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            self.conn.autocommit = True
        except Exception as e:
            print(f"[DBManager] Connection failed: {e}")

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

