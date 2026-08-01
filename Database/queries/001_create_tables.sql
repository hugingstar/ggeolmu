-- 001_create_tables.sql
-- 기초 테이블 초기화 스크립트 (SQL Injection 방지 및 중앙 관리를 위함)

CREATE TABLE IF NOT EXISTS public.raw_stock_data (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(100),
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    change NUMERIC,
    UNIQUE (date, symbol)
);

CREATE TABLE IF NOT EXISTS public.prompt_logs (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    generated_prompt TEXT,
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.market_cap (
    date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    market_cap_krw NUMERIC,
    PRIMARY KEY (date, symbol)
);

CREATE TABLE IF NOT EXISTS public.zscore_features (
    date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    freq VARCHAR(10) NOT NULL,
    zscore NUMERIC,
    PRIMARY KEY (date, symbol, freq)
);

CREATE TABLE IF NOT EXISTS public.clustering_results (
    target_date DATE NOT NULL,
    market VARCHAR(20),
    symbol VARCHAR(20) NOT NULL,
    cluster_id INTEGER,
    method VARCHAR(50),
    PRIMARY KEY (target_date, symbol, method)
);
