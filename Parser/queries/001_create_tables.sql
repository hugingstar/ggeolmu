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
