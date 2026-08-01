-- 012_create_technical_tables.sql
-- 기술적 분석 지표 (technical_indicators) 및 트레이딩 시그널 (trading_signals) 테이블 생성

CREATE TABLE IF NOT EXISTS public.technical_indicators (
    date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(100),
    market VARCHAR(20),
    ma5 NUMERIC,
    ma20 NUMERIC,
    ma60 NUMERIC,
    ma120 NUMERIC,
    ma200 NUMERIC,
    rsi NUMERIC,
    macd NUMERIC,
    macd_signal NUMERIC,
    adx NUMERIC,
    bollinger_high NUMERIC,
    bollinger_low NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, symbol)
);

CREATE TABLE IF NOT EXISTS public.trading_signals (
    date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(100),
    market VARCHAR(20),
    signal_type VARCHAR(50),
    signal_strength NUMERIC,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, symbol, signal_type)
);

CREATE INDEX IF NOT EXISTS idx_tech_indicators_symbol_date ON public.technical_indicators(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_trading_signals_symbol_date ON public.trading_signals(symbol, date DESC);
