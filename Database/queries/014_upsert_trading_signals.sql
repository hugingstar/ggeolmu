-- 014_upsert_trading_signals.sql
-- 시그널 분석 데이터 (trading_signals) 배치 Upsert

INSERT INTO public.trading_signals (
    date, symbol, name, market, signal_type, signal_strength, description
) VALUES %s
ON CONFLICT (date, symbol, signal_type) 
DO UPDATE SET
    name = EXCLUDED.name,
    market = EXCLUDED.market,
    signal_strength = EXCLUDED.signal_strength,
    description = EXCLUDED.description,
    created_at = CURRENT_TIMESTAMP;
