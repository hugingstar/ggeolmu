-- 013_upsert_technical_indicators.sql
-- 기술적 분석 지표 (technical_indicators) 배치 Upsert

INSERT INTO public.technical_indicators (
    date, symbol, name, market, ma5, ma20, ma60, ma120, ma200, rsi, macd, macd_signal, adx, bollinger_high, bollinger_low
) VALUES %s
ON CONFLICT (date, symbol) 
DO UPDATE SET
    name = EXCLUDED.name,
    market = EXCLUDED.market,
    ma5 = EXCLUDED.ma5,
    ma20 = EXCLUDED.ma20,
    ma60 = EXCLUDED.ma60,
    ma120 = EXCLUDED.ma120,
    ma200 = EXCLUDED.ma200,
    rsi = EXCLUDED.rsi,
    macd = EXCLUDED.macd,
    macd_signal = EXCLUDED.macd_signal,
    adx = EXCLUDED.adx,
    bollinger_high = EXCLUDED.bollinger_high,
    bollinger_low = EXCLUDED.bollinger_low,
    created_at = CURRENT_TIMESTAMP;
