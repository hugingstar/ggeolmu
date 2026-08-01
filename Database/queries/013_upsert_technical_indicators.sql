-- 013_upsert_technical_indicators.sql
-- 기술적 분석 지표 (technical_indicators) 배치 Upsert

INSERT INTO public.technical_indicators (
    date, symbol, name, market, ma5, ma20, ma60, ma120, ma200, rsi, macd, macd_signal, adx, bollinger_high, bollinger_low,
    rsi_signal_sum, rsi_bulldiv_sum, rsi_beardiv_sum, rsi_hidden_bulldiv_sum, rsi_hidden_beardiv_sum, rsi_uptrend_sum, rsi_downtrend_sum, cci_signal_sum
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
    rsi_signal_sum = EXCLUDED.rsi_signal_sum,
    rsi_bulldiv_sum = EXCLUDED.rsi_bulldiv_sum,
    rsi_beardiv_sum = EXCLUDED.rsi_beardiv_sum,
    rsi_hidden_bulldiv_sum = EXCLUDED.rsi_hidden_bulldiv_sum,
    rsi_hidden_beardiv_sum = EXCLUDED.rsi_hidden_beardiv_sum,
    rsi_uptrend_sum = EXCLUDED.rsi_uptrend_sum,
    rsi_downtrend_sum = EXCLUDED.rsi_downtrend_sum,
    cci_signal_sum = EXCLUDED.cci_signal_sum,
    created_at = CURRENT_TIMESTAMP;
