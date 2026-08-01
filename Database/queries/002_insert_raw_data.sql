-- 002_insert_raw_data.sql
-- Raw 데이터 벌크 삽입을 위한 UPSERT 쿼리

INSERT INTO public.raw_stock_data (date, symbol, name, open, high, low, close, volume, change)
VALUES %s
ON CONFLICT (date, symbol) 
DO UPDATE SET 
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    change = EXCLUDED.change;
