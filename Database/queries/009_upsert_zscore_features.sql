-- 009_upsert_zscore_features.sql
-- Z-Score 피처 데이터 파라미터 바인딩 배치 Upsert (SQL Injection 방지)

INSERT INTO public.zscore_features (date, symbol, freq, zscore)
VALUES %s
ON CONFLICT (date, symbol, freq) DO UPDATE SET
    zscore = EXCLUDED.zscore;
