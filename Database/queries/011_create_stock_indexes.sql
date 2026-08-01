-- 011_create_stock_indexes.sql
-- PostgreSQL B-Tree 복합 인덱스 생성 (초고속 Sub-5ms 조회 속도 최적화)

CREATE INDEX IF NOT EXISTS idx_raw_stock_symbol_date ON public.raw_stock_data(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_raw_stock_name ON public.raw_stock_data(name);
CREATE INDEX IF NOT EXISTS idx_clustering_results_market_symbol ON public.clustering_results(market, symbol);
CREATE INDEX IF NOT EXISTS idx_zscore_features_freq_date ON public.zscore_features(freq, date DESC);
