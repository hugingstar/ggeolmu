-- 010_upsert_clustering_results.sql
-- 시계열 클러스터링 결과 파라미터 바인딩 배치 Upsert (SQL Injection 방지)

INSERT INTO public.clustering_results (target_date, market, symbol, cluster_id, method)
VALUES %s
ON CONFLICT (target_date, symbol, method) DO UPDATE SET
    market = EXCLUDED.market,
    cluster_id = EXCLUDED.cluster_id;
