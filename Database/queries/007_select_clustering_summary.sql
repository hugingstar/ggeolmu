-- 007_select_clustering_summary.sql
-- 시계열 클러스터링 결과 및 군집별 소속 종목 목록 조회 (마켓 대소문자 통합 쿼리)

SELECT 
    cr.target_date, 
    cr.market, 
    cr.method, 
    cr.cluster_id, 
    cr.symbol, 
    COALESCE(r.name, cr.symbol) as name
FROM public.clustering_results cr
LEFT JOIN (
    SELECT DISTINCT symbol, name FROM public.raw_stock_data
) r ON cr.symbol = r.symbol
WHERE (%s = 'ALL' OR UPPER(cr.market) = UPPER(%s))
ORDER BY cr.cluster_id ASC, cr.symbol ASC;
