-- 008_select_cluster_series.sql
-- 시계열 클러스터링 군집별 대표 시계열 파형 (Z-Score 및 주가 추세) 데이터 조회

SELECT 
    cr.cluster_id, 
    zf.date, 
    AVG(zf.zscore) as avg_zscore
FROM public.clustering_results cr
JOIN public.zscore_features zf ON cr.symbol = zf.symbol
WHERE (%s = 'ALL' OR cr.market = %s)
GROUP BY cr.cluster_id, zf.date
ORDER BY cr.cluster_id ASC, zf.date ASC;
