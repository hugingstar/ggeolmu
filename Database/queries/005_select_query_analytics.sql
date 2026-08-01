-- 005_select_query_analytics.sql
-- 데이터 조회 건수 및 프롬프트 로그 기반 종목별 조회 통계 추출

SELECT symbol, COUNT(*) as query_count, MAX(created_at) as last_queried
FROM public.prompt_logs
GROUP BY symbol
ORDER BY query_count DESC
LIMIT 10;
