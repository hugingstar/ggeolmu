-- 003_select_prompt_data.sql
-- 프롬프트 에이전트가 최근 5일치 종목 정보를 가져오기 위해 사용하는 쿼리

SELECT id, date, symbol, name, open, close, volume, change 
FROM public.raw_stock_data 
WHERE symbol = %s 
ORDER BY date DESC 
LIMIT 5;
