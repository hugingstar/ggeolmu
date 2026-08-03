-- 006_select_stock_history.sql
-- 특정 종목의 지정된 기간(LIMIT 파라미터) 주가 이력을 데이터베이스에서 안전하게 조회 (종목코드 또는 종목명 동시 지원)
-- 주의: psycopg2는 SQL 주석 안의 퍼센트-에스 자리표시자도 인식하므로 이 주석에는 그 표기를 쓰지 말 것

SELECT id, date, symbol, name, open, high, low, close, volume, change 
FROM public.raw_stock_data 
WHERE symbol = %s OR name = %s
ORDER BY date DESC 
LIMIT %s;
