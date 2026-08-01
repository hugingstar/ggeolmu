-- 004_insert_prompt_log.sql
-- 에이전트의 검토 및 생성된 프롬프트 결과를 로그 테이블에 기록
INSERT INTO public.prompt_logs (symbol, generated_prompt, status)
VALUES (%s, %s, %s);
