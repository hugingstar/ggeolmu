-- 016_upsert_pipeline_logs.sql
-- 파이프라인 실행 상태 및 마감 로깅 UPSERT 쿼리

INSERT INTO public.pipeline_execution_logs (
    execution_id,
    market,
    start_time,
    end_time,
    duration_seconds,
    status,
    step_details,
    error_message
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (execution_id)
DO UPDATE SET
    end_time = EXCLUDED.end_time,
    duration_seconds = EXCLUDED.duration_seconds,
    status = EXCLUDED.status,
    step_details = EXCLUDED.step_details,
    error_message = EXCLUDED.error_message;
