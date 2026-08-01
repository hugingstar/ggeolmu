-- 015_create_pipeline_logs.sql
-- 파이프라인 실행 시작/끝 시간, 소요시간, 실행 상태(SUCCESS/FAILED/RUNNING) 및 에러 로그 모니터링 테이블 생성

CREATE TABLE IF NOT EXISTS public.pipeline_execution_logs (
    execution_id VARCHAR(100) PRIMARY KEY,
    market VARCHAR(50) NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    duration_seconds NUMERIC,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    step_details JSONB,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
