# -*- coding: utf-8 -*-
"""
Manager Tier: PipelineLifecycleAgent
파이프라인 실행 시작/끝 시간, 소요시간, 세부 단계 상태 및 에러 트레이스백을 MLFlow처럼 모니터링 관리하는 에이전트
"""

import uuid
import json
import traceback
from datetime import datetime
import pytz

class PipelineLifecycleAgent:
    def __init__(self, db_manager=None):
        self.kst = pytz.timezone('Asia/Seoul')
        self.db = db_manager

    def _get_db(self):
        if self.db is None:
            try:
                from db_manager import DBManager
                self.db = DBManager()
            except ImportError:
                import sys, os
                project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                sys.path.append(os.path.join(project_root, "Parser"))
                from db_manager import DBManager
                self.db = DBManager()
        return self.db

    def start_pipeline(self, market: str) -> str:
        """
        파이프라인 실행 시작 로깅 (status='RUNNING')
        """
        execution_id = f"EXEC_{market}_{datetime.now(self.kst).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        start_time = datetime.now(self.kst)
        
        db = self._get_db()
        db.initialize_tables() # 015_create_pipeline_logs 테이블 확인
        
        step_details = json.dumps({"initialized": True, "steps": []}, ensure_ascii=False)
        
        db.upsert_pipeline_log(
            execution_id=execution_id,
            market=market,
            start_time=start_time,
            end_time=None,
            duration_seconds=None,
            status='RUNNING',
            step_details=step_details,
            error_message=None
        )
        print(f"[{datetime.now(self.kst).strftime('%Y-%m-%d %H:%M:%S')}] 🤖 [PipelineAgent] {market} 파이프라인 기동 로깅 (ID: {execution_id})")
        return execution_id

    def _normalize_dt(self, dt):
        if dt is None:
            return datetime.now(self.kst)
        if dt.tzinfo is None:
            return self.kst.localize(dt)
        return dt.astimezone(self.kst)

    def log_step(self, execution_id: str, market: str, start_time: datetime, step_name: str, status: str = "SUCCESS", info: dict = None):
        """
        중간 세부 단계 진행 상태 기록
        """
        db = self._get_db()
        current_time = datetime.now(self.kst)
        st_norm = self._normalize_dt(start_time)
        duration = round((current_time - st_norm).total_seconds(), 2)
        
        detail_data = {
            "step_name": step_name,
            "status": status,
            "timestamp": current_time.strftime('%Y-%m-%d %H:%M:%S'),
            "info": info or {}
        }
        
        rows = db.read_query_direct("SELECT step_details FROM public.pipeline_execution_logs WHERE execution_id = %s;", (execution_id,))
        steps_list = []
        if rows and rows[0] and rows[0][0]:
            try:
                raw_json = rows[0][0]
                parsed = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
                steps_list = parsed.get("steps", [])
            except Exception:
                steps_list = []
                
        steps_list.append(detail_data)
        updated_details = json.dumps({"steps": steps_list}, ensure_ascii=False)
        
        db.upsert_pipeline_log(
            execution_id=execution_id,
            market=market,
            start_time=st_norm,
            end_time=current_time,
            duration_seconds=duration,
            status='RUNNING',
            step_details=updated_details,
            error_message=None
        )

    def finish_pipeline(self, execution_id: str, market: str, start_time: datetime, status: str = "SUCCESS", error: Exception = None):
        """
        파이프라인 마감 처리 (SUCCESS / FAILED, 소요시간 및 최종 상태 로깅)
        """
        end_time = datetime.now(self.kst)
        st_norm = self._normalize_dt(start_time)
        duration_seconds = round((end_time - st_norm).total_seconds(), 2)
        error_msg = None
        
        if error:
            error_msg = f"{type(error).__name__}: {str(error)}\n{traceback.format_exc()}"
            
        db = self._get_db()
        
        rows = db.read_query_direct("SELECT step_details FROM public.pipeline_execution_logs WHERE execution_id = %s;", (execution_id,))
        step_details = "{}"
        if rows and rows[0] and rows[0][0]:
            raw_json = rows[0][0]
            step_details = json.dumps(raw_json, ensure_ascii=False) if isinstance(raw_json, dict) else str(raw_json)

        db.upsert_pipeline_log(
            execution_id=execution_id,
            market=market,
            start_time=st_norm,
            end_time=end_time,
            duration_seconds=duration_seconds,
            status=status,
            step_details=step_details,
            error_message=error_msg
        )
        
        status_icon = "✅" if status == "SUCCESS" else "❌"
        print(f"[{datetime.now(self.kst).strftime('%Y-%m-%d %H:%M:%S')}] 🤖 [PipelineAgent] {market} 파이프라인 종료 {status_icon} (상태: {status}, 총 소요시간: {duration_seconds}초)")
