# 4-Tier 통합 중앙 보안 및 라이프사이클 매니저 (Security & Lifecycle Manager)
import datetime
from typing import Dict, List, Any

from agents.web_security_agent import WEBSecurityAgent
from agents.was_security_agent import WASSecurityAgent
from agents.db_security_agent import DBSecurityAgent

class SecurityManager:
    """
    4-Tier 아키텍처 (WEB-WAS-DB-Manager) 중 Manager 계층으로서
    1. WEB, WAS, DB 각 계층에서 발생하는 취약점을 종합 수집/모니터링
    2. MLFlow 등 AI 모델 라이프사이클 관리 인터페이스 제공
    """
    def __init__(self):
        self.web_agent = WEBSecurityAgent()
        self.was_agent = WASSecurityAgent()
        self.db_agent = DBSecurityAgent()
        
        # 보안 이벤트 통합 중앙 감사 로그
        self.security_logs: List[Dict[str, Any]] = []

    def inspect_all_tiers(self, payload: str) -> Dict[str, Any]:
        """
        입력 파라미터 또는 요청에 대하여 WEB -> WAS -> DB 순서로 계층별 취약점 탐구를 종합적으로 수행합니다.
        """
        timestamp = datetime.datetime.now().isoformat()
        
        # 1. WEB 계층 검사
        web_res = self.web_agent.inspect_client_input(payload)
        if not web_res["is_safe"]:
            log_item = {"tier": "WEB", "timestamp": timestamp, "payload": payload, **web_res}
            self.security_logs.append(log_item)
            return {"is_safe": False, "failed_tier": "WEB", "details": web_res}

        # 2. WAS 계층 검사
        was_res = self.was_agent.inspect_request_path_and_params(payload)
        if not was_res["is_safe"]:
            log_item = {"tier": "WAS", "timestamp": timestamp, "payload": payload, **was_res}
            self.security_logs.append(log_item)
            return {"is_safe": False, "failed_tier": "WAS", "details": was_res}

        # 3. DB 계층 검사
        db_res = self.db_agent.inspect_query_parameter(payload)
        if not db_res["is_safe"]:
            log_item = {"tier": "DB", "timestamp": timestamp, "payload": payload, **db_res}
            self.security_logs.append(log_item)
            return {"is_safe": False, "failed_tier": "DB", "details": db_res}

        return {"is_safe": True, "message": "전 계층 (WEB-WAS-DB) 취약점 탐구 및 보안 검사 통과"}

    def get_security_summary(self) -> Dict[str, Any]:
        """
        Manager 계층에서 모니터링할 종합 보안 리포트 반환
        """
        web_fails = sum(1 for log in self.security_logs if log.get("tier") == "WEB")
        was_fails = sum(1 for log in self.security_logs if log.get("tier") == "WAS")
        db_fails = sum(1 for log in self.security_logs if log.get("tier") == "DB")
        
        return {
            "total_vulnerability_events": len(self.security_logs),
            "tier_failures": {
                "WEB": web_fails,
                "WAS": was_fails,
                "DB": db_fails
            },
            "latest_logs": self.security_logs[-10:] if self.security_logs else []
        }

    def log_mlflow_lifecycle(self, model_name: str, metric_name: str, metric_val: float):
        """
        Manager 계층에서 AIOps (MLFlow 등) 파이프라인 지표 및 라이프사이클 추적 로깅
        """
        print(f"[Manager AIOps] Logged {model_name} -> {metric_name}: {metric_val}")
