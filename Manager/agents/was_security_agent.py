# WAS 취약점 전담 탐구 에이전트 (WAS Security Audit Agent)
import re

class WASSecurityAgent:
    def __init__(self):
        self.was_vulnerability_patterns = [
            r"(\.\./|\.\.\\)", r"(<script.*?>)", r"(javascript:)", 
            r"(onload=)", r"(onerror=)", r"(eval\(.*?\))"
        ]

    def inspect_request_path_and_params(self, param: str) -> dict:
        if not isinstance(param, str):
            return {"is_safe": True, "reason": "문자열 파라미터가 아니므로 안전함"}
            
        for pattern in self.was_vulnerability_patterns:
            if re.search(pattern, param, re.IGNORECASE):
                return {
                    "is_safe": False,
                    "reason": f"WAS 취약점 탐구 감지: XSS 또는 경로 이탈(Path Traversal) 공격 패턴 발견 ('{pattern}')",
                    "severity": "HIGH"
                }
        return {"is_safe": True, "reason": "WAS 요청 보안 통과"}
