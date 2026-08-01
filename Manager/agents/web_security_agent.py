# WEB 취약점 전담 탐구 에이전트 (WEB Security Audit Agent)
import re

class WEBSecurityAgent:
    def __init__(self):
        self.web_vulnerability_patterns = [
            r"^(http://|https://)", r"(data:text/html)", r"(vbscript:)", r"(blob:)"
        ]

    def inspect_client_input(self, input_val: str) -> dict:
        if not isinstance(input_val, str):
            return {"is_safe": True, "reason": "안전함"}
            
        for pattern in self.web_vulnerability_patterns:
            if re.search(pattern, input_val, re.IGNORECASE):
                return {
                    "is_safe": False,
                    "reason": f"WEB 취약점 탐구 감지: 원격 스크립트 또는 오픈 리다이렉트 가능성 발견 ('{pattern}')",
                    "severity": "MEDIUM"
                }
        return {"is_safe": True, "reason": "WEB 자원 보안 통과"}
