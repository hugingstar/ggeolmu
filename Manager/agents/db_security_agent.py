# DB 취약점 전담 탐구 에이전트 (DB Security Audit Agent)
import re

class DBSecurityAgent:
    def __init__(self):
        self.sql_injection_patterns = [
            r"(--)", r"(/\*|\*/)", r"(\bUNION\b)", r"(\bDROP\b)", 
            r"(\bDELETE\b)", r"(\bTRUNCATE\b)", r"(\bALTER\b)", r"(\bEXEC\b)",
            r"('OR')", r"(\"OR\")", r"(1=1)", r"('1'='1')"
        ]

    def inspect_query_parameter(self, param: str) -> dict:
        if not isinstance(param, str):
            return {"is_safe": True, "reason": "문자열 파라미터가 아니므로 안전함"}
            
        for pattern in self.sql_injection_patterns:
            if re.search(pattern, param, re.IGNORECASE):
                return {
                    "is_safe": False,
                    "reason": f"DB 취약점 탐구 감지: 파라미터에 악성 SQL 패턴 발견 ('{pattern}')",
                    "severity": "HIGH"
                }
        return {"is_safe": True, "reason": "DB 파라미터 보안 통과"}

    def verify_query_file_prefix(self, query_filename: str) -> dict:
        if not re.match(r"^\d{3}_.*\.sql$", query_filename):
            return {
                "is_safe": False,
                "reason": f"DB 쿼리 규칙 위반: '{query_filename}' 은 '001_' 과 같은 숫자 접두어 규칙을 따라야 합니다.",
                "severity": "MEDIUM"
            }
        return {"is_safe": True, "reason": "쿼리 파일 접두어 규칙 준수"}
