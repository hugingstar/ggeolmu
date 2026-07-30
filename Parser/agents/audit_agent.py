# 불필요한 것을 계속 검토하는 에이전트 (Optimization & Audit Agent)
import re

class AuditAgent:
    def __init__(self):
        # 파이프라인에서 가져온 배제 키워드 목록
        self.exclude_words = [
            "TIGER", "KODEX", "RISE", "TIME", "SOL", "ACE", "TDF", "ETN", "PLUS", 
            "1Q", "KIWOOM", "KoAct", "WON", "HANARO", "KCGI", "FOCUS", "에셋플러스", 
            "DAISHIN", "MIDAS", "TRUSTON", "스팩", "SPAC", "KOSEF", "Fund", "Unit", 
            "Right", "Rights", "VIX", "국고채", "채권", "혼합", "KBSTAR", "KINDEX", 
            "ARIRANG", "마이티", "TREX", "특수채", "회사채", "UNICORN", "2차전지양극재", 
            "IBK K-AI", "액티브", "온디바이스AI", "인덱스", "Acquisition", "Warrant", 
            "ETF", "Trust", "200"
        ]
        
        # 위험한 SQL 패턴
        self.sql_injection_patterns = [
            r"(--)", r"(;)", r"(\bUNION\b)", r"(\bSELECT\b.*\bFROM\b)", 
            r"(\bDROP\b)", r"(\bDELETE\b)", r"(\bUPDATE\b)", r"('OR')", r"(\"OR\")"
        ]

    def audit_stock(self, stock_name: str) -> dict:
        """
        특정 종목명이 분석에 불필요한 종목(ETF, 채권 등)인지 검토합니다.
        """
        for word in self.exclude_words:
            if word.upper() in stock_name.upper():
                return {
                    "is_valid": False, 
                    "reason": f"불필요한 파생/펀드 종목 감지: '{word}'"
                }
        return {"is_valid": True, "reason": "유효한 주식 종목"}

    def audit_prompt(self, prompt: str) -> dict:
        """
        생성된 프롬프트나 텍스트 안에 잠재적 공격(Prompt Injection, SQLi)이 있는지 검토.
        """
        for pattern in self.sql_injection_patterns:
            if re.search(pattern, prompt, re.IGNORECASE):
                return {
                    "is_safe": False, 
                    "reason": "악성 SQL 인젝션 패턴 감지"
                }
        return {"is_safe": True, "reason": "안전함"}
