from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sys
import os

# 모듈 인식을 위해 상위 디렉토리(Parser)를 패스에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_manager import DBManager
from agents.audit_agent import AuditAgent
from agents.prompt_maker_agent import PromptMakerAgent

app = FastAPI(title="Ggeolmu Parser Multi-Agent API")

# CORS 설정 (WEB 프론트엔드 통신 허용)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 인스턴스 초기화
db = DBManager()
audit_agent = AuditAgent()
prompt_maker = PromptMakerAgent(db)

class PromptResponse(BaseModel):
    symbol: str
    is_valid: bool
    audit_reason: str
    generated_prompt: str = ""

@app.on_event("startup")
def startup_event():
    # 테이블 초기화 시도
    db.initialize_tables()

@app.get("/api/prompt", response_model=PromptResponse)
def get_prompt_for_symbol(symbol: str = Query(..., description="종목 코드 또는 이름")):
    """
    사용자가 종목을 검색했을 때 호출되는 엔드포인트.
    1. AuditAgent가 검색어를 검사합니다.
    2. 유효하면 PromptMakerAgent가 DB 조회를 통해 메타 프롬프트를 생성합니다.
    """
    # 1. Audit (검토 에이전트 개입)
    audit_res = audit_agent.audit_stock(symbol)
    
    if not audit_res["is_valid"]:
        return PromptResponse(
            symbol=symbol,
            is_valid=False,
            audit_reason=audit_res["reason"]
        )
    
    # 2. Prompt Making (프롬프트 생성 에이전트 개입)
    meta_prompt = prompt_maker.generate_prompt(symbol)
    
    # 3. 추가 Audit (생성된 프롬프트 내용에 SQL 인젝션 패턴이 들어갔는지 등)
    prompt_audit = audit_agent.audit_prompt(meta_prompt)
    if not prompt_audit["is_safe"]:
        return PromptResponse(
            symbol=symbol,
            is_valid=False,
            audit_reason=f"생성된 프롬프트가 안전하지 않습니다: {prompt_audit['reason']}"
        )

    return PromptResponse(
        symbol=symbol,
        is_valid=True,
        audit_reason="안전 (검토 통과)",
        generated_prompt=meta_prompt
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
