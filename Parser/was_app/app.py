from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
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
        db.write_query("004_insert_prompt_log.sql", (symbol, None, "REJECTED"))
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
        db.write_query("004_insert_prompt_log.sql", (symbol, meta_prompt, "REJECTED"))
        return PromptResponse(
            symbol=symbol,
            is_valid=False,
            audit_reason=f"생성된 프롬프트가 안전하지 않습니다: {prompt_audit['reason']}"
        )

    # 4. 성공 기록
    db.write_query("004_insert_prompt_log.sql", (symbol, meta_prompt, "PASS"))

    return PromptResponse(
        symbol=symbol,
        is_valid=True,
        audit_reason="안전 (검토 통과)",
        generated_prompt=meta_prompt
    )

@app.get("/api/search")
def search_stock(q: str = Query(..., description="검색어 (종목 코드 또는 이름)")):
    """
    종목 코드나 종목명의 일부를 입력하면 자동완성에 쓸 수 있는 매칭 목록을 반환합니다.
    """
    if not q or len(q.strip()) < 2:
        return []
    
    search_pattern = f"%{q.strip()}%"
    query = """
        SELECT DISTINCT symbol, name 
        FROM public.raw_stock_data 
        WHERE symbol ILIKE %s OR name ILIKE %s 
        LIMIT 10;
    """
    records = db.read_query_direct(query, (search_pattern, search_pattern))
    return [{"symbol": r[0], "name": r[1]} for r in records]

@app.get("/api/stock/{symbol}")
def get_stock_detail(symbol: str):
    """
    특정 종목의 상세 정보(이름, 최근 5일치 가격 배열)를 반환합니다.
    """
    records = db.read_query_direct("003_select_prompt_data.sql", (symbol,))
    if not records:
        raise HTTPException(status_code=404, detail="Stock not found")
    
    name = records[0][3]
    
    history = []
    for r in records:
        history.append({
            "date": str(r[1]),
            "open": float(r[4]) if r[4] is not None else None,
            "close": float(r[5]) if r[5] is not None else None,
            "volume": float(r[6]) if r[6] is not None else None,
            "change": float(r[7]) if r[7] is not None else None,
        })
    
    history.reverse()
    
    return {
        "symbol": symbol,
        "name": name,
        "history": history
    }

@app.get("/api/logs")
def get_audit_logs(limit: int = 50, offset: int = 0):
    """
    에이전트 검토 로그 목록을 최근순으로 반환합니다.
    """
    query = """
        SELECT id, symbol, generated_prompt, status, created_at 
        FROM public.prompt_logs 
        ORDER BY created_at DESC 
        LIMIT %s OFFSET %s;
    """
    records = db.read_query_direct(query, (limit, offset))
    return [
        {
            "id": r[0],
            "symbol": r[1],
            "generated_prompt": r[2],
            "status": r[3],
            "created_at": r[4].isoformat() if r[4] else None
        }
        for r in records
    ]

@app.get("/api/stats")
def get_system_stats():
    """
    시스템 및 데이터베이스 요약 메트릭을 반환합니다.
    """
    try:
        total_rows = db.read_query_direct("SELECT COUNT(*) FROM public.raw_stock_data;")[0][0]
    except Exception:
        total_rows = 0
        
    try:
        unique_stocks = db.read_query_direct("SELECT COUNT(DISTINCT symbol) FROM public.raw_stock_data;")[0][0]
    except Exception:
        unique_stocks = 0
        
    try:
        total_logs = db.read_query_direct("SELECT COUNT(*) FROM public.prompt_logs;")[0][0]
    except Exception:
        total_logs = 0
        
    try:
        latest_date_rec = db.read_query_direct("SELECT MAX(date) FROM public.raw_stock_data;")
        latest_date = str(latest_date_rec[0][0]) if latest_date_rec and latest_date_rec[0][0] else None
    except Exception:
        latest_date = None
    
    return {
        "total_rows": total_rows,
        "unique_stocks": unique_stocks,
        "total_logs": total_logs,
        "latest_date": latest_date,
        "status": "online"
    }

# 정적 파일 마운트 및 SPA Fallback 라우트 설정
static_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "web_static")

if os.path.exists(os.path.join(static_dir, "css")):
    app.mount("/css", StaticFiles(directory=os.path.join(static_dir, "css")), name="css")
if os.path.exists(os.path.join(static_dir, "js")):
    app.mount("/js", StaticFiles(directory=os.path.join(static_dir, "js")), name="js")

@app.get("/")
def read_index():
    return FileResponse(os.path.join(static_dir, "index.html"))

@app.get("/{catchall:path}")
def read_catchall(catchall: str):
    if catchall.startswith("api/"):
        raise HTTPException(status_code=404, detail="API route not found")
    return FileResponse(os.path.join(static_dir, "index.html"))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
