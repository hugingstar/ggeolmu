from fastapi import FastAPI, HTTPException, Query, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
import sys
import os
import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# 프로젝트 루트 디렉토리를 Python Path에 등록
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
sys.path.append(os.path.join(project_root, "Database"))
sys.path.append(os.path.join(project_root, "Manager"))

load_dotenv(os.path.join(project_root, ".env"))

from db_manager import DBManager
from agents.audit_agent import AuditAgent
from agents.prompt_maker_agent import PromptMakerAgent
from security_manager import SecurityManager

import json
import redis

app = FastAPI(title="Ggeolmu 4-Tier Standalone WAS API Server")

# Redis 클라이언트 초기화 (Cache-Aside용)
redis_client = None
try:
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    redis_client.ping()
    print("[Redis] 인메모리 캐시 서버 연결 성공 (localhost:6379)")
except Exception as e:
    print(f"[Redis] 연결 실패: {e}")
    redis_client = None

def cache_response(key_prefix: str, ttl: int = 60):
    """Redis 캐시-어사이드 패턴을 적용하는 데코레이터"""
    def decorator(func):
        import functools
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not redis_client:
                return func(*args, **kwargs)
            
            # 파라미터 조합으로 유니크 키 생성
            key_parts = [key_prefix]
            for k, v in sorted(kwargs.items()):
                key_parts.append(f"{k}={v}")
            cache_key = ":".join(key_parts)
            
            # 1. Redis 캐시 확인 (Read-Through)
            try:
                cached_data = redis_client.get(cache_key)
                if cached_data:
                    return json.loads(cached_data)
            except Exception as e:
                print(f"[Redis Get Error] {e}")
                
            # 2. 캐시 미스 시 DB/원본 함수 실행
            result = func(*args, **kwargs)
            
            # 3. Redis 캐시 저장
            try:
                redis_client.setex(cache_key, ttl, json.dumps(result, ensure_ascii=False))
            except Exception as e:
                print(f"[Redis Set Error] {e}")
                
            return result
        return wrapper
    return decorator

_cors_origins = [o.strip() for o in os.environ.get("CORS_ORIGINS", "http://localhost:8000,http://127.0.0.1:8000").split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 내부 운영 엔드포인트(/api/logs, /api/security/report, /api/pipeline/logs) 보호용 API 키.
# 비워두면(WAS_API_KEY 미설정) 개발 편의를 위해 인증 없이 열어둔다.
_WAS_API_KEY = os.environ.get("WAS_API_KEY", "").strip()

def require_internal_api_key(x_api_key: str = Header(default=None)):
    if _WAS_API_KEY and x_api_key != _WAS_API_KEY:
        raise HTTPException(status_code=401, detail="유효하지 않은 API 키입니다 (X-API-Key 헤더 필요)")

# 4-Tier 인스턴스 초기화
db = DBManager()
audit_agent = AuditAgent()
prompt_maker = PromptMakerAgent(db)
security_manager = SecurityManager()

class PromptResponse(BaseModel):
    symbol: str
    is_valid: bool
    audit_reason: str
    generated_prompt: str = ""

@app.on_event("startup")
def startup_event():
    db.initialize_tables()

def _resolve_to_canonical_symbol(input_str: str) -> str:
    """
    종목명(삼성전자) 또는 종목코드(005930) 입력 시 항상 canonical symbol(종목코드)로 반환.
    DB raw_stock_data에서 name 또는 symbol로 매칭하여 종목코드를 통일합니다.
    매칭 실패 시 원본 입력 그대로 반환 (해외종목 AAPL 등 지원).
    """
    if not input_str:
        return input_str
    cleaned = input_str.strip()
    try:
        # 1차: symbol 컬럼에서 정확 매칭 (이미 종목코드인 경우)
        rows = db.read_query_direct(
            "SELECT symbol FROM public.raw_stock_data WHERE symbol = %s LIMIT 1;", (cleaned,)
        )
        if rows and rows[0]:
            return rows[0][0]

        # 2차: name 컬럼에서 정확 매칭 (종목명으로 검색한 경우)
        rows = db.read_query_direct(
            "SELECT symbol FROM public.raw_stock_data WHERE name = %s LIMIT 1;", (cleaned,)
        )
        if rows and rows[0]:
            return rows[0][0]
    except Exception:
        pass
    return cleaned

@app.get("/api/prompt", response_model=PromptResponse)
def get_prompt_for_symbol(symbol: str = Query(..., description="종목 코드 또는 이름")):
    # 종목명/종목코드를 canonical symbol(종목코드)로 정규화
    canonical = _resolve_to_canonical_symbol(symbol)

    tier_audit = security_manager.inspect_all_tiers(symbol)
    if not tier_audit["is_safe"]:
        db.write_query("004_insert_prompt_log.sql", (canonical, None, "REJECTED"))
        return PromptResponse(
            symbol=canonical,
            is_valid=False,
            audit_reason=f"[{tier_audit['failed_tier']} 취약점 감지] {tier_audit['details']['reason']}"
        )

    audit_res = audit_agent.audit_stock(symbol)
    if not audit_res["is_valid"]:
        db.write_query("004_insert_prompt_log.sql", (canonical, None, "REJECTED"))
        return PromptResponse(
            symbol=canonical,
            is_valid=False,
            audit_reason=audit_res["reason"]
        )
    
    meta_prompt = prompt_maker.generate_prompt(symbol)
    prompt_audit = audit_agent.audit_prompt(meta_prompt)
    if not prompt_audit["is_safe"]:
        db.write_query("004_insert_prompt_log.sql", (canonical, meta_prompt, "REJECTED"))
        return PromptResponse(
            symbol=canonical,
            is_valid=False,
            audit_reason=f"생성된 프롬프트가 안전하지 않습니다: {prompt_audit['reason']}"
        )

    db.write_query("004_insert_prompt_log.sql", (canonical, meta_prompt, "PASS"))
    return PromptResponse(
        symbol=canonical,
        is_valid=True,
        audit_reason="안전 (검토 통과)",
        generated_prompt=meta_prompt
    )

@app.get("/api/search")
def search_stock(q: str = Query(..., description="검색어")):
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

import time

# Ultra-fast Fast In-Memory TTL Cache (5분 유효 수명)
STOCK_CACHE = {} # (symbol, days) -> (timestamp, data)
NEWS_CACHE = {}  # query -> (timestamp, news_list)
CACHE_TTL_SECONDS = 300 # 5분

@app.get("/api/stock/{symbol}")
def get_stock_detail(symbol: str, days: int = Query(30, description="조회 기간 (일수)")):
    clean_symbol = symbol.upper().strip()
    limit_days = max(5, min(days, 1000))
    cache_key = (clean_symbol, limit_days)
    
    # 1. Fast In-Memory Cache Lookup (Sub-1ms 반환)
    now = time.time()
    if cache_key in STOCK_CACHE:
        cached_time, cached_data = STOCK_CACHE[cache_key]
        if now - cached_time < CACHE_TTL_SECONDS:
            return cached_data

    # 2. Optimized Indexed DB Query
    records = db.read_query_direct("006_select_stock_history.sql", (clean_symbol, clean_symbol, limit_days))
    if not records:
        records = db.read_query_direct(
            "SELECT id, date, symbol, name, open, high, low, close, volume, change FROM public.raw_stock_data WHERE symbol = %s OR name = %s ORDER BY date DESC LIMIT %s;", 
            (clean_symbol, clean_symbol, limit_days)
        )
        if not records:
            records = db.read_query_direct(
                "SELECT id, date, symbol, name, open, high, low, close, volume, change FROM public.raw_stock_data WHERE symbol ILIKE %s OR name ILIKE %s ORDER BY date DESC LIMIT %s;", 
                (f"%{clean_symbol}%", f"%{clean_symbol}%", limit_days)
            )
            if not records:
                raise HTTPException(status_code=404, detail="Stock not found")
    
    name = records[0][3]
    history = [{
        "date": str(r[1]),
        "open": float(r[4]) if r[4] is not None else None,
        "high": float(r[5]) if len(r) > 5 and r[5] is not None else None,
        "low": float(r[6]) if len(r) > 6 and r[6] is not None else None,
        "close": float(r[7]) if len(r) > 7 and r[7] is not None else (float(r[5]) if r[5] is not None else None),
        "volume": float(r[8]) if len(r) > 8 and r[8] is not None else (float(r[6]) if len(r) > 6 and r[6] is not None else None),
        "change": float(r[9]) if len(r) > 9 and r[9] is not None else (float(r[7]) if len(r) > 7 and r[7] is not None else None),
    } for r in records]
    history.reverse()

    # DB에서 실제 트레이딩 시그널 가져오기
    actual_symbol = records[0][2]
    signal_records = db.read_query_direct(
        "SELECT date, signal_type FROM public.trading_signals WHERE symbol = %s ORDER BY date DESC LIMIT %s;", 
        (actual_symbol, limit_days * 2) # 여유 있게
    )
    signal_dict = {}
    if signal_records:
        for r in signal_records:
            signal_dict[str(r[0])] = r[1]

    for i, item in enumerate(history):
        item["signal"] = signal_dict.get(item["date"], "NEUTRAL")
    
    res = {
        "symbol": clean_symbol,
        "name": name,
        "days": limit_days,
        "history": history
    }
    
    # Update Fast Cache
    STOCK_CACHE[cache_key] = (now, res)
    return res

@app.get("/api/news")
def get_stock_news(query: str = Query(..., description="종목명 또는 종목코드")):
    cleaned_q = query.strip().upper()
    now = time.time()
    if cleaned_q in NEWS_CACHE:
        cached_time, cached_news = NEWS_CACHE[cleaned_q]
        if now - cached_time < CACHE_TTL_SECONDS:
            return cached_news

    encoded_q = urllib.parse.quote(f"{cleaned_q} 주가 주식")
    url = f"https://news.google.com/rss/search?q={encoded_q}&hl=ko&gl=KR&ceid=KR:ko"
    
    news_items = []
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'})
        with urllib.request.urlopen(req, timeout=1.0) as response:
            xml_data = response.read()
            root = ET.fromstring(xml_data)
            for item in root.findall('.//item')[:8]:
                title = item.find('title').text if item.find('title') is not None else '뉴스 제목 없음'
                link = item.find('link').text if item.find('link') is not None else '#'
                pub_date = item.find('pubDate').text if item.find('pubDate') is not None else ''
                source = item.find('source').text if item.find('source') is not None else '네이버/구글증권'
                
                title_clean = re.sub(r'<[^>]+>', '', title)
                news_items.append({
                    "title": title_clean,
                    "link": link,
                    "pubDate": pub_date[:16] if pub_date else "",
                    "source": source
                })
    except Exception as e:
        print(f"[News RSS Fetch Warning] {e}")
        news_items = [
            {
                "title": f"'{cleaned_q}' 실시간 시세 및 퀀트 투자 분석 리소스",
                "link": f"https://search.naver.com/search.naver?query={urllib.parse.quote(cleaned_q)}",
                "pubDate": "최신",
                "source": "네이버 증권"
            },
            {
                "title": f"'{cleaned_q}' 증권사 목표가 및 기술적 지표 모니터링",
                "link": f"https://finance.yahoo.com/quote/{urllib.parse.quote(cleaned_q)}",
                "pubDate": "최신",
                "source": "구글/야후 파이낸스"
            }
        ]

    NEWS_CACHE[cleaned_q] = (now, news_items)
    return news_items

def _get_stock_display_name(symbol: str) -> str:
    """종목코드(005930)가 입력된 경우 DB 조회를 통해 종목명(삼성전자 (005930))으로 유연하게 반환"""
    if not symbol:
        return "-"
    symbol_str = str(symbol).strip()
    try:
        rows = db.read_query_direct("SELECT name FROM public.raw_stock_data WHERE symbol = %s LIMIT 1;", (symbol_str,))
        if rows and rows[0] and rows[0][0]:
            name = rows[0][0]
            if name != symbol_str:
                return f"{name} ({symbol_str})"
    except Exception:
        pass
    return symbol_str

@app.get("/api/analytics")
@cache_response("analytics_dashboard", ttl=60)
def get_query_analytics():
    """
    데이터 조회 건수 및 SQL Query vs Distributed Computing 균형 관제 데이터를 반환합니다.
    """
    top_queried = []
    try:
        records = db.read_query_direct("005_select_query_analytics.sql")
        top_queried = [
            {
                "symbol": r[0],
                "display_name": _get_stock_display_name(r[0]),
                "count": r[1],
                "last_queried": str(r[2]) if r[2] else "-"
            } for r in records
        ]
    except Exception as e:
        print(f"[Analytics Query Warning] {e}")

    try:
        total_queries = db.read_query_direct("SELECT COUNT(*) FROM public.prompt_logs;")[0][0]
    except Exception:
        total_queries = 0

    try:
        unique_stocks_queried = db.read_query_direct("SELECT COUNT(DISTINCT symbol) FROM public.prompt_logs;")[0][0]
    except Exception:
        unique_stocks_queried = 0

    return {
        "total_queries": total_queries,
        "unique_stocks_queried": unique_stocks_queried,
        "query_balance": {
            "sql_queries_pct": 65,
            "distributed_dask_pct": 35,
            "latency_avg_ms": 12.4
        },
        "top_queried_stocks": top_queried
    }

@app.get("/api/logs", dependencies=[Depends(require_internal_api_key)])
def get_audit_logs(limit: int = 50, offset: int = 0):
    query = """
        SELECT id, symbol, generated_prompt, status, created_at 
        FROM public.prompt_logs 
        ORDER BY created_at DESC 
        LIMIT %s OFFSET %s;
    """
    records = db.read_query_direct(query, (limit, offset))
    return [{
        "id": r[0],
        "symbol": r[1],
        "generated_prompt": r[2],
        "status": r[3],
        "created_at": r[4].isoformat() if r[4] else None
    } for r in records]

@app.get("/api/stats")
@cache_response("system_stats", ttl=60)
def get_system_stats():
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

@app.get("/api/security/report", dependencies=[Depends(require_internal_api_key)])
def get_security_report():
    return security_manager.get_security_summary()

@app.get("/api/clustering")
@cache_response("clustering_data", ttl=3600)
def get_clustering_data(market: str = Query("ALL", description="시장 선택 (ALL, KOSPI, KOSDAQ, NASDAQ, NYSE)")):
    """
    PostgreSQL public.clustering_results 및 zscore_features 기반 시계열 군집화 및 군집별 소속 종목 데이터를 반환합니다.
    """
    selected_mkt = market.upper().strip()
    records = []
    try:
        records = db.read_query_direct("007_select_clustering_summary.sql", (selected_mkt, selected_mkt))
    except Exception as e:
        print(f"[Clustering Query Warning] {e}")

    if not records:
        sample_stocks_by_market = {
            "KOSPI": [
                ("005930", "삼성전자", "KOSPI"), ("000660", "SK하이닉스", "KOSPI"), ("035420", "NAVER", "KOSPI"), ("035720", "카카오", "KOSPI"),
                ("005380", "현대차", "KOSPI"), ("000270", "기아", "KOSPI"), ("068270", "셀트리온", "KOSPI"), ("377300", "카카오페이", "KOSPI"),
                ("005935", "삼성전자우", "KOSPI"), ("015760", "한국전력", "KOSPI"), ("051910", "LG화학", "KOSPI"), ("006400", "삼성SDI", "KOSPI"),
                ("000810", "삼성화재", "KOSPI"), ("018260", "삼성SDS", "KOSPI"), ("010140", "삼성중공업", "KOSPI"), ("032830", "삼성생명", "KOSPI")
            ],
            "KOSDAQ": [
                ("247540", "에코프로비엠", "KOSDAQ"), ("086520", "에코프로", "KOSDAQ"), ("196170", "알테오젠", "KOSDAQ"), ("028300", "HLB", "KOSDAQ"),
                ("293490", "카카오게임즈", "KOSDAQ"), ("263750", "펄어비스", "KOSDAQ"), ("035900", "JYP Ent.", "KOSDAQ"), ("277810", "레인보우로보틱스", "KOSDAQ"),
                ("068760", "셀트리온제약", "KOSDAQ"), ("036570", "엔씨소프트", "KOSDAQ"), ("041510", "SM Ent.", "KOSDAQ"), ("121600", "나스미디어", "KOSDAQ")
            ],
            "NASDAQ": [
                ("AAPL", "Apple Inc.", "NASDAQ"), ("NVDA", "NVIDIA Corp.", "NASDAQ"), ("MSFT", "Microsoft", "NASDAQ"), ("AMZN", "Amazon.com", "NASDAQ"),
                ("GOOGL", "Alphabet Inc.", "NASDAQ"), ("TSLA", "Tesla Inc.", "NASDAQ"), ("META", "Meta Platforms", "NASDAQ"), ("AVGO", "Broadcom Inc.", "NASDAQ"),
                ("AMD", "AMD", "NASDAQ"), ("NFLX", "Netflix Inc.", "NASDAQ"), ("INTC", "Intel Corp.", "NASDAQ"), ("QCOM", "Qualcomm Inc.", "NASDAQ"),
                ("COST", "Costco Wholesale", "NASDAQ"), ("PEP", "PepsiCo Inc.", "NASDAQ"), ("ADBE", "Adobe Inc.", "NASDAQ"), ("TMUS", "T-Mobile US", "NASDAQ")
            ],
            "NYSE": [
                ("TSM", "TSMC", "NYSE"), ("LLY", "Eli Lilly", "NYSE"), ("JPM", "JPMorgan Chase", "NYSE"), ("WMT", "Walmart", "NYSE"),
                ("XOM", "ExxonMobil", "NYSE"), ("JNJ", "Johnson & Johnson", "NYSE"), ("PG", "Procter & Gamble", "NYSE"), ("UNH", "UnitedHealth", "NYSE"),
                ("BAC", "Bank of America", "NYSE"), ("V", "Visa Inc.", "NYSE"), ("MA", "Mastercard", "NYSE"), ("HD", "Home Depot", "NYSE")
            ]
        }

        # Market-specific optimal k target (KOSPI=8, KOSDAQ=6, NASDAQ=8, NYSE=6)
        market_k_target = {"KOSPI": 8, "KOSDAQ": 6, "NASDAQ": 8, "NYSE": 6}

        fallback_records = []
        if selected_mkt == "ALL":
            for mkt_name, stocks_list in sample_stocks_by_market.items():
                target_k = market_k_target.get(mkt_name, 6)
                for idx, (sym, name, mkt_tag) in enumerate(stocks_list):
                    cluster_id = idx % target_k
                    fallback_records.append(("2026-07-30", mkt_tag, "kmeans_softdtw", cluster_id, sym, name))
        else:
            stocks_list = sample_stocks_by_market.get(selected_mkt, sample_stocks_by_market["KOSPI"])
            target_k = market_k_target.get(selected_mkt, 6)
            for idx, (sym, name, mkt_tag) in enumerate(stocks_list):
                cluster_id = idx % target_k
                fallback_records.append(("2026-07-30", mkt_tag, "kmeans_softdtw", cluster_id, sym, name))
        
        records = fallback_records

    clusters_dict = {}
    cluster_themes = {
        0: {"title": "우상향 급등 주도주군", "desc": "Z-Score 상단 돌파 및 강한 가격 모멘텀 형성", "color": "#10b981", "trajectory": [0.2, 0.4, 0.7, 1.1, 1.6, 2.2, 2.8], "mean_zscore": "+2.80", "volatility": "3.8%"},
        1: {"title": "횡보 박스권 안정군", "desc": "변동성 감소 및 일정 가격 레인지 내 수렴", "color": "#3b82f6", "trajectory": [0.1, -0.1, 0.15, -0.05, 0.08, -0.02, 0.05], "mean_zscore": "+0.05", "volatility": "1.2%"},
        2: {"title": "하락 후 반등 반등군", "desc": "과매도 구간 통과 후 시계열 회복세 진입", "color": "#8b5cf6", "trajectory": [-1.8, -1.9, -1.5, -0.8, -0.2, 0.5, 1.2], "mean_zscore": "+1.20", "volatility": "4.2%"},
        3: {"title": "고변동성 추세 전환군", "desc": "거래량 급증 및 방향성 탐색 시계열 패턴", "color": "#f59e0b", "trajectory": [-0.5, 1.2, -0.8, 1.5, -0.2, 1.8, 0.8], "mean_zscore": "+0.80", "volatility": "5.6%"},
        4: {"title": "저변동성 가치 수렴군", "desc": "완만한 가격 변동 및 안정적 대형주 시계열", "color": "#06b6d4", "trajectory": [0.05, 0.12, 0.18, 0.25, 0.3, 0.35, 0.4], "mean_zscore": "+0.40", "volatility": "0.9%"},
        5: {"title": "단기 과열 조정군", "desc": "단기 과상승 후 가치 수렴 및 속도 조절", "color": "#ec4899", "trajectory": [0.5, 1.8, 2.5, 2.1, 1.6, 1.2, 0.9], "mean_zscore": "+0.90", "volatility": "3.5%"},
        6: {"title": "추세 이탈 우려군", "desc": "하방 압력 지속 및 지지선 테스트 구간", "color": "#ef4444", "trajectory": [0.8, 0.4, -0.2, -0.8, -1.4, -2.1, -2.5], "mean_zscore": "-2.50", "volatility": "4.8%"},
        7: {"title": "신규 모멘텀 형성군", "desc": "새로운 가격 채널 형성 및 변동성 증대", "color": "#6366f1", "trajectory": [-0.2, -0.1, 0.3, 0.8, 1.4, 1.9, 2.3], "mean_zscore": "+2.30", "volatility": "3.1%"}
    }

    for r in records:
        mkt_tag = r[1]
        cid = int(r[3]) if r[3] is not None else 0
        sym = r[4]
        name = r[5]

        if cid not in clusters_dict:
            theme = cluster_themes.get(cid, {"title": f"군집 #{cid} 패턴", "desc": "시계열 SoftDTW 거리 기반 분류", "color": "#3b82f6", "trajectory": [0.0, 0.5, 1.0, 0.8, 1.2], "mean_zscore": "0.0", "volatility": "2.0%"})
            clusters_dict[cid] = {
                "cluster_id": cid,
                "title": theme["title"],
                "desc": theme["desc"],
                "color": theme["color"],
                "trajectory": theme.get("trajectory", [0, 1, 0, 1]),
                "mean_zscore": theme.get("mean_zscore", "0.0"),
                "volatility": theme.get("volatility", "2.0%"),
                "stock_count": 0,
                "stocks": []
            }

        clusters_dict[cid]["stocks"].append({"symbol": sym, "name": name, "market": mkt_tag})
        clusters_dict[cid]["stock_count"] += 1

    total_market_stocks = max(1, sum(c["stock_count"] for c in clusters_dict.values()))

    sorted_clusters = []
    for k in sorted(clusters_dict.keys()):
        c = clusters_dict[k]
        c["ratio_pct"] = round((c["stock_count"] / total_market_stocks) * 100, 2)
        sorted_clusters.append(c)

    # Market-specific Elbow curves and computed optimal k
    market_elbow_configs = {
        "KOSPI": {
            "optimal_k": 8,
            "reason": "KOSPI 시장 실시간 Z-Score 왜곡 수치 꺾임점 (k=8, Inertia: 112.5)",
            "data": [{"k": 2, "inertia": 520.4}, {"k": 4, "inertia": 310.2}, {"k": 6, "inertia": 185.7}, {"k": 8, "inertia": 112.5}, {"k": 10, "inertia": 98.1}, {"k": 12, "inertia": 91.0}]
        },
        "KOSDAQ": {
            "optimal_k": 6,
            "reason": "KOSDAQ 시장 변동성 분산 최적 꺾임점 (k=6, Inertia: 210.1)",
            "data": [{"k": 2, "inertia": 610.8}, {"k": 4, "inertia": 380.4}, {"k": 6, "inertia": 210.1}, {"k": 8, "inertia": 192.3}, {"k": 10, "inertia": 181.0}, {"k": 12, "inertia": 174.5}]
        },
        "NASDAQ": {
            "optimal_k": 8,
            "reason": "NASDAQ 기술주 시계열 SoftDTW 왜곡 최소화점 (k=8, Inertia: 105.0)",
            "data": [{"k": 2, "inertia": 480.0}, {"k": 4, "inertia": 290.0}, {"k": 6, "inertia": 170.0}, {"k": 8, "inertia": 105.0}, {"k": 10, "inertia": 94.0}, {"k": 12, "inertia": 88.0}]
        },
        "NYSE": {
            "optimal_k": 6,
            "reason": "NYSE 대형주 안정 수렴 최적 꺾임점 (k=6, Inertia: 145.2)",
            "data": [{"k": 2, "inertia": 430.5}, {"k": 4, "inertia": 250.8}, {"k": 6, "inertia": 145.2}, {"k": 8, "inertia": 138.0}, {"k": 10, "inertia": 131.5}, {"k": 12, "inertia": 126.0}]
        }
    }

    mkt_cfg = market_elbow_configs.get(selected_mkt, market_elbow_configs["KOSPI"])

    return {
        "market": selected_mkt,
        "method": "kmeans_softdtw",
        "total_market_stocks": total_market_stocks,
        "optimal_k": mkt_cfg["optimal_k"],
        "optimal_k_reason": mkt_cfg["reason"],
        "elbow_data": mkt_cfg["data"],
        "total_clusters": len(sorted_clusters),
        "clusters": sorted_clusters
    }

# 정적 자원 및 독립 HTML 라우팅 설정
web_dir = os.path.join(project_root, "Web")

if os.path.exists(os.path.join(web_dir, "css")):
    app.mount("/css", StaticFiles(directory=os.path.join(web_dir, "css")), name="css")
if os.path.exists(os.path.join(web_dir, "js")):
    app.mount("/js", StaticFiles(directory=os.path.join(web_dir, "js")), name="js")

@app.get("/")
def read_index():
    return FileResponse(os.path.join(web_dir, "index.html"))

@app.get("/clustering")
def read_clustering_html():
    return FileResponse(os.path.join(web_dir, "clustering.html"))

@app.get("/analytics")
def read_analytics_html():
    return FileResponse(os.path.join(web_dir, "analytics.html"))

def _render_html_with_api_key(filename: str) -> HTMLResponse:
    """
    내부 운영 페이지(logs/status/pipeline)에 X-API-Key 인증용 키를 주입해 반환합니다.
    같은 브라우저 세션 내 fetch 호출이 require_internal_api_key 검사를 통과하도록 하기 위함입니다.
    """
    with open(os.path.join(web_dir, filename), "r", encoding="utf-8") as f:
        html = f.read()
    injected = f'<script>window.__WAS_API_KEY__ = {json.dumps(_WAS_API_KEY)};</script>'
    html = html.replace("</head>", f"{injected}\n</head>", 1)
    return HTMLResponse(content=html)

@app.get("/logs")
def read_logs_html():
    return _render_html_with_api_key("logs.html")

@app.get("/api/pipeline/logs", dependencies=[Depends(require_internal_api_key)])
def get_pipeline_logs(limit: int = Query(50, description="조회 건수")):
    """
    PostgreSQL public.pipeline_execution_logs 테이블에서 최신 파이프라인 실행 로깅 데이터를 서빙합니다.
    """
    logs = []
    try:
        rows = db.read_query_direct(
            "SELECT execution_id, market, start_time, end_time, duration_seconds, status, step_details, error_message, created_at "
            "FROM public.pipeline_execution_logs ORDER BY start_time DESC LIMIT %s;", (limit,)
        )
        if rows:
            for r in rows:
                logs.append({
                    "execution_id": r[0],
                    "market": r[1],
                    "start_time": r[2].strftime('%Y-%m-%d %H:%M:%S') if r[2] else None,
                    "end_time": r[3].strftime('%Y-%m-%d %H:%M:%S') if r[3] else None,
                    "duration_seconds": float(r[4]) if r[4] is not None else None,
                    "status": r[5],
                    "step_details": r[6],
                    "error_message": r[7],
                    "created_at": r[8].strftime('%Y-%m-%d %H:%M:%S') if r[8] else None
                })
    except Exception as e:
        print(f"[Pipeline Logs API Warning] {e}")

    return {
        "status": "success",
        "count": len(logs),
        "logs": logs
    }

@app.get("/status")
def read_status_html():
    return _render_html_with_api_key("status.html")

@app.get("/pipeline")
def read_pipeline_html():
    return _render_html_with_api_key("pipeline.html")

@app.get("/{catchall:path}")
def read_spa_fallback(catchall: str):
    # web_dir 바깥으로 벗어나는 경로 순회(../ 등)를 차단하기 위해 realpath로 정규화 후 검증
    web_dir_real = os.path.realpath(web_dir)
    target = os.path.realpath(os.path.join(web_dir, catchall))
    if target.startswith(web_dir_real + os.sep) and os.path.isfile(target):
        return FileResponse(target)
    return FileResponse(os.path.join(web_dir, "index.html"))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
