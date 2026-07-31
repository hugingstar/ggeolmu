# 🤖 Ggeolmu Multi-Agent Stock Parser & Analyzer

## 1. 개요
`Ggeolmu` 프로젝트는 국내(KOSPI, KOSDAQ) 및 해외(NASDAQ, NYSE) 주식 데이터를 수집하고 가공하여 **PostgreSQL 데이터베이스에 적재**하고, 이를 시각화하여 멀티 에이전트(Multi-Agent) 기반의 맞춤형 분석 프롬프트를 조회할 수 있는 **주식 검색 Single Page Application (SPA) 웹 서비스**입니다.

n8n과 같은 무거운 스케줄러 관리 툴 없이, 로컬 Python 환경 및 Docker 기반 데이터베이스와 FastAPI 단독 서비스로 결합하여 최적화된 구동 방식을 지원합니다.

---

## 2. 시스템 아키텍처 및 데이터 파이프라인 흐름

시스템은 데이터 보존용 DB 컨테이너와 정적 웹 UI 및 API 서버 역할을 병행하는 FastAPI WAS 서버의 2-Tier 구조를 따르며, 전체 데이터 흐름 및 단계별 모듈은 아래와 같습니다.

```mermaid
flowchart TD
    %% 스타일 정의
    classDef source fill:#1e293b,stroke:#3b82f6,stroke-width:2px,color:#fff;
    classDef process fill:#0f172a,stroke:#8b5cf6,stroke-width:2px,color:#fff;
    classDef storage fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#fff;
    classDef agent fill:#701a75,stroke:#d946ef,stroke-width:2px,color:#fff;

    %% 1단계: 수집 및 DB 적재
    subgraph DataScraping ["1단계: 데이터 수집 & 적재 (Ingestion)"]
        FDR[("FinanceDataReader <br> (KOSPI, KOSDAQ, NASDAQ, NYSE)")]
        MainPy["python main.py <br> (Pipeline Controller)"]
        RawParquet[/"Raw Parquet Files"/]
        DB_Raw[("PostgreSQL <br> public.raw_stock_data")]
        
        FDR -->|1. 주가 수집| MainPy
        MainPy -->|2. 임시 보존| RawParquet
        RawParquet -->|3. UPSERT Bulk Insert| DB_Raw
    end

    %% 2단계: 가공 및 지표 생성
    subgraph FeatureEngineering ["2단계: 지표 가공 & 시그널 생성 (Dask)"]
        DaskA1["process_a1.py <br> (기술적 지표 계산)"]
        DaskB1["process_b1.py <br> (시그널 분석 시트 생성)"]
        
        DB_Raw -->|4. 데이터 가공 리드| DaskA1
        DaskA1 -->|5. 시그널 추출| DaskB1
        DaskB1 -->|6. 기술 분석 데이터 저장| DB_Raw
    end

    %% 3단계: 분석 및 클러스터링
    subgraph AnalyticsClustering ["3단계: 고급 분석 & 시계열 클러스터링"]
        M1_Cap["process_m1_cap.py <br> (시가총액 데이터 추출)"]
        C1_ZScore["process_c1.py <br> (1d/1w/1m Z-Score 계산)"]
        C2_Clustering["process_c2.py <br> (K-Means SoftDTW 클러스터링)"]
        
        DB_Raw -->|7. 기초 데이터 제공| M1_Cap
        M1_Cap -->|8. 주기별 가공| C1_ZScore
        C1_ZScore -->|9. Top 1000 종목 시계열 패턴화| C2_Clustering
        C2_Clustering -->|10. 군집 레이블 저장| DB_Raw
    end

    %% 4단계: 프론트 웹 서비스 및 멀티 에이전트
    subgraph WebService ["4단계: 웹 서비스 & Multi-Agent 분석 (SPA UI)"]
        Browser[/"사용자 브라우저 (SPA Router)"/]
        FastAPI["FastAPI Web Server <br> (app.py)"]
        AuditAgent["AuditAgent <br> (SPAC/ETF 필터 & SQLi 차단)"]
        PromptAgent["PromptMakerAgent <br> (최근 5일 데이터 프롬프트화)"]
        DB_Logs[("PostgreSQL <br> public.prompt_logs")]

        Browser -->|11. 종목 검색 / Route 이동| FastAPI
        FastAPI -->|12. 입력어 1차 검사| AuditAgent
        AuditAgent -->|13. PASS 시 메타데이터 조회| PromptAgent
        PromptAgent -->|14. 5일 요약 데이터 추출| DB_Raw
        PromptAgent -->|15. 최종 프롬프트 생성| FastAPI
        FastAPI -->|16. 검사 상태 및 프롬프트 기록| DB_Logs
        FastAPI -->|17. 차트 & 분석 콘솔 렌더링| Browser
    end

    %% 클래스 지정
    class FDR source;
    class MainPy,DaskA1,DaskB1,M1_Cap,C1_ZScore,C2_Clustering,FastAPI process;
    class DB_Raw,DB_Logs,RawParquet storage;
    class AuditAgent,PromptAgent agent;
```


### 2.1 개체 관계도 (ER Diagram)

데이터베이스(PostgreSQL)의 주요 테이블 구조와 관계입니다.

```mermaid
erDiagram
    RAW_STOCK_DATA {
      int id PK
      date date "UK (수집일)"
      varchar symbol "UK (종목코드)"
      varchar name "종목명"
      numeric open "시가"
      numeric high "고가"
      numeric low "저가"
      numeric close "종가"
      numeric volume "거래량"
      numeric change "등락률"
    }
    
    PROMPT_LOGS {
      int id PK
      varchar symbol "NOT NULL (검색된 종목코드)"
      text generated_prompt "생성된 프롬프트 결과"
      varchar status "안전(Audit) 통과 여부"
      timestamp created_at "생성 일시"
    }

    MARKET_CAP {
      date date PK
      varchar symbol PK
      numeric market_cap_krw "시가총액(원)"
    }

    ZSCORE_FEATURES {
      date date PK
      varchar symbol PK
      varchar freq PK "주기 (1d, 1w, 1m)"
      numeric zscore "Z-Score"
    }

    CLUSTERING_RESULTS {
      date target_date PK "기준일"
      varchar symbol PK "종목코드"
      varchar method PK "클러스터링 기법"
      varchar market "시장 (KOSPI 등)"
      int cluster_id "소속 군집 번호"
    }

    RAW_STOCK_DATA ||--o{ PROMPT_LOGS : "기반으로 메타프롬프트 분석"
    RAW_STOCK_DATA ||--|| MARKET_CAP : "일별 시가총액"
    RAW_STOCK_DATA ||--o{ ZSCORE_FEATURES : "주기별 Z-Score 지표"
    RAW_STOCK_DATA ||--o{ CLUSTERING_RESULTS : "군집화 결과"
```

---

## 3. 파이프라인 단계 및 모듈 역할

### 3.1. 데이터 수집 및 DB 적재 파이프라인 (`main.py`)
- **`get_fdr.py`**: FinanceDataReader 기반 초기 데이터 크롤링 수행
- **`process_a1.py`, `process_b1.py`**: Dask 기반 시그널 데이터 및 기술적 지표 시트 분할 생성
- **`db_manager.py`**: 파이프라인 진행 및 DB 연결을 제어하며 `insert_raw_data` 및 `write_query` 메소드를 통해 데이터베이스 갱신
- **`queries/` 디렉토리**: SQL 인젝션 공격 방지를 위해 쿼리를 독립된 파일(`001_` ~ `004_`)로 분리 관리

### 3.2. 보안 및 프롬프트 에이전트 (`agents/`)
- **`AuditAgent` (`audit_agent.py`)**: 스팩(SPAC), ETF 등 불필요한 종목을 필터링하고, 잠재적인 SQL 인젝션 및 프롬프트 주입 공격을 사전 검증.
- **`PromptMakerAgent` (`prompt_maker_agent.py`)**: 최근 5일 데이터 흐름을 추적하여 퀀트 관점에서 최적화된 프롬프트 생성.

---

## 4. 실행 방법

### 1) PostgreSQL 데이터베이스 실행
프로젝트 최상단 디렉토리에서 실행하여 DB 컨테이너를 구동합니다.
```bash
docker compose up -d
```

### 2) 웹 애플리케이션 및 API 서버 구동
FastAPI 애플리케이션을 구동하여 정적 파일 서빙과 API 연동을 시작합니다.
```bash
python Parser/was_app/app.py
```
서버 구동 후 브라우저에서 `http://localhost:8000`으로 접속하여 주식 검색 웹 서비스(SPA)를 이용하실 수 있습니다.

### 3) 데이터 수집 및 갱신 파이프라인 실행 (수동/스케줄러)
새로운 주식 데이터를 수집하고 적재하려면 아래 파이프라인 명령을 실행합니다.
```bash
python Parser/main.py
```
*(매일 마감 후 주기적으로 실행되도록 OS의 Cron이나 작업 스케줄러에 등록하여 자동화할 수 있습니다.)*
