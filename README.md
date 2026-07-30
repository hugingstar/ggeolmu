# 🤖 Ggeolmu Multi-Agent Stock Parser & Analyzer

## 1. 개요
`Ggeolmu Parser` 모듈은 국내(KOSPI, KOSDAQ) 및 해외(NASDAQ, NYSE) 주식 데이터를 수집하고 가공하여 **PostgreSQL 데이터베이스에 적재**하는 자동화 파이프라인입니다. 
추가적으로, 적재된 시계열 데이터를 바탕으로 LLM 기반 분석 프롬프트를 자동으로 생성해 내는 **멀티 에이전트(Multi-Agent) 시스템**과 **웹(Web) 프론트엔드**를 통합 제공합니다.

## 2. 시스템 아키텍처 및 도커 환경

시스템은 4-Tier 아키텍처(WEB, WAS, DB, Manager) 구조를 따르며, `docker compose up -d --build` 명령어를 통해 손쉽게 로컬에서 전체 환경을 기동할 수 있습니다.

```mermaid
flowchart TD
    %% 사용자 및 로컬 호스트
    User(("사용자"))
    Admin(("관리자"))
    
    subgraph Host ["Host Machine (Mac/PC)"]
        LocalData[/"./Data/pgdata"/]
        LocalParser[/"./Parser (Source Code)"/]
    end

    %% 도커 네트워크 내부
    subgraph DockerNetwork ["Docker Internal Network (ggeolmu_default)"]
        
        %% Manager Tier (Scheduler & Workflow)
        Manager["manager (n8n) <br> (Port: 5678) <br> * 워크플로우 엔진"]
        
        %% DB Tier
        DB[("postgres:15-alpine <br> (Port: 5432) <br> * 데이터 저장소")]
        
        %% WAS Tier (Backend)
        WAS["was (FastAPI) <br> (Port: 8000) <br> * 데이터 서빙"]
        
        %% WEB Tier (Frontend)
        WEB["web (Nginx) <br> (Port: 3000) <br> * UI 프론트엔드"]
    end

    %% 실행 트리거 및 의존성 (depends_on)
    DB -. "1순위 구동" .-> WAS
    DB -. "1순위 구동" .-> Manager
    WAS -. "2순위 구동" .-> WEB

    %% 파일 시스템 볼륨 매핑
    LocalData <==>|Volume Mount : 데이터 영구보존| DB
    LocalData <==>|Volume Mount : n8n 설정 보존| Manager
    LocalParser -->|Build COPY| WAS
    LocalParser -->|Build COPY| WEB
    LocalParser <==>|Volume Mount : 실시간 파이썬 접근| Manager

    %% 데이터 흐름 및 접근
    User == "1. 브라우저 접속 (http://localhost:3000)" ==> WEB
    WEB -- "2. API 요청 (Ajax/Fetch)" --> WAS
    WAS -- "3. SQL 쿼리 (psycopg2)" --> DB
    
    %% 관리자 데이터 파이프라인 트리거 (자동화)
    Admin == "4. 워크플로우 및 스케줄 등록 (localhost:5678)" ==> Manager
    Manager -- "5. 매일 정해진 스케줄(Cron)에 따라 <br> 컨테이너 내부에서 python main.py 실행" --> LocalParser
    LocalParser -- "6. Dask 가공 후 DB 적재 (Bulk Insert)" --> DB

    classDef container fill:#1e293b,stroke:#3b82f6,stroke-width:2px,color:#fff;
    classDef storage fill:#334155,stroke:#10b981,stroke-width:2px,color:#fff;
    
    class DB,WAS,WEB container;
    class LocalData,LocalParser storage;
```

### 2.1 개체 관계도 (ER Diagram)

시스템 내 데이터베이스(PostgreSQL)의 주요 테이블 구조와 논리적 관계입니다.

```mermaid
erDiagram
    RAW_STOCK_DATA {
        int id PK
        date date "NOT NULL (수집일)"
        varchar symbol "NOT NULL (종목코드)"
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

    RAW_STOCK_DATA ||--o{ PROMPT_LOGS : "기반으로 메타프롬프트 분석"
```

## 3. 파이프라인 단계 및 모듈 역할

### 3.1. 데이터 수집 및 DB 적재 파이프라인 (`main.py`)
파이프라인 관리 모듈은 로컬 `DBManager`를 통해 PostgreSQL 데이터베이스에 데이터를 직접 적재합니다.
- **`get_fdr.py`**: FinanceDataReader 기반 초기 데이터 크롤링 수행
- **`process_a1.py`, `process_b1.py`**: Dask 기반 시그널 데이터 및 기술적 지표 시트 분할 생성
- **`db_manager.py`**: 파이프라인 1.5단계에서 `insert_raw_data` 쿼리 스크립트를 사용하여 수집된 Raw 데이터를 PostgreSQL로 Bulk Insert 함
- **`queries/` 디렉토리**: SQL 인젝션 공격 방지를 위해 쿼리를 독립된 파일(`001_`, `002_`, `003_`)로 분리 관리

#### 📊 파이프라인 데이터 흐름 시퀀스
아래는 n8n 스케줄러가 정해진 시간에 파이프라인을 자동 트리거했을 때, 데이터가 어떻게 수집되고 가공되어 최종적으로 데이터베이스에 들어가는지를 보여줍니다.

```mermaid
sequenceDiagram
    autonumber
    actor Admin as 관리자 (Admin)
    box rgba(30, 41, 59, 0.1) Docker n8n 컨테이너 내부
    participant N8N as n8n (Manager Tier)
    participant Main as main.py (Pipeline)
    end
    participant FDR as get_fdr.py (수집)
    participant Dask as Dask Processor (가공)
    participant DBManager as db_manager.py
    participant DB as PostgreSQL (DB)

    Admin->>N8N: n8n 워크플로우(Schedule Trigger) 활성화
    Note over N8N, Main: -- 예약된 시간 도달 --
    N8N->>Main: [Execute Command 노드] python3 main.py 실행
    activate Main
    Main->>FDR: 1단계: 종목 데이터 수집 명령
    activate FDR
    FDR-->>Main: 원시 데이터 (Parquet) 반환
    deactivate FDR

    Main->>DBManager: 1.5단계: Bulk Insert 요청
    activate DBManager
    DBManager->>DB: 002_insert_raw_data.sql 실행 (UPSERT)
    DB-->>DBManager: 데이터베이스 갱신 완료
    DBManager-->>Main: 적재 성공 응답
    deactivate DBManager

    Main->>Dask: 2~3단계: 기술적 지표 및 시그널 분할
    activate Dask
    Dask-->>Main: A1, B1 시트 생성 완료 (Local 저장)
    deactivate Dask

    Main-->>Admin: 전체 파이프라인 완료 알림
    deactivate Main
```

### 3.2. 멀티 에이전트 시스템 (`agents/`)
사용자가 웹사이트에서 종목을 검색하면, 두 에이전트가 협업하여 작동합니다.
- **`AuditAgent` (`audit_agent.py`)**: 검색된 종목이 스팩(SPAC), ETF, 채권 등 불필요한 종목인지 실시간으로 검열합니다. 또한 생성된 프롬프트나 쿼리에 SQL 템플릿 주입 등의 보안 위협이 없는지 감시합니다.
- **`PromptMakerAgent` (`prompt_maker_agent.py`)**: 검색된 종목의 최근 5일치 시계열 데이터를 PostgreSQL에서 가져온 뒤, LLM이 즉시 퀀트 분석을 시작할 수 있도록 컨텍스트가 부여된 메타 프롬프트를 자동으로 조립합니다.

## 4. 실행 방법

1. **Docker 컨테이너 동시 실행 (DB + API + WEB + Manager)**
   ```bash
   # 프로젝트 최상단 디렉토리에서 실행
   docker compose up -d --build
   ```
   이후 `http://localhost:3000` 으로 접속하면 종목 검색용 웹 화면이 나타납니다.

2. **자동 스케줄러(n8n) 접속 및 파이프라인 활성화**
   - 브라우저에서 `http://localhost:5678` 로 접속합니다.
   - n8n 워크플로우에 `Execute Command` 노드를 추가하고 `cd /app/Parser && python3 main.py`를 실행하도록 설정합니다. (또는 제공된 `n8n_workflow_template.json`을 Import)
   - 스케줄 트리거를 설정해두면 관리자 개입 없이 매일 데이터가 백그라운드에서 적재됩니다.
