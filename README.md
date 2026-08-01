# 🤖 Ggeolmu Multi-Agent Stock Parser & Analyzer

## 1. 개요
`Ggeolmu` 프로젝트는 국내(KOSPI, KOSDAQ) 및 해외(NASDAQ, NYSE) 주식 데이터를 수집하고 가공하여 **PostgreSQL 데이터베이스에 적재**하고, 이를 시각화하여 멀티 에이전트(Multi-Agent) 기반의 맞춤형 분석 프롬프트를 조회할 수 있는 **4-Tier (WEB-WAS-DB-Manager) 주식 분석 웹 서비스**입니다.

n8n과 같은 무거운 스케줄러 관리 툴 없이, 로컬 Python 환경 및 Dask 분산 컴퓨팅 기반의 **안정성 강화 증분 수집(Delta Ingestion)** 파이프라인을 구축하여 최신 데이터를 수 초 내로 갱신 및 분석합니다.

---

## 2. 시스템 아키텍처 및 파이프라인 (System Architecture)

### 2.1 전체 데이터 파이프라인 흐름도 (Mermaid Pipeline Flowchart)

```mermaid
flowchart TD
    %% 스타일 정의
    classDef source fill:#1e293b,stroke:#3b82f6,stroke-width:2px,color:#fff;
    classDef process fill:#0f172a,stroke:#8b5cf6,stroke-width:2px,color:#fff;
    classDef storage fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#fff;
    classDef agent fill:#701a75,stroke:#d946ef,stroke-width:2px,color:#fff;

    %% 1단계: 증분 수집 및 DB 적재
    subgraph Step1 ["1단계: 증분 데이터 수집 & 원자적 적재 (Delta Ingestion)"]
        FDR[("FinanceDataReader <br> (KOSPI, KOSDAQ, NASDAQ, NYSE)")]
        MainPy["python main.py <br> (Pipeline Controller)"]
        GetFDR["get_fdr.py <br> (3일 Safety Overlap 감지)"]
        AtomicWrite[/"Atomic Write (.tmp & .bak) <br> raw_data.parquet 교체"/]
        DB_Raw[("PostgreSQL <br> public.raw_stock_data")]
        
        MainPy -->|1. 증분 수집 제어| GetFDR
        GetFDR -->|2. 3일 Safety Overlap 수집| FDR
        FDR -->|3. 최근 1~3일치 핀포인트 수집| GetFDR
        GetFDR -->|4. 원자적 파일 저장| AtomicWrite
        MainPy -->|5. 증분 델타만 UPSERT 적재| DB_Raw
    end

    %% 2단계: 가공 및 지표 생성
    subgraph Step2 ["2단계: 지표 가공 & 시그널 생성 (Dask 병렬 연산)"]
        DaskA1["process_a1.py <br> (MA5~200, RSI, MACD, ADX 연산)"]
        DaskB1["process_b3.py <br> (상승/하락/다이버전스 시그널 생성)"]
        DB_Tech[("PostgreSQL <br> public.technical_indicators")]
        DB_Signal[("PostgreSQL <br> public.trading_signals")]
        
        DB_Raw -->|6. raw 데이터 로드| DaskA1
        DaskA1 -->|7. 기술적 지표 UPSERT| DB_Tech
        DaskA1 -->|8. 지표 시계열 릴레이| DaskB1
        DaskB1 -->|9. 분석 시그널 UPSERT| DB_Signal
    end

    %% 3단계: 분석 및 클러스터링
    subgraph Step3 ["3단계: 시가총액 & SoftDTW 시계열 클러스터링"]
        M1_Cap["process_m1_cap.py <br> (시가총액 데이터 가공)"]
        C1_ZScore["process_c1.py <br> (1d/1w/1m Z-Score 산출)"]
        C2_Clustering["process_c2.py <br> (SoftDTW K-Means 군집화)"]
        DB_Cap[("PostgreSQL <br> public.market_cap")]
        DB_ZScore[("PostgreSQL <br> public.zscore_features")]
        DB_Cluster[("PostgreSQL <br> public.clustering_results")]
        
        DB_Raw -->|10. 기초 시세 데이터| M1_Cap
        M1_Cap -->|11. 시가총액 저장| DB_Cap
        M1_Cap -->|12. 주기별 정규화| C1_ZScore
        C1_ZScore -->|13. Z-Score 피처 저장| DB_ZScore
        C1_ZScore -->|14. Top 1000 군집화| C2_Clustering
        C2_Clustering -->|15. 군집 결과 저장| DB_Cluster
    end

    %% 4단계: 4-Tier 웹 서비스 및 에이전트
    subgraph Step4 ["4단계: 4-Tier 웹 서비스 & Multi-Agent 분석 (WEB-WAS-DB-Manager)"]
        Browser[/"WEB: 사용자 브라우저 <br> (Vanilla JS SPA Glassmorphism UI)"/]
        WAS["WAS: FastAPI / Django Web Server <br> (app.py)"]
        AuditAgent["Manager: AuditAgent <br> (SPAC/ETF 필터 & SQLi 차단)"]
        PromptAgent["Manager: PromptMakerAgent <br> (최근 5일 퀀트 흐름 프롬프트)"]
        SecurityAgent["Manager: WebSecurityAgent <br> (WEB-WAS-DB 종합 취약점 관리)"]
        DB_Logs[("DB: PostgreSQL <br> public.prompt_logs")]

        Browser -->|16. 종목 검색 및 REST API 요청| WAS
        WAS -->|17. 검색어 검증 위임| AuditAgent
        AuditAgent -->|18. PASS 통과 시| PromptAgent
        PromptAgent -->|19. 5일 시세/지표 데이터 조회| DB_Raw
        PromptAgent -->|20. 퀀트 메타 프롬프트 생성| WAS
        WAS -->|21. 프롬프트 이력 기록| DB_Logs
        WAS -->|22. 대시보드 & 3D 차트 응답| Browser
        SecurityAgent -.->|23. 보안 취약점 종합 모니터링| Browser
        SecurityAgent -.->|23. 보안 취약점 종합 모니터링| WAS
        SecurityAgent -.->|23. 보안 취약점 종합 모니터링| DB_Logs
    end

    %% 클래스 지정
    class FDR source;
    class MainPy,GetFDR,DaskA1,DaskB1,M1_Cap,C1_ZScore,C2_Clustering,WAS process;
    class DB_Raw,DB_Tech,DB_Signal,DB_Cap,DB_ZScore,DB_Cluster,DB_Logs storage;
    class AuditAgent,PromptAgent,SecurityAgent agent;
```

---

### 2.2 개체 관계도 (ER Diagram)

PostgreSQL 데이터베이스의 7개 핵심 테이블 스키마 구조와 테이블 간 릴레이션 관계입니다.

```mermaid
erDiagram
    RAW_STOCK_DATA {
        int id PK "자동 증가 식별자"
        date date "수집일 (UK)"
        varchar symbol "종목코드 (UK)"
        varchar name "종목명"
        numeric open "시가"
        numeric high "고가"
        numeric low "저가"
        numeric close "종가"
        numeric volume "거래량"
        numeric change "등락률"
    }

    MARKET_CAP {
        date date PK "기준일"
        varchar symbol PK "종목코드"
        numeric market_cap_krw "시가총액(원)"
    }

    TECHNICAL_INDICATORS {
        date date PK "수집일"
        varchar symbol PK "종목코드"
        varchar name "종목명"
        varchar market "시장구분 (KOSPI 등)"
        numeric ma5 "5일 이동평균"
        numeric ma20 "20일 이동평균"
        numeric ma60 "60일 이동평균"
        numeric ma120 "120일 이동평균"
        numeric ma200 "200일 이동평균"
        numeric rsi "RSI 지표"
        numeric macd "MACD 지표"
        numeric macd_signal "MACD 시그널"
        numeric adx "ADX 추세강도"
        numeric bollinger_high "볼린저 상한"
        numeric bollinger_low "볼린저 하한"
        timestamp created_at "생성일시"
    }

    TRADING_SIGNALS {
        date date PK "수집일"
        varchar symbol PK "종목코드"
        varchar signal_type PK "시그널 유형 (BullDiv, BearDiv 등)"
        varchar name "종목명"
        varchar market "시장구분"
        numeric signal_strength "시그널 강도"
        text description "시그널 분석 상세"
        timestamp created_at "생성일시"
    }

    ZSCORE_FEATURES {
        date date PK "기준일"
        varchar symbol PK "종목코드"
        varchar freq PK "주기 (1d, 1w, 1m)"
        numeric zscore "정규화 Z-Score 수치"
    }

    CLUSTERING_RESULTS {
        date target_date PK "기준일"
        varchar symbol PK "종목코드"
        varchar method PK "클러스터링 기법 (SoftDTW 등)"
        varchar market "시장구분"
        int cluster_id "소속 군집 번호"
    }

    PROMPT_LOGS {
        int id PK "자동 증가 식별자"
        varchar symbol "검색된 종목코드"
        text generated_prompt "생성된 프롬프트 내용"
        varchar status "Audit 보안 검사 상태"
        timestamp created_at "생성일시"
    }

    %% 논리적 개체 관계 설정 (Date, Symbol 매핑)
    RAW_STOCK_DATA ||--|| MARKET_CAP : "일별 시가총액 (1:1)"
    RAW_STOCK_DATA ||--o{ TECHNICAL_INDICATORS : "기술적 보조지표 (1:1)"
    RAW_STOCK_DATA ||--o{ TRADING_SIGNALS : "매수/매도 시그널 (1:N)"
    RAW_STOCK_DATA ||--o{ ZSCORE_FEATURES : "주기별 Z-Score (1:N)"
    RAW_STOCK_DATA ||--o{ CLUSTERING_RESULTS : "시계열 군집 결과 (1:N)"
    RAW_STOCK_DATA ||--o{ PROMPT_LOGS : "에이전트 프롬프트 기록 (1:N)"
```

---

## 3. 4-Tier 아키텍처 및 모듈 구성

프로젝트는 Cloud 및 On-premise 환경 배포를 고려하여 **4-Tier (WEB - WAS - DB - Manager)** 독립 계층 구조로 설계되었습니다.

### 3.1. 계층별 구성 (4-Tier Architecture)
- **WEB (Static Frontend)**: `Web/` 디렉토리에 위치. Vanilla JS 및 Glassmorphism CSS 디자인 시스템 기반의 SPA 레이아웃. 브라우저 크기 변경 시 상대적 비율을 유지하도록 다이내믹 포맷 구현.
- **WAS (Web Application Server)**: `WAS/app.py` 또는 `Parser/was_app/app.py`. FastAPI 기반 REST API 엔드포인트 제공 및 정적 파일 서빙.
- **DB (Database Storage)**: PostgreSQL RDBMS. SQL Injection 방지를 위하여 `Database/queries/`에 `001_`~`014_` 접두어 스크립트로 쿼리를 안전하게 일원화 및 분리 관리.
- **Manager (AIOps & Multi-Agent & Security)**:
  - **`AuditAgent` (`audit_agent.py`)**: 불필요한 ETF, SPAC 종목 1차 필터링 및 프롬프트 주입/SQLi 공격 검증.
  - **`PromptMakerAgent` (`prompt_maker_agent.py`)**: 최근 5일 시세/지표 기반 퀀트 메타 프롬프트 생성 및 라이프사이클 관리.
  - **`WebSecurityAgent` (`web_security_agent.py`)**: WEB, WAS, DB의 종합적인 취약점 탐지 및 탐구 모니터링 관리.

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
python WAS/app.py
```
서버 구동 후 브라우저에서 `http://localhost:8000`으로 접속하여 주식 검색 및 3D 클러스터링 웹 서비스(SPA)를 이용하실 수 있습니다.

### 3) 데이터 증분 수집 및 갱신 파이프라인 실행 (수동/스케줄러)
새로운 주식 데이터를 3일 오버랩 핀포인트 증분 수집(Delta Ingestion)하고 원자적으로 적재하려면 아래 파이프라인 명령을 실행합니다.
```bash
python Parser/main.py
```
*(매일 장 마감 후 주기적으로 실행되도록 OS의 Cron이나 작업 스케줄러에 등록하여 자동화할 수 있습니다.)*
