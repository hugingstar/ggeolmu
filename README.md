# 🤖 Ggeolmu Multi-Agent Stock Parser & Analyzer

## 1. 개요
`Ggeolmu` 프로젝트는 국내(KOSPI, KOSDAQ) 및 해외(NASDAQ, NYSE) 주식 데이터를 수집·가공하여 **PostgreSQL 데이터베이스에 적재**하고, 이를 멀티 에이전트(Multi-Agent) 기반으로 분석하여 시각화하는 **4-Tier (WEB-WAS-DB-Manager) 주식 분석 웹 서비스**입니다.

Dask 분산 컴퓨팅 및 **안정성 강화 증분 수집(Delta Ingestion)** 구조를 적용하여 최신 주가 및 기술적 지표를 안전하고 빠르게 갱신합니다.

---

## 2. 시스템 아키텍처 및 파이프라인 (System Architecture)

### 2.1 전체 데이터 파이프라인 흐름도 (Data Pipeline Architecture)

```mermaid
flowchart TD
    %% 커스텀 색상 및 폰트 크기 스타일 정의
    classDef pythonEngine fill:#1e3a8a,stroke:#60a5fa,stroke-width:2.5px,color:#ffffff,font-size:15px,font-weight:bold;
    classDef dbStorage fill:#064e3b,stroke:#34d399,stroke-width:2.5px,color:#ffffff,font-size:15px,font-weight:bold;
    classDef agentManager fill:#831843,stroke:#f472b6,stroke-width:2.5px,color:#ffffff,font-size:15px,font-weight:bold;
    classDef webServer fill:#1e293b,stroke:#94a3b8,stroke-width:2.5px,color:#ffffff,font-size:15px,font-weight:bold;

    %% 1단계: 증분 수집 및 원자적 적재
    subgraph Step1 ["1단계: 증분 데이터 수집 (Delta Ingestion)"]
        direction TB
        FDR[/"🌐 FinanceDataReader API"/] -->|3일 Overlap 수집| GetFDR["🐍 get_fdr.py<br>(3일 Safety Overlap 수집)"]
        GetFDR -->|원자적 저장 .tmp & .bak| RawParquet[/"📦 raw_data.parquet"/]
        GetFDR -->|증분 델타 적재| DB_Raw[("💾 DB: raw_stock_data")]
    end

    %% 2단계: 기술적 지표 및 시그널 연산
    subgraph Step2 ["2단계: 지표 가공 & 시그널 생성 (Dask Engine)"]
        direction TB
        ProcessA1["🐍 process_a1.py<br>(MA5~200, RSI, MACD 연산)"] -->|지표 저장| DB_Tech[("💾 DB: technical_indicators")]
        ProcessA1 -->|지표 릴레이| ProcessB3["🐍 process_b3.py<br>(상승/하락/다이버전스 시그널)"]
        ProcessB3 -->|시그널 저장| DB_Signal[("💾 DB: trading_signals")]
    end

    %% 3단계: 시계열 클러스터링
    subgraph Step3 ["3단계: 시가총액 & 시계열 클러스터링"]
        direction TB
        ProcessM1["🐍 process_m1_cap.py<br>(시가총액 데이터 가공)"] -->|시총 저장| DB_Cap[("💾 DB: market_cap")]
        ProcessM1 -->|정규화| ProcessC1["🐍 process_c1.py<br>(1d/1w/1m Z-Score 산출)"]
        ProcessC1 -->|Z-Score 저장| DB_ZScore[("💾 DB: zscore_features")]
        ProcessC1 -->|SoftDTW 군집화| ProcessC2["🐍 process_c2.py<br>(SoftDTW K-Means 군집화)"]
        ProcessC2 -->|군집 결과 저장| DB_Cluster[("💾 DB: clustering_results")]
    end

    %% 4단계: 4-Tier 웹 서비스 & 멀티 에이전트
    subgraph Step4 ["4단계: 4-Tier 웹 서비스 & Multi-Agent"]
        direction TB
        UI[/"🖥️ WEB: Vanilla JS SPA UI"/] <-->|REST API| WAS["⚙️ WAS: FastAPI Server"]
        WAS <--> Audit["🤖 Manager: AuditAgent<br>(SPAC/ETF 필터 & SQLi 검사)"]
        WAS <--> PromptAgent["🤖 Manager: PromptMakerAgent<br>(5일 시세/지표 퀀트분석)"]
        WAS -->|프롬프트 기록| DB_Logs[("💾 DB: prompt_logs")]
        SecAgent["🤖 Manager: WebSecurityAgent<br>(WEB-WAS-DB 취약점 탐지)"] -.- WAS
    end

    %% [DB 핀포인트 흐름 연계선 (어떤 DB -> 어떤 DB/모듈)]
    DB_Raw ==>|1. raw_stock_data 읽기| ProcessA1
    DB_Raw ==>|2. raw_stock_data 읽기| ProcessM1
    DB_Raw ==>|3. 5일 raw_stock_data 읽기| PromptAgent
    DB_Tech ==>|4. technical_indicators 퀀트 공급| PromptAgent
    DB_Signal ==>|5. trading_signals 시그널 조회| WAS
    DB_Cluster ==>|6. clustering_results 3D 조회| WAS

    %% 노드 스타일 지정 (.py: 파란색, DB: 녹색, 에이전트: 핑크, Web/WAS: 슬레이트)
    class GetFDR,ProcessA1,ProcessB3,ProcessM1,ProcessC1,ProcessC2 pythonEngine;
    class DB_Raw,DB_Tech,DB_Signal,DB_Cap,DB_ZScore,DB_Cluster,DB_Logs,RawParquet dbStorage;
    class Audit,PromptAgent,SecAgent agentManager;
    class UI,WAS,FDR webServer;
```

---

### 2.2 개체 관계도 (ER Diagram)

PostgreSQL 데이터베이스 7개 핵심 테이블 간의 키(PK/UK) 구조와 연관 관계입니다.

```mermaid
erDiagram
    RAW_STOCK_DATA {
        int id PK
        date date UK
        varchar symbol UK
        varchar name
        numeric close
        numeric volume
    }

    MARKET_CAP {
        date date PK
        varchar symbol PK
        numeric market_cap_krw
    }

    TECHNICAL_INDICATORS {
        date date PK
        varchar symbol PK
        numeric rsi
        numeric macd
    }

    TRADING_SIGNALS {
        date date PK
        varchar symbol PK
        varchar signal_type PK
        numeric signal_strength
    }

    ZSCORE_FEATURES {
        date date PK
        varchar symbol PK
        varchar freq PK
        numeric zscore
    }

    CLUSTERING_RESULTS {
        date target_date PK
        varchar symbol PK
        varchar method PK
        int cluster_id
    }

    PROMPT_LOGS {
        int id PK
        varchar symbol
        varchar status
    }

    %% 키 기준 방사형 연결 관계
    RAW_STOCK_DATA ||--|| MARKET_CAP : "일별 시총"
    RAW_STOCK_DATA ||--o{ TECHNICAL_INDICATORS : "보조 지표"
    RAW_STOCK_DATA ||--o{ TRADING_SIGNALS : "분석 시그널"
    RAW_STOCK_DATA ||--o{ ZSCORE_FEATURES : "Z-Score"
    RAW_STOCK_DATA ||--o{ CLUSTERING_RESULTS : "패턴 군집"
    RAW_STOCK_DATA ||--o{ PROMPT_LOGS : "에이전트 기록"
```

---

## 3. 4-Tier 아키텍처 구성 및 역할

- **WEB Tier (`Web/`)**: Vanilla JS 및 CSS Glassmorphism 기반 SPA. 반응형 상대 크기 조절 레이아웃 적용.
- **WAS Tier (`WAS/app.py`)**: FastAPI 기반 비동기 REST API 서빙 및 정적 웹 리소스 제공.
- **DB Tier (`Database/`)**: PostgreSQL DBMS. `queries/001_`~`014_` 수록 SQL 파일로 쿼리 중앙 관리 (SQL Injection 방지).
- **Manager Tier (`Manager/`)**:
  - `AuditAgent`: 불필요 종목(ETF/SPAC) 필터링 및 프롬프트 주입/SQLi 검사.
  - `PromptMakerAgent`: 5일 주가 흐름 기반 퀀트 메타 프롬프트 일괄 생성.
  - `WebSecurityAgent`: WEB-WAS-DB 계층 취약점 탐지 및 보안 관리.

---

## 4. 실행 방법

### 1) PostgreSQL DB 구동
```bash
docker compose up -d
```

### 2) 웹/WAS 애플리케이션 실행
```bash
python WAS/app.py
```
접속 URL: `http://localhost:8000`

### 3) 데이터 증분 수집 파이프라인 실행
```bash
python Parser/main.py
```
