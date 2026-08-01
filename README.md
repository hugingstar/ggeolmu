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
        FDR[/"🌐 FinanceDataReader API"/] -->|3일 Overlap 수집| GetFDR["🐍 get_fdr.py"]
        GetFDR -->|원자적 저장 .tmp & .bak| RawParquet[/"📦 raw_data.parquet"/]
        GetFDR -->|증분 델타 적재| DB_Raw[("💾 DB: raw_stock_data")]
    end

    %% 2단계: 기술적 지표 및 시그널 연산
    subgraph Step2 ["2단계: 지표 가공 & 시그널 생성 (Dask Engine)"]
        direction TB
        ProcessA1["🐍 process_a1.py (MA, RSI, MACD)"] -->|지표 저장| DB_Tech[("💾 DB: technical_indicators")]
        ProcessA1 -->|지표 릴레이| ProcessB3["🐍 process_b3.py (시그널 분석)"]
        ProcessB3 -->|시그널 저장| DB_Signal[("💾 DB: trading_signals")]
    end

    %% 3단계: 시계열 클러스터링
    subgraph Step3 ["3단계: 시가총액 & 시계열 클러스터링"]
        direction TB
        ProcessM1["🐍 process_m1_cap.py"] -->|시총 저장| DB_Cap[("💾 DB: market_cap")]
        ProcessM1 -->|정규화| ProcessC1["🐍 process_c1.py (Z-Score)"]
        ProcessC1 -->|Z-Score 저장| DB_ZScore[("💾 DB: zscore_features")]
        ProcessC1 -->|SoftDTW 군집화| ProcessC2["🐍 process_c2.py (K-Means)"]
        ProcessC2 -->|군집 결과 저장| DB_Cluster[("💾 DB: clustering_results")]
    end

    %% 4단계: 4-Tier 웹 서비스 & 멀티 에이전트
    subgraph Step4 ["4단계: 4-Tier 웹 서비스 & Multi-Agent"]
        direction TB
        UI[/"🖥️ WEB: Vanilla JS SPA UI"/] <-->|REST API| WAS["⚙️ WAS: FastAPI Server"]
        WAS <--> Audit["🤖 Manager: AuditAgent (검증/보안)"]
        WAS <--> PromptAgent["🤖 Manager: PromptMakerAgent (퀀트분석)"]
        WAS -->|프롬프트 기록| DB_Logs[("💾 DB: prompt_logs")]
        SecAgent["🤖 Manager: WebSecurityAgent"] -.- WAS
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

PostgreSQL 데이터베이스 7개 핵심 테이블 간의 논리적 키(Date, Symbol) 연관 관계입니다.

```mermaid
erDiagram
    RAW_STOCK_DATA {
        int id PK "식별자"
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
        numeric market_cap_krw "시가총액"
    }

    TECHNICAL_INDICATORS {
        date date PK "수집일"
        varchar symbol PK "종목코드"
        numeric ma5 "5일 이평"
        numeric ma20 "20일 이평"
        numeric ma60 "60일 이평"
        numeric rsi "RSI"
        numeric macd "MACD"
        numeric adx "ADX"
    }

    TRADING_SIGNALS {
        date date PK "수집일"
        varchar symbol PK "종목코드"
        varchar signal_type PK "시그널 유형"
        numeric signal_strength "시그널 강도"
        text description "상세 설명"
    }

    ZSCORE_FEATURES {
        date date PK "기준일"
        varchar symbol PK "종목코드"
        varchar freq PK "주기(1d/1w/1m)"
        numeric zscore "Z-Score"
    }

    CLUSTERING_RESULTS {
        date target_date PK "기준일"
        varchar symbol PK "종목코드"
        varchar method PK "군집기법"
        int cluster_id "군집 번호"
    }

    PROMPT_LOGS {
        int id PK "식별자"
        varchar symbol "종목코드"
        text generated_prompt "생성 프롬프트"
        varchar status "Audit 상태"
    }

    %% 수직/수평 방사형 관계 배치
    RAW_STOCK_DATA ||--|| MARKET_CAP : "시가총액 매핑"
    RAW_STOCK_DATA ||--o{ TECHNICAL_INDICATORS : "보조지표 산출"
    RAW_STOCK_DATA ||--o{ TRADING_SIGNALS : "시그널 추출"
    RAW_STOCK_DATA ||--o{ ZSCORE_FEATURES : "Z-Score 정규화"
    RAW_STOCK_DATA ||--o{ CLUSTERING_RESULTS : "패턴 군집화"
    RAW_STOCK_DATA ||--o{ PROMPT_LOGS : "에이전트 분석 기록"
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
