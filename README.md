# 🤖 Ggeolmu Multi-Agent Stock Parser & Analyzer

## 1. 개요
`Ggeolmu` 프로젝트는 국내(KOSPI, KOSDAQ) 및 해외(NASDAQ, NYSE) 주식 데이터를 수집·가공하여 **PostgreSQL 데이터베이스에 적재**하고, 이를 멀티 에이전트(Multi-Agent) 기반으로 분석하여 시각화하는 **4-Tier (WEB-WAS-DB-Manager) 주식 분석 웹 서비스**입니다.

macOS 환경에 맞춘 시스템 파일 디스크립터 상향(`ulimit -n 65,536`), **`get_dynamic_cluster_config()` 동적 자원 자동 감지 모듈**, **`Database/queries/` 14개 SQL 다중 자동 로딩**, **`_safe_read_file` 범용 파라미터 파일 호환 로더** 및 **`fdr.StockListing` 기반 0.5초 초고속 DB 직행 증분 수집(DB-Centric Bulk Ingestion)** 구조를 적용하여 최신 주가 및 기술적 지표를 안전하고 빠르게 갱신합니다.

---

## 2. 시스템 아키텍처 및 파이프라인 (System Architecture)

### 2.1 전체 데이터 파이프라인 흐름도 (Data Pipeline Architecture)

```mermaid
flowchart TD
    %% 수직 순차 흐름 및 큼직한 폰트(16px, bold) 스타일 정의
    classDef pythonEngine fill:#1e3a8a,stroke:#60a5fa,stroke-width:2.5px,color:#ffffff,font-size:16px,font-weight:bold;
    classDef dbStorage fill:#064e3b,stroke:#34d399,stroke-width:2.5px,color:#ffffff,font-size:16px,font-weight:bold;
    classDef agentManager fill:#831843,stroke:#f472b6,stroke-width:2.5px,color:#ffffff,font-size:16px,font-weight:bold;
    classDef webServer fill:#1e293b,stroke:#94a3b8,stroke-width:2.5px,color:#ffffff,font-size:15px,font-weight:bold;

    %% 1단계: DB 중심 초고속 증분 수집 및 DB 직행 적재
    subgraph Step1 ["1단계: 0.5초 DB 중심 증분 수집 (Delta Ingestion)"]
        direction TB
        FDR[/"🌐 FinanceDataReader API"/] -->|0.5초 일괄 증분 수집| GetFDR["🐍 get_fdr.py<br>(DB MAX date 탐지)"]
        GetFDR -->|증분 델타 직행 적재| DB_Raw[("💾 DB: raw_stock_data<br>(14개 SQL 중앙 관리)")]
    end

    %% 2단계: 기술적 지표 및 시그널 연산
    subgraph Step2 ["2단계: 지표 가공 & 시그널 생성 (Dynamic RAM Control)"]
        direction TB
        ProcessA1["🐍 process_a1.py<br>(Dynamic RAM Control / MA,RSI,MACD)"] -->|지표 저장| DB_Tech[("💾 DB: technical_indicators")]
        ProcessA1 -->|지표 릴레이| ProcessB3["🐍 process_b3.py<br>(상승/하락/다이버전스 시그널)"]
        ProcessB3 -->|시그널 저장| DB_Signal[("💾 DB: trading_signals")]
    end

    %% 3단계: 시계열 클러스터링
    subgraph Step3 ["3단계: 시가총액 & 시계열 클러스터링 (_safe_read_file 적용)"]
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

    %% [위에서 아래로 이어지는 수직 메인 데이터 흐름선]
    Step1 ==>|1. raw_stock_data 공급| Step2
    Step1 ==>|2. raw_stock_data 공급| Step3
    Step2 & Step3 ==>|3. 분석 지표/시그널/군집 공급| Step4

    %% 노드 스타일 지정 (.py: 파란색, DB: 녹색, 에이전트: 핑크, Web/WAS: 슬레이트)
    class GetFDR,ProcessA1,ProcessB3,ProcessM1,ProcessC1,ProcessC2 pythonEngine;
    class DB_Raw,DB_Tech,DB_Signal,DB_Cap,DB_ZScore,DB_Cluster,DB_Logs dbStorage;
    class Audit,PromptAgent,SecAgent agentManager;
    class UI,WAS,FDR webServer;
```

---

### 2.2 개체 관계도 (ER Diagram)

PostgreSQL 데이터베이스 7개 핵심 테이블 간의 수직 방사형 연관 관계입니다.

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

    %% 수직 순차 방사형 관계 배치
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
- **DB Tier (`Database/`)**: PostgreSQL DBMS. `Database/queries/` 수록 `001_`~`014_` SQL 쿼리 중앙 통합 관리 (SQL Injection 방지).
- **Manager Tier (`Manager/`)**:
  - `AuditAgent`: 불필요 종목(ETF/SPAC) 필터링 및 프롬프트 주입/SQLi 검사.
  - `PromptMakerAgent`: 5일 주가 흐름 기반 퀀트 메타 프롬프트 일괄 생성.
  - `WebSecurityAgent`: WEB-WAS-DB 계층 취약점 탐지 및 보안 관리.

---

## 4. 파이프라인 핵심 기술 특장점 (Key Features)

1. **동적 자원 자동 스케일링 (`get_dynamic_cluster_config`)**
   - 하드웨어 RAM과 CPU 코어 수를 자동 측정하여 Dask 클러스터를 동적 튜닝합니다.
2. **PostgreSQL DB-Centric 초고속 직행 적재**
   - `fdr.StockListing` 기반 0.5초 일괄 증분 수집 및 DB 최신일(`SELECT MAX(date)`) 직행 쿼리 조회를 결합하여 1초 만에 최신 시세를 DB에 반영합니다.
3. **범용 파일 호환 로더 (`_safe_read_file`)**
   - `.parquet` 및 `.csv` 파티션 파일 손상 방지 및 인코딩 2차 폴백(`utf-8-sig` ➡ `cp949`)을 적용하여 파이프라인 무결성을 유지합니다.

---

### 🖥️ 기기 스펙별 자동 유연 변환 및 Dask 병렬 컴퓨팅 장점

| 구동 자원 환경 | 감지된 물리 RAM & CPU | 자동 튜닝 클러스터 스펙 | 병렬 컴퓨팅(Parallel Computing) 주요 장점 |
| :--- | :--- | :--- | :--- |
| **개발 및 테스트 환경**<br>(MacBook 16GB RAM) | `RAM: 16GB`<br>`CPU: 8 Core` | **`n_workers: 2`**<br>**`memory_limit: 6GB`**<br>`threads: 6` | - 메모리를 워커당 **6GB로 2배 확대**하여 메모리 킬 소멸<br>- 2,463개 종목 지표 계산 시 스레드 분산 병렬 처리 |
| **고성능 PC / 워크스테이션**<br>(32GB RAM 장비) | `RAM: 32GB`<br>`CPU: 12~16 Core` | **`n_workers: 4`**<br>**`memory_limit: 6GB`**<br>`threads: 2` | - 워커를 4개로 확장하여 **지표 가공 연산 속도 2배 향상**<br>- CPU 코어 멀티프로세싱 병렬 수집 |
| **클라우드 / 온프레미스**<br>(64GB~128GB+ 서버) | `RAM: 64GB+`<br>`CPU: 32 Core+` | **`n_workers: 8~16`**<br>**`memory_limit: 8GB~16GB`**<br>`npartitions: 32` | - 대규모 분산 컴퓨팅 모드로 수천 종목을 **수초 만에 연산**<br>- 코드 수정 없는 **Zero-Config 클라우드 확장** |

---

## 5. 실행 방법

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
