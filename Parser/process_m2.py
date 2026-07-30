import os
import time
import io
import re
from datetime import datetime
import pytz
import requests
import pandas as pd
from bs4 import BeautifulSoup
import yfinance as yf
from apscheduler.schedulers.blocking import BlockingScheduler

# S3 업로드 모듈 Import (비활성화)
# from deploy_s3 import upload_parquet_to_s3


# ======================================================================
# 1. KOSPI, KOSDAQ 크롤러 클래스
# ======================================================================
class FinanceDataCrawler:
    def __init__(self, base_path: str, target_date: str):
        self.base_path = base_path
        self.target_date = target_date
        
        self.domestic_base_url = "https://finance.naver.com/sise/sise_market_sum.naver"
        self.domestic_markets = {
            "KOSPI": 0,
            "KOSDAQ": 1,
        }
        
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }

        self.target_metrics = [
            "매출액", "영업이익", "영업이익률", "순이익률", 
            "ROE(지배주주)", "부채비율", "당좌비율", "유보율", 
            "EPS(원)", "PER(배)", "BPS(원)", "PBR(배)",
            "주당배당금(원)", "시가배당률(%)", "배당성향(%)"
        ]

    def _get_last_page(self, sosok: int) -> int:
        params = {"sosok": sosok, "page": 1}
        res = requests.get(self.domestic_base_url, params=params, headers=self.headers, timeout=10)
        res.encoding = "euc-kr"
        soup = BeautifulSoup(res.text, "html.parser")
        
        last_link = soup.select_one("td.pgRR a")
        if last_link and "page=" in last_link.get("href", ""):
            return int(last_link["href"].split("page=")[-1])
        return 1

    def _get_stock_list(self, sosok: int) -> pd.DataFrame:
        last_page = self._get_last_page(sosok)
        names, codes = [], []
        seen = set()

        print("종목 리스트를 수집 중입니다...")
        for page in range(1, last_page + 1):
            params = {"sosok": sosok, "page": page}
            res = requests.get(self.domestic_base_url, params=params, headers=self.headers, timeout=10)
            res.encoding = "euc-kr"
            soup = BeautifulSoup(res.text, "html.parser")

            for tr in soup.select("table.type_2 tbody tr"):
                a = tr.select_one("a.tltle")
                if not a:
                    continue
                
                name = a.get_text(strip=True)
                code = a.get("href", "").split("code=")[-1]

                if code not in seen:
                    seen.add(code)
                    names.append(name)
                    codes.append(code)

        return pd.DataFrame({"Name": names, "Symbol": codes})

    def _clean_numeric_value(self, val):
        if pd.isna(val):
            return None
            
        if isinstance(val, str):
            val = val.replace(',', '').strip()
            if val in ('', '-', 'N/A', 'N/A(IFRS)'):
                return None
                
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def _get_financial_data(self, code: str) -> dict:
        result_dict = {
            "상장주식수": None,
            "유통주식수": None,
            "주식유통율": None
        }
        
        # 1. 메인 페이지
        url_main = f"https://finance.naver.com/item/main.naver?code={code}"
        try:
            res_main = requests.get(url_main, headers=self.headers, timeout=10)
            soup = BeautifulSoup(res_main.text, "html.parser")
            listed_th = soup.find('th', string=lambda t: t and '상장주식수' in t)
            if listed_th:
                td = listed_th.find_next_sibling('td')
                if td:
                    val = td.get_text(strip=True).replace(',', '').replace('주', '')
                    if val.isdigit():
                        result_dict['상장주식수'] = float(val)

            dfs = pd.read_html(io.StringIO(res_main.text), encoding='euc-kr')
        except Exception:
            dfs = []

        # 2. Company Info
        url_comp = f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}"
        try:
            res_comp = requests.get(url_comp, headers=self.headers, timeout=10)
            match = re.search(r'([\d,]+)\s*주\s*/\s*([\d.]+)\s*%', res_comp.text)
            if match:
                listed_shares = float(match.group(1).replace(',', ''))
                floating_ratio = float(match.group(2))
                
                result_dict['상장주식수'] = listed_shares
                result_dict['유통주식수'] = listed_shares * (floating_ratio / 100.0)
                result_dict['주식유통율'] = floating_ratio 
        except Exception:
            pass

        # 3. 재무제표
        fin_df = None
        for df in dfs:
            if isinstance(df.columns, pd.MultiIndex):
                top_level = [c[0] for c in df.columns]
                if '최근 연간 실적' in top_level:
                    fin_df = df
                    break
        
        if fin_df is not None:
            if fin_df.columns.nlevels >= 3:
                fin_df.columns = fin_df.columns.droplevel(2)

            annual_cols = [c for c in fin_df.columns if c[0] == '최근 연간 실적']
            target_cols = annual_cols[-4:] if len(annual_cols) >= 4 else annual_cols

            metrics_col = fin_df.columns[0]
            fin_df.set_index(metrics_col, inplace=True)

            labels = ["Y-3", "Y-2", "Y-1", "Y0"]
            if len(target_cols) < 4:
                labels = labels[-len(target_cols):]

            for col, label in zip(target_cols, labels):
                year_str = str(col[1])
                final_label = f"{label}(E)" if "(E)" in year_str else label

                for metric in self.target_metrics:
                    key = f"{final_label}_{metric}"
                    try:
                        val = fin_df.loc[metric, col]
                        if isinstance(val, pd.Series):
                            val = val.iloc[0]
                        result_dict[key] = self._clean_numeric_value(val)
                    except KeyError:
                        result_dict[key] = None

        return result_dict

    def run_process(self, market: str, limit: int = None):
        market = market.upper()
        if market not in self.domestic_markets:
            print(f"❌ 지원하지 않는 시장 이름입니다: {market}. (KOSPI 또는 KOSDAQ만 지원)")
            return

        print(f"[{market}] 재무제표 크롤링 시작 (기준일: {self.target_date})...")
        sosok = self.domestic_markets[market]
        
        df_stocks = self._get_stock_list(sosok)
        if limit:
            df_stocks = df_stocks.head(limit)
            print(f"⚠️ [테스트 모드] 상위 {limit}개 종목만 진행합니다.")
        
        print(f"총 {len(df_stocks)}개 종목의 재무 데이터 수집을 시작합니다. (시간이 다소 소요됩니다.)")

        finance_data_list = []
        for idx, row in df_stocks.iterrows():
            code = row['Symbol']
            name = row['Name']
            
            if (idx + 1) % 100 == 0:
                print(f"진행 중... ({idx + 1} / {len(df_stocks)})")
                
            fin_data = self._get_financial_data(code)
            fin_data['Name'] = name
            fin_data['Symbol'] = code
            finance_data_list.append(fin_data)
            
            time.sleep(0.3) 

        df_finance = pd.DataFrame(finance_data_list)
        df_finance['기준일'] = self.target_date
        
        front_cols = ['Name', 'Symbol', '기준일', '상장주식수', '유통주식수', '주식유통율']
        
        def sort_chronological(c):
            if c.startswith('Y-3'): return '1_' + c
            if c.startswith('Y-2'): return '2_' + c
            if c.startswith('Y-1'): return '3_' + c
            if c.startswith('Y0'):  return '4_' + c
            return '5_' + c
            
        other_cols = [c for c in df_finance.columns if c not in front_cols]
        other_cols = sorted(other_cols, key=sort_chronological)
        
        final_columns = [c for c in (front_cols + other_cols) if c in df_finance.columns]
        df_finance = df_finance[final_columns]
        
        df_finance.insert(0, "id", range(len(df_finance)))

        target_dir = os.path.join(self.base_path, market, "M2Sheet")
        os.makedirs(target_dir, exist_ok=True)

        csv_filename = os.path.join(target_dir, "df_finance.csv")
        parquet_filename = os.path.join(target_dir, "df_finance.parquet")

        df_finance.to_csv(csv_filename, index=False, encoding="utf-8-sig")
        df_finance.to_parquet(parquet_filename, index=False)

        print(f"✅ [{market}] 완료 - {len(df_finance)}개 종목 재무 데이터 저장 완료")
        print(f"📂 저장 경로: {target_dir}")


# ======================================================================
# 2. NASDAQ, NYSE 크롤러 클래스
# ======================================================================
class USFinanceDataCrawler:
    def __init__(self, base_path: str, target_date: str):
        self.base_path = base_path
        self.target_date = target_date
        
        self.us_markets = ["NASDAQ", "NYSE"]
        
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }

        self.target_metrics = [
            "매출액", "영업이익", "영업이익률(%)", "순이익률(%)", 
            "ROE", "부채비율", "당좌비율", "유보율(대체)", 
            "EPS(USD)", "PER", "BPS(USD)", "PBR",
            "주당배당금(USD)", "시가배당률(%)", "배당성향(%)"
        ]

    def _get_stock_list(self, exchange: str, delay: float = 0.3) -> pd.DataFrame:
        print(f"[{exchange}] 종목 리스트를 수집 중입니다 (네이버 금융 API)...")
        names, codes = [], []
        page = 1
        seen = set()

        while True:
            url = f"https://api.stock.naver.com/stock/exchange/{exchange}/marketValue"
            params = {"page": page, "pageSize": 100}
            
            res = requests.get(url, params=params, headers=self.headers, timeout=10)
            if res.status_code != 200:
                break
                
            try:
                data = res.json()
            except Exception:
                break

            if isinstance(data, dict):
                stocks = data.get("stocks", [])
            elif isinstance(data, list):
                stocks = data
            else:
                break

            if not stocks:
                break

            for s in stocks:
                name = s.get("stockName") or s.get("stockNameEng") or s.get("name", "")
                code = s.get("symbolCode") or s.get("reutersCode") or s.get("symbol", "")
                
                if code:
                    code = code.replace(".", "-")

                if not code or code in seen:
                    continue
                
                seen.add(code)
                names.append(name)
                codes.append(code)

            if len(stocks) < 100:
                break

            page += 1
            time.sleep(delay)

        return pd.DataFrame({"Name": names, "Symbol": codes})

    def _clean_numeric_value(self, val):
        if pd.isna(val) or val in ('', '-', 'N/A', 'Infinity', 'NaN'):
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def _get_financial_data(self, code: str) -> dict:
        result_dict = {
            "상장주식수": None,
            "유통주식수": None,
            "주식유통율": None
        }
        
        try:
            ticker = yf.Ticker(code)
            info = ticker.info
            
            shares_out = info.get("sharesOutstanding")
            float_shares = info.get("floatShares")
            
            result_dict['상장주식수'] = self._clean_numeric_value(shares_out)
            result_dict['유통주식수'] = self._clean_numeric_value(float_shares)
            
            if shares_out and float_shares and shares_out > 0:
                result_dict['주식유통율'] = round((float_shares / shares_out) * 100, 2)

            static_ratios = {
                "영업이익률(%)": info.get("operatingMargins", 0) * 100 if info.get("operatingMargins") else None,
                "순이익률(%)": info.get("profitMargins", 0) * 100 if info.get("profitMargins") else None,
                "ROE": info.get("returnOnEquity", 0) * 100 if info.get("returnOnEquity") else None,
                "부채비율": info.get("debtToEquity"),
                "당좌비율": info.get("quickRatio"),
                "EPS(USD)": info.get("trailingEps"),
                "PER": info.get("trailingPE"),
                "BPS(USD)": info.get("bookValue"),
                "PBR": info.get("priceToBook"),
                "주당배당금(USD)": info.get("dividendRate"),
                "시가배당률(%)": info.get("dividendYield", 0) * 100 if info.get("dividendYield") else None,
                "배당성향(%)": info.get("payoutRatio", 0) * 100 if info.get("payoutRatio") else None,
            }
            
            financials = ticker.financials
            labels = ["Y-3", "Y-2", "Y-1", "Y0"]
            
            if not financials.empty:
                cols = sorted(financials.columns)
                target_cols = cols[-4:] if len(cols) >= 4 else cols
                current_labels = labels[-len(target_cols):]
                
                for col, label in zip(target_cols, current_labels):
                    try:
                        revenue = financials.loc["Total Revenue", col]
                        result_dict[f"{label}_매출액"] = self._clean_numeric_value(revenue)
                    except KeyError:
                        result_dict[f"{label}_매출액"] = None
                        
                    try:
                        op_income = financials.loc["Operating Income", col]
                        result_dict[f"{label}_영업이익"] = self._clean_numeric_value(op_income)
                    except KeyError:
                        result_dict[f"{label}_영업이익"] = None

            for metric, value in static_ratios.items():
                result_dict[f"Y0_{metric}"] = self._clean_numeric_value(value)

        except Exception:
            pass
            
        return result_dict

    def run_process(self, market: str, limit: int = None):
        market = market.upper()
        if market not in self.us_markets:
            print(f"❌ 지원하지 않는 시장 이름입니다: {market}. (NASDAQ 또는 NYSE만 지원)")
            return

        print(f"[{market}] 재무제표 크롤링 시작 (기준일: {self.target_date})...")
        
        df_stocks = self._get_stock_list(market)
        
        if df_stocks.empty:
            print("종목 데이터를 불러오지 못해 종료합니다.")
            return

        if limit:
            df_stocks = df_stocks.head(limit)
            print(f"⚠️ [테스트 모드] 상위 {limit}개 종목만 진행합니다.")
        
        print(f"총 {len(df_stocks)}개 종목의 재무 데이터 수집을 시작합니다. (시간이 다소 소요됩니다.)")

        finance_data_list = []
        for idx, row in df_stocks.iterrows():
            code = row['Symbol']
            name = row['Name']
            
            if (idx + 1) % 50 == 0:
                print(f"진행 중... ({idx + 1} / {len(df_stocks)})")
                
            fin_data = self._get_financial_data(code)
            fin_data['Name'] = name
            fin_data['Symbol'] = code
            finance_data_list.append(fin_data)
            
            time.sleep(0.3)

        df_finance = pd.DataFrame(finance_data_list)
        df_finance['기준일'] = self.target_date
        
        front_cols = ['Name', 'Symbol', '기준일', '상장주식수', '유통주식수', '주식유통율']
        
        def sort_chronological(c):
            if c.startswith('Y-3'): return '1_' + c
            if c.startswith('Y-2'): return '2_' + c
            if c.startswith('Y-1'): return '3_' + c
            if c.startswith('Y0'):  return '4_' + c
            return '5_' + c
            
        other_cols = [c for c in df_finance.columns if c not in front_cols]
        other_cols = sorted(other_cols, key=sort_chronological)
        
        final_columns = [c for c in (front_cols + other_cols) if c in df_finance.columns]
        df_finance = df_finance[final_columns]
        
        df_finance.insert(0, "id", range(len(df_finance)))

        target_dir = os.path.join(self.base_path, market, "M2Sheet")
        os.makedirs(target_dir, exist_ok=True)

        csv_filename = os.path.join(target_dir, "df_finance.csv")
        parquet_filename = os.path.join(target_dir, "df_finance.parquet")

        df_finance.to_csv(csv_filename, index=False, encoding="utf-8-sig")
        df_finance.to_parquet(parquet_filename, index=False)

        print(f"✅ [{market}] 완료 - {len(df_finance)}개 종목 재무 데이터 저장 완료")
        print(f"📂 저장 경로: {target_dir}")


# ======================================================================
# 3. 스케줄러를 위한 작업(Job) 함수 정의
# ======================================================================
SAVE_BASE_PATH = r"C:\Users\yslee\PycharmProjects\FinanceMLOps\Data"
S3_BUCKET_NAME = "yslee-s3-bucket"
KST = pytz.timezone('Asia/Seoul')

def run_kr_crawling():
    """국내 시장(KOSPI, KOSDAQ) 크롤링 및 S3 업로드 파이프라인"""
    target_date = datetime.now(KST).strftime("%Y-%m-%d")
    print(f"\n{'='*60}\n[{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}] 국내 시장 크롤링 파이프라인 시작\n{'='*60}")
    
    crawler = FinanceDataCrawler(base_path=SAVE_BASE_PATH, target_date=target_date)
    market_list = ["KOSPI", "KOSDAQ"]
    
    # 1. 크롤링 진행
    for market in market_list:
        crawler.run_process(market)

    # 2. S3 업로드 진행 (비활성화)
    # print("\n[*] KOSPI, KOSDAQ 재무 데이터 S3 업로드 시작...")
    # for market in ["KOSPI", "KOSDAQ"]:
    #     market_base_dir_m2sheet = os.path.join(SAVE_BASE_PATH, market, "M2Sheet")
    #     if os.path.exists(market_base_dir_m2sheet):
    #         s3_folder_prefix_m2sheet = f"Data/{market}/M2Sheet"
    #         # upload_parquet_to_s3(market_base_dir_m2sheet, S3_BUCKET_NAME, s3_folder_prefix_m2sheet)
            
    print(f"[{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}] 국내 시장 파이프라인 완료\n")


def run_us_crawling():
    """미국 시장(NASDAQ, NYSE) 크롤링 및 S3 업로드 파이프라인"""
    target_date = datetime.now(KST).strftime("%Y-%m-%d")
    print(f"\n{'='*60}\n[{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}] 미국 시장 크롤링 파이프라인 시작\n{'='*60}")
    
    crawler = USFinanceDataCrawler(base_path=SAVE_BASE_PATH, target_date=target_date)
    market_list = ["NASDAQ", "NYSE"]
    
    # 1. 크롤링 진행
    for market in market_list:
        crawler.run_process(market)

    # 2. S3 업로드 진행 (비활성화)
    # print("\n[*] NASDAQ, NYSE 재무 데이터 S3 업로드 시작...")
    # for market in ["NASDAQ", "NYSE"]:
    #     market_base_dir_m2sheet = os.path.join(SAVE_BASE_PATH, market, "M2Sheet")
    #     if os.path.exists(market_base_dir_m2sheet):
    #         s3_folder_prefix_m2sheet = f"Data/{market}/M2Sheet"
    #         # upload_parquet_to_s3(market_base_dir_m2sheet, S3_BUCKET_NAME, s3_folder_prefix_m2sheet)
            
    print(f"[{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}] 미국 시장 파이프라인 완료\n")


# ======================================================================
# 4. 메인 루프 (스케줄러 설정 및 실행)
# ======================================================================
if __name__ == "__main__":
    scheduler = BlockingScheduler(timezone=KST)

    # 한국시간 기준 매주 토요일 오후 15시 (오후 3시) -> KOSPI, KOSDAQ 실행
    scheduler.add_job(
        run_kr_crawling,
        trigger='cron',
        day_of_week='sat',
        hour=15,
        minute=0
    )

    # 한국시간 기준 매주 일요일 오후 15시 (오후 3시) -> NASDAQ, NYSE 실행
    scheduler.add_job(
        run_us_crawling,
        trigger='cron',
        day_of_week='sun',
        hour=15,
        minute=0
    )

    print(f"\n{'='*60}")
    print(f"[{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}] 재무제표 크롤링 스케줄러가 정상 가동되었습니다.")
    print("- KOSPI, KOSDAQ : 매주 토요일 15:00 (KST)")
    print("- NASDAQ, NYSE  : 매주 일요일 15:00 (KST)")
    print("종료하려면 Ctrl+C를 누르세요.")
    print(f"{'='*60}\n")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n스케줄러가 안전하게 종료되었습니다.")