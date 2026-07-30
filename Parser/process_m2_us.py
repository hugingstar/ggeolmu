import os
import time
import requests
import pandas as pd
from datetime import datetime
import yfinance as yf  # pip install yfinance 필요


class USFinanceDataCrawler:
    def __init__(self, base_path: str, target_date: str):
        """
        초기 설정
        :param base_path: 데이터가 저장될 최상위 기본 디렉터리 경로
        :param target_date: 크롤링 기준 날짜 (예: "2026-06-06")
        """
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

        # 수집할 타겟 지표 15개 (미국 기준 매핑)
        self.target_metrics = [
            "매출액", "영업이익", "영업이익률(%)", "순이익률(%)", 
            "ROE", "부채비율", "당좌비율", "유보율(대체)", 
            "EPS(USD)", "PER", "BPS(USD)", "PBR",
            "주당배당금(USD)", "시가배당률(%)", "배당성향(%)"
        ]

    def _get_stock_list(self, exchange: str, delay: float = 0.3) -> pd.DataFrame:
        """
        [적용 완료] 네이버 금융 API를 통해 미국 시장(NASDAQ/NYSE)의 종목 리스트를 수집합니다.
        """
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
                
                # Yahoo Finance 호환성을 위해 티커의 마침표(.)를 하이픈(-)으로 변경 (예: BRK.B -> BRK-B)
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
        """숫자형(float)으로 안전하게 변환 (Parquet 호환용)"""
        if pd.isna(val) or val in ('', '-', 'N/A', 'Infinity', 'NaN'):
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def _get_financial_data(self, code: str) -> dict:
        """
        개별 종목 데이터를 추출 (yfinance 라이브러리 활용)
        """
        result_dict = {
            "상장주식수": None,
            "유통주식수": None,
            "주식유통율": None
        }
        
        try:
            ticker = yf.Ticker(code)
            info = ticker.info
            
            # 1. 주식 수 및 유통 비율 추출
            shares_out = info.get("sharesOutstanding")
            float_shares = info.get("floatShares")
            
            result_dict['상장주식수'] = self._clean_numeric_value(shares_out)
            result_dict['유통주식수'] = self._clean_numeric_value(float_shares)
            
            if shares_out and float_shares and shares_out > 0:
                result_dict['주식유통율'] = round((float_shares / shares_out) * 100, 2)

            # 2. 고정 재무 비율 (yfinance info에서 바로 가져올 수 있는 항목들)
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
            
            # 3. 연간 재무제표 (매출액, 영업이익) 파싱 (최근 4년)
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
        """
        시장 전체 종목의 재무 데이터를 크롤링하고 M2Sheet 폴더에 저장
        """
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

        # DataFrame 병합
        df_finance = pd.DataFrame(finance_data_list)
        df_finance['기준일'] = self.target_date
        
        # -----------------------------------------------------------
        # [컬럼 순서 완벽 정렬 로직]
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
        # -----------------------------------------------------------

        # id 컬럼 추가
        df_finance.insert(0, "id", range(len(df_finance)))

        target_dir = os.path.join(self.base_path, market, "M2Sheet")
        os.makedirs(target_dir, exist_ok=True)

        csv_filename = os.path.join(target_dir, "df_finance.csv")
        parquet_filename = os.path.join(target_dir, "df_finance.parquet")

        df_finance.to_csv(csv_filename, index=False, encoding="utf-8-sig")
        df_finance.to_parquet(parquet_filename, index=False)

        print(f"✅ [{market}] 완료 - {len(df_finance)}개 종목 재무 데이터 저장 완료")
        print(f"📂 저장 경로: {target_dir}")


if __name__ == "__main__":
    SAVE_BASE_PATH = r"C:\Users\yslee\PycharmProjects\FinanceMLOps\Data"
    TARGET_DATE = datetime.now().strftime("%Y-%m-%d")
    
    # crawler = USFinanceDataCrawler(base_path=SAVE_BASE_PATH, target_date=TARGET_DATE)
    
    # 재무재표 크롤링
    MARKET_LIST = ["NASDAQ", "NYSE"]
    for market in MARKET_LIST:
        crawler.run_process(market)

    # 2. S3 업로드 진행 (비활성화)
    # print("\n[*] NASDAQ, NYSE 재무 데이터 S3 업로드 시작...")
    # for market in ["NASDAQ", "NYSE"]:
    #     market_base_dir_m2sheet = os.path.join(BASE_PATH, market, "M2Sheet")
    #     if os.path.exists(market_base_dir_m2sheet):
    #         s3_folder_prefix_m2sheet = f"Data/{market}/M2Sheet"
    #         # upload_parquet_to_s3(market_base_dir_m2sheet, S3_BUCKET_NAME, s3_folder_prefix_m2sheet)
