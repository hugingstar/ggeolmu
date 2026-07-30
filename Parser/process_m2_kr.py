import os
import time
import io
import re
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup


class FinanceDataCrawler:
    def __init__(self, base_path: str, target_date: str):
        """
        초기 설정
        :param base_path: 데이터가 저장될 최상위 기본 디렉터리 경로
        :param target_date: 크롤링 기준 날짜 (예: "2026-06-06")
        """
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

        # 수집할 타겟 지표 15개 (배당 관련 지표 3개 추가)
        self.target_metrics = [
            "매출액", "영업이익", "영업이익률", "순이익률", 
            "ROE(지배주주)", "부채비율", "당좌비율", "유보율", 
            "EPS(원)", "PER(배)", "BPS(원)", "PBR(배)",
            "주당배당금(원)", "시가배당률(%)", "배당성향(%)"
        ]

    def _get_last_page(self, sosok: int) -> int:
        """해당 시장의 마지막 페이지 번호를 추출합니다."""
        params = {"sosok": sosok, "page": 1}
        res = requests.get(self.domestic_base_url, params=params, headers=self.headers, timeout=10)
        res.encoding = "euc-kr"
        soup = BeautifulSoup(res.text, "html.parser")
        
        last_link = soup.select_one("td.pgRR a")
        if last_link and "page=" in last_link.get("href", ""):
            return int(last_link["href"].split("page=")[-1])
        return 1

    def _get_stock_list(self, sosok: int) -> pd.DataFrame:
        """해당 시장의 모든 종목명과 종목코드를 수집합니다."""
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
        """쉼표를 제거하고 숫자형(float)으로 안전하게 변환 (Parquet 호환용)"""
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
        """
        개별 종목 데이터를 추출 (상장/유통주식수/주식유통율 + 동적 연도 재무제표)
        """
        result_dict = {
            "상장주식수": None,
            "유통주식수": None,
            "주식유통율": None
        }
        
        # 1. 메인 페이지 (재무제표 및 상장주식수 1차 수집)
        url_main = f"https://finance.naver.com/item/main.naver?code={code}"
        try:
            res_main = requests.get(url_main, headers=self.headers, timeout=10)
            
            # BeautifulSoup을 이용해 상장주식수 우선 확보 (백업용)
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

        # 2. Company Info (Wisereport)에서 상장주식수 및 유동(유통)주식수, 주식유통율 정밀 추출
        url_comp = f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}"
        try:
            res_comp = requests.get(url_comp, headers=self.headers, timeout=10)
            # 정규식을 이용해 '발행주식수 / 유동비율' 추출 (예: 5,969,782,550주 / 75.39%)
            match = re.search(r'([\d,]+)\s*주\s*/\s*([\d.]+)\s*%', res_comp.text)
            if match:
                listed_shares = float(match.group(1).replace(',', ''))
                floating_ratio = float(match.group(2))
                
                result_dict['상장주식수'] = listed_shares
                result_dict['유통주식수'] = listed_shares * (floating_ratio / 100.0)
                # 요청하신 100 * 유통주식수 / 상장주식수 값은 곧 이 floating_ratio(%) 와 동일함.
                result_dict['주식유통율'] = floating_ratio 
        except Exception:
            pass

        # 3. 재무제표(기업실적분석) 파싱 및 상대 연도(Y-3 ~ Y0) 적용
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

            # 절대 연도 대신 상대 연도 라벨링 (Y-3, Y-2, Y-1, Y0)
            labels = ["Y-3", "Y-2", "Y-1", "Y0"]
            if len(target_cols) < 4:
                labels = labels[-len(target_cols):]

            for col, label in zip(target_cols, labels):
                year_str = str(col[1])
                # 예상치인 경우 (E) 꼬리표 추가
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
        """
        시장 전체 종목의 재무 데이터를 크롤링하고 M2Sheet 폴더에 저장
        """
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

        # DataFrame 병합
        df_finance = pd.DataFrame(finance_data_list)
        
        # 계산일(기준일) 컬럼 추가
        df_finance['기준일'] = self.target_date
        
        # -----------------------------------------------------------
        # [컬럼 순서 완벽 정렬 로직]
        # 1. 고정 헤더 우선 배치 (주식유통율 포함)
        front_cols = ['Name', 'Symbol', '기준일', '상장주식수', '유통주식수', '주식유통율']
        
        # 2. 나머지(Y-3 ~ Y0) 컬럼은 시간의 흐름 순서대로 정렬되게 키 지정
        def sort_chronological(c):
            if c.startswith('Y-3'): return '1_' + c
            if c.startswith('Y-2'): return '2_' + c
            if c.startswith('Y-1'): return '3_' + c
            if c.startswith('Y0'):  return '4_' + c
            return '5_' + c
            
        other_cols = [c for c in df_finance.columns if c not in front_cols]
        other_cols = sorted(other_cols, key=sort_chronological)
        
        # 최종 컬럼 적용 (데이터프레임에 존재하는 컬럼만 안전하게 필터)
        final_columns = [c for c in (front_cols + other_cols) if c in df_finance.columns]
        df_finance = df_finance[final_columns]
        # -----------------------------------------------------------

        # id 컬럼 추가
        df_finance.insert(0, "id", range(len(df_finance)))

        # 저장 디렉터리 설정 (날짜 폴더 제외, M2Sheet 직속)
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
    # %Y-%m-%d 형식으로 "2026-06-06" 형태로 자동 추출됩니다.
    TARGET_DATE = datetime.now().strftime("%Y-%m-%d")
    
    # crawler = FinanceDataCrawler(base_path=SAVE_BASE_PATH, target_date=TARGET_DATE)
    
    # 재무재표 크롤링
    MARKET_LIST = ["KOSPI", "KOSDAQ"]
    for market in MARKET_LIST:
        crawler.run_process(market)

    # 2. S3 업로드 진행 (비활성화)
    # print("\n[*] KOSPI, KOSDAQ 재무 데이터 S3 업로드 시작...")
    # for market in ["KOSPI", "KOSDAQ"]:
    #     market_base_dir_m2sheet = os.path.join(BASE_PATH, market, "M2Sheet")
    #     if os.path.exists(market_base_dir_m2sheet):
    #         s3_folder_prefix_m2sheet = f"Data/{market}/M2Sheet"
    #         # upload_parquet_to_s3(market_base_dir_m2sheet, S3_BUCKET_NAME, s3_folder_prefix_m2sheet)
