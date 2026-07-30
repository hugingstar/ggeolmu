"""
네이버 금융에서 지정된 특정 시장(KOSPI, KOSDAQ, NASDAQ, NYSE) 종목을 크롤링하여
CSV 및 Parquet 파일로 지정된 날짜별 경로에 저장하는 스크립트. (실시간 환율 적용)

- 저장 컬럼:
    id               : 0, 1, 2, 3 ... 인덱스 번호
    Name             : 종목 명
    Symbol           : 종목 번호 (국내는 6자리 코드, 해외는 티커)
    MarketCap_KRW    : 시가총액 (원화 / 억원 단위)
    MarketCap_USD    : 시가총액 (미화 / 억 USD 단위)
    ExchangeRate     : 환산 시 적용된 원/달러 환율

필요 패키지:
    pip install requests beautifulsoup4 pandas pyarrow
"""

import os
import time
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup


class MarketCapCrawler:
    def __init__(self, base_path: str, target_date: str):
        """
        초기 설정 및 환경 변수 등록
        :param base_path: 데이터가 저장될 최상위 기본 디렉터리 경로
        :param target_date: 저장될 폴더명으로 쓰일 기준 날짜 (예: "2025-06-05")
        """
        self.base_path = base_path
        self.target_date = target_date
        self.exchange_rate = None  # 환율 캐싱용 변수
        
        # URL 설정
        self.domestic_base_url = "https://finance.naver.com/sise/sise_market_sum.naver"
        self.exchange_rate_url = "https://finance.naver.com/marketindex/exchangeList.naver"
        
        # 시장 구분 매핑
        self.domestic_markets = {
            "KOSPI": 0,
            "KOSDAQ": 1,
        }
        self.us_markets = ["NASDAQ", "NYSE"]
        
        # 크롤링 헤더 설정
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }

    def get_usd_krw_rate(self) -> float:
        """네이버 금융에서 현재 달러(USD) 대비 원화(KRW) 환율을 가져옵니다."""
        # 이미 환율을 가져왔다면 캐싱된 값을 반환
        if self.exchange_rate is not None:
            return self.exchange_rate

        try:
            res = requests.get(self.exchange_rate_url, headers=self.headers, timeout=10)
            res.raise_for_status()
            res.encoding = 'euc-kr'
            soup = BeautifulSoup(res.text, "html.parser")
            
            # 환율 목록 테이블에서 첫 번째(미국 USD) 매매기준율 추출
            td_sale = soup.select_one("td.sale")
            if td_sale:
                rate_str = td_sale.get_text(strip=True).replace(",", "")
                self.exchange_rate = float(rate_str)
                return self.exchange_rate
        except Exception as e:
            print(f"환율 정보를 가져오는 중 오류 발생: {e}")
            
        print("기본 환율 1,350원으로 대체합니다.")
        self.exchange_rate = 1350.0
        return self.exchange_rate

    def _get_soup(self, sosok: int, page: int) -> BeautifulSoup:
        """특정 시장(sosok)·페이지의 HTML을 가져와 BeautifulSoup 객체로 반환."""
        params = {"sosok": sosok, "page": page}
        res = requests.get(self.domestic_base_url, params=params, headers=self.headers, timeout=10)
        res.raise_for_status()
        res.encoding = "euc-kr"
        return BeautifulSoup(res.text, "html.parser")

    def _get_last_page(self, sosok: int) -> int:
        """페이지 하단의 '맨뒤(pgRR)' 링크에서 마지막 페이지 번호를 추출."""
        soup = self._get_soup(sosok, 1)
        last_link = soup.select_one("td.pgRR a")
        if last_link and "page=" in last_link.get("href", ""):
            return int(last_link["href"].split("page=")[-1])
        return 1

    def crawl_domestic_market(self, sosok: int, delay: float = 0.3) -> pd.DataFrame:
        """
        국내 시장(KOSPI/KOSDAQ) 크롤링.
        원본: 한화(억원) -> MarketCap_USD(억USD)로 환산.
        """
        exchange_rate = self.get_usd_krw_rate()
        last_page = self._get_last_page(sosok)
        names, codes, mcaps_krw, mcaps_usd = [], [], [], []
        seen = set()

        for page in range(1, last_page + 1):
            soup = self._get_soup(sosok, page)

            thead_ths = soup.select("table.type_2 thead th")
            mcap_idx = -1
            for i, th in enumerate(thead_ths):
                if "시가총액" in th.get_text():
                    mcap_idx = i
                    break

            for tr in soup.select("table.type_2 tbody tr"):
                a = tr.select_one("a.tltle")
                if not a:
                    continue
                    
                name = a.get_text(strip=True)
                href = a.get("href", "")
                if "code=" not in href:
                    continue
                code = href.split("code=")[-1]

                if code in seen:
                    continue
                seen.add(code)

                mcap_val_krw = None
                mcap_val_usd = None
                if mcap_idx != -1:
                    tds = tr.select("td")
                    if len(tds) > mcap_idx:
                        mcap_str = tds[mcap_idx].get_text(strip=True).replace(",", "")
                        try:
                            # 1. 원본: 한화 억원 (MarketCap)
                            mcap_val_krw = float(mcap_str)
                            # 2. 환산: 달러 억USD (MarketCap_global)
                            mcap_val_usd = round(mcap_val_krw / exchange_rate, 2)
                        except ValueError:
                            pass

                names.append(name)
                codes.append(code)
                mcaps_krw.append(mcap_val_krw)
                mcaps_usd.append(mcap_val_usd)

            time.sleep(delay)

        df = pd.DataFrame({
            "Name": names, 
            "Symbol": codes, 
            "MarketCap_KRW": mcaps_krw,
            "MarketCap_USD": mcaps_usd,
            "ExchangeRate": exchange_rate
        })
        df.insert(0, "id", range(len(df)))
        return df

    def crawl_us_market(self, exchange: str, delay: float = 0.3) -> pd.DataFrame:
        """
        미국 시장(NASDAQ/NYSE) 크롤링.
        원본: 달러(억USD) -> MarketCap(억원)으로 환산.
        """
        exchange_rate = self.get_usd_krw_rate()
        names, codes, mcaps_krw, mcaps_usd = [], [], [], []
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
                
                # API에서 시가총액 키 추출 (안전한 파싱)
                mcap_raw = s.get("marketCap")
                if mcap_raw is None:
                    mcap_raw = s.get("marketValue")
                    
                mcap_val_usd = None
                mcap_val_krw = None
                
                if mcap_raw is not None:
                    try:
                        if isinstance(mcap_raw, str):
                            mcap_raw = mcap_raw.replace(",", "")
                        # 1. 원본: 달러 억USD (MarketCap_global)
                        mcap_val_usd = float(mcap_raw)
                        # 2. 환산: 한화 억원 (MarketCap)
                        mcap_val_krw = round(mcap_val_usd * exchange_rate, 2)
                    except ValueError:
                        pass

                if not code or code in seen:
                    continue
                seen.add(code)

                names.append(name)
                codes.append(code)
                mcaps_krw.append(mcap_val_krw)
                mcaps_usd.append(mcap_val_usd)

            if len(stocks) < 100:
                break

            page += 1
            time.sleep(delay)

        df = pd.DataFrame({
            "Name": names, 
            "Symbol": codes, 
            "MarketCap_KRW": mcaps_krw,
            "MarketCap_USD": mcaps_usd,
            "ExchangeRate": exchange_rate
        })
        df.insert(0, "id", range(len(df)))
        return df

    def run_process(self, market: str):
        """
        단일 시장(market)을 대상으로 데이터 수집 및 저장 실행
        """
        market = market.upper()
        current_exchange_rate = self.get_usd_krw_rate()
        
        print(f"💵 [기준 환율] 1 USD = {current_exchange_rate:,.2f} KRW")
        print(f"📅 [저장 기준 날짜] {self.target_date}")
        print(f"[{market}] 시가총액 크롤링 시작...\n")

        # 시장에 따른 분기 처리
        if market in self.domestic_markets:
            sosok = self.domestic_markets[market]
            df = self.crawl_domestic_market(sosok)
        elif market in self.us_markets:
            df = self.crawl_us_market(market)
        else:
            print(f"❌ 지원하지 않는 시장 이름입니다: {market}")
            return
        
        # 시장별 디렉터리 경로 동적 생성 및 폴더 생성
        target_dir = os.path.join(self.base_path, market, "M1Sheet", self.target_date)
        os.makedirs(target_dir, exist_ok=True)
        
        csv_filename = os.path.join(target_dir, "df_cap.csv")
        
        df.to_csv(csv_filename, index=False, encoding="utf-8-sig")
        
        print(f"[{market}] 완료 - {len(df)}개 종목 저장 완료")
        print(f"📂 저장 경로: {target_dir}")
        print(df.head(), "\n")

# 외부 모듈에서 Import하여 사용할 경우를 대비한 래퍼 함수
def run_process(base_path: str, target_date: str, market: str):
    """
    기존 호환성을 유지하기 위한 외부 호출용 함수.
    """
    crawler = MarketCapCrawler(base_path=base_path, target_date=target_date)
    crawler.run_process(market=market)


if __name__ == "__main__":
    # 1. 데이터가 저장될 최상위 경로를 설정합니다.
    SAVE_BASE_PATH = r"C:\Users\yslee\PycharmProjects\FinanceMLOps\Data"
    
    # 2. 저장 기준이 될 날짜를 설정합니다.
    TARGET_DATE = datetime.now().strftime("%Y-%m-%d")
    
    # 3. 클래스 인스턴스화
    crawler = MarketCapCrawler(base_path=SAVE_BASE_PATH, target_date=TARGET_DATE)
    
    # 4. 이제 원하는 마켓만 개별적으로 지정해서 실행할 수 있습니다.
    crawler.run_process("KOSPI")
    # crawler.run_process("KOSDAQ")
    # crawler.run_process("NASDAQ")
    # crawler.run_process("NYSE")