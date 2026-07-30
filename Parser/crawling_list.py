"""
네이버 금융(시가총액 페이지)에서 KOSPI / KOSDAQ 종목을 크롤링하여
CSV 파일로 저장하는 스크립트.

- FinanceDataReader, pykrx 미사용 (requests + BeautifulSoup만 사용)
- 저장 컬럼
    id     : 0, 1, 2, 3 ... 인덱스 번호
    Name   : 종목 명
    Symbol : 종목 번호 (네이버 href의 code 값)

필요 패키지:
    pip install requests beautifulsoup4 pandas
"""

import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE_URL = "https://finance.naver.com/sise/sise_market_sum.naver"

# sosok=0 -> KOSPI, sosok=1 -> KOSDAQ
MARKETS = {
    "KOSPI": 0,
    "KOSDAQ": 1,
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


def _get_soup(sosok: int, page: int) -> BeautifulSoup:
    """특정 시장(sosok)·페이지의 HTML을 가져와 BeautifulSoup 객체로 반환."""
    params = {"sosok": sosok, "page": page}
    res = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=10)
    res.raise_for_status()
    # 네이버 금융 페이지는 euc-kr 인코딩
    res.encoding = "euc-kr"
    return BeautifulSoup(res.text, "html.parser")


def _get_last_page(sosok: int) -> int:
    """페이지 하단의 '맨뒤(pgRR)' 링크에서 마지막 페이지 번호를 추출."""
    soup = _get_soup(sosok, 1)
    last_link = soup.select_one("td.pgRR a")
    if last_link and "page=" in last_link.get("href", ""):
        return int(last_link["href"].split("page=")[-1])
    return 1


def crawl_market(sosok: int, delay: float = 0.3) -> pd.DataFrame:
    """
    하나의 시장(KOSPI 또는 KOSDAQ) 전체 종목을 크롤링.
    반환: id / Name / Symbol 컬럼을 가진 DataFrame
    """
    last_page = _get_last_page(sosok)
    names, codes = [], []
    seen = set()  # 중복(끝 페이지를 넘어갔을 때) 방지용

    for page in range(1, last_page + 1):
        soup = _get_soup(sosok, page)

        # 종목명 링크: <a href="/item/main.naver?code=005930" class="tltle">삼성전자</a>
        for a in soup.select("a.tltle"):
            name = a.get_text(strip=True)
            href = a.get("href", "")
            if "code=" not in href:
                continue
            code = href.split("code=")[-1]  # 6자리 종목코드 (문자열 유지)

            if code in seen:
                continue
            seen.add(code)

            names.append(name)
            codes.append(code)

        time.sleep(delay)  # 서버 부하/차단 방지

    # ----- DataFrame 구성 -----
    df = pd.DataFrame({"Name": names, "Symbol": codes})

    # 혹시 컬럼이 'Code'로 들어오는 경우 'Symbol'로 변경 (안전장치)
    if "Code" in df.columns:
        df = df.rename(columns={"Code": "Symbol"})

    # id 컬럼: 0, 1, 2, 3 ... 인덱스 번호
    df.insert(0, "id", range(len(df)))

    # 컬럼 순서 고정
    df = df[["id", "Name", "Symbol"]]
    return df


def main():
    for market_name, sosok in MARKETS.items():
        print(f"[{market_name}] 크롤링 시작...")
        df = crawl_market(sosok)

        filename = f"{market_name.lower()}.csv"
        # utf-8-sig: 엑셀에서 한글 깨짐 방지, Symbol의 앞자리 0 보존
        df.to_csv(filename, index=False, encoding="utf-8-sig")

        print(f"[{market_name}] 완료 - {len(df)}개 종목 저장 → {filename}")
        print(df.head(), "\n")


if __name__ == "__main__":
    main()