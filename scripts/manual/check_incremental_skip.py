# 수동 스모크 테스트: DB에 저장된 최신 날짜와 실제 시장 최신 거래일을 비교해 증분 수집 스킵 로직을 확인합니다.
# pytest가 자동 수집하지 않도록 파일명에 test_ 접두사를 쓰지 않습니다.
import os
import sys

_repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, _repo_root)

import pandas as pd
import FinanceDataReader as fdr
from Parser.get_fdr import get_db_max_date

if __name__ == '__main__':
    max_date = get_db_max_date()
    bm_df = fdr.DataReader('005930', (pd.Timestamp.now() - pd.Timedelta(days=7)).strftime('%Y-%m-%d'))
    actual_latest_date = bm_df.index.max()

    print("max_date:", max_date, type(max_date))
    print("actual:", actual_latest_date, type(actual_latest_date))
    print("actual <= max:", actual_latest_date <= max_date)
