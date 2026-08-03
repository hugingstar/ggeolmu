# 수동 스모크 테스트: process_c1(Z-Score 산출)을 단독 실행해 결과를 눈으로 확인합니다.
# pytest가 자동 수집하지 않도록 파일명에 test_ 접두사를 쓰지 않습니다.
# 실행하면 실제 DB에 upsert가 발생하므로 개발용 DB에서만 실행하세요.
import os
import sys

_repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, _repo_root)

from Parser.process_c1 import get_kospi200_dask_data

if __name__ == '__main__':
    BASE_PATH = os.path.join(_repo_root, "Data")

    get_kospi200_dask_data(
        start_date="2025-01-01",
        end_date="2026-08-01",
        num_stocks=4000,
        input_path=BASE_PATH,
        output_path=BASE_PATH,
        sleep_interval=0.05,
        market_name="KOSDAQ",
        frequencies=["1d", "1w", "1m"],
        exclude_keywords=[]
    )
