import os
from datetime import datetime
import pytz
from apscheduler.schedulers.blocking import BlockingScheduler

from process_m2_kr import FinanceDataCrawler
from process_m2_us import USFinanceDataCrawler

# S3 업로드 모듈 Import (비활성화)
# from deploy_s3 import upload_parquet_to_s3

# ======================================================================
# 3. 스케줄러를 위한 작업(Job) 함수 정의
# ======================================================================
SAVE_BASE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Data")
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