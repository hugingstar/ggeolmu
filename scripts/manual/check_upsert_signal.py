# 수동 스모크 테스트: FinancePipeline._upsert_a1가 실제로 어떤 파일을 읽는지 확인합니다.
# pytest가 자동 수집하지 않도록 파일명에 test_ 접두사를 쓰지 않습니다.
# 실행하면 실제 DB에 upsert가 발생하므로 개발용 DB에서만 실행하세요.
import os
import sys

_repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(_repo_root, "Parser"))
sys.path.insert(0, os.path.join(_repo_root, "Database"))

from main import FinancePipeline

if __name__ == '__main__':
    pipeline = FinancePipeline(base_path=os.path.join(_repo_root, "Data"))

    # _safe_read_file 호출을 가로채 실제로 어떤 파일이 읽히는지 출력으로 확인
    original_read = pipeline._safe_read_file
    def mock_read(path):
        print(f"Reading {path}")
        return original_read(path)

    pipeline._safe_read_file = mock_read
    pipeline._upsert_a1("KOSPI")
