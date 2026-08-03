"""
WAS/app.py의 정적 파일 서빙 라우트가 Web/ 디렉토리 밖으로 벗어나지 못하도록 막는지에 대한 회귀 테스트.
DB/Redis 연결 여부와 무관하게 동작해야 하므로 라우트 함수를 직접 호출한다 (HTTP 계층을 거치지 않음).
"""
import os
import sys

_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _repo_root)
sys.path.insert(0, os.path.join(_repo_root, "WAS"))
sys.path.insert(0, os.path.join(_repo_root, "Database"))
sys.path.insert(0, os.path.join(_repo_root, "Manager"))

from app import read_spa_fallback, web_dir


def test_catchall_blocks_path_traversal_outside_web_dir():
    resp = read_spa_fallback("../Database/db_manager.py")
    assert resp.path == os.path.join(web_dir, "index.html")


def test_catchall_blocks_nested_path_traversal():
    resp = read_spa_fallback("css/../../Database/db_manager.py")
    assert resp.path == os.path.join(web_dir, "index.html")


def test_catchall_serves_known_static_file():
    resp = read_spa_fallback("status.html")
    assert resp.path == os.path.join(web_dir, "status.html")


def test_catchall_falls_back_to_index_for_unknown_path():
    resp = read_spa_fallback("this-page-does-not-exist")
    assert resp.path == os.path.join(web_dir, "index.html")
