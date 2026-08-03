"""
Database/queries/*.sql 회귀 테스트.

psycopg2는 SQL 주석 안에 있는 %s 도 실제 파라미터 자리표시자로 인식해 버린다.
(006_select_stock_history.sql의 사람이 읽는 주석에 "%s"라는 문구가 들어 있어서
 실제 자리표시자 개수와 어긋나 "tuple index out of range" 오류가 발생했던 사례가 있다.)
이 테스트는 SQL 주석 줄에 %s 표기가 다시 섞여 들어가는 것을 방지한다.
"""
import glob
import os

_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_QUERY_DIR = os.path.join(_repo_root, "Database", "queries")


def test_no_percent_s_placeholder_inside_sql_comments():
    sql_files = sorted(glob.glob(os.path.join(_QUERY_DIR, "*.sql")))
    assert sql_files, f"쿼리 디렉토리를 찾을 수 없습니다: {_QUERY_DIR}"

    offenders = []
    for path in sql_files:
        with open(path, "r", encoding="utf-8") as f:
            for lineno, line in enumerate(f, start=1):
                stripped = line.strip()
                if stripped.startswith("--") and "%s" in stripped:
                    offenders.append(f"{os.path.basename(path)}:{lineno}: {stripped}")

    assert not offenders, (
        "SQL 주석에 %s 자리표시자 표기가 포함되어 있습니다 (psycopg2가 실제 파라미터로 오인함): \n"
        + "\n".join(offenders)
    )
