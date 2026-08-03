"""
Instagram 자동 업로드 스크립트
- KOSPI, KOSDAQ, NASDAQ, NYSE의 동일 날짜 sanky.webp 파일을 Instagram에 업로드
- Meta Instagram Graph API 사용
- 사용 방법: python instagram_auto_upload.py --date 2026-06-05
"""

import os
import sys
import glob
import time
import argparse
import requests
from datetime import datetime, date
from pathlib import Path
import mimetypes
import base64
from PIL import Image
import io
import json
import logging

# ─────────────────────────────────────────────
# 로깅 설정
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("instagram_upload.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# 설정 (환경변수 또는 직접 입력)
# ─────────────────────────────────────────────
class Config:
    # ⚠️ 아래 값들을 본인의 Meta 개발자 계정 정보로 교체하세요.
    # 환경변수로 관리하는 것을 권장합니다.
    INSTAGRAM_USER_ID = os.environ.get("INSTAGRAM_USER_ID", "YOUR_INSTAGRAM_USER_ID")
    ACCESS_TOKEN = os.environ.get("INSTAGRAM_ACCESS_TOKEN", "YOUR_ACCESS_TOKEN")

    # 파일 경로 설정
    BASE_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) / "Data"
    MARKETS = ["KOSPI", "KOSDAQ", "NASDAQ", "NYSE"]
    SUB_DIR = "B2Sheet"
    FILE_NAME = "sanky.webp"

    # Instagram Graph API
    API_VERSION = "v21.0"
    BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

    # 이미지 임시 저장 경로 (업로드용 PNG 변환)
    TEMP_DIR = Path("./temp_upload")

    # 업로드 간 대기 시간 (초) - Instagram API rate limit 방지
    UPLOAD_DELAY = 5


# ─────────────────────────────────────────────
# 이미지 유틸리티
# ─────────────────────────────────────────────

def webp_to_jpeg(webp_path: Path, output_dir: Path) -> Path:
    """
    Instagram은 JPEG/PNG만 허용하므로 WEBP → JPEG 변환
    최소 320x320, 최대 1440px (종횡비 4:5 ~ 1.91:1 권장)
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / (webp_path.stem + ".jpg")

    with Image.open(webp_path) as img:
        # RGBA → RGB 변환 (JPEG는 투명도 미지원)
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")

        # 최소 크기 보장 (320x320)
        w, h = img.size
        if w < 320 or h < 320:
            scale = max(320 / w, 320 / h)
            img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)

        img.save(output_path, "JPEG", quality=95)
        logger.info(f"  변환 완료: {webp_path.name} → {output_path.name} ({img.size})")

    return output_path


# ─────────────────────────────────────────────
# Instagram Graph API 클라이언트
# ─────────────────────────────────────────────

class InstagramUploader:
    def __init__(self, user_id: str, access_token: str):
        self.user_id = user_id
        self.access_token = access_token
        self.base_url = Config.BASE_URL

    def _get(self, endpoint: str, params: dict = None) -> dict:
        params = params or {}
        params["access_token"] = self.access_token
        url = f"{self.base_url}/{endpoint}"
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _post(self, endpoint: str, data: dict = None) -> dict:
        data = data or {}
        data["access_token"] = self.access_token
        url = f"{self.base_url}/{endpoint}"
        resp = requests.post(url, data=data, timeout=60)
        resp.raise_for_status()
        return resp.json()

    # ── 1단계: 미디어 컨테이너 생성 ──────────────────────────────────────────
    def create_media_container(
        self,
        image_url: str,
        caption: str,
        is_carousel_item: bool = False,
    ) -> str:
        """
        이미지 URL을 사용해 미디어 컨테이너 생성 후 creation_id 반환.
        image_url은 공개적으로 접근 가능한 HTTPS URL이어야 합니다.
        """
        endpoint = f"{self.user_id}/media"
        payload = {
            "image_url": image_url,
            "caption": caption,
        }
        if is_carousel_item:
            payload["is_carousel_item"] = "true"

        result = self._post(endpoint, payload)
        creation_id = result.get("id")
        if not creation_id:
            raise ValueError(f"미디어 컨테이너 생성 실패: {result}")
        logger.info(f"  컨테이너 생성 완료 (creation_id={creation_id})")
        return creation_id

    def create_carousel_container(self, children_ids: list[str], caption: str) -> str:
        """캐러셀(슬라이드) 컨테이너 생성"""
        endpoint = f"{self.user_id}/media"
        payload = {
            "media_type": "CAROUSEL",
            "children": ",".join(children_ids),
            "caption": caption,
        }
        result = self._post(endpoint, payload)
        creation_id = result.get("id")
        if not creation_id:
            raise ValueError(f"캐러셀 컨테이너 생성 실패: {result}")
        logger.info(f"  캐러셀 컨테이너 생성 완료 (creation_id={creation_id})")
        return creation_id

    # ── 2단계: 컨테이너 상태 확인 ────────────────────────────────────────────
    def wait_for_container_ready(
        self, creation_id: str, max_retries: int = 10, wait_sec: int = 5
    ) -> bool:
        """FINISHED 상태가 될 때까지 폴링"""
        for attempt in range(1, max_retries + 1):
            result = self._get(creation_id, {"fields": "status_code,status"})
            status = result.get("status_code", "")
            logger.info(f"  컨테이너 상태 확인 ({attempt}/{max_retries}): {status}")
            if status == "FINISHED":
                return True
            if status in ("ERROR", "EXPIRED"):
                logger.error(f"  컨테이너 오류: {result}")
                return False
            time.sleep(wait_sec)
        logger.error("  컨테이너 준비 타임아웃")
        return False

    # ── 3단계: 게시 ─────────────────────────────────────────────────────────
    def publish_container(self, creation_id: str) -> str:
        """준비된 컨테이너를 실제 게시"""
        endpoint = f"{self.user_id}/media_publish"
        result = self._post(endpoint, {"creation_id": creation_id})
        media_id = result.get("id")
        if not media_id:
            raise ValueError(f"게시 실패: {result}")
        logger.info(f"  게시 완료 (media_id={media_id})")
        return media_id

    # ── 단일 이미지 업로드 (편의 래퍼) ──────────────────────────────────────
    def upload_single_image(self, image_url: str, caption: str) -> str:
        logger.info(f"  단일 이미지 업로드 시작...")
        creation_id = self.create_media_container(image_url, caption)
        if not self.wait_for_container_ready(creation_id):
            raise RuntimeError("컨테이너 준비 실패")
        return self.publish_container(creation_id)

    # ── 캐러셀 업로드 (편의 래퍼) ────────────────────────────────────────────
    def upload_carousel(self, image_urls: list[str], caption: str) -> str:
        logger.info(f"  캐러셀 업로드 시작 ({len(image_urls)}장)...")

        # 각 이미지의 carousel_item 컨테이너 생성
        children_ids = []
        for i, url in enumerate(image_urls, 1):
            logger.info(f"  슬라이드 {i}/{len(image_urls)}: {url[:60]}...")
            cid = self.create_media_container(url, caption="", is_carousel_item=True)
            children_ids.append(cid)
            time.sleep(1)

        # 캐러셀 컨테이너 생성
        carousel_id = self.create_carousel_container(children_ids, caption)
        if not self.wait_for_container_ready(carousel_id):
            raise RuntimeError("캐러셀 컨테이너 준비 실패")

        return self.publish_container(carousel_id)

    def check_rate_limit(self) -> dict:
        """남은 API 호출 횟수 확인"""
        result = self._get(f"{self.user_id}", {"fields": "name,username"})
        return result


# ─────────────────────────────────────────────
# 이미지 호스팅 (로컬 → 공개 URL)
# imgbb 무료 호스팅 사용 (API 키 필요)
# 또는 ngrok / S3 / GitHub Raw 등 대체 가능
# ─────────────────────────────────────────────

class ImageHostingService:
    """
    Instagram API는 공개 HTTPS URL로 이미지를 받아야 합니다.
    로컬 파일을 임시로 imgbb에 업로드하여 URL을 획득합니다.

    대안:
    - AWS S3 사전 서명 URL
    - GitHub Raw (파일을 레포에 push 후 raw URL 사용)
    - ngrok으로 로컬 서버 터널링
    - Cloudinary 무료 플랜
    """

    IMGBB_API_KEY = os.environ.get("IMGBB_API_KEY", "YOUR_IMGBB_API_KEY")
    IMGBB_UPLOAD_URL = "https://api.imgbb.com/1/upload"

    @classmethod
    def upload_to_imgbb(cls, image_path: Path) -> str:
        """imgbb에 이미지 업로드 후 URL 반환"""
        with open(image_path, "rb") as f:
            image_data = base64.b64encode(f.read()).decode("utf-8")

        payload = {
            "key": cls.IMGBB_API_KEY,
            "image": image_data,
            "expiration": 600,  # 10분 후 만료 (업로드 완료 후 필요 없음)
        }
        resp = requests.post(cls.IMGBB_UPLOAD_URL, data=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if not data.get("success"):
            raise ValueError(f"imgbb 업로드 실패: {data}")

        url = data["data"]["url"]
        logger.info(f"  imgbb 업로드 완료: {url[:60]}...")
        return url


# ─────────────────────────────────────────────
# 파일 탐색
# ─────────────────────────────────────────────

def find_sanky_files(target_date: str) -> dict[str, Path]:
    """
    target_date 형식: YYYY-MM-DD (예: 2026-06-05)
    반환: {market_name: Path} — 파일이 없는 마켓은 포함되지 않음
    """
    found = {}
    for market in Config.MARKETS:
        path = (
            Config.BASE_DIR
            / market
            / Config.SUB_DIR
            / target_date
            / Config.FILE_NAME
        )
        if path.exists():
            found[market] = path
            logger.info(f"  ✅ 발견: {path}")
        else:
            logger.warning(f"  ❌ 없음: {path}")
    return found


# ─────────────────────────────────────────────
# 캡션 생성
# ─────────────────────────────────────────────

def build_caption(markets: list[str], target_date: str) -> str:
    """게시물 캡션 자동 생성"""
    date_obj = datetime.strptime(target_date, "%Y-%m-%d")
    date_str = date_obj.strftime("%Y년 %m월 %d일")
    market_str = " · ".join(markets)

    caption = f"""📊 {date_str} 글로벌 시장 Sankey 차트

{market_str}

각 시장의 자금 흐름을 한눈에 확인하세요.
섹터별 매수/매도 동향을 Sankey 다이어그램으로 시각화했습니다.

#주식 #증시 #KOSPI #KOSDAQ #NASDAQ #NYSE
#글로벌시장 #투자 #주식차트 #Sankey #FinanceMLOps
#시장분석 #자금흐름 #{date_obj.strftime('%Y%m%d')}"""

    return caption


# ─────────────────────────────────────────────
# 메인 실행 흐름
# ─────────────────────────────────────────────

def run(target_date: str, dry_run: bool = False, upload_mode: str = "carousel"):
    """
    Parameters
    ----------
    target_date : str
        업로드할 날짜 (YYYY-MM-DD)
    dry_run : bool
        True이면 실제 업로드 없이 파일 탐색만 수행
    upload_mode : str
        'carousel' — 4장을 하나의 슬라이드 게시물로
        'single'   — 각 마켓을 별도 게시물로 (4회 업로드)
    """
    logger.info("=" * 60)
    logger.info(f"Instagram 자동 업로드 시작")
    logger.info(f"  대상 날짜 : {target_date}")
    logger.info(f"  업로드 모드: {upload_mode}")
    logger.info(f"  Dry run   : {dry_run}")
    logger.info("=" * 60)

    # ── 파일 탐색 ───────────────────────────────────────────────────────────
    logger.info("\n[1/5] sanky.webp 파일 탐색...")
    sanky_files = find_sanky_files(target_date)

    if not sanky_files:
        logger.error("업로드할 파일이 하나도 없습니다. 종료합니다.")
        sys.exit(1)

    logger.info(f"  발견된 마켓: {list(sanky_files.keys())}")

    if dry_run:
        logger.info("\n[Dry run] 실제 업로드는 건너뜁니다.")
        return

    # ── WEBP → JPEG 변환 ────────────────────────────────────────────────────
    logger.info("\n[2/5] WEBP → JPEG 변환...")
    Config.TEMP_DIR.mkdir(parents=True, exist_ok=True)
    jpeg_files: dict[str, Path] = {}
    for market, webp_path in sanky_files.items():
        logger.info(f"  {market}: {webp_path.name}")
        jpeg_files[market] = webp_to_jpeg(webp_path, Config.TEMP_DIR)

    # ── 이미지 호스팅 업로드 ────────────────────────────────────────────────
    logger.info("\n[3/5] 이미지 호스팅 업로드 (imgbb)...")
    image_urls: dict[str, str] = {}
    for market, jpeg_path in jpeg_files.items():
        logger.info(f"  {market}: {jpeg_path.name}")
        image_urls[market] = ImageHostingService.upload_to_imgbb(jpeg_path)
        time.sleep(1)

    # ── Instagram 업로드 ────────────────────────────────────────────────────
    logger.info("\n[4/5] Instagram Graph API 업로드...")
    uploader = InstagramUploader(
        user_id=Config.INSTAGRAM_USER_ID,
        access_token=Config.ACCESS_TOKEN,
    )

    markets_uploaded = list(image_urls.keys())
    caption = build_caption(markets_uploaded, target_date)
    logger.info(f"\n  캡션 미리보기:\n{'─'*40}\n{caption}\n{'─'*40}\n")

    if upload_mode == "carousel" and len(image_urls) > 1:
        # 캐러셀: 최대 10장, 최소 2장 필요
        urls_ordered = [image_urls[m] for m in Config.MARKETS if m in image_urls]
        media_id = uploader.upload_carousel(urls_ordered, caption)
        logger.info(f"\n  ✅ 캐러셀 게시 완료 (media_id={media_id})")

    else:
        # 단일 이미지 모드 (각 마켓별 개별 게시)
        for i, (market, url) in enumerate(image_urls.items(), 1):
            logger.info(f"\n  [{i}/{len(image_urls)}] {market} 업로드...")
            single_caption = build_caption([market], target_date)
            media_id = uploader.upload_single_image(url, single_caption)
            logger.info(f"  ✅ {market} 게시 완료 (media_id={media_id})")
            if i < len(image_urls):
                logger.info(f"  {Config.UPLOAD_DELAY}초 대기 중...")
                time.sleep(Config.UPLOAD_DELAY)

    # ── 임시 파일 정리 ──────────────────────────────────────────────────────
    logger.info("\n[5/5] 임시 파일 정리...")
    for jpeg_path in jpeg_files.values():
        jpeg_path.unlink(missing_ok=True)
    try:
        Config.TEMP_DIR.rmdir()
    except OSError:
        pass
    logger.info("  정리 완료")

    logger.info("\n" + "=" * 60)
    logger.info("✅ 모든 작업 완료!")
    logger.info("=" * 60)


# ─────────────────────────────────────────────
# CLI 진입점
# ─────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Instagram에 Sankey 차트 자동 업로드"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=date.today().strftime("%Y-%m-%d"),
        help="업로드할 날짜 (YYYY-MM-DD, 기본값: 오늘)",
    )
    parser.add_argument(
        "--mode",
        choices=["carousel", "single"],
        default="carousel",
        help="업로드 방식 (carousel: 슬라이드, single: 개별 게시)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="파일 탐색만 수행, 실제 업로드 안 함",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        target_date=args.date,
        dry_run=args.dry_run,
        upload_mode=args.mode,
    )