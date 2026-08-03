import os
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta

class SignalSankeyGenerator:
    def __init__(self, base_dir: str):
        """
        초기화 메서드
        :param base_dir: Data 폴더까지의 기본 경로
        """
        self.base_dir = base_dir
        
        # 제공해주신 시그널 카테고리 딕셔너리
        self.SIGNAL_CATEGORIES = {
            "bull": {
                "label": "Bull · 상승 신호",
                "emoji": "🔴",
                "accent": "#D62728",
                "family_desc": "상승(매수) 계열입니다. 한국 증시 관례에 따라 빨간 계열로 표시합니다. (빨강 = 상승)",
                "signals": [
                    {"key": "uptrend",     "label": "매수 신호(Up)",             "file": "df_uptrend.parquet",     "color": "#7F1D1D", "desc": "분석 지표 저점이 높아졌어요 · 짙은 빨강"},
                    {"key": "bull_div",    "label": "상승으로 추세 전환(Bull Div.)", "file": "df_bull.parquet",      "color": "#D81B60", "desc": "힘들었던 구간을 지날 것 같아요 · 자홍(핑크 레드)"},
                    {"key": "bull_hidden", "label": "상승 추세 한번 더(H.Bull)",   "file": "df_bull_hidden.parquet", "color": "#FCA5A5", "desc": "한번 더 상승할 일만 남았어요 · 연한 빨강"},
                ],
            },
            "bear": {
                "label": "Bear · 하락 신호",
                "emoji": "🔵",
                "accent": "#1D4ED8",
                "family_desc": "하락(매도) 계열입니다. 한국 증시 관례에 따라 파란 계열로 표시합니다. (파랑 = 하락)",
                "signals": [
                    {"key": "sell",        "label": "매도 신호(Sell)",             "file": "df_sell.parquet",        "color": "#1D4ED8", "desc": "일반 지표들이 모두 과열이에요 · 선명한 파랑"},
                    {"key": "downtrend",   "label": "매도 신호(Down)",             "file": "df_downtrend.parquet",   "color": "#0F172A", "desc": "분석 지표 고점이 낮아졌어요 · 짙은 남색"},
                    {"key": "bear_div",    "label": "하락으로 추세 전환(Bear Div.)", "file": "df_bear.parquet",      "color": "#5C6BC0", "desc": "가격 조정이 필요한 것 같아요 · 인디고"},
                    {"key": "bear_hidden", "label": "하락 추세 한번 더(H.Bear)",    "file": "df_bear_hidden.parquet", "color": "#93C5FD", "desc": "한번 더 하락할 것 같아요 · 연한 파랑"},
                ],
            },
            "overbought_oversold": {
                "label": "과매수 · 과매도",
                "emoji": "🟣",
                "accent": "#7C3AED",
                "family_desc": "과열/소외 관망 신호입니다. 과매수(상단)는 파랑·시안 계열, 과매도(하단)는 빨강·주황 계열로 구분합니다.",
                "signals": [
                    {"key": "cci_buy",  "label": "CCI 과매수 진입(관망)", "file": "df_over_buy_cci.parquet",  "color": "#3182F6", "desc": "과열 초기 상태 진입 · 파랑"},
                    {"key": "rsi_buy",  "label": "RSI 과매수 진입(관망)", "file": "df_over_buy_rsi.parquet",  "color": "#00B4D8", "desc": "과열 상태 확실 · 시안"},
                    {"key": "cci_sell", "label": "CCI 과매도 진입(관망)", "file": "df_over_sell_cci.parquet", "color": "#F04452", "desc": "소외 초기 상태 진입 · 빨강"},
                    {"key": "rsi_sell", "label": "RSI 과매도 진입(관망)", "file": "df_over_sell_rsi.parquet", "color": "#FF8A65", "desc": "소외 상태 확실 · 주황"},
                ],
            },
        }

    def _get_date_range(self, start_date: str, end_date: str) -> list:
        """시작일과 종료일 사이의 날짜 리스트 생성 (주말 제외)"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        dates = []
        for i in range((end - start).days + 1):
            current_date = start + timedelta(days=i)
            # weekday() 결과: 0(월), 1(화), 2(수), 3(목), 4(금), 5(토), 6(일)
            if current_date.weekday() < 5:  # 주말(5, 6)이 아닌 평일만 추가
                dates.append(current_date.strftime("%Y-%m-%d"))
        return dates

    def _format_korean_date(self, date_str: str) -> str:
        """'2026-06-09' 형태의 문자열을 '2026년 6월 9일 (화)' 형태로 변환"""
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        weekdays = ["월", "화", "수", "목", "금", "토", "일"]
        day_of_week = weekdays[dt.weekday()]
        return f"{dt.year}년 {dt.month}월 {dt.day}일 ({day_of_week})"

    def _hex_to_rgba(self, hex_color: str, alpha: float = 0.4) -> str:
        """Hex 색상 코드를 rgba 문자열로 변환 (Sankey Link의 투명도 조절용)"""
        hex_color = hex_color.lstrip('#')
        if len(hex_color) == 6:
            r, g, b = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
            return f"rgba({r}, {g}, {b}, {alpha})"
        return hex_color

    def process_date(self, market: str, date_str: str):
        """특정 날짜와 시장의 데이터를 읽어 Sankey 다이어그램을 생성 및 저장"""
        input_dir = os.path.join(self.base_dir, market, "B1Sheet", date_str)
        output_dir = os.path.join(self.base_dir, market, "B2Sheet", date_str)
        
        # 입력 디렉토리가 없으면 해당 날짜 건너뜀 (공휴일 등)
        if not os.path.exists(input_dir):
            print(f"[{date_str} | {market}] ⚠️ 폴더가 존재하지 않습니다 (휴장일 추정): {input_dir}")
            return

        # WEBP 및 JPG 두 가지 확장자의 출력 경로 정의
        output_file_webp = os.path.join(output_dir, "sanky.webp")
        output_file_jpg = os.path.join(output_dir, "sanky.jpg")

        signal_counts = {}
        category_counts = {}
        total_signals = 0

        # 1. 파일 읽어서 개수(Row 카운트) 세기
        for cat_key, cat_info in self.SIGNAL_CATEGORIES.items():
            cat_sum = 0
            for sig in cat_info["signals"]:
                file_path = os.path.join(input_dir, sig["file"])
                count = 0
                if os.path.exists(file_path):
                    try:
                        df = pd.read_parquet(file_path)
                        count = len(df)
                    except Exception as e:
                        print(f"[{date_str} | {market}] 파일 읽기 오류 ({sig['file']}): {e}")
                
                signal_counts[sig["key"]] = count
                cat_sum += count
            
            category_counts[cat_key] = cat_sum
            total_signals += cat_sum

        # 발생한 시그널이 전혀 없다면 다이어그램 생성 안함 (폴더 생성 방지)
        if total_signals == 0:
            print(f"[{date_str} | {market}] ℹ️ 해당 날짜에 시그널 데이터가 0개이므로 차트를 생성하지 않습니다.")
            return

        # 2. Sankey에 들어갈 Node(노드)와 Link(연결선) 구성
        node_labels = [f"<b>전체 시그널<br>({total_signals}건, 100%)</b>"]
        node_colors = ["#9CA3AF"] 
        
        source = []
        target = []
        value = []
        link_colors = []

        cat_idx_map = {}
        
        # 2-1. Total -> Category 연결
        for cat_key, cat_info in self.SIGNAL_CATEGORIES.items():
            if category_counts[cat_key] > 0:
                idx = len(node_labels)
                pct = (category_counts[cat_key] / total_signals) * 100
                node_labels.append(f"<b>{cat_info['emoji']} {cat_info['label']}<br>({category_counts[cat_key]}건, {pct:.1f}%)</b>")
                node_colors.append(cat_info["accent"])
                cat_idx_map[cat_key] = idx
                
                source.append(0)  
                target.append(idx) 
                value.append(category_counts[cat_key])
                link_colors.append(self._hex_to_rgba(cat_info["accent"], 0.3))

        # 2-2. Category -> Specific Signal 연결
        for cat_key, cat_info in self.SIGNAL_CATEGORIES.items():
            if category_counts[cat_key] == 0:
                continue
            
            current_cat_idx = cat_idx_map[cat_key]
            for sig in cat_info["signals"]:
                count = signal_counts[sig["key"]]
                if count > 0:
                    idx = len(node_labels)
                    pct = (count / total_signals) * 100
                    node_labels.append(f"<b>{sig['label']}<br>({count}건, {pct:.1f}%)</b>")
                    node_colors.append(sig["color"])
                    
                    source.append(current_cat_idx)
                    target.append(idx)
                    value.append(count)
                    link_colors.append(self._hex_to_rgba(sig["color"], 0.4))

        # 3. Plotly Figure 생성
        fig = go.Figure(data=[go.Sankey(
            # x 도메인을 확장하여 양옆 여백 대폭 줄임
            domain=dict(x=[0.05, 0.95], y=[0.05, 0.95]), 
            node=dict(
                pad=65,          
                thickness=40,    
                line=dict(color="black", width=0.5),
                label=node_labels,
                color=node_colors
            ),
            link=dict(
                source=source,
                target=target,
                value=value,
                color=link_colors
            )
        )])

        korean_date_str = self._format_korean_date(date_str)

        # 4. 레이아웃 업데이트
        fig.update_layout(
            title_text=f"<b>{korean_date_str} {market} 요약</b>",
            title_font_size=40,
            title_x=0.5, 
            font=dict(
                size=18, 
                family="Pretendard, Malgun Gothic, AppleGothic, sans-serif", 
                color="#000000"
            ),
            # 좌우 기본 margin을 약간 늘려 긴 글씨가 바깥으로 잘리지 않게 방지
            margin=dict(l=40, r=40, t=100, b=40),
            paper_bgcolor='white',
            plot_bgcolor='white'
        )

        # 다이어그램을 최종적으로 그리기 직전에만 출력 폴더를 생성
        os.makedirs(output_dir, exist_ok=True)

        # 5. 이미지로 저장 (webp 및 jpg 둘 다 저장)
        fig.write_image(output_file_webp, width=1200, height=1200, scale=3.0)
        fig.write_image(output_file_jpg, width=1200, height=1200, scale=3.0)
        print(f"[{date_str} | {market}] ✅ 생키 다이어그램 저장 완료 -> {output_dir}")

    def run(self, markets: list, start_date: str, end_date: str):
        """지정된 기간 동안 모든 시장의 다이어그램을 일괄 생성 (날짜 우선 순회)"""
        dates = self._get_date_range(start_date, end_date)
        print(f"총 {len(dates)}일(주말 제외)의 데이터 처리를 시작합니다. ({start_date} ~ {end_date})")
        
        # 날짜를 기준으로 먼저 루프를 돕니다
        for date_str in dates:
            print(f"\n==================================================")
            print(f"🚀 [{date_str}] 데이터 처리 시작")
            print(f"==================================================")
            
            # 해당 날짜에 대해 각 시장별로 프로세스 진행
            for market in markets:
                self.process_date(market, date_str)
                
        print("\n✨ 모든 시장의 작업이 성공적으로 종료되었습니다!")


# ==========================================
# 실제 실행 코드 예시
# ==========================================
if __name__ == "__main__":
    BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Data")
    START_DATE = "2026-05-01"
    END_DATE = "2026-06-09"
    
    # 여러 시장 리스트 정의
    MARKETS = ["KOSPI", "KOSDAQ", "NASDAQ", "NYSE"]
    
    # 클래스 인스턴스화 (시장 정보 제외)
    sankey_maker = SignalSankeyGenerator(base_dir=BASE_DIR)
    
    # 일괄 실행 (시장 리스트를 함께 전달)
    sankey_maker.run(markets=MARKETS, start_date=START_DATE, end_date=END_DATE)