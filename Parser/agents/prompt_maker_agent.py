# 종목을 검색하면 프롬프트를 만드는 프롬프트 생성 에이전트 (Prompt Maker Agent)
import json

class PromptMakerAgent:
    def __init__(self, db_manager):
        self.db = db_manager

    def generate_prompt(self, symbol: str) -> str:
        """
        특정 종목을 검색했을 때, 데이터베이스에서 메타데이터를 조회하여
        해당 종목을 가장 잘 분석할 수 있는 맞춤형 프롬프트를 생성합니다.
        """
        # 1. DB에서 해당 종목 정보 가져오기 (예외 처리 포함)
        # 하드코딩된 쿼리를 삭제하고 003_select_prompt_data.sql을 이용
        try:
            records = self.db.read_query_direct("003_select_prompt_data.sql", params=(symbol,))
        except Exception as e:
            return f"시스템 에러: 데이터를 불러올 수 없습니다. ({e})"
        
        if not records:
            return f"'{symbol}' 종목에 대한 데이터를 데이터베이스에서 찾을 수 없습니다. (종목 코드를 확인하세요)"
        
        # 2. 메타데이터 조립
        latest = records[0]
        recent_trend_data = json.dumps([{
            "date": str(r[1]),
            "close": float(r[5]) if r[5] else None,
            "volume": float(r[6]) if r[6] else None
        } for r in records])

        # 3. 메타-프롬프트 구성
        meta_prompt = f"""당신은 월스트리트 최고 수준의 시계열 데이터 분석가이자 퀀트 트레이더입니다.
현재 '{symbol}' 종목에 대한 최근 5일치 데이터를 기반으로 향후 추세를 예측해야 합니다.

[종목 최근 데이터 요약 (가장 최신순)]
{recent_trend_data}

[지시사항]
1. 최근 5일간의 가격 흐름과 거래량(volume) 변화를 바탕으로 매수/매도 압력을 분석하세요.
2. 위 데이터에서 가격의 상승/하락 추세(Divergence)나 변동성(Volatility)을 찾아내어 간결하게 요약하세요.
3. 분석 결과를 바탕으로, 향후 단기(3~5일) 주가 흐름에 대한 예측과 함께, 사용자에게 제시할 핵심 인사이트 3가지를 정리하세요.

결과는 반드시 'JSON' 형식(insight, prediction, trend 속성 포함)으로 반환하세요.
"""
        return meta_prompt
