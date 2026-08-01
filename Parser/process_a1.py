import dask.dataframe as dd
import dask
from dask.distributed import Client, LocalCluster
import pandas as pd
import numpy as np
import os
import re
import ta
import logging
from scipy.signal import argrelextrema
from datetime import datetime


# ======================================================================
# 클러스터 / 파티션 설정
# ----------------------------------------------------------------------
# 환경에 맞춰 아래 값만 조정하세요.
# ======================================================================
CLUSTER_CONFIG = {
    "n_workers": 4,
    "threads_per_worker": 2,
    "memory_limit": "3GB",
    "npartitions": 16,
    "use_persist": False,
}


class DaskFinanceProcessor:
    def __init__(self, config):
        self.input_path = config.get("input_path")
        self.output_path = config.get("output_path")
        self.market_name = config.get("market_name")
        self.input_file = "raw_data.parquet"
        
        # ===== [수정] CSV 저장 디렉터리 설정 =====
        self.save_dir_csv = os.path.join(self.output_path, self.market_name, "A1Sheet")

        # ===== 클러스터 / 파티션 설정 =====
        self.cluster_config = CLUSTER_CONFIG

        # ===== 다이버전스 파라미터 =====
        self.rsi_rollback = 120
        self.rsi_hidden_rollback = 180
        self.rsi_trend_rollback = 120  # 일반 추세 lookback

        # swing 극값 탐색 민감도 (order=1은 너무 노이즈가 많음)
        self.swing_order = 2

        # Regular Divergence: 가격은 비율(%), RSI는 절대 차이(point)
        # 기존 비율 기반 임계값을 RSI 0~100 스케일에 맞춰 환산
        self.up_div_price = [-0.15, 0.0]
        self.up_div_rsi_diff = [3.0, 12.0]      # RSI 상승 폭 (point)
        self.down_div_price = [0.0, 0.20]
        self.down_div_rsi_diff = [-15.0, -1.0]  # RSI 하락 폭 (point)

        # Hidden Divergence
        self.up_hide_price = [0.03, 0.12]
        self.up_hide_rsi_diff = [-10.0, 0.0]   # RSI 하락 폭 (point)
        self.down_hide_price = [-0.12, -0.03]
        self.down_hide_rsi_diff = [0.0, 10.0]   # RSI 상승 폭 (point)

        # Regular Trend (가격과 RSI가 같은 방향)
        # 상승 추세: 가격 저점 ↑, RSI 저점 ↑
        # 하락 추세: 가격 고점 ↓, RSI 고점 ↓
        self.up_trend_price = [0.03, 0.20]      # 가격 상승 폭 (비율)
        self.up_trend_rsi_diff = [5.0, 25.0]    # RSI 상승 폭 (point)
        self.down_trend_price = [-0.20, -0.03]  # 가격 하락 폭 (비율)
        self.down_trend_rsi_diff = [-25.0, -5.0]  # RSI 하락 폭 (point)

        # ===== Sell_Signal 파라미터 =====
        self.sell_signal_config = {
            "adx_threshold": 25,          # 추세 강도 기준
            "dead_cross_diff": -15,       # PDI - MDI 임계값
            "mdi_acceleration_mult": 2.0, # MDI 급등 배수
            "mdi_acceleration_window": 3, # MDI 평균 윈도우
        }

        # ===== Manipulation(스톱 헌트) 매수 신호 파라미터 =====
        # BullDiv 발생 → 다이버전스 저점 아래로 급락(휩쏘) → 회복 시 매수
        self.manip_buy_config = {
            "div_lookback": 20,   # BullDiv 발생 후 신호가 유효한 구간(캔들 수)
            "min_drop": 0.02,     # 다이버전스 저점 대비 최소 급락폭(2%) — 진짜 휩쏘만 인정
            "max_drop": 0.25,     # 추세 붕괴로 간주하는 급락 상한(25%) — 초과 시 매수 금지
            "cooldown": 5,        # 신호 발생 후 재무장 대기(캔들 수) — 중복 매수 방지
        }

    # ------------------------------------------------------------------
    # 지표 계산
    # ------------------------------------------------------------------
    def calculate_indicators(self, data):
        """기술적 지표 계산 및 유동성 사냥(조작) 패턴 필터링"""
        df = data.copy()

        # ----- MA -----
        ma_cols = {f"MA{idx}": df["Close"].rolling(window=int(idx)).mean()
                   for idx in ["1", "2", "3", "4", "5", "6", "7", "8", "9", "20", "50", "60", "90", "120", "200", "224"]}
        df = pd.concat([df, pd.DataFrame(ma_cols, index=df.index)], axis=1)

        all_new_features = {}
        all_new_features['Close_diff_first'] = df['Close'].diff()
        all_new_features['Close_rate_first'] = df['Close'].pct_change() * 100
        
        # ----- CurrencyVolume (거래대금) -----
        all_new_features['CurrencyVolume'] = df['Close'] * df['Volume']

        # ===== [추가] 20일 평균 CurrencyVolume 대비 당일 거래대금 변동률(%) =====
        cv_ma20 = all_new_features['CurrencyVolume'].rolling(window=20).mean()
        all_new_features['CurrencyVolume_Ratio_MA20'] = ((all_new_features['CurrencyVolume'] - cv_ma20) / cv_ma20) * 100

        # ----- OBV -----
        obv_change = np.where(all_new_features['Close_diff_first'] > 0, df['Volume'],
                              np.where(all_new_features['Close_diff_first'] < 0, -df['Volume'], 0))
        all_new_features['OBV'] = pd.Series(obv_change, index=df.index).cumsum()
        all_new_features['MOBV'] = all_new_features['OBV'] * 1e-6
        all_new_features['DeltaMOBV'] = all_new_features['MOBV'].diff()

        # ----- RSI + Divergence + Trend -----
        rsi_base = ta.momentum.RSIIndicator(df["Close"], window=14).rsi()
        all_new_features["RSI"] = rsi_base
        for r_idx in ["", "2", "3", "4", "5", "6", "7", "8", "9"]:
            # r_idx=""는 원본 RSI, 그 외는 RSI의 r_idx일 이동평균
            target_rsi = rsi_base if r_idx == "" else rsi_base.rolling(window=int(r_idx)).mean()
            if r_idx != "":
                all_new_features[f"RSI{r_idx}"] = target_rsi
            all_new_features[f"RSI_Signal{r_idx}"] = np.where(
                target_rsi >= 70, 1, np.where(target_rsi <= 30, -1, 0)
            )
            # 일반 다이버전스
            bull, bear = self.divergence_optimized(df["MA5"], target_rsi, self.rsi_rollback)
            all_new_features[f"RSI_BullDiv{r_idx}"] = bull
            all_new_features[f"RSI_BearDiv{r_idx}"] = bear
            # 히든 다이버전스
            h_bull, h_bear = self.hidden_divergence_optimized(df["MA5"], target_rsi, self.rsi_hidden_rollback)
            all_new_features[f"RSI_Hidden_BullDiv{r_idx}"] = h_bull
            all_new_features[f"RSI_Hidden_BearDiv{r_idx}"] = h_bear
            # 일반 상승 추세
            up_trend, down_trend = self.trend_optimized(df["MA5"], target_rsi, self.rsi_trend_rollback)
            all_new_features[f"RSI_UpTrend{r_idx}"] = up_trend
            all_new_features[f"RSI_DownTrend{r_idx}"] = down_trend
        
        # 신호의 합 컬럼
        r_indices = ["", "2", "3", "4", "5", "6", "7", "8", "9"]
        # 1. RSI_Signal_Sum 계산
        all_new_features["RSI_Signal_Sum"] = sum(
            all_new_features[f"RSI_Signal{idx}"] 
            for idx in r_indices 
            if f"RSI_Signal{idx}" in all_new_features
        )
        
        # 2. RSI_BullDiv_Sum 계산
        all_new_features["RSI_BullDiv_Sum"] = sum(
            all_new_features[f"RSI_BullDiv{idx}"] 
            for idx in r_indices 
            if f"RSI_BullDiv{idx}" in all_new_features
        )
        
        # 3. RSI_BearDiv_Sum 계산
        all_new_features["RSI_BearDiv_Sum"] = sum(
            all_new_features[f"RSI_BearDiv{idx}"] 
            for idx in r_indices 
            if f"RSI_BearDiv{idx}" in all_new_features
        )
        
        # 4. RSI_Hidden_BullDiv_Sum 계산
        all_new_features["RSI_Hidden_BullDiv_Sum"] = sum(
            all_new_features[f"RSI_Hidden_BullDiv{idx}"] 
            for idx in r_indices 
            if f"RSI_Hidden_BullDiv{idx}" in all_new_features
        )
        
        # 5. RSI_Hidden_BearDiv_Sum 계산
        all_new_features["RSI_Hidden_BearDiv_Sum"] = sum(
            all_new_features[f"RSI_Hidden_BearDiv{idx}"] 
            for idx in r_indices 
            if f"RSI_Hidden_BearDiv{idx}" in all_new_features
        )
        
        # 6. RSI_UpTrend_Sum 계산
        all_new_features["RSI_UpTrend_Sum"] = sum(
            all_new_features[f"RSI_UpTrend{idx}"] 
            for idx in r_indices 
            if f"RSI_UpTrend{idx}" in all_new_features
        )
        
        # 7. RSI_DownTrend_Sum 계산
        all_new_features["RSI_DownTrend_Sum"] = sum(
            all_new_features[f"RSI_DownTrend{idx}"] 
            for idx in r_indices 
            if f"RSI_DownTrend{idx}" in all_new_features
        )

        # ----- Bollinger Band -----
        # all_new_features에 모아 마지막에 한 번에 concat
        all_new_features['STD20'] = df['Close'].rolling(window=20).std()
        all_new_features['BB_Upper'] = df['MA20'] + (2 * all_new_features['STD20'])
        all_new_features['BB_Lower'] = df['MA20'] - (2 * all_new_features['STD20'])
        all_new_features['BB_width'] = (
            (all_new_features['BB_Upper'] - all_new_features['BB_Lower']) / df["MA20"]
        )

        # ----- CCI -----
        cci_base = ta.trend.CCIIndicator(
            high=df["High"], low=df["Low"], close=df["Close"], window=20
        ).cci()
        all_new_features["CCI"] = cci_base
        for r_idx in ["", "2", "3", "4", "5", "6", "7", "8", "9"]:
            target_cci = cci_base if r_idx == "" else cci_base.rolling(window=int(r_idx)).mean()
            if r_idx != "":
                all_new_features[f"CCI{r_idx}"] = target_cci
            all_new_features[f"CCI_Signal{r_idx}"] = np.where(
                target_cci >= 100, 1, np.where(target_cci <= -100, -1, 0)
            )

         # 8. CCI_Signal_Sum 계산
        all_new_features["CCI_Signal_Sum"] = sum(
            all_new_features[f"CCI_Signal{idx}"] 
            for idx in r_indices 
            if f"CCI_Signal{idx}" in all_new_features
        )

        # ----- MACD -----
        macd = ta.trend.MACD(close=df["Close"], window_slow=26, window_fast=12, window_sign=9)
        all_new_features["MACD"]          = macd.macd()
        all_new_features["MACD_Base"]     = macd.macd_signal()
        all_new_features["MACD_Hist"]     = macd.macd_diff()
        # ----- MACD Histogram Acceleration -----
        all_new_features["MACD_Hist_Vel"] = all_new_features["MACD_Hist"].diff(1)
        acc_pct = all_new_features["MACD_Hist_Vel"].pct_change(1) * 100
        acc_pct = acc_pct.replace([np.inf, -np.inf], np.nan).fillna(0)
        all_new_features["MACD_Hist_Acc_Pct"] = acc_pct
        all_new_features["MACD_Positive"] = np.where(macd.macd() > 0, 1, -1)
        all_new_features["MACD_Signal"]   = np.where(
            macd.macd() > macd.macd_signal(), 1,
            np.where(macd.macd() < macd.macd_signal(), -1, 0)
        )

        # ----- ADX / PDI / MDI -----
        try:
            ind = ta.trend.ADXIndicator(high=df["High"], low=df["Low"], close=df["Close"], window=14)
            all_new_features["PDI"] = ind.adx_pos()
            all_new_features["MDI"] = ind.adx_neg()
            all_new_features["ADX"] = ind.adx()
        except IndexError:
            # 결측치(NaN) 등으로 인해 유효 길이가 window(14)보다 짧을 경우 발생하는 에러 방지
            all_new_features["PDI"] = pd.Series(np.nan, index=df.index)
            all_new_features["MDI"] = pd.Series(np.nan, index=df.index)
            all_new_features["ADX"] = pd.Series(np.nan, index=df.index)

        # ----- Momentum -----
        # 단기 모멘텀
        all_new_features["MOM10"] = (df["Close"] / df["Close"].shift(10)) * 100
        all_new_features["MOM14"] = (df["Close"] / df["Close"].shift(14)) * 100

        # 중기 모멘텀
        all_new_features["MOM25"] = (df["Close"] / df["Close"].shift(25)) * 100
        all_new_features["MOM28"] = (df["Close"] / df["Close"].shift(28)) * 100

        # 단기 시그널
        all_new_features["MOM10_Signal"] = np.where(all_new_features["MOM10"] > 100, 1, 0)
        all_new_features["MOM14_Signal"] = np.where(all_new_features["MOM14"] > 100, 1, 0)

        # 중기 시그널
        all_new_features["MOM25_Signal"] = np.where(all_new_features["MOM25"] > 100, 1, 0)
        all_new_features["MOM28_Signal"] = np.where(all_new_features["MOM28"] > 100, 1, 0)

        # ----- Sell_Signal -----
        cfg = self.sell_signal_config
        # rs, cs = cfg["rsi_suffix"], cfg["cci_suffix"]

        # 1. 상승 추세 유지 조건: 상승 에너지(PDI)가 하락 에너지(MDI)보다 크고 ADX가 임계 이상
        trend_positive = (
            (all_new_features["PDI"] > all_new_features["MDI"]) &
            (all_new_features["ADX"] > cfg["adx_threshold"])
        )

        # 2. 광기 구간 필터: ADX 과열 중 MDI가 고개를 들기 시작할 때
        extreme_filter = (all_new_features["MDI"] > all_new_features["MDI"].shift(1))

        # 3. 경로 A: 과열 + 하락 에너지 태동
        complex_condition = (
            (all_new_features["MACD_Positive"] == 1) &      # MACD 0선 위
            (all_new_features["MACD_Signal"] == 1) &        # MACD 골든크로스 유지
            (all_new_features[f"RSI_Signal_Sum"] > 0) &    # RSI 과매수
            (all_new_features[f"CCI_Signal_Sum"] > 0) &    # CCI 과매수
            trend_positive &                                # 상승장 내에서만 작동
            extreme_filter                                  # 광기 필터 통과
        )

        # 4. 경로 B: 데드크로스 신호
        mdi_mean = all_new_features["MDI"].rolling(window=cfg["mdi_acceleration_window"]).mean()
        mdi_acceleration = all_new_features["MDI"] > (mdi_mean * cfg["mdi_acceleration_mult"])
        dead_cross = (
            (all_new_features["ADX"] > cfg["adx_threshold"]) &
            (all_new_features["PDI"] - all_new_features["MDI"] < cfg["dead_cross_diff"]) &
            mdi_acceleration
        )

        all_new_features['High_watermark'] = df['Close'].cummax()
        all_new_features['MDD'] = 100 * (df['Close'] - all_new_features['High_watermark']) / all_new_features['High_watermark']

        # 5. 최종 매도 신호: 경로 A OR 경로 B
        all_new_features["Sell_Signal"] = np.where(
            complex_condition | dead_cross, 1, 0
        )

        # ==================================================================
        # [추가] 유동성 사냥(Liquidity Sweep / Stop Hunt) 탐지 및 스마트 매수 신호
        # ==================================================================
        
        # 1. 캔들 꼬리(Wick) 분석: 하단 꼬리가 몸통보다 2배 이상 긴 핀바(Pinbar) 캔들 판별
        body = abs(df['Open'] - df['Close'])
        lower_wick = df[['Open', 'Close']].min(axis=1) - df['Low']
        # 도지(Doji) 캔들 방어를 위해 아주 작은 상수(1e-5) 추가
        all_new_features['Is_Pinbar'] = np.where(lower_wick > (body * 2.0) + 1e-5, 1, 0)

        # 2. 볼린저 밴드 이탈 후 회귀 (Fakeout) 판별
        all_new_features['BB_Fakeout'] = np.where(
            (df['Low'] < all_new_features['BB_Lower']) & 
            (df['Close'] > all_new_features['BB_Lower']) & 
            (all_new_features['Is_Pinbar'] == 1), 1, 0
        )

        # 3. 거래량 급증 (Volume Climax) 판별 - 20일 이평 대비 1.5배 이상
        vol_ma20 = df['Volume'].rolling(window=20).mean()
        all_new_features['Volume_Climax'] = np.where(df['Volume'] > (vol_ma20 * 2.5), 1, 0)

        # ==================================================================
        # [추가] 유동성 사냥 이후 스마트 매수 신호
        #   BullDiv 발생 → 다이버전스 저점 아래로 급락(휩쏘) → 회복(reclaim) 시 매수
        #   * 반드시 Is_Pinbar / Volume_Climax / RSI_BullDiv 가 계산된 뒤에 호출할 것
        # ==================================================================
        all_new_features["Manip_Buy_Signal"] = self.manipulation_buy_signal(df, all_new_features)
        # ==================================================================

        df = pd.concat([df, pd.DataFrame(all_new_features, index=df.index)], axis=1)
        return df

    # ------------------------------------------------------------------
    # Regular Divergence
    # ------------------------------------------------------------------
    def divergence_optimized(self, price, rsi, lookback):
        n = len(price)
        bull_div = np.zeros(n, dtype=np.int8)
        bear_div = np.zeros(n, dtype=np.int8)
        if n < lookback:
            return bull_div, bear_div

        price_vals = price.values
        rsi_vals = rsi.values
        valid_mask = np.isfinite(price_vals) & np.isfinite(rsi_vals)

        trough_idx = argrelextrema(price_vals, np.less, order=self.swing_order)[0]
        peak_idx = argrelextrema(price_vals, np.greater, order=self.swing_order)[0]
        trough_idx = trough_idx[valid_mask[trough_idx]]
        peak_idx = peak_idx[valid_mask[peak_idx]]

        for idx_list, div_arr, p_range, r_range in [
            (trough_idx, bull_div, self.up_div_price, self.up_div_rsi_diff),
            (peak_idx, bear_div, self.down_div_price, self.down_div_rsi_diff),
        ]:
            for i in range(len(idx_list) - 1):
                a, b = idx_list[i], idx_list[i + 1]
                if b - a > lookback:
                    continue

                p_a, p_b = price_vals[a], price_vals[b]
                r_a, r_b = rsi_vals[a], rsi_vals[b]

                if p_a == 0:
                    continue

                p_pct = (p_b - p_a) / p_a
                r_diff = r_b - r_a   # RSI는 절대 차이(point) 사용

                if (p_range[0] <= p_pct <= p_range[1]) and (r_range[0] <= r_diff <= r_range[1]):
                    div_arr[b] = 1

        return bull_div, bear_div

    # ------------------------------------------------------------------
    # Hidden Divergence
    # ------------------------------------------------------------------
    def hidden_divergence_optimized(self, price, rsi, lookback):
        n = len(price)
        h_bull = np.zeros(n, dtype=np.int8)
        h_bear = np.zeros(n, dtype=np.int8)
        if n < lookback:
            return h_bull, h_bear

        price_vals = price.values
        rsi_vals = rsi.values
        valid_mask = np.isfinite(price_vals) & np.isfinite(rsi_vals)

        trough_idx = argrelextrema(price_vals, np.less, order=self.swing_order)[0]
        peak_idx = argrelextrema(price_vals, np.greater, order=self.swing_order)[0]
        trough_idx = trough_idx[valid_mask[trough_idx]]
        peak_idx = peak_idx[valid_mask[peak_idx]]

        # --- Bullish Hidden Divergence ---
        for i in range(len(trough_idx) - 1):
            a, b = trough_idx[i], trough_idx[i + 1]
            if b - a > lookback:
                continue

            p_a, p_b = price_vals[a], price_vals[b]
            r_a, r_b = rsi_vals[a], rsi_vals[b]

            if p_a == 0:
                continue

            p_pct = (p_b - p_a) / p_a
            r_diff = r_b - r_a

            if (self.up_hide_price[0] <= p_pct <= self.up_hide_price[1]) and \
               (self.up_hide_rsi_diff[0] <= r_diff <= self.up_hide_rsi_diff[1]):
                h_bull[b] = 1

        # --- Bearish Hidden Divergence ---
        for i in range(len(peak_idx) - 1):
            a, b = peak_idx[i], peak_idx[i + 1]
            if b - a > lookback:
                continue

            p_a, p_b = price_vals[a], price_vals[b]
            r_a, r_b = rsi_vals[a], rsi_vals[b]

            if p_a == 0:
                continue

            p_pct = (p_b - p_a) / p_a
            r_diff = r_b - r_a

            if (self.down_hide_price[0] <= p_pct <= self.down_hide_price[1]) and \
               (self.down_hide_rsi_diff[0] <= r_diff <= self.down_hide_rsi_diff[1]):
                h_bear[b] = 1

        return h_bull, h_bear

    # ------------------------------------------------------------------
    # Regular Trend
    # ------------------------------------------------------------------
    def trend_optimized(self, price, rsi, lookback):
        n = len(price)
        up_trend = np.zeros(n, dtype=np.int8)
        down_trend = np.zeros(n, dtype=np.int8)
        if n < lookback:
            return up_trend, down_trend

        price_vals = price.values
        rsi_vals = rsi.values
        valid_mask = np.isfinite(price_vals) & np.isfinite(rsi_vals)

        trough_idx = argrelextrema(price_vals, np.less, order=self.swing_order)[0]
        peak_idx = argrelextrema(price_vals, np.greater, order=self.swing_order)[0]
        trough_idx = trough_idx[valid_mask[trough_idx]]
        peak_idx = peak_idx[valid_mask[peak_idx]]

        # --- Up Trend ---
        for i in range(len(trough_idx) - 1):
            a, b = trough_idx[i], trough_idx[i + 1]
            if b - a > lookback:
                continue

            p_a, p_b = price_vals[a], price_vals[b]
            r_a, r_b = rsi_vals[a], rsi_vals[b]

            if p_a == 0:
                continue

            p_pct = (p_b - p_a) / p_a
            r_diff = r_b - r_a

            if (self.up_trend_price[0] <= p_pct <= self.up_trend_price[1]) and \
               (self.up_trend_rsi_diff[0] <= r_diff <= self.up_trend_rsi_diff[1]):
                up_trend[b] = 1

        # --- Down Trend ---
        for i in range(len(peak_idx) - 1):
            a, b = peak_idx[i], peak_idx[i + 1]
            if b - a > lookback:
                continue

            p_a, p_b = price_vals[a], price_vals[b]
            r_a, r_b = rsi_vals[a], rsi_vals[b]

            if p_a == 0:
                continue

            p_pct = (p_b - p_a) / p_a
            r_diff = r_b - r_a

            if (self.down_trend_price[0] <= p_pct <= self.down_trend_price[1]) and \
               (self.down_trend_rsi_diff[0] <= r_diff <= self.down_trend_rsi_diff[1]):
                down_trend[b] = 1

        return up_trend, down_trend

    # ------------------------------------------------------------------
    # 유동성 사냥(Stop Hunt) 이후 스마트 매수 신호
    # ------------------------------------------------------------------
    def manipulation_buy_signal(self, df, features):
        cfg = self.manip_buy_config
        n = len(df)
        signal = np.zeros(n, dtype=np.int8)
        if n == 0:
            return signal

        low      = df["Low"].values
        close    = df["Close"].values
        bull     = np.asarray(features["RSI_BullDiv"])      
        is_pin   = np.asarray(features["Is_Pinbar"])
        vol_clmx = np.asarray(features["Volume_Climax"])
        rsi      = np.asarray(features["RSI"])

        div_lookback = cfg["div_lookback"]
        min_drop     = cfg["min_drop"]
        max_drop     = cfg["max_drop"]
        cooldown     = cfg["cooldown"]

        last_div_idx = -1      
        div_low      = np.nan  
        swept        = False   
        sweep_low    = np.nan  
        cd           = 0       

        for i in range(n):
            if cd > 0:
                cd -= 1

            if bull[i] == 1:
                last_div_idx = i
                div_low      = low[i]
                swept        = False
                sweep_low    = np.nan
                continue

            if last_div_idx < 0 or (i - last_div_idx) > div_lookback:
                continue
            if not np.isfinite(div_low) or div_low == 0:
                continue

            if low[i] < div_low:
                swept = True
                sweep_low = low[i] if not np.isfinite(sweep_low) else min(sweep_low, low[i])

            if not swept:
                continue

            drop_pct = (div_low - sweep_low) / div_low

            if drop_pct > max_drop:
                last_div_idx = -1
                swept        = False
                sweep_low    = np.nan
                continue

            reclaim  = close[i] > div_low                                      
            confirm  = (is_pin[i] == 1) or (vol_clmx[i] == 1)   
            rsi_turn = (i > 0) and (rsi[i] > rsi[i - 1])        

            if (drop_pct >= min_drop) and reclaim and (confirm or rsi_turn) and cd == 0:
                signal[i] = 1
                cd = cooldown
                last_div_idx = -1
                swept        = False
                sweep_low    = np.nan

        return signal

    # ------------------------------------------------------------------
    # 파일명 sanitize (Windows 금지문자 처리)
    # ------------------------------------------------------------------
    @staticmethod
    def _sanitize_filename(name):
        if not isinstance(name, str):
            name = str(name)
        return re.sub(r'[<>:"/\\|?*]', '_', name).strip()

    # ------------------------------------------------------------------
    # [수정] 파티션 처리: CSV 및 Parquet 동시 저장
    # ------------------------------------------------------------------
    def _process_partition(self, pdf):
        """파티션(Pandas DF) 단위로 종목별 처리."""
        if pdf.empty:
            return pd.Series(dtype=object)

        results = []
        for symbol, group in pdf.groupby(level=0):
            if not isinstance(symbol, str) or not symbol or symbol.lower() == 'symbol':
                continue

            try:
                name = group['Name'].iloc[0]
                df_sorted = group.sort_values('Date').reset_index(drop=True)

                if len(df_sorted) < 26:
                    print(f"[Skip] {symbol} ({name}): 데이터 길이 부족 ({len(df_sorted)} rows < 26)")
                    continue

                processed_df = self.calculate_indicators(df_sorted)
                safe_name = self._sanitize_filename(name)
                
                # 1. Parquet 파일로 저장
                filename_parquet = f"{safe_name}({symbol}).parquet"
                filepath_parquet = os.path.join(self.save_dir_csv, filename_parquet)
                processed_df.to_parquet(filepath_parquet, index=False, engine='pyarrow')
                
                results.append(symbol)
            except Exception as e:
                import traceback
                print(f"[ERROR] {symbol}: {e}\n{traceback.format_exc()}")
                continue

        return pd.Series(results, dtype=object)

    # ------------------------------------------------------------------
    # 실행
    # ------------------------------------------------------------------
    def run(self):
        logging.getLogger("distributed.shuffle._scheduler_plugin").setLevel(logging.ERROR)
        
        # ===== [수정] CSV 저장 경로 디렉터리 생성 조치 =====
        os.makedirs(self.save_dir_csv, exist_ok=True)
        
        input_full_path = os.path.join(self.input_path, self.market_name, self.input_file)

        cfg = self.cluster_config
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Dask 시스템 가동... "
              f"(workers={cfg['n_workers']}, "
              f"mem={cfg['memory_limit']}, "
              f"npartitions={cfg['npartitions']}, "
              f"persist={cfg['use_persist']})")

        with Client(
            n_workers=cfg["n_workers"],
            threads_per_worker=cfg["threads_per_worker"],
            memory_limit=cfg["memory_limit"],
        ) as client:
            print(f"대시보드: {client.dashboard_link}")

            ddf = dd.read_parquet(
                input_full_path,
                engine='pyarrow'
            )

            print(f"데이터 셔플링 및 인덱스 설정 중 (Symbol 기준, npartitions={cfg['npartitions']})...")
            
            ddf = ddf.set_index('Symbol', npartitions=cfg["npartitions"])
            if cfg["use_persist"]:
                ddf = ddf.persist()

            print("병렬 지표 계산 및 파일(CSV/Parquet) 저장 시작...")
            process_task = ddf.map_partitions(
                self._process_partition,
                meta=pd.Series([], dtype=object),
            )
            results = process_task.compute()

            print(f"\n--- 작업 완료: 총 {len(results)}개 종목 처리됨 (CSV & Parquet 저장 완료) ---")


if __name__ == "__main__":
    for market in ["KOSPI", "KOSDAQ", "NASDAQ", "NYSE"]: #  
        CONFIG = {
            "input_path": "C:/Users/yslee/PycharmProjects/FinanceMLOps/Data",
            "output_path": "C:/Users/yslee/PycharmProjects/FinanceMLOps/Data",
            "market_name": market,
        }
        print(CONFIG["market_name"])
        try:
            processor = DaskFinanceProcessor(CONFIG)
            processor.run()
        except Exception as e:
            import traceback
            traceback.print_exc()