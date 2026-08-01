import warnings
warnings.filterwarnings("ignore")

from sklearn.preprocessing import MinMaxScaler, StandardScaler, RobustScaler
from sklearn.cluster import DBSCAN
from tslearn.clustering import TimeSeriesKMeans
from tslearn.metrics import cdist_dtw
from tslearn.metrics import cdist_soft_dtw
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt

class run_cluster():
    def __init__(self, config, source_data):

        self.SAVE_PATH = config.get('SAVE_PATH', '')
        self.TIME_INDEX = config.get('TIME', '')

        self.SCALER = config.get('SCALER', 'standard')
        self.N_CLUSTER_RANGE = config.get('N_CLUSTER_RANGE', [8, 8])
        self.METHOD = config.get('METHOD', 'kmeans_softdtw')

        # DBSCAN 전용 파라미터
        self.DBSCAN_EPS = config.get('DBSCAN_EPS', 0.5)
        self.DBSCAN_MIN_SAMPLES = config.get('DBSCAN_MIN_SAMPLES', 3)
        self.SOFT_DTW_GAMMA = config.get('SOFT_DTW_GAMMA', 1.0)

        # 필터링 옵션 (config로 조정 가능)
        self.MIN_VALID_RATIO = config.get('MIN_VALID_RATIO', 0.5)  # 유효값 최소 비율
        self.DROP_CONSTANT = config.get('DROP_CONSTANT', True)     # 상수 컬럼 제거 여부

        self.create_folder("{}/{}".format(self.SAVE_PATH, self.METHOD))

        # ------------------------------------------------------------------ #
        # [강화된 데이터 정제 단계]
        # ------------------------------------------------------------------ #
        source_data = self._clean_source_data(source_data)
        # ------------------------------------------------------------------ #

        self.data_info = source_data.describe()
        self.data_info.to_csv("{}/{}/inputData_info.csv".format(self.SAVE_PATH, self.METHOD), encoding="utf-8-sig")
        
        source_data.to_csv("{}/{}/inputData.csv".format(self.SAVE_PATH, self.METHOD), encoding="utf-8-sig")

        # 시계열 클러스터링을 위해 Transpose (행: 대상, 열: 시간 흐름)
        data = source_data.transpose()
        print(data)

        # 스케일러 설정
        if self.SCALER == 'standard':
            self.scaler = StandardScaler()
        elif self.SCALER == 'min_max':
            self.scaler = MinMaxScaler()
        elif self.SCALER == 'robust':
            self.scaler = RobustScaler()
        elif self.SCALER == "no_scaler":
            self.scaler = None
        else:
            self.scaler = StandardScaler()

        if self.SCALER == "no_scaler":
            self._data = data.values.astype(float)
        else:
            # StandardScaler는 컬럼(시점) 단위로 스케일링되므로, 시계열 분포 보존을 위해
            # transpose 후 fit_transform을 그대로 사용 (기존 로직 유지)
            self._data = self.scaler.fit_transform(data)

        # 스케일러 통과 후에도 혹시 NaN/Inf가 남아있다면 제거 (안전장치)
        self._data = self._sanitize_array(self._data, data.index)

        if self._data.shape[0] == 0:
            raise ValueError("정제 후 클러스터링 가능한 종목이 0개입니다. "
                             "MIN_VALID_RATIO를 낮추거나 입력 데이터를 확인하세요.")

        print("최종 클러스터링 입력 shape: {}".format(self._data.shape))

        # ------------------------------------------------------------------ #
        # 1. DBSCAN 실행 로직
        # ------------------------------------------------------------------ #
        if self.METHOD == "dbscan_dtw":
            print("Running DBSCAN + DTW  (eps={}, min_samples={})".format(
                self.DBSCAN_EPS, self.DBSCAN_MIN_SAMPLES))

            y_pred, n_clusters, n_noise = self.DBSCAN_DTW(data=self._data)

            print("클러스터 수: {}  /  노이즈 포인트: {}".format(n_clusters, n_noise))

            _data_res = pd.DataFrame(self._data, columns=data.columns, index=self._valid_index)
            _data_res["clusters"] = y_pred
            _data_res.to_csv(
                "{}/{}/normalized_dbscan.csv".format(self.SAVE_PATH, self.METHOD),
                encoding="utf-8-sig"
            )
            
        elif self.METHOD == "dbscan_softdtw":
            print("Running DBSCAN + Soft-DTW (eps={}, min_samples={}, gamma={})".format(
                self.DBSCAN_EPS, self.DBSCAN_MIN_SAMPLES, self.SOFT_DTW_GAMMA))

            y_pred, n_clusters, n_noise = self.DBSCAN_SoftDTW(data=self._data)

            print("클러스터 수: {}  /  노이즈 포인트: {}".format(n_clusters, n_noise))

            _data_res = pd.DataFrame(self._data, columns=data.columns, index=self._valid_index)
            _data_res["clusters"] = y_pred
            _data_res.to_csv(
                "{}/{}/normalized_dbscan_softdtw.csv".format(self.SAVE_PATH, self.METHOD),
                encoding="utf-8-sig"
            )

        # ------------------------------------------------------------------ #
        # 2. KMeans 계열 실행 로직 (Elbow Method 포함)
        # ------------------------------------------------------------------ #
        else:
            self.num_list = []
            self.sse = []
            
            for n in range(int(self.N_CLUSTER_RANGE[0]), int(self.N_CLUSTER_RANGE[1]) + 1):
                # 샘플 수보다 클러스터 수가 많거나 같으면 스킵
                if n >= self._data.shape[0]:
                    print("샘플 수({})보다 큰/같은 n_clusters={}는 스킵합니다.".format(
                        self._data.shape[0], n))
                    continue

                print("Cluster number : {}".format(n))
                y_pred, elbow = self.TSKMeans(data=self._data, n_clusters=n)

                self.sse.append(elbow)
                self.num_list.append(n)

                _data_res = pd.DataFrame(self._data, columns=data.columns, index=self._valid_index)
                _data_res["clusters"] = y_pred
                _data_res.to_parquet(
                    "{}/{}/normalized_{}.parquet".format(self.SAVE_PATH, self.METHOD, str(n))
                )
            
            # Elbow 결과 저장 및 시각화
            if len(self.num_list) > 0:
                self.save_elbow_results()
                self.plot_elbow_chart()
            else:
                print("Elbow에 사용할 결과가 없습니다.")

    # ------------------------------------------------------------------ #
    # [신규] 입력 데이터 정제 메서드
    # ------------------------------------------------------------------ #
    def _clean_source_data(self, df):
        """
        source_data (행: 시간, 열: 종목)를 강하게 정제한다.
          1) Inf -> NaN 치환
          2) 전체가 NaN인 컬럼 제거
          3) 유효값 비율이 MIN_VALID_RATIO 미만인 컬럼 제거
          4) 상수 컬럼(분산 0) 제거 (옵션)
          5) ffill -> bfill로 결측치 보간
          6) 그래도 NaN이 남은 컬럼은 최종 제거
        """
        original_cols = df.shape[1]
        df = df.copy()

        # 1) Inf 처리
        df = df.replace([np.inf, -np.inf], np.nan)

        # 2) 전체가 NaN인 컬럼 제거
        all_nan_cols = df.columns[df.isna().all()].tolist()
        if all_nan_cols:
            print("[정제] 전체 NaN 컬럼 {}개 제거: {}".format(
                len(all_nan_cols), all_nan_cols[:10]))
            df = df.drop(columns=all_nan_cols)

        # 3) 유효값 비율 기준 필터링
        if self.MIN_VALID_RATIO > 0:
            valid_ratio = df.notna().mean()
            low_quality_cols = valid_ratio[valid_ratio < self.MIN_VALID_RATIO].index.tolist()
            if low_quality_cols:
                print("[정제] 유효값 비율 {} 미만 컬럼 {}개 제거: {}".format(
                    self.MIN_VALID_RATIO, len(low_quality_cols), low_quality_cols[:10]))
                df = df.drop(columns=low_quality_cols)

        # 4) 결측치 보간
        df = df.ffill().bfill()

        # 5) 그래도 NaN이 남은 컬럼 제거 (보간이 불가능했던 케이스)
        residual_nan_cols = df.columns[df.isna().any()].tolist()
        if residual_nan_cols:
            print("[정제] 보간 후에도 NaN이 남은 컬럼 {}개 제거: {}".format(
                len(residual_nan_cols), residual_nan_cols[:10]))
            df = df.drop(columns=residual_nan_cols)

        # 6) 상수 컬럼 제거 (StandardScaler에서 0으로 나누기 방지)
        if self.DROP_CONSTANT and df.shape[1] > 0:
            std = df.std(axis=0)
            constant_cols = std[std == 0].index.tolist()
            if constant_cols:
                print("[정제] 상수 컬럼 {}개 제거: {}".format(
                    len(constant_cols), constant_cols[:10]))
                df = df.drop(columns=constant_cols)

        print("[정제 완료] {} -> {} 컬럼 (제거: {}개)".format(
            original_cols, df.shape[1], original_cols - df.shape[1]))

        if df.shape[1] == 0:
            raise ValueError("정제 후 사용 가능한 종목(컬럼)이 0개입니다. "
                             "입력 데이터 또는 MIN_VALID_RATIO 설정을 확인하세요.")

        return df

    # ------------------------------------------------------------------ #
    # [신규] 스케일러 통과 후 안전장치
    # ------------------------------------------------------------------ #
    def _sanitize_array(self, arr, index):
        """
        스케일링 결과 배열에 NaN/Inf가 포함된 행을 제거하고 valid index를 저장.
        """
        arr = np.asarray(arr, dtype=float)
        bad_mask = ~np.isfinite(arr).all(axis=1)
        if bad_mask.any():
            bad_items = np.array(index)[bad_mask].tolist()
            print("[안전장치] 스케일링 후 NaN/Inf 포함 행 {}개 제거: {}".format(
                bad_mask.sum(), bad_items[:10]))
            arr = arr[~bad_mask]
            self._valid_index = pd.Index(np.array(index)[~bad_mask])
        else:
            self._valid_index = pd.Index(index)
        return arr

    # ------------------------------------------------------------------ #

    def DBSCAN_DTW(self, data):
        """
        DTW 거리 행렬을 사전 계산한 뒤 DBSCAN(metric='precomputed')으로 클러스터링.
        """
        data_3d = data.reshape(data.shape[0], data.shape[1], 1)

        print("DTW 거리 행렬 계산 중...  shape={}".format(data_3d.shape))
        dist_matrix = cdist_dtw(data_3d)

        model = DBSCAN(
            eps=self.DBSCAN_EPS,
            min_samples=self.DBSCAN_MIN_SAMPLES,
            metric='precomputed'
        )
        y_pred = model.fit_predict(dist_matrix)

        n_clusters = len(set(y_pred)) - (1 if -1 in y_pred else 0)
        n_noise = int(np.sum(y_pred == -1))

        return y_pred, n_clusters, n_noise
    
    def DBSCAN_SoftDTW(self, data):
        """
        Soft-DTW Divergence 기반 거리 행렬 + DBSCAN.
        """
        data_3d = data.reshape(data.shape[0], data.shape[1], 1)

        print("Soft-DTW 거리 행렬 계산 중... gamma={}".format(self.SOFT_DTW_GAMMA))
        raw_dist_matrix = cdist_soft_dtw(data_3d, gamma=self.SOFT_DTW_GAMMA)

        diag = np.diag(raw_dist_matrix).reshape(-1, 1)
        dist_matrix = raw_dist_matrix - 0.5 * (diag + diag.T)
        dist_matrix = np.maximum(dist_matrix, 0)

        model = DBSCAN(
            eps=self.DBSCAN_EPS,
            min_samples=self.DBSCAN_MIN_SAMPLES,
            metric='precomputed'
        )
        y_pred = model.fit_predict(dist_matrix)

        n_clusters = len(set(y_pred)) - (1 if -1 in y_pred else 0)
        n_noise = int(np.sum(y_pred == -1))

        return y_pred, n_clusters, n_noise

    # ------------------------------------------------------------------ #

    def TSKMeans(self, data, n_clusters):
        # 3D 변환: (n_samples, n_timestamps, 1)
        if data.ndim == 2:
            data = data.reshape(data.shape[0], data.shape[1], 1)
        
        if self.METHOD == "kmeans_dtw":
            model = TimeSeriesKMeans(n_clusters=n_clusters, metric="dtw", tol=1e-4,
                                     max_iter=5, random_state=0, n_jobs=-1, verbose=True)
        elif self.METHOD == "kmeans_softdtw":
            model = TimeSeriesKMeans(n_clusters=n_clusters, metric="softdtw",
                                     max_iter=5, tol=1e-4,
                                     random_state=0, n_jobs=1,
                                     max_iter_barycenter=10,
                                     metric_params={"gamma": self.SOFT_DTW_GAMMA},  # gamma 명시
                                     n_init=2, verbose=True)   # 빈 클러스터 회피
        elif self.METHOD == "kmeans_euclidean":
            model = TimeSeriesKMeans(n_clusters=n_clusters, metric="euclidean", tol=1e-4,
                                     max_iter=5, random_state=0, n_jobs=-1, verbose=True)

        y_pred = model.fit_predict(data)
        return y_pred, model.inertia_

    # ------------------------------------------------------------------ #
    
    def plot_elbow_chart(self):
        plt.figure(figsize=(10, 6))
        plt.plot(self.num_list, self.sse, marker='o', linestyle='--', color='b')
        plt.xlabel('Number of Clusters (k)')
        plt.ylabel('Inertia (SSE)')
        plt.title('Elbow Method for Optimal k ({})'.format(self.METHOD))
        plt.grid(True)
        
        save_file = "{}/{}/elbow_chart.png".format(self.SAVE_PATH, self.METHOD)
        plt.savefig(save_file)
        print("Elbow chart saved at: {}".format(save_file))
        plt.close()

    def save_elbow_results(self):
        elbow_df = pd.DataFrame({
            'n_clusters': self.num_list,
            'inertia_sse': self.sse
        })
        elbow_df['sse_diff'] = elbow_df['inertia_sse'].diff().abs()
        
        save_file = "{}/{}/elbow_results.csv".format(self.SAVE_PATH, self.METHOD)
        elbow_df.to_csv(save_file, index=False, encoding="utf-8-sig")
        
        print("Elbow results CSV saved at: {}".format(save_file))

    def create_folder(self, directory):
        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
        except OSError:
            print('Error: creating directory. ' + directory)

    def DataFiltering(self, data):
        return data

# =========================================================================
import sys
import os

def _safe_read_file(path):
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        if path.endswith('.parquet'):
            return pd.read_parquet(path)
        else:
            return pd.read_csv(path)
    except Exception as e:
        print(f"[Warning] Failed to read {path}: {e}")
        return pd.DataFrame()

_safe_read_csv = _safe_read_file

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("Usage: python process_c2.py <market> <target_date> <base_path>")
        sys.exit(1)

    _market = sys.argv[1]
    _target_date = sys.argv[2]
    _base_path = sys.argv[3]

    # =========================================================================
    # 2. 통합 CONFIG 딕셔너리 (모든 환경 변수 및 파라미터 포함)
    # =========================================================================
    CONFIG = {
        # --- 환경 경로 및 기본 세팅 ---
        'MARKET': _market, 
        'TARGET_DATE': _target_date,
        'BASE_PATH': _base_path,
        'CAP_PATH': f"{_base_path}/{_market}/M1Sheet/{_target_date}/df_cap.csv",
        'LOAD_PATH': f"{_base_path}/{_market}/C1Sheet/df_zscore_1w.csv",
        'SAVE_PATH': f"{_base_path}/{_market}/C2Sheet/{_target_date}",
        
        # --- 시가총액 추출 및 필터링 설정 ---
        'CAP_COLUMN': "MarketCap_KRW",             # df_cap.parquet에서 기준으로 삼을 시가총액 컬럼
        'TOP_N': 1000,                             # 제외 단어 필터링 후 시가총액 기준 상위 N개 추출
        'EXCLUDE_WORDS': [
            "TIGER", "KODEX", "RISE", "TIME", "SOL", "ACE", "TDF", "ETN", "PLUS", 
            "1Q", "KIWOOM", "KoAct", "WON", "HANARO", "KCGI", "FOCUS", "에셋플러스", 
            "DAISHIN", "MIDAS", "TRUSTON", "스팩", "SPAC", "KOSEF", "Fund", "Unit", 
            "Right", "Rights", "VIX", "국고채", "채권", "혼합", "KBSTAR", "KINDEX", 
            "ARIRANG", "마이티", "TREX", "특수채", "회사채", "UNICORN", "2차전지양극재", 
            "IBK K-AI", "액티브", "온디바이스AI", "인덱스", "Acquisition", "Warrant", 
            "ETF", "Trust", "200"
        ],
        
        # --- 클러스터링 기본 설정 ---
        'TIME': 'Date',                            # 시간 인덱스명 설정
        'SCALER': 'standard',                      # 'standard', 'min_max', 'robust', 'no_scaler'
        'N_CLUSTER_RANGE': [2, 10],                 # KMeans K 범위
        'METHOD': 'kmeans_softdtw',                # 'kmeans_dtw', 'kmeans_softdtw', 'kmeans_euclidean', 'dbscan_dtw', 'dbscan_softdtw'
        
        # --- 클러스터링 알고리즘 파라미터 ---
        'DBSCAN_EPS': 0.5,                         # 반경
        'DBSCAN_MIN_SAMPLES': 2,                   # 최소 샘플 수
        'SOFT_DTW_GAMMA': 1.0,                     # 평활화 정도
        
        # --- 전처리 필터링 옵션 ---
        'MIN_VALID_RATIO': 0.5,                    # 유효값 최소 비율
        'DROP_CONSTANT': True                      # 상수 컬럼 제거 여부
    }

    # =========================================================================
    # 3. 데이터 로드 및 종목 사전 필터링 -> 상위 N개 추출 
    # =========================================================================
    print(f"Loading Market Cap Data: {CONFIG['CAP_PATH']}")
    df_cap = _safe_read_csv(CONFIG['CAP_PATH']) # 에러 우회 함수 적용

    cap_col = CONFIG['CAP_COLUMN']
    if cap_col not in df_cap.columns:
        print(f"에러: 설정하신 시가총액 컬럼 '{cap_col}'이 df_cap.csv에 존재하지 않습니다.")
        df_cap_filtered = df_cap.copy()
    else:
        # 3-1. 심볼 데이터 준비 (컬럼 또는 인덱스 지원)
        if 'Symbol' in df_cap.columns:
            symbol_series = df_cap['Symbol']
        else:
            symbol_series = df_cap.index.to_series()

        # 3-2. 전체 데이터에 대해 제외 단어(Exclude Words) 먼저 필터링
        def is_valid_symbol(sym):
            sym_str = str(sym).upper()
            return not any(word.upper() in sym_str for word in CONFIG['EXCLUDE_WORDS'])

        valid_mask = symbol_series.apply(is_valid_symbol)
        df_cap_filtered = df_cap[valid_mask]

        # 3-3. 순수 종목들만 남은 상태에서 시가총액 기준 내림차순 정렬 후 상위 TOP_N개 추출
        df_cap_filtered = df_cap_filtered.sort_values(by=cap_col, ascending=False).head(CONFIG['TOP_N'])

    # 최종 추출된 Symbol 리스트 생성
    if 'Symbol' in df_cap_filtered.columns:
        filtered_symbols = df_cap_filtered['Symbol'].tolist()
    else:
        filtered_symbols = df_cap_filtered.index.tolist()

    print(f"[필터링 결과] 제외 단어 필터링 후 시가총액 상위 {CONFIG['TOP_N']} 종목 추출 완료 (실제 크기: {len(filtered_symbols)}개)")

    # =========================================================================
    # 4. 시계열 데이터 로드 및 최종 종목 적용
    # =========================================================================
    print(f"Loading Time-Series Data: {CONFIG['LOAD_PATH']}")
    source_data_raw = _safe_read_csv(CONFIG['LOAD_PATH']) # 에러 우회 함수 적용

    if not source_data_raw.empty:
        # 시계열 데이터의 열(Columns) 중에서 최종 추출된 filtered_symbols에 해당하는 것만 교집합으로 남김
        valid_cols = [col for col in source_data_raw.columns if col in filtered_symbols]
        source_data = source_data_raw[valid_cols]

        print(f"[데이터 준비 완료] 필터링 후 적용될 시계열 데이터 shape: {source_data.shape}")

        # =========================================================================
        # 5. 클러스터링 클래스 실행
        # =========================================================================
        print(f"--- Clustering Process Started for {CONFIG['MARKET']} ({CONFIG['TARGET_DATE']}) ---")
        run = run_cluster(config=CONFIG, source_data=source_data)
        print("--- Clustering Process Finished ---")