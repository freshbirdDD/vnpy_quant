"""
单边分析的分析工具代码库，同时也在这里进行分析代码在单天数据上的校验和可视化
"""
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
from tqdm import tqdm

def iter_continuous_segments(df):
    """
    把一天数据切成互不连续的交易段。

    当前连续边界：
    - instrument
    - session: AM / PM

    PREOPEN 等其他 session 不参与趋势统计和打标。
    """

    if "session" not in df.columns:
        raise ValueError(
            "df must contain 'session' column"
        )

    work = df[
        df["session"].isin(["AM", "PM"])
    ]

    # TODO 这里直接把df如果有多合约的情况下拆开了，而不是外部先指定一个合约
    if "instrument" in work.columns:
        group_cols = [
            "instrument",
            "session"
        ]
    else:
        group_cols = [
            "session"
        ]

    for _, segment in work.groupby(
        group_cols,
        sort=False,
        observed=True
    ):
        segment = (
            segment
            .sort_values("exchange_ts")
        )

        if len(segment) >= 2:
            yield segment


def calc_linear_efficiency(prices, times):
    """
    基于真实时间的线性回归趋势指标。

    参数
    ----------
    prices:
        价格序列

    times:
        与 prices 一一对应的真实时间戳序列，
        例如 exchange_ts。

    返回
    ----------
    r2:
        线性拟合优度，范围 0~1。

    slope:
        线性回归斜率，单位为 price / second。

        slope > 0:
            上涨趋势

        slope < 0:
            下跌趋势

        abs(slope) 越大:
            单位时间价格移动越快
    """

    if len(prices) < 2:
        return 0.0, 0.0

    if len(prices) != len(times):
        raise ValueError(
            "prices and times must have the same length"
        )

    y = np.asarray(
        prices,
        dtype=float
    )

    time_index = pd.DatetimeIndex(
        pd.to_datetime(times)
    )

    # 以窗口第一条数据为 t=0，
    # 转换成真实经过秒数
    x = (
        time_index
        -
        time_index[0]
    ).total_seconds().to_numpy(
        dtype=float
    )

    # 防止脏数据
    valid = (
        np.isfinite(x)
        &
        np.isfinite(y)
    )

    x = x[valid]
    y = y[valid]

    if len(y) < 2:
        return 0.0, 0.0

    # 至少需要两个不同的真实时间点
    if np.ptp(x) <= 0:
        return 0.0, 0.0

    # price = slope * time + intercept
    slope, intercept = np.polyfit(
        x,
        y,
        1
    )

    y_pred = (
        slope * x
        +
        intercept
    )

    ss_tot = np.sum(
        (y - np.mean(y)) ** 2
    )

    ss_res = np.sum(
        (y - y_pred) ** 2
    )

    if ss_tot <= 0:
        r2 = 0.0

    else:
        r2 = (
            1.0
            -
            ss_res / ss_tot
        )

    # 浮点误差可能产生轻微越界
    r2 = float(
        np.clip(
            r2,
            0.0,
            1.0
        )
    )

    return r2, float(slope)


def calc_path_efficiency(prices):
    """
    趋势效率: 理想情况每一跳都在单边
    """

    if len(prices) < 2:
        return 0

    displacement = abs(prices[-1] - prices[0])

    path = np.sum(
        np.abs(np.diff(prices))
    )

    if path == 0:
        return 0

    return displacement / path




def collect_trend_window_features(
    df,
    window_seconds=10,
    step_seconds=0.5,
    tick_size=0.2,
):
    """
    收集一天数据所有合法滑窗的趋势特征。

    自动隔离：
    - instrument
    - AM / PM

    返回：
        DataFrame，每一行对应一个时间滑窗。

    字段：
        instrument
        session
        window_start
        window_end
        return_tick
        path_efficiency
        linear_r2
        slope_tick_per_second
        abs_slope_tick_per_second
    """

    window_delta = pd.Timedelta(
        seconds=window_seconds
    )

    step_delta = pd.Timedelta(
        seconds=step_seconds
    )

    rows = []

    for segment in iter_continuous_segments(
        df
    ):

        segment = (
            segment
            .sort_values("exchange_ts")
            .reset_index(drop=True)
        )

        times = pd.to_datetime(
            segment["exchange_ts"]
        )

        prices = (
            segment["last_price"]
            .to_numpy()
        )

        instrument = (
            segment["instrument"].iloc[0]
            if "instrument" in segment.columns
            else None
        )

        session = (
            segment["session"].iloc[0]
        )

        current_time = times.iloc[0]
        segment_end_time = times.iloc[-1]

        while (
            current_time + window_delta
            <=
            segment_end_time
        ):

            window_end_time = (
                current_time
                +
                window_delta
            )

            start_idx = times.searchsorted(
                current_time,
                side="left"
            )

            end_idx = times.searchsorted(
                window_end_time,
                side="right"
            )

            window_prices = prices[
                start_idx:end_idx
            ]

            window_times = times.iloc[
                start_idx:end_idx
            ]

            if len(window_prices) >= 2:

                return_tick = (
                    abs(
                        window_prices[-1]
                        -
                        window_prices[0]
                    )
                    /
                    tick_size
                )

                path_efficiency = (
                    calc_path_efficiency(
                        window_prices
                    )
                )

                linear_r2, slope = (
                    calc_linear_efficiency(
                        window_prices,
                        window_times
                    )
                )

                slope_tick_per_second = (
                    slope
                    /
                    tick_size
                )

                rows.append({
                    "instrument": instrument,
                    "session": session,
                    "window_start": current_time,
                    "window_end": window_end_time,
                    "return_tick": return_tick,
                    "path_efficiency": path_efficiency,
                    "linear_r2": linear_r2,
                    "slope_tick_per_second": (
                        slope_tick_per_second
                    ),
                    "abs_slope_tick_per_second": (
                        abs(
                            slope_tick_per_second
                        )
                    ),
                })

            current_time += step_delta

    return pd.DataFrame(rows)

def plot_trend_distribution(
    df,
    window_seconds=10,
    step_seconds=0.5,
    tick_size=0.2,
    bins=50,
    plot_now = True
):
    """
    用于随机抽取一天或几天数据，
    肉眼观察各趋势特征的整体分布。
    """

    features = collect_trend_window_features(
        df,
        window_seconds=window_seconds,
        step_seconds=step_seconds,
        tick_size=tick_size,
    )

    if features.empty:
        print("No valid windows.")
        return features

    if not plot_now:
        return features

    columns = [
        "return_tick",
        "path_efficiency",
        "linear_r2",
        "abs_slope_tick_per_second",
    ]

    titles = [
        "Return Tick",
        "Path Efficiency",
        "Linear R2",
        "Abs Slope (tick/s)",
    ]

    fig, axes = plt.subplots(
        2,
        2,
        figsize=(14, 10)
    )

    axes = axes.flatten()

    for ax, column, title in zip(
        axes,
        columns,
        titles
    ):
        ax.hist(
            features[column],
            bins=bins
        )

        ax.set_title(title)

    plt.tight_layout()
    plt.show()

    return features

def summarize_trend_distribution(
    df,
    window_seconds=10,
    step_seconds=0.5,
    tick_size=0.2,
    quantiles=(0.50, 0.75, 0.90, 0.95, 0.99),
):
    """
    对单个 contract-day 的所有时间滑窗做趋势特征统计。

    使用场景：
        后续遍历大量 日期 × 合约，
        每个 contract-day 返回一行统计结果。

    输入要求：
        df 必须只包含：
        - 一个 trading_day
        - 一个 instrument

    返回：
        dict，可直接用于构造 DataFrame。
    """

    if df.empty:
        return {
            "trading_day": None,
            "instrument": None,
            "daily_volume": 0,
            "window_count": 0,
        }

    # =====================================
    # 1. 确认输入是单日、单合约
    # =====================================

    instruments = (
        df["instrument"]
        .dropna()
        .unique()
    )

    if len(instruments) != 1:
        raise ValueError(
            "summarize_trend_distribution "
            "expects exactly one instrument, "
            f"got {len(instruments)}: {instruments}"
        )

    instrument = instruments[0]


    if "trading_day" in df.columns:

        trading_days = (
            df["trading_day"]
            .dropna()
            .unique()
        )

        if len(trading_days) != 1:
            raise ValueError(
                "summarize_trend_distribution "
                "expects exactly one trading_day, "
                f"got {len(trading_days)}: {trading_days}"
            )

        trading_day = trading_days[0]

    else:

        trading_day = (
            pd.to_datetime(
                df["exchange_ts"].iloc[0]
            )
            .date()
        )


    # =====================================
    # 2. 当天总成交量
    # =====================================
    #
    # volume 是日内累计成交量，
    # 因此取当天最大值即可。
    #
    # 注意：
    # 绝对不能 sum(volume)。

    daily_volume = (
        pd.to_numeric(
            df["volume"],
            errors="coerce"
        )
        .max()
    )

    if pd.isna(daily_volume):
        daily_volume = 0


    # =====================================
    # 3. 计算当天所有滑窗特征
    # =====================================

    features = collect_trend_window_features(
        df,
        window_seconds=window_seconds,
        step_seconds=step_seconds,
        tick_size=tick_size,
    )


    result = {
        "trading_day": trading_day,
        "instrument": instrument,
        "daily_volume": daily_volume,
        "window_count": len(features),
    }


    if features.empty:
        return result


    # =====================================
    # 4. 每个指标的关键分位数
    # =====================================

    columns = [
        "return_tick",
        "path_efficiency",
        "linear_r2",
        "abs_slope_tick_per_second",
    ]


    for column in columns:

        values = features[column]

        result[
            f"{column}_min"
        ] = values.min()

        result[
            f"{column}_max"
        ] = values.max()


        for q in quantiles:

            percentile = int(
                round(q * 100)
            )

            result[
                f"{column}_p{percentile}"
            ] = values.quantile(q)


    return result

class TrendLabeler:
    """
    基于价格序列的单边趋势事件筛选器

    时间窗口基于 exchange_ts 滑动，
    不假设相邻两条数据固定相差 0.5s。

    输出:
    O
    B_TREND
    I_TREND
    E_TREND
    """

    def __init__(
        self,
        window_seconds=10.0,
        stride_seconds=0.5,
        min_return_tick=10.0,
        tick_size=0.2,
        path_efficiency_threshold=0.6,
        linear_r2_threshold=0.7,
        min_abs_slope_tick_per_second=0.0,
        max_drawdown_ratio=0.4,
        extend_step_seconds=0.5,
        max_extend_seconds=120.0,
    ):
        # 初始扫描窗口的真实时间宽度
        self.window_seconds = window_seconds

        # 窗口起点每次在时间轴上向右移动多少秒
        self.stride_seconds = stride_seconds

        self.tick_size = tick_size

        # 初始窗口最小价格位移，单位 tick
        self.min_return_tick = min_return_tick

        # 单边路径效率阈值
        self.efficiency_threshold = path_efficiency_threshold

        # 暂时保留，当前 check_window 里还没有使用
        self.linear_r2_threshold = (
            linear_r2_threshold
        )

        self.min_abs_slope_tick_per_second = (
            min_abs_slope_tick_per_second
        )

        # 最大回撤 / 当前趋势位移
        self.max_drawdown_ratio = max_drawdown_ratio

        # 找到 seed 后，每次向右扩展多少真实时间
        self.extend_step_seconds = extend_step_seconds

        # seed 窗口之后最多继续扩展多少真实时间
        self.max_extend_seconds = max_extend_seconds


    def calc_return_tick(self, prices):
        """
        窗口首尾价格位移，单位 tick
        """
        return (
            abs(prices[-1] - prices[0])
            /
            self.tick_size
        )

    def check_window(
            self,
            prices,
            times
    ):
        """
        判断窗口是否满足趋势候选
        """

        if len(prices) < 2:
            return False

        ret = self.calc_return_tick(
            prices
        )

        path_efficiency = (
            calc_path_efficiency(
                prices
            )
        )

        linear_r2, slope = (
            calc_linear_efficiency(
                prices,
                times
            )
        )

        slope_tick_per_second = (
                slope
                /
                self.tick_size
        )

        if (
                ret
                <
                self.min_return_tick
        ):
            return False

        if (
                path_efficiency
                <
                self.efficiency_threshold
        ):
            return False

        if (
                linear_r2
                <
                self.linear_r2_threshold
        ):
            return False

        if (
                abs(
                    slope_tick_per_second
                )
                <
                self.min_abs_slope_tick_per_second
        ):
            return False

        return True

    def check_extension(
            self,
            prices,
            recent_prices,
            recent_times
    ):
        """
        判断趋势是否允许继续扩展。

        recent_prices / recent_times：
            当前末端最近一个 window_seconds 的窗口。

        扩展要求：
        1. 最近窗口仍然满足完整趋势条件
           - return
           - path efficiency
           - R2
           - slope
        2. 整个累计趋势段没有发生过大回撤
        """

        if len(prices) < 2:
            return False

        # ---------------------------------
        # 最近一个时间窗必须仍然是趋势
        # ---------------------------------

        if not self.check_window(
                recent_prices,
                recent_times
        ):
            return False

        # ---------------------------------
        # 整段最大回撤限制
        # ---------------------------------

        start = prices[0]

        direction = np.sign(
            prices[-1] - start
        )

        if direction > 0:

            peak = np.maximum.accumulate(
                prices
            )

            drawdown = (
                    peak - prices
            )

        elif direction < 0:

            trough = np.minimum.accumulate(
                prices
            )

            drawdown = (
                    prices - trough
            )

        else:
            return False

        max_dd = np.max(
            drawdown
        )

        trend_move = abs(
            prices[-1] - start
        )

        if trend_move == 0:
            return False

        if (
                max_dd
                >
                trend_move
                *
                self.max_drawdown_ratio
        ):
            return False

        return True


    def _label_segment(self, df):

        # 和 analyze_trend_distribution 保持一致：
        # 先按真实交易所时间排序
        df = (
            df.sort_values(
                "exchange_ts"
            )
            .reset_index(drop=True)
            .copy()
        )

        df["trend_tag"] = "O"


        if len(df) < 2:
            return df


        times = pd.to_datetime(
            df["exchange_ts"]
        )

        prices = (
            df["last_price"]
            .to_numpy()
        )

        n = len(df)


        # 所有窗口长度都转换成真实 timedelta，
        # 不再转换成“多少行”
        window_delta = pd.Timedelta(
            seconds=self.window_seconds
        )

        stride_delta = pd.Timedelta(
            seconds=self.stride_seconds
        )

        extend_step_delta = pd.Timedelta(
            seconds=self.extend_step_seconds
        )

        max_extend_delta = pd.Timedelta(
            seconds=self.max_extend_seconds
        )


        start_time = times.iloc[0]

        last_time = times.iloc[-1]

        current_time = start_time


        while (
            current_time + window_delta
            <=
            last_time
        ):

            # =====================================
            # 1. 构造真实时间 seed window
            # =====================================

            seed_end_time = (
                current_time
                +
                window_delta
            )


            # 对应:
            #
            # current_time
            # <= exchange_ts
            # <= seed_end_time
            #
            # 和 analyze_trend_distribution 的 mask
            # 语义一致。

            start_idx = times.searchsorted(
                current_time,
                side="left"
            )

            seed_end_exclusive = (
                times.searchsorted(
                    seed_end_time,
                    side="right"
                )
            )


            # 时间窗里不足两个真实 tick
            if (
                seed_end_exclusive
                -
                start_idx
                <
                2
            ):

                current_time += (
                    stride_delta
                )

                continue

            seed_prices = prices[
                start_idx:
                seed_end_exclusive
            ]

            seed_times = times.iloc[
                start_idx:
                seed_end_exclusive
            ]


            # =====================================
            # 2. 判断 seed 是否为趋势候选
            # =====================================

            if not self.check_window(
                    seed_prices,
                    seed_times
            ):

                # 注意：
                # 这里只推进真实时间，
                # 不再默认“下一行 = 0.5 秒”
                current_time += (
                    stride_delta
                )

                continue


            # 找到了趋势 seed
            #
            # seed_end_exclusive 是右开区间，
            # 所以真实最后一条数据是 -1
            end_idx = (
                seed_end_exclusive - 1
            )


            # =====================================
            # 3. 基于真实时间向右扩展
            # =====================================

            max_end_time = (
                seed_end_time
                +
                max_extend_delta
            )

            candidate_end_time = (
                seed_end_time
                +
                extend_step_delta
            )


            while (
                candidate_end_time
                <=
                max_end_time
                and
                end_idx < n - 1
            ):

                # 找到 candidate_end_time 之前
                # 所有实际存在的 tick
                candidate_end_exclusive = (
                    times.searchsorted(
                        candidate_end_time,
                        side="right"
                    )
                )

                candidate_end_idx = (
                    candidate_end_exclusive
                    -
                    1
                )


                # 这一时间步内没有新的 tick
                #
                # 例如：
                # 上一条数据 09:30:10.2
                # 下一条数据 09:30:11.2
                #
                # 即使 extend_step_seconds=0.5，
                # 09:30:10.7 时也不会凭空认为
                # 存在一条数据。
                if (
                    candidate_end_idx
                    <=
                    end_idx
                ):

                    candidate_end_time += (
                        extend_step_delta
                    )

                    continue

                test_prices = prices[
                    start_idx:
                    candidate_end_exclusive
                ]

                # 当前末端最近一个 window_seconds
                recent_start_time = (
                        candidate_end_time
                        -
                        window_delta
                )

                recent_start_idx = (
                    times.searchsorted(
                        recent_start_time,
                        side="left"
                    )
                )

                recent_prices = prices[
                    recent_start_idx:
                    candidate_end_exclusive
                ]

                recent_times = times.iloc[
                    recent_start_idx:
                    candidate_end_exclusive
                ]

                if not self.check_extension(
                        test_prices,
                        recent_prices,
                        recent_times
                ):
                    break


                # 扩展成功
                end_idx = (
                    candidate_end_idx
                )


                candidate_end_time += (
                    extend_step_delta
                )


            # =====================================
            # 4. 写 BIO 风格标签
            # =====================================

            df.loc[
                start_idx,
                "trend_tag"
            ] = "B_TREND"


            if (
                end_idx
                >
                start_idx + 1
            ):

                df.loc[
                    start_idx + 1:
                    end_idx - 1,
                    "trend_tag"
                ] = "I_TREND"


            df.loc[
                end_idx,
                "trend_tag"
            ] = "E_TREND"


            # =====================================
            # 5. 跳过已经识别出来的整个事件
            # =====================================

            if end_idx >= n - 1:
                break


            # 这里同样基于真实时间推进，
            # 而不是 end_idx + 1 等价于 0.5 秒
            current_time = (
                times.iloc[end_idx]
                +
                stride_delta
            )


        return df

    def label(self, df):
        """
        对一天数据进行趋势打标。

        自动隔离：
        - instrument
        - AM / PM

        不允许趋势跨 session。
        """

        result = (
            df
            .reset_index(drop=True)
            .copy()
        )

        result["trend_tag"] = "O"

        for segment in iter_continuous_segments(
            result
        ):

            # segment 当前 index 对应 result 的真实行号
            source_indices = (
                segment.index.to_numpy()
            )

            segment_input = (
                segment
                .reset_index(drop=True)
            )

            labeled = (
                self._label_segment(
                    segment_input
                )
            )

            result.loc[
                source_indices,
                "trend_tag"
            ] = (
                labeled["trend_tag"]
                .to_numpy()
            )

        return result

if __name__ == "__main__":
    year = 2024
    trading_day = "0104"
    # trading_day = "0123"
    DATA_PATH = f"/Users/jinhongdou/股指/product=IF/year={year}/trading_day={year}{trading_day}/data.parquet"
    instrument = "IF2403"
    SAVE_PATH = "./labeled_demo.csv"

    df = pd.read_parquet(DATA_PATH)

    # 去掉开盘前字段
    df = df[df["session"] != 'PREOPEN']
    df = df[df['instrument'] == instrument]

    # 计算参数相关性
    # features = plot_trend_distribution(df)

    # feature_list = ['return_tick', 'path_efficiency', 'linear_r2', 'abs_slope_tick_per_second']
    #
    # for i in range(len(feature_list)):
    #     for j in range(i+1, len(feature_list)):
    #         print(f"{feature_list[i]}, {feature_list[j]}: {features[feature_list[i]].corr(features[feature_list[j]])}")
    #
    # print("ok")


    # 单天数据打标
    labeler = TrendLabeler(
        window_seconds=10,
        min_return_tick=10,
        path_efficiency_threshold=0.65,
        linear_r2_threshold=0.65,
        min_abs_slope_tick_per_second=0.0,
    )

    labeled_df = labeler.label(
        df
    )

    labeled_df = labeled_df[['instrument', 'exchange_ts', 'bid_price_1', 'ask_price_1', 'last_price', 'volume', 'session', 'trend_tag']]

    labeled_df.to_csv(SAVE_PATH, index=False)


