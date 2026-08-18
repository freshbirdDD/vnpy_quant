import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os

def calc_linear_efficiency(prices):
    """
    基于线性回归的趋势效率

    返回:
        efficiency: 0~1
        slope: 趋势方向斜率

    R²越高，说明价格越符合线性趋势
    """

    if len(prices) < 2:
        return 0, 0

    x = np.arange(len(prices))

    y = np.array(prices)

    # 一阶线性拟合
    slope, intercept = np.polyfit(x, y, 1)

    y_pred = slope * x + intercept

    # 总平方和
    ss_tot = np.sum(
        (y - np.mean(y)) ** 2
    )

    # 残差平方和
    ss_res = np.sum(
        (y - y_pred) ** 2
    )

    if ss_tot == 0:
        r2 = 0

    else:
        r2 = 1 - ss_res / ss_tot

    return max(r2, 0), slope


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




def analyze_trend_distribution(
    df,
    window_seconds=10,
    step_seconds=0.5,
    tick_size=0.2,
    bins=50,
    min_return_tick=0.0
):
    """
    基于真实时间窗口统计tick趋势指标分布
    """

    df = (
        df.sort_values("exchange_ts")
        .reset_index(drop=True)
    )

    times = df["exchange_ts"]
    prices = df["last_price"].values


    if len(df) < 2:
        return {}


    window_delta = pd.Timedelta(
        seconds=window_seconds
    )

    step_delta = pd.Timedelta(
        seconds=step_seconds
    )


    path_eff_list = []
    linear_R2_list = []
    scope_list = []
    return_list = []


    start_time = times.iloc[0]
    end_time = times.iloc[-1]

    current_time = start_time


    while current_time + window_delta <= end_time:

        end_window_time = (
            current_time + window_delta
        )

        mask = (
            (times >= current_time)
            &
            (times <= end_window_time)
        )


        window = prices[
            mask.values
        ]


        if len(window) >= 2:

            return_tick = (
                abs(window[-1] - window[0])
                /
                tick_size
            )


            if return_tick >= min_return_tick:

                path_eff = calc_path_efficiency(
                    window
                )

                linear_R2, scope = calc_linear_efficiency(
                    window
                )


                path_eff_list.append(
                    path_eff
                )

                linear_R2_list.append(
                    linear_R2
                )

                scope_list.append(
                    scope
                )

                return_list.append(
                    return_tick
                )


        # 无论是否continue，都推进窗口
        current_time += step_delta



    path_eff_arr = np.array(
        path_eff_list
    )

    linear_R2_arr = np.array(
        linear_R2_list
    )

    scope_arr = np.array(
        scope_list
    )

    return_arr = np.array(
        return_list
    )


    result = {
        "path_efficiency": path_eff_arr,
        "linear_R2": linear_R2_arr,
        "scope": scope_arr,
        "return_tick": return_arr
    }


    print("path_efficiency:")
    print(
        np.min(path_eff_arr),
        np.max(path_eff_arr)
    )

    print("\nlinear_R2:")
    print(
        np.min(linear_R2_arr),
        np.max(linear_R2_arr)
    )

    print("\nscope:")
    print(
        np.min(scope_arr),
        np.max(scope_arr)
    )

    print("\nreturn_tick:")
    print(
        np.min(return_arr),
        np.max(return_arr)
    )


    fig, axes = plt.subplots(
        1,
        3,
        figsize=(18,5)
    )


    axes[0].hist(
        path_eff_arr,
        bins=bins
    )

    axes[0].set_title(
        "Path Efficiency"
    )


    axes[1].hist(
        linear_R2_arr,
        bins=bins
    )

    axes[1].set_title(
        "Linear R2"
    )


    axes[2].hist(
        return_arr,
        bins=bins
    )

    axes[2].set_title(
        "Return Tick"
    )


    plt.tight_layout()
    plt.show()


    return result



class TrendLabeler:
    """
    基于价格序列的单边趋势事件筛选器

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
        linear_efficiency_threshold=0.7,
        max_drawdown_ratio=0.4,
        extend_step_seconds=0.5,
        max_extend_seconds=120.0,
    ):
        # 扫描窗口初始时间宽度
        self.window_seconds = window_seconds
        # 两行数据代表的间隔
        self.stride_seconds = stride_seconds

        self.tick_size = tick_size

        # 最小的
        self.min_return_tick = min_return_tick
        # 单边路径效率阈值，0 < efficiency_threshold <= 1
        self.efficiency_threshold = path_efficiency_threshold
        # 单边线性回归斜率阈值
        self.linear_efficiency_threshold = linear_efficiency_threshold
        # 最大回撤比率
        self.max_drawdown_ratio = max_drawdown_ratio

        # 扩展边界步长
        self.extend_step_seconds = extend_step_seconds
        # 最大扩展距离
        self.max_extend_seconds = max_extend_seconds

    def calc_return_tick(self, prices):
        """
        窗口价差
        """
        return abs(prices[-1] - prices[0]) / self.tick_size

    def check_window(self, prices):
        """
        判断窗口是否满足趋势候选
        """

        ret = self.calc_return_tick(prices)

        efficiency = calc_path_efficiency(prices)

        if ret < self.min_return_tick:
            return False

        if efficiency < self.efficiency_threshold:
            return False

        return True

    def check_extension(self, prices):
        """
        判断趋势是否还能继续延伸
        """

        # ret = self.calc_return_tick(prices)

        efficiency = calc_path_efficiency(prices)
        if efficiency < self.efficiency_threshold:
            return False
        # 最大回撤判断
        start = prices[0]

        direction = np.sign(prices[-1] - start)
        if direction > 0:

            peak = np.maximum.accumulate(prices)

            drawdown = peak - prices

        else:

            trough = np.minimum.accumulate(prices)

            drawdown = prices - trough
        max_dd = np.max(drawdown)

        trend_move = abs(prices[-1] - start)
        if trend_move == 0:
            return False
        if max_dd > trend_move * self.max_drawdown_ratio:
            return False
        return True

    def label(self, df):

        df = df.copy()

        df["trend_tag"] = "O"
        # times = df["datetime"].values
        prices = df["last_price"].values
        n = len(df)
        window_size = int(
            self.window_seconds /
            self.stride_seconds
        )
        step = 1
        i = 0 #窗口起点
        while i < n - window_size:
            seed_prices = prices[
                i:i+window_size
            ]
            # 没有趋势候选，窗口起点右移
            if not self.check_window(seed_prices):

                i += step
                continue
            # 找到了B
            start_idx = i
            end_idx = i + window_size - 1
            # 向右扩展
            extend_limit = int(
                self.max_extend_seconds /
                self.stride_seconds
            )
            j = end_idx
            while j < min(
                n - 1,
                end_idx + extend_limit
            ):
                test_prices = prices[
                    start_idx:j+1
                ]
                if not self.check_extension(test_prices):

                    break
                j += 1
            end_idx = j - 1
            # 太短的不算
            if end_idx <= start_idx:

                i += step
                continue
            # 写标签

            df.loc[
                start_idx,
                "trend_tag"
            ] = "B_TREND"
            if end_idx > start_idx + 1:

                df.loc[
                    start_idx+1:end_idx-1,
                    "trend_tag"
                ] = "I_TREND"
            df.loc[
                end_idx,
                "trend_tag"
            ] = "E_TREND"
            # 跳过整个事件

            i = end_idx + 1
        return df


if __name__ == "__main__":
    year = 2024
    trading_day = "0104"
    DATA_PATH = f"/Users/jinhongdou/股指/product=IF/year={year}/trading_day={year}{trading_day}/data.parquet"
    instrument = "IF2403"

    df = pd.read_parquet(DATA_PATH)

    # 去掉开盘前字段
    df = df[df["session"] != 'PREOPEN']

    # 区分上午下午
    # for session in ["AM", "PM"]:
    for session in ["AM",]:
        df_session = df[df["session"] == session]
        df_session = (
            df_session[df_session["instrument"] == instrument]
            .sort_values("exchange_ts")
            .reset_index(drop=True)
        )
        # TODO labeler里的时间逻辑还是按照相邻数据必然是0.5s的，没有统一改成按真实时间戳间隔
        labeler = TrendLabeler(min_return_tick=0.2, path_efficiency_threshold=0.1)
        # df = labeler.label(df)

        dist = analyze_trend_distribution(
            df_session,
            window_seconds=10,
            step_seconds=0.5,
            # min_return_tick=30
        )

        for k, v in dist.items():
            print(
                k,
                np.percentile(
                    v,
                    [50, 90, 95, 99]
                )
            )


