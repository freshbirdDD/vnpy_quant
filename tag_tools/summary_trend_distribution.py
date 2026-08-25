import pandas as pd
import numpy as np


INPUT_CSV = (
    "./"
    "IF_trend_distribution_summary.csv"
)

OUTPUT_CSV = (
    "./"
    "trend_hyperparam_reference.csv"
)


# ==========================================================
# 配置
# ==========================================================

LOW_VOLUME_PERCENTILE = 0.2

FEATURES = [
    "return_tick",
    "path_efficiency",
    "linear_r2",
    "abs_slope_tick_per_second",
]

STAT_PERCENTILES = [
    0.50,
    0.75,
    0.90,
    0.95,
    0.99,
]


# ==========================================================
# 加权统计
# ==========================================================

def weighted_quantile(
    values,
    weights,
    quantile
):
    """
    weighted percentile
    """

    values = np.asarray(
        values
    )

    weights = np.asarray(
        weights
    )

    mask = (
        np.isfinite(values)
        &
        np.isfinite(weights)
    )

    values = values[mask]
    weights = weights[mask]

    if len(values) == 0:
        return np.nan


    sorter = np.argsort(
        values
    )

    values = values[sorter]
    weights = weights[sorter]

    cumulative_weight = np.cumsum(
        weights
    )

    cutoff = (
        quantile
        *
        cumulative_weight[-1]
    )

    idx = np.searchsorted(
        cumulative_weight,
        cutoff
    )

    idx = min(
        idx,
        len(values)-1
    )

    return values[idx]



def weighted_feature_summary(
    df,
    name
):

    result = {
        "group": name,
        "sample_count": len(df),
        "total_volume": (
            df["daily_volume"]
            .sum()
        )
    }


    for feature in FEATURES:

        for p in STAT_PERCENTILES:

            column = (
                f"{feature}_p"
                +
                str(int(p*100))
            )

            result[
                f"{column}_weighted"
            ] = weighted_quantile(
                df[column],
                df["daily_volume"],
                p
            )

    return result



# ==========================================================
# 主流程
# ==========================================================


df = pd.read_csv(
    INPUT_CSV
)


df["trading_day"] = pd.to_datetime(
    df["trading_day"]
)


# ----------------------------------------------------------
# 1. 合约类型
# ----------------------------------------------------------

def is_quarter_contract(
    instrument
):
    """
    IF2403 / IF2406 / IF2409 / IF2412
    """

    month = int(
        instrument[-2:]
    )

    return month in [
        3,
        6,
        9,
        12
    ]


df["contract_type"] = (
    df["instrument"]
    .apply(
        lambda x:
        "quarterly"
        if is_quarter_contract(x)
        else "monthly"
    )
)



# ----------------------------------------------------------
# 2. 去掉低成交量20%
# ----------------------------------------------------------

volume_threshold = (
    df["daily_volume"]
    .quantile(
        LOW_VOLUME_PERCENTILE
    )
)


print(
    "volume threshold:",
    volume_threshold
)


filtered = df[
    df["daily_volume"]
    >=
    volume_threshold
].copy()


print(
    "before:",
    len(df),
    "after:",
    len(filtered)
)



# ----------------------------------------------------------
# 3. 不同样本空间统计
# ----------------------------------------------------------

results = []


# 全部
results.append(
    weighted_feature_summary(
        filtered,
        "all_liquid"
    )
)


# 月度/季月
for contract_type, group in (
    filtered
    .groupby("contract_type")
):

    results.append(
        weighted_feature_summary(
            group,
            contract_type
        )
    )



# ----------------------------------------------------------
# 4. 输出
# ----------------------------------------------------------

result_df = pd.DataFrame(
    results
)


result_df.to_csv(
    OUTPUT_CSV,
    index=False,
    encoding="utf-8-sig"
)


print(
    result_df
)

print(
    "saved:",
    OUTPUT_CSV
)