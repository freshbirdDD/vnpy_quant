# clean_if_tick_raw_v2.py
# -*- coding: utf-8 -*-

"""
IF 股指期货 tick 原始数据清洗脚本 v2

定位：
- 只做数据理解和清洗；
- 不做策略优化；
- 不做交易信号；
- 不直接物理删除原始数据；
- 用 alias 统一字段名；
- 标记异常，而不是擅自修复异常；
- 输出全量数据、连续竞价数据、严格连续竞价数据和质量报告。

核心假设：
1. volume / turnover 是当日累计字段；
2. last_volume 在当前数据源中不可靠，后续统一使用 delta_volume；
3. datetime 是行情事件时间，localtime 只作为接收/落库时间参考；
4. IF 连续竞价主要使用 morning / afternoon；
5. before_auction / auction / after_close 原始保留，但第一阶段策略分析不使用。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd


# ============================================================
# 1. 字段 alias 配置
# ============================================================

COLUMN_ALIASES: Dict[str, List[str]] = {
    # ----------------------------
    # 合约与时间
    # ----------------------------
    "symbol": [
        "symbol", "Symbol", "instrument", "InstrumentID", "instrument_id",
        "合约", "合约代码",
    ],
    "exchange": [
        "exchange", "Exchange", "ExchangeID", "exchange_id", "交易所",
    ],
    "datetime": [
        "datetime", "date_time", "Datetime", "DateTime",
        "timestamp", "Timestamp", "行情时间", "时间",
    ],
    "localtime": [
        "localtime", "local_time", "LocalTime", "recv_time",
        "receive_time", "gateway_time", "本地时间", "接收时间",
    ],

    # ----------------------------
    # 成交与持仓
    # ----------------------------
    "last_price": [
        "last_price", "LastPrice", "lastPrice", "last", "price",
        "最新价", "最新成交价",
    ],
    "last_volume": [
        "last_volume", "LastVolume", "lastVolume", "最新成交量",
    ],
    "volume": [
        "volume", "Volume", "cum_volume", "累计成交量", "成交量",
    ],
    "turnover": [
        "turnover", "Turnover", "amount", "Amount", "成交额", "累计成交额",
    ],
    "open_interest": [
        "open_interest", "OpenInterest", "openInterest", "持仓量",
    ],

    # ----------------------------
    # 价格状态
    # ----------------------------
    "open_price": [
        "open_price", "OpenPrice", "open", "开盘价",
    ],
    "high_price": [
        "high_price", "HighestPrice", "high", "最高价",
    ],
    "low_price": [
        "low_price", "LowestPrice", "low", "最低价",
    ],
    "pre_close": [
        "pre_close", "PreClosePrice", "preClose", "昨收价", "昨收", "前收盘价",
    ],
    "pre_settlement": [
        "preSettlementPrice", "PreSettlementPrice", "pre_settlement",
        "pre_settlement_price", "昨结算价", "前结算价",
    ],
    "settlement_price": [
        "settlementPrice", "SettlementPrice", "settlement_price", "结算价",
    ],
    "close_price": [
        "closePrice", "ClosePrice", "close_price", "收盘价",
    ],
    "average_price": [
        "averagePrice", "AveragePrice", "average_price", "均价",
    ],
    "limit_up": [
        "limit_up", "UpperLimitPrice", "upper_limit", "涨停价",
    ],
    "limit_down": [
        "limit_down", "LowerLimitPrice", "lower_limit", "跌停价",
    ],

    # ----------------------------
    # 买盘价格 1-5
    # ----------------------------
    "bid_price_1": ["bid_price_1", "BidPrice1", "bidPrice1", "bid1", "买一价"],
    "bid_price_2": ["bid_price_2", "BidPrice2", "bidPrice2", "bid2", "买二价"],
    "bid_price_3": ["bid_price_3", "BidPrice3", "bidPrice3", "bid3", "买三价"],
    "bid_price_4": ["bid_price_4", "BidPrice4", "bidPrice4", "bid4", "买四价"],
    "bid_price_5": ["bid_price_5", "BidPrice5", "bidPrice5", "bid5", "买五价"],

    # ----------------------------
    # 卖盘价格 1-5
    # ----------------------------
    "ask_price_1": ["ask_price_1", "AskPrice1", "askPrice1", "ask1", "卖一价"],
    "ask_price_2": ["ask_price_2", "AskPrice2", "askPrice2", "ask2", "卖二价"],
    "ask_price_3": ["ask_price_3", "AskPrice3", "askPrice3", "ask3", "卖三价"],
    "ask_price_4": ["ask_price_4", "AskPrice4", "askPrice4", "ask4", "卖四价"],
    "ask_price_5": ["ask_price_5", "AskPrice5", "askPrice5", "ask5", "卖五价"],

    # ----------------------------
    # 买盘数量 1-5
    # ----------------------------
    "bid_volume_1": ["bid_volume_1", "BidVolume1", "bidVolume1", "bid_size_1", "买一量"],
    "bid_volume_2": ["bid_volume_2", "BidVolume2", "bidVolume2", "bid_size_2", "买二量"],
    "bid_volume_3": ["bid_volume_3", "BidVolume3", "bidVolume3", "bid_size_3", "买三量"],
    "bid_volume_4": ["bid_volume_4", "BidVolume4", "bidVolume4", "bid_size_4", "买四量"],
    "bid_volume_5": ["bid_volume_5", "BidVolume5", "bidVolume5", "bid_size_5", "买五量"],

    # ----------------------------
    # 卖盘数量 1-5
    # ----------------------------
    "ask_volume_1": ["ask_volume_1", "AskVolume1", "askVolume1", "ask_size_1", "卖一量"],
    "ask_volume_2": ["ask_volume_2", "AskVolume2", "askVolume2", "ask_size_2", "卖二量"],
    "ask_volume_3": ["ask_volume_3", "AskVolume3", "askVolume3", "ask_size_3", "卖三量"],
    "ask_volume_4": ["ask_volume_4", "AskVolume4", "askVolume4", "ask_size_4", "卖四量"],
    "ask_volume_5": ["ask_volume_5", "AskVolume5", "askVolume5", "ask_size_5", "卖五量"],
}


# ============================================================
# 2. 配置参数
# ============================================================

@dataclass
class CleanConfig:
    # IF 合约乘数：每点 300 元
    multiplier: float = 300.0

    # IF 最小变动价位，通常为 0.2 点
    tick_size: float = 0.2

    # mild / medium / severe gap 阈值，单位秒
    mild_gap_seconds: float = 5.0
    medium_gap_seconds: float = 10.0
    severe_gap_seconds: float = 30.0

    # 连续竞价 session
    regular_sessions: Tuple[str, ...] = ("morning", "afternoon")

    # 是否保留原始行号
    keep_raw_row_id: bool = True


# ============================================================
# 3. alias 映射
# ============================================================

def build_rename_map(
    raw_columns: List[str],
    aliases: Dict[str, List[str]],
) -> Dict[str, str]:
    """
    根据原始列名和 alias 表构造 rename 映射。

    返回：
        {原始列名: 标准列名}
    """
    raw_set = set(raw_columns)
    rename_map: Dict[str, str] = {}

    for standard_name, candidates in aliases.items():
        matched = [c for c in candidates if c in raw_set]

        if not matched:
            continue

        # 如果多个 alias 同时命中，优先使用候选列表里的第一个。
        # 当前策略是温和处理，不主动报错。
        raw_name = matched[0]

        # 避免两个原始字段重命名到同一个标准字段后冲突。
        # 如果标准字段本身已经存在，则不覆盖。
        if standard_name in raw_set and raw_name != standard_name:
            continue

        rename_map[raw_name] = standard_name

    return rename_map


def apply_alias(df: pd.DataFrame) -> pd.DataFrame:
    """
    把原始列名统一映射成标准列名。
    """
    df = df.copy()
    rename_map = build_rename_map(list(df.columns), COLUMN_ALIASES)
    df = df.rename(columns=rename_map)
    return df


def check_required_columns(
    df: pd.DataFrame,
    required_cols: List[str],
) -> Tuple[List[str], List[str]]:
    """
    检查必需字段是否存在。
    """
    existing_cols = [c for c in required_cols if c in df.columns]
    missing_cols = [c for c in required_cols if c not in df.columns]
    return existing_cols, missing_cols


# ============================================================
# 4. 时间字段处理
# ============================================================

def parse_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    解析 datetime / localtime 字段。
    """
    df = df.copy()

    if "datetime" not in df.columns:
        raise ValueError(
            "缺少 datetime 字段。请在 COLUMN_ALIASES 中补充原始时间列名。"
        )

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

    if "localtime" in df.columns:
        df["localtime"] = pd.to_datetime(df["localtime"], errors="coerce")
    else:
        df["localtime"] = pd.NaT

    df["date"] = df["datetime"].dt.date
    df["time_str"] = df["datetime"].dt.strftime("%H:%M:%S")

    return df


# ============================================================
# 5. session 标记
# ============================================================

def assign_session(df: pd.DataFrame) -> pd.DataFrame:
    """
    标记 IF 日内时段。

    before_auction: 09:25 前
    auction:        09:25-09:30
    morning:        09:30-11:30
    mid_break:      11:30-13:00
    afternoon:      13:00-15:00
    after_close:    15:00 后
    other:          兜底
    """
    df = df.copy()
    t = df["time_str"]

    df["session"] = "other"

    df.loc[t < "09:25:00", "session"] = "before_auction"

    df.loc[
        (t >= "09:25:00") & (t < "09:30:00"),
        "session",
    ] = "auction"

    df.loc[
        (t >= "09:30:00") & (t <= "11:30:00"),
        "session",
    ] = "morning"

    df.loc[
        (t > "11:30:00") & (t < "13:00:00"),
        "session",
    ] = "mid_break"

    df.loc[
        (t >= "13:00:00") & (t <= "15:00:00"),
        "session",
    ] = "afternoon"

    df.loc[t > "15:00:00", "session"] = "after_close"

    df["is_regular_session"] = df["session"].isin(["morning", "afternoon"])
    df["is_auction"] = df["session"].eq("auction")
    df["is_bad_session"] = ~df["session"].isin(["auction", "morning", "afternoon"])

    return df


# ============================================================
# 6. 数值字段处理
# ============================================================

def coerce_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    把核心行情字段转成 numeric。
    """
    df = df.copy()

    numeric_cols = [
        # 成交与持仓
        "last_price", "last_volume", "volume", "turnover", "open_interest",

        # 价格状态
        "average_price", "open_price", "high_price", "low_price",
        "pre_close", "pre_settlement", "settlement_price", "close_price",
        "limit_up", "limit_down",

        # 一到五档价格
        "bid_price_1", "bid_price_2", "bid_price_3", "bid_price_4", "bid_price_5",
        "ask_price_1", "ask_price_2", "ask_price_3", "ask_price_4", "ask_price_5",

        # 一到五档数量
        "bid_volume_1", "bid_volume_2", "bid_volume_3", "bid_volume_4", "bid_volume_5",
        "ask_volume_1", "ask_volume_2", "ask_volume_3", "ask_volume_4", "ask_volume_5",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ============================================================
# 7. 排序与基础特征
# ============================================================

def sort_ticks(df: pd.DataFrame) -> pd.DataFrame:
    """
    按 symbol + datetime + localtime 排序。
    """
    df = df.copy()

    sort_cols: List[str] = []
    if "symbol" in df.columns:
        sort_cols.append("symbol")

    sort_cols.append("datetime")

    if "localtime" in df.columns:
        sort_cols.append("localtime")

    df = df.sort_values(sort_cols).reset_index(drop=True)

    return df


def _group_day_cols(df: pd.DataFrame) -> List[str]:
    if "symbol" in df.columns:
        return ["symbol", "date"]
    return ["date"]


def _group_session_cols(df: pd.DataFrame) -> List[str]:
    if "symbol" in df.columns:
        return ["symbol", "date", "session"]
    return ["date", "session"]


def add_basic_features(df: pd.DataFrame, config: CleanConfig) -> pd.DataFrame:
    """
    构造基础数据理解字段。
    """
    df = df.copy()

    group_day = _group_day_cols(df)
    group_session = _group_session_cols(df)

    # --------------------------------------------------------
    # 1. 累计成交量 / 成交额的增量
    # --------------------------------------------------------
    if "volume" in df.columns:
        df["delta_volume"] = df.groupby(group_day)["volume"].diff().fillna(0)

    if "turnover" in df.columns:
        df["delta_turnover"] = df.groupby(group_day)["turnover"].diff().fillna(0)

    if "open_interest" in df.columns:
        df["delta_open_interest"] = df.groupby(group_day)["open_interest"].diff()

    # --------------------------------------------------------
    # 2. VWAP
    # --------------------------------------------------------
    if {"turnover", "volume"}.issubset(df.columns):
        df["cum_vwap"] = df["turnover"] / df["volume"] / config.multiplier
        df.loc[df["volume"] <= 0, "cum_vwap"] = np.nan

    if {"delta_turnover", "delta_volume"}.issubset(df.columns):
        df["tick_vwap"] = df["delta_turnover"] / df["delta_volume"] / config.multiplier
        df.loc[df["delta_volume"] <= 0, "tick_vwap"] = np.nan

    # --------------------------------------------------------
    # 3. 一档盘口
    # --------------------------------------------------------
    if {"bid_price_1", "ask_price_1"}.issubset(df.columns):
        df["mid_price"] = (df["bid_price_1"] + df["ask_price_1"]) / 2
        df["spread"] = df["ask_price_1"] - df["bid_price_1"]
        df["spread_ticks"] = df["spread"] / config.tick_size

    if {"bid_volume_1", "ask_volume_1"}.issubset(df.columns):
        denom = df["bid_volume_1"] + df["ask_volume_1"]
        df["obi_1"] = (df["bid_volume_1"] - df["ask_volume_1"]) / denom
        df.loc[denom <= 0, "obi_1"] = np.nan

    if {
        "bid_price_1", "ask_price_1",
        "bid_volume_1", "ask_volume_1",
    }.issubset(df.columns):
        denom = df["bid_volume_1"] + df["ask_volume_1"]
        df["micro_price"] = (
            df["ask_price_1"] * df["bid_volume_1"]
            + df["bid_price_1"] * df["ask_volume_1"]
        ) / denom
        df.loc[denom <= 0, "micro_price"] = np.nan

    # --------------------------------------------------------
    # 4. 五档深度，后续不用也没关系
    # --------------------------------------------------------
    bid_vol_cols = [f"bid_volume_{i}" for i in range(1, 6)]
    ask_vol_cols = [f"ask_volume_{i}" for i in range(1, 6)]

    if set(bid_vol_cols).issubset(df.columns):
        df["bid_depth_5"] = df[bid_vol_cols].sum(axis=1)

    if set(ask_vol_cols).issubset(df.columns):
        df["ask_depth_5"] = df[ask_vol_cols].sum(axis=1)

    if {"bid_depth_5", "ask_depth_5"}.issubset(df.columns):
        total_depth = df["bid_depth_5"] + df["ask_depth_5"]
        df["depth_5"] = total_depth
        df["obi_5"] = (df["bid_depth_5"] - df["ask_depth_5"]) / total_depth
        df.loc[total_depth <= 0, "obi_5"] = np.nan

    # --------------------------------------------------------
    # 5. session 内时间间隔
    # --------------------------------------------------------
    df["dt_gap"] = df.groupby(group_session)["datetime"].diff()
    df["dt_gap_seconds"] = df["dt_gap"].dt.total_seconds()

    return df


# ============================================================
# 8. 异常标记
# ============================================================

def add_quality_flags(df: pd.DataFrame, config: CleanConfig) -> pd.DataFrame:
    """
    添加数据质量标记。
    这里只标记，不删除。
    """
    df = df.copy()

    # --------------------------------------------------------
    # 1. 时间与 session
    # --------------------------------------------------------
    df["flag_bad_datetime"] = df["datetime"].isna()
    df["flag_non_regular_session"] = ~df["is_regular_session"]

    # --------------------------------------------------------
    # 2. 累计字段增量异常
    # --------------------------------------------------------
    df["flag_negative_delta_volume"] = (
        df["delta_volume"].lt(0) if "delta_volume" in df.columns else False
    )
    df["flag_negative_delta_turnover"] = (
        df["delta_turnover"].lt(0) if "delta_turnover" in df.columns else False
    )

    # --------------------------------------------------------
    # 3. 拆分价格非正异常
    # --------------------------------------------------------
    df["flag_non_positive_last_price"] = (
        df["last_price"].le(0) if "last_price" in df.columns else False
    )

    df["flag_non_positive_l1_price"] = (
        (df["bid_price_1"].le(0) | df["ask_price_1"].le(0))
        if {"bid_price_1", "ask_price_1"}.issubset(df.columns)
        else False
    )

    df["flag_non_positive_limit_price"] = (
        (df["limit_up"].le(0) | df["limit_down"].le(0))
        if {"limit_up", "limit_down"}.issubset(df.columns)
        else False
    )

    df["flag_non_positive_ohl_price"] = (
        (
            df["open_price"].le(0)
            | df["high_price"].le(0)
            | df["low_price"].le(0)
        )
        if {"open_price", "high_price", "low_price"}.issubset(df.columns)
        else False
    )

    df["flag_any_non_positive_price"] = (
        df["flag_non_positive_last_price"]
        | df["flag_non_positive_l1_price"]
        | df["flag_non_positive_limit_price"]
        | df["flag_non_positive_ohl_price"]
    )

    # --------------------------------------------------------
    # 4. spread 异常
    # --------------------------------------------------------
    df["flag_bad_spread"] = (
        (
            df["bid_price_1"].gt(0)
            & df["ask_price_1"].gt(0)
            & df["spread"].le(0)
        )
        if {"bid_price_1", "ask_price_1", "spread"}.issubset(df.columns)
        else False
    )

    df["flag_spread_not_tick_multiple"] = (
        (
            df["spread_ticks"].notna()
            & ((df["spread_ticks"].round() - df["spread_ticks"]).abs() > 1e-6)
        )
        if "spread_ticks" in df.columns
        else False
    )

    # --------------------------------------------------------
    # 5. 涨跌停越界
    # --------------------------------------------------------
    df["flag_last_price_out_of_limit"] = (
        (
            df["last_price"].gt(0)
            & df["limit_up"].gt(0)
            & df["limit_down"].gt(0)
            & (
                (df["last_price"] > df["limit_up"])
                | (df["last_price"] < df["limit_down"])
            )
        )
        if {"last_price", "limit_up", "limit_down"}.issubset(df.columns)
        else False
    )

    # --------------------------------------------------------
    # 6. high / low / last 一致性
    # --------------------------------------------------------
    df["flag_bad_high_low_last"] = (
        (
            df["last_price"].gt(0)
            & df["high_price"].gt(0)
            & df["low_price"].gt(0)
            & (
                (df["high_price"] < df["low_price"])
                | (df["last_price"] > df["high_price"])
                | (df["last_price"] < df["low_price"])
            )
        )
        if {"high_price", "low_price", "last_price"}.issubset(df.columns)
        else False
    )

    # --------------------------------------------------------
    # 7. gap 分级
    # --------------------------------------------------------
    df["gap_level"] = "normal"

    df.loc[
        df["dt_gap_seconds"] > config.mild_gap_seconds,
        "gap_level",
    ] = "mild_gap"

    df.loc[
        df["dt_gap_seconds"] > config.medium_gap_seconds,
        "gap_level",
    ] = "medium_gap"

    df.loc[
        df["dt_gap_seconds"] > config.severe_gap_seconds,
        "gap_level",
    ] = "severe_gap"

    df["flag_large_gap"] = df["gap_level"].ne("normal")

    # dt_gap_seconds 是记录在 gap 后第一条 tick 上。
    df["flag_after_medium_or_severe_gap"] = df["gap_level"].isin(
        ["medium_gap", "severe_gap"]
    )

    # --------------------------------------------------------
    # 8. 重复 datetime
    # --------------------------------------------------------
    duplicate_keys = ["datetime"]
    if "symbol" in df.columns:
        duplicate_keys = ["symbol", "datetime"]

    df["flag_duplicate_symbol_datetime"] = df.duplicated(
        duplicate_keys,
        keep=False,
    )

    # --------------------------------------------------------
    # 9. last_volume 可靠性提示
    # --------------------------------------------------------
    # 当前数据源中 last_volume 大量不等于 volume.diff()，
    # 后续统一使用 delta_volume。这个 flag 只是信息提示，不参与核心异常。
    df["flag_last_volume_mismatch"] = (
        (
            df["last_volume"].notna()
            & df["delta_volume"].notna()
            & (df["last_volume"] != df["delta_volume"])
        )
        if {"last_volume", "delta_volume"}.issubset(df.columns)
        else False
    )

    df["is_last_volume_reliable"] = False

    # --------------------------------------------------------
    # 10. 核心异常：raw / regular 分开
    # --------------------------------------------------------
    core_flag_cols = [
        "flag_bad_datetime",
        "flag_negative_delta_volume",
        "flag_negative_delta_turnover",
        "flag_bad_spread",
        "flag_last_price_out_of_limit",
        "flag_bad_high_low_last",
    ]

    df["flag_core_bad_raw"] = df[core_flag_cols].any(axis=1)
    df["flag_core_bad_regular"] = df["is_regular_session"] & df["flag_core_bad_raw"]

    return df


# ============================================================
# 9. 数据质量报告
# ============================================================

def _describe_series(s: pd.Series) -> Dict[str, float]:
    return s.describe(percentiles=[0.5, 0.9, 0.99, 0.999]).to_dict()


def build_quality_report(df: pd.DataFrame) -> Dict[str, object]:
    """
    生成基础数据质量报告。
    """
    report: Dict[str, object] = {}

    report["n_rows"] = len(df)
    report["columns"] = list(df.columns)

    if "symbol" in df.columns:
        report["n_symbols"] = df["symbol"].nunique(dropna=True)
        report["symbols"] = sorted(df["symbol"].dropna().unique().tolist())
    else:
        report["n_symbols"] = None
        report["symbols"] = None

    if "datetime" in df.columns:
        report["datetime_min"] = df["datetime"].min()
        report["datetime_max"] = df["datetime"].max()
        report["n_dates"] = df["date"].nunique(dropna=True)

    if "session" in df.columns:
        report["session_counts"] = df["session"].value_counts(dropna=False).to_dict()

    flag_cols = [c for c in df.columns if c.startswith("flag_")]

    report["flag_counts"] = {
        c: int(df[c].sum()) for c in flag_cols if df[c].dtype == bool
    }

    df_regular = df[df["is_regular_session"]].copy()

    report["regular_n_rows"] = len(df_regular)

    report["regular_flag_counts"] = {
        c: int(df_regular[c].sum()) for c in flag_cols if df_regular[c].dtype == bool
    }

    if "gap_level" in df.columns:
        report["gap_level_counts"] = (
            df["gap_level"].value_counts(dropna=False).to_dict()
        )
        report["regular_gap_level_counts"] = (
            df_regular["gap_level"].value_counts(dropna=False).to_dict()
        )

    if "dt_gap_seconds" in df.columns:
        report["dt_gap_seconds_describe"] = _describe_series(df["dt_gap_seconds"])
        report["regular_dt_gap_seconds_describe"] = _describe_series(
            df_regular["dt_gap_seconds"]
        )

    for col in [
        "delta_volume",
        "delta_turnover",
        "spread",
        "cum_vwap",
        "tick_vwap",
        "obi_1",
        "obi_5",
    ]:
        if col in df.columns:
            report[f"{col}_describe"] = _describe_series(df[col])
        if col in df_regular.columns:
            report[f"regular_{col}_describe"] = _describe_series(df_regular[col])

    return report


# ============================================================
# 10. 主清洗函数
# ============================================================

def clean_if_tick_data(
    df_raw: pd.DataFrame,
    config: Optional[CleanConfig] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, object]]:
    """
    清洗 IF tick 原始数据。

    返回：
        df_all:
            全量数据，保留所有 session，带 flag。
        df_regular:
            只保留 morning / afternoon 连续竞价数据，不因为 gap 直接删除。
        df_regular_strict:
            连续竞价严格版，去掉核心异常和 medium/severe gap 后第一条。
        quality_report:
            数据质量报告。
    """
    if config is None:
        config = CleanConfig()

    df = df_raw.copy()

    if config.keep_raw_row_id:
        df["_raw_row_id"] = np.arange(len(df))

    # 1. alias 标准化
    df = apply_alias(df)

    # 2. 检查核心字段
    required_core = [
        "datetime",
        "last_price",
        "volume",
        "turnover",
        "open_interest",
        "bid_price_1",
        "ask_price_1",
        "bid_volume_1",
        "ask_volume_1",
    ]

    _, missing_core = check_required_columns(df, required_core)

    if missing_core:
        print("[WARN] 缺少核心字段：", missing_core)
        print("[WARN] 请检查 COLUMN_ALIASES 是否需要补充。")

    # 3. 类型与排序
    df = parse_datetime_columns(df)
    df = coerce_numeric_columns(df)
    df = sort_ticks(df)

    # 4. session
    df = assign_session(df)

    # 5. 基础字段与 flag
    df = add_basic_features(df, config)
    df = add_quality_flags(df, config)

    # 6. 连续竞价样本
    df_regular = df[
        df["session"].isin(config.regular_sessions)
        & ~df["flag_bad_datetime"]
    ].copy()

    # 7. 严格版连续竞价样本
    # 注意：不删除 mild_gap，只删除 medium/severe gap 后第一条。
    df_regular_strict = df_regular[
        ~df_regular["flag_core_bad_regular"]
        & ~df_regular["flag_after_medium_or_severe_gap"]
    ].copy()

    # 8. 报告
    quality_report = build_quality_report(df)

    return df, df_regular, df_regular_strict, quality_report


# ============================================================
# 11. 使用示例
# ============================================================

if __name__ == "__main__":
    # 示例：
    #
    # import pandas as pd
    #
    # df_raw = pd.read_csv("IF_tick_sample.csv")
    #
    # config = CleanConfig(
    #     multiplier=300.0,
    #     tick_size=0.2,
    #     mild_gap_seconds=5.0,
    #     medium_gap_seconds=10.0,
    #     severe_gap_seconds=30.0,
    # )
    #
    # df_all, df_regular, df_regular_strict, report = clean_if_tick_data(df_raw, config)
    #
    # print(report)
    #
    # df_all.to_parquet("IF_tick_clean_all.parquet", index=False)
    # df_regular.to_parquet("IF_tick_regular.parquet", index=False)
    # df_regular_strict.to_parquet("IF_tick_regular_strict.parquet", index=False)

    pass
