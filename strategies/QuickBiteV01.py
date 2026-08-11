from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Deque, List, Optional, Dict, Any

import numpy as np

from vnpy_ctastrategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
)
from vnpy.trader.constant import Direction, Offset, Status


# -----------------------------
# 滚动分位数（低频刷新阈值，避免每tick算分位数）
# -----------------------------
@dataclass
class RollingQuantiles:
    maxlen: int
    values: Deque[float]

    def __init__(self, maxlen: int):
        self.maxlen = maxlen
        self.values = deque(maxlen=maxlen)

    def push(self, x: float) -> None:
        if x is None:
            return
        if np.isfinite(x):
            self.values.append(float(x))

    def ready(self, min_n: int) -> bool:
        return len(self.values) >= min_n

    def quantile(self, q: float) -> float:
        if not self.values:
            return 0.0
        arr = np.asarray(self.values, dtype=float)
        return float(np.quantile(arr, q))

    def mean(self) -> float:
        if not self.values:
            return 0.0
        return float(np.mean(self.values))


def _clip01(x: float) -> float:
    return 0.0 if x <= 0 else (1.0 if x >= 1.0 else x)


class MicroEventScalpIF_Scored(CtaTemplate):
    """
    IF Tick 级微观结构事件剥头皮（打分模型 + 分档执行）

    数据需求：
    - 五档盘口：bid/ask price_1..5, volume_1..5
    - tick字段：volume, turnover, open_interest
    - tick频率：0.5s（也可更快/更慢，但窗口换算会变化）

    事件体系：
    1) Event 总触发（event_score）：
       - 价格冲击强度：max(rv_norm, mom_norm)
       - 成交强度：max(dvol_norm, dturn_norm)
       二者都要 >=1（超过各自滚动分位阈值）且 event_score >= enter_score 才进入事件。

    2) Event 分型（E1 vs E2）：
       - E1 Withdrawal：spread_excess、depth_low、lambda_high、且 flow_not_extreme
       - E2 Absorption：flow_very_high、spread_good、depth_good、lambda_low
       取高分者，且超过 min_type_score、并且领先对方 diff 才定型，否则视为“非可交易事件”。

    3) 分档执行（tier 1/2/3）：
       - tier由 event_score 映射（越强越高）
       - 默认 max_exec_tier=2（稳健：不启用最激进档）
       - 每个事件类型在不同 tier 下有不同的：入场门槛、最大spread、追价、止损、追踪、最长持仓、加仓次数等。

    手续费模型（人为设定）：
    - 开仓手续费 open_fee_rmb
    - 平仓手续费：每日前 close_fee_first_n 手为 close_fee_first_rmb，之后为 close_fee_after_rmb
    - 成本折算 ticks 用于：锁盈门槛/追踪退出判断等
    """

    author = "Quant"

    # =========================
    # 基础参数（合约&数据）
    # =========================
    tick_interval_sec: float = 0.5

    pricetick_override: float = 0.0     # 0则尝试用合约属性；IF样本常见0.2
    contract_size_override: int = 0     # 0则尝试用合约属性；IF常见300

    # =========================
    # 窗口参数（秒 -> tick数）
    # =========================
    win_mom_sec: float = 6.0            # 建议 4~10s
    win_rv_sec: float = 6.0             # 建议 4~10s
    win_ofi_sec: float = 2.0            # 建议 1~3s
    win_doi_sec: float = 10.0           # 建议 6~15s

    pullback_min_ticks: int = 1         # 回踩最小1跳
    pullback_lookback_sec: float = 6.0  # 回踩状态机参考时长（用于“峰值”更新的记忆）
                                         # 注：这里用 pending_peak_mid，不用完整lookback极值，够稳健

    # =========================
    # 阈值统计窗口（滚动分位数）
    # =========================
    stat_window_sec: float = 480.0      # 8分钟；建议 300~900s
    stat_min_points: int = 200
    threshold_refresh_sec: float = 5.0  # 建议 2~10s

    # Event总触发分位数（越高越挑剔）
    q_rv: float = 0.80                  # 建议 0.75~0.90
    q_mom: float = 0.80                 # 建议 0.75~0.90
    q_dvol: float = 0.70                # 建议 0.60~0.85
    q_dturn: float = 0.70               # 建议 0.60~0.85

    # 分型阈值分位数
    q_depth_low: float = 0.20           # depth低阈值；建议 0.10~0.30
    q_dvol_high: float = 0.90           # “成交极高”；建议 0.85~0.95
    q_lambda_high: float = 0.90         # lambda_proxy高；建议 0.85~0.95
    q_spread_med: float = 0.50
    q_spread_p90: float = 0.90

    # =========================
    # 打分&分档（可调）
    # =========================
    # event_score 进入阈值：建议 35~60（越高越少交易、越稳）
    event_enter_score: float = 40.0

    # event_score -> tier 映射阈值
    tier2_score: float = 55.0           # >=55 进入 tier2
    tier3_score: float = 75.0           # >=75 进入 tier3（默认不启用）
    max_exec_tier: int = 2              # 默认2：稳健，不启用最激进档（可改3）

    # 分型打分最小要求/领先差
    min_type_score: float = 35.0        # 建议 25~50
    type_score_diff: float = 5.0        # 建议 3~15

    # event_score权重（价格/成交）
    w_price: float = 0.5
    w_flow: float = 0.5
    # 归一化的饱和尺度：norm从1到(1+scale)映射到0~1
    price_scale: float = 1.0            # norm=2时饱和；建议 0.6~1.5
    flow_scale: float = 1.0             # 同上

    # =========================
    # 盘口可交易硬约束（风控）
    # =========================
    spread_hard_stop: int = 6           # spread>=此值：不交易/持仓强平；建议 4~8
    depth_hard_min: int = 25            # depth5<此值：不交易/持仓强平；建议 20~35

    # =========================
    # 手续费 & 滑点（用于成本折算ticks）
    # =========================
    open_fee_rmb: float = 26.0
    close_fee_first_n: int = 10
    close_fee_first_rmb: float = 26.0
    close_fee_after_rmb: float = 260.0

    slippage_ticks: int = 2             # 单边预期滑点（ticks）
                                         # 样本报价滑点代理P95多为2~3
                                         # 若按你们“2点=10ticks”保守：改成10

    # =========================
    # 基础手数 & 日内风控
    # =========================
    base_volume: int = 1
    max_trades_per_day: int = 200
    max_daily_loss_rmb: float = 10000.0

    # =============== VN显示参数列表 ===============
    parameters = [
        "tick_interval_sec",
        "pricetick_override", "contract_size_override",
        "win_mom_sec", "win_rv_sec", "win_ofi_sec", "win_doi_sec",
        "pullback_min_ticks", "pullback_lookback_sec",
        "stat_window_sec", "stat_min_points", "threshold_refresh_sec",
        "q_rv", "q_mom", "q_dvol", "q_dturn",
        "q_depth_low", "q_dvol_high", "q_lambda_high", "q_spread_med", "q_spread_p90",
        "event_enter_score", "tier2_score", "tier3_score", "max_exec_tier",
        "min_type_score", "type_score_diff",
        "w_price", "w_flow", "price_scale", "flow_scale",
        "spread_hard_stop", "depth_hard_min",
        "open_fee_rmb", "close_fee_first_n", "close_fee_first_rmb", "close_fee_after_rmb",
        "slippage_ticks",
        "base_volume", "max_trades_per_day", "max_daily_loss_rmb",
    ]

    # =========================
    # 运行时变量（VN界面可见）
    # =========================
    event_type: int = 0        # 0=无事件/不交易, 1=E1 Withdrawal, 2=E2 Absorption
    exec_tier: int = 0         # 0=无, 1/2/3执行档
    event_score: float = 0.0
    score_e1: float = 0.0
    score_e2: float = 0.0

    mom6: float = 0.0
    rv6: float = 0.0
    spread_ticks: float = 0.0
    depth5: float = 0.0
    imb5: float = 0.0
    ofi2: float = 0.0
    dvol6: float = 0.0
    dturn6: float = 0.0
    doi10: float = 0.0

    variables = [
        "event_type", "exec_tier", "event_score", "score_e1", "score_e2",
        "mom6", "rv6",
        "spread_ticks", "depth5", "imb5", "ofi2",
        "dvol6", "dturn6", "doi10",
    ]

    # =========================
    # 初始化
    # =========================
    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        # 合约参数
        self._pricetick: float = 0.0
        self._size: int = 0

        # 窗口长度（ticks）
        self._n_mom = max(1, int(self.win_mom_sec / self.tick_interval_sec))
        self._n_rv = max(1, int(self.win_rv_sec / self.tick_interval_sec))
        self._n_ofi = max(1, int(self.win_ofi_sec / self.tick_interval_sec))
        self._n_doi = max(1, int(self.win_doi_sec / self.tick_interval_sec))

        self._n_stat = max(200, int(self.stat_window_sec / self.tick_interval_sec))
        self._refresh_n = max(1, int(self.threshold_refresh_sec / self.tick_interval_sec))

        # tick序列缓存
        self.mid_q: Deque[float] = deque(maxlen=max(self._n_stat + 10, 3000))
        self.ret_ticks_q: Deque[float] = deque(maxlen=max(self._n_stat + 10, 3000))
        self.vol_q: Deque[float] = deque(maxlen=max(self._n_stat + 10, 3000))
        self.turn_q: Deque[float] = deque(maxlen=max(self._n_stat + 10, 3000))
        self.oi_q: Deque[float] = deque(maxlen=max(self._n_stat + 10, 3000))

        self.spread_q: Deque[float] = deque(maxlen=max(self._n_stat + 10, 3000))
        self.depth_q: Deque[float] = deque(maxlen=max(self._n_stat + 10, 3000))
        self.imb_q: Deque[float] = deque(maxlen=max(self._n_stat + 10, 3000))

        self.ofi1_q: Deque[float] = deque(maxlen=max(self._n_stat + 10, 3000))
        self.ofiN_q: Deque[float] = deque(maxlen=max(self._n_stat + 10, 3000))

        # 滚动统计
        self.rv_stat = RollingQuantiles(self._n_stat)
        self.mom_stat = RollingQuantiles(self._n_stat)
        self.dvol_stat = RollingQuantiles(self._n_stat)
        self.dturn_stat = RollingQuantiles(self._n_stat)
        self.depth_stat = RollingQuantiles(self._n_stat)
        self.lam_stat = RollingQuantiles(self._n_stat)
        self.spread_stat = RollingQuantiles(self._n_stat)

        # 动态阈值
        self._thr_rv = 0.0
        self._thr_mom = 0.0
        self._thr_dvol = 0.0
        self._thr_dturn = 0.0
        self._thr_depth_low = float(self.depth_hard_min)
        self._thr_dvol_high = 1e18
        self._thr_lambda_high = 1e18
        self._thr_spread_med = 2.0
        self._thr_spread_p90 = 4.0

        self._tick_count = 0

        # 订单管理
        self.active_orderids: List[str] = []
        self.last_order_dt: Optional[datetime] = None
        self._last_tick: Optional[TickData] = None

        # 持仓管理状态
        self.entry_dt: Optional[datetime] = None
        self.entry_price: float = 0.0
        self.entry_event_type: int = 0
        self.entry_exec_tier: int = 0

        self.peak_fav_ticks: float = 0.0
        self.lock_done: bool = False
        self.add_count: int = 0
        self.last_add_dt: Optional[datetime] = None

        # 回踩状态机（稳妥）
        self.pending_dir: int = 0         # 1=准备做多，-1=准备做空
        self.pending_peak_mid: float = 0.0

        # OFI 需要上一tick最优价量
        self._prev_bid1: Optional[float] = None
        self._prev_ask1: Optional[float] = None
        self._prev_bidv1: Optional[float] = None
        self._prev_askv1: Optional[float] = None

        # 日内统计（用于平仓手续费分段/风控）
        self.cur_trading_day: Optional[str] = None
        self.daily_close_volume: int = 0
        self.daily_trade_count: int = 0
        self.daily_realized_pnl_rmb: float = 0.0
        self.daily_stop: bool = False

    # =========================
    # 生命周期
    # =========================
    def on_init(self):
        self.write_log("MicroEventScalpIF_Scored init")
        self.load_bar(1)

    def on_start(self):
        self.write_log("MicroEventScalpIF_Scored start")
        self.daily_stop = False

    def on_stop(self):
        self.write_log("MicroEventScalpIF_Scored stop")

    # =========================
    # Tick 主循环
    # =========================
    def on_tick(self, tick: TickData):
        self._last_tick = tick

        # 合约参数（只取一次）
        if self._pricetick <= 0:
            self._pricetick = float(self.pricetick_override) if self.pricetick_override > 0 else float(getattr(self, "pricetick", 0.0) or 0.0)
            if self._pricetick <= 0:
                self._pricetick = 0.2  # IF常见

        if self._size <= 0:
            self._size = int(self.contract_size_override) if self.contract_size_override > 0 else int(getattr(self, "size", 0) or 0)
            if self._size <= 0:
                self._size = 300

        # 日内切换（简化用自然日；若你们要严格TradingDay，可改用tick.trading_day）
        td = tick.datetime.strftime("%Y%m%d")
        if self.cur_trading_day is None or td != self.cur_trading_day:
            self._reset_daily(td)

        # 日停：拒绝开仓，尽快平仓
        if self.daily_stop:
            if self.pos != 0 and not self.active_orderids:
                self._close_all_aggressive("DAILY_STOP")
            return

        # 盘口有效性
        bid1 = tick.bid_price_1
        ask1 = tick.ask_price_1
        if bid1 <= 0 or ask1 <= 0 or ask1 < bid1:
            return

        # 基础特征
        mid = (bid1 + ask1) / 2.0
        spread_ticks = (ask1 - bid1) / self._pricetick
        depth5 = self._sum_depth5(tick)
        imb5 = self._calc_imbalance5(tick, depth5)

        # OFI
        ofi1 = self._calc_ofi1_best(tick)
        self.ofi1_q.append(ofi1)
        ofiN = float(np.sum(list(self.ofi1_q)[-self._n_ofi:])) if len(self.ofi1_q) >= self._n_ofi else 0.0

        # ret_ticks
        prev_mid = self.mid_q[-1] if self.mid_q else mid
        ret_ticks = (mid - prev_mid) / self._pricetick

        # 序列维护
        self.mid_q.append(mid)
        self.ret_ticks_q.append(ret_ticks)
        self.vol_q.append(float(getattr(tick, "volume", 0.0)))
        self.turn_q.append(float(getattr(tick, "turnover", 0.0)))
        self.oi_q.append(float(getattr(tick, "open_interest", 0.0)))

        self.spread_q.append(float(spread_ticks))
        self.depth_q.append(float(depth5))
        self.imb_q.append(float(imb5))
        self.ofiN_q.append(float(ofiN))

        # 窗口指标
        self.mom6 = self._calc_mom_ticks(self._n_mom)
        self.rv6 = self._calc_rv_ticks(self._n_rv)
        self.dvol6 = self._calc_diff(self.vol_q, self._n_rv)
        self.dturn6 = self._calc_diff(self.turn_q, self._n_rv)
        self.doi10 = self._calc_diff(self.oi_q, self._n_doi)

        self.spread_ticks = spread_ticks
        self.depth5 = depth5
        self.imb5 = imb5
        self.ofi2 = ofiN

        # 更新统计与阈值刷新
        self._tick_count += 1
        self._update_stats()
        if self._tick_count % self._refresh_n == 0:
            self._refresh_thresholds()

        # 订单超时撤单（避免挂单风险）
        self._check_order_timeout(tick.datetime)

        # 事件打分与分型
        self.event_type, self.exec_tier, self.event_score, self.score_e1, self.score_e2 = self._score_and_classify()

        # 持仓管理优先
        if self.pos != 0:
            self._manage_position(tick)
            self.put_event()
            return

        # 无持仓：有订单就不再开
        if self.active_orderids:
            self.put_event()
            return

        # 日内交易次数限制
        if self.daily_trade_count >= int(self.max_trades_per_day):
            self.daily_stop = True
            return

        # 开仓（按事件类型 + tier）
        self._try_open(tick)

        self.put_event()

    # =========================
    # 订单/成交回调
    # =========================
    def on_order(self, order: OrderData):
        if order.vt_orderid in self.active_orderids:
            if order.status in {Status.ALLTRADED, Status.CANCELLED, Status.REJECTED}:
                self.active_orderids.remove(order.vt_orderid)
                if not self.active_orderids:
                    self.last_order_dt = None

    def on_trade(self, trade: TradeData):
        self.daily_trade_count += 1

        # OPEN：记录入场事件类型与tier（用于持仓期间固定风格）
        if trade.offset == Offset.OPEN:
            # 记录entry
            if self.entry_dt is None or abs(self.pos) == trade.volume:
                self.entry_dt = trade.datetime
                self.entry_price = trade.price
                self.entry_event_type = int(self.event_type)
                self.entry_exec_tier = int(self.exec_tier)

                self.peak_fav_ticks = 0.0
                self.lock_done = False
                self.add_count = 0
                self.last_add_dt = None
            else:
                # 加权均价（简化）
                new_pos = abs(self.pos)
                old_pos = new_pos - trade.volume
                if old_pos > 0:
                    self.entry_price = (self.entry_price * old_pos + trade.price * trade.volume) / new_pos
                else:
                    self.entry_price = trade.price

        else:
            # CLOSE：更新每日平仓手数（用于费率分段）
            self.daily_close_volume += int(trade.volume)

            # 简化已实现PnL（足够用于日内亏损止损；若要更精确可做逐笔匹配队列）
            if self.entry_price > 0 and self._size > 0:
                if trade.direction == Direction.SHORT:
                    pnl = (trade.price - self.entry_price) * trade.volume * self._size
                else:
                    pnl = (self.entry_price - trade.price) * trade.volume * self._size
                self.daily_realized_pnl_rmb += pnl

            if self.pos == 0:
                self.entry_dt = None
                self.entry_price = 0.0
                self.entry_event_type = 0
                self.entry_exec_tier = 0
                self.peak_fav_ticks = 0.0
                self.lock_done = False
                self.add_count = 0
                self.last_add_dt = None
                self.pending_dir = 0
                self.pending_peak_mid = 0.0

            if self.daily_realized_pnl_rmb <= -abs(float(self.max_daily_loss_rmb)):
                self.daily_stop = True

    def on_stop_order(self, stop_order: StopOrder):
        pass

    # =========================
    # 打分与分型
    # =========================
    def _update_stats(self) -> None:
        # lambda_proxy：|mom| / (|OFI|+1) —— 同样流量造成更大位移 => 更“脆”
        lam = abs(self.mom6) / (abs(self.ofi2) + 1.0)

        self.rv_stat.push(self.rv6)
        self.mom_stat.push(abs(self.mom6))
        self.dvol_stat.push(max(0.0, self.dvol6))
        self.dturn_stat.push(max(0.0, self.dturn6))
        self.depth_stat.push(self.depth5)
        self.lam_stat.push(lam)
        self.spread_stat.push(self.spread_ticks)

    def _refresh_thresholds(self) -> None:
        if not (self.rv_stat.ready(self.stat_min_points)
                and self.mom_stat.ready(self.stat_min_points)
                and self.dvol_stat.ready(self.stat_min_points)
                and self.dturn_stat.ready(self.stat_min_points)
                and self.depth_stat.ready(self.stat_min_points)
                and self.lam_stat.ready(self.stat_min_points)
                and self.spread_stat.ready(self.stat_min_points)):
            return

        self._thr_rv = self.rv_stat.quantile(self.q_rv)
        self._thr_mom = self.mom_stat.quantile(self.q_mom)
        self._thr_dvol = max(1e-9, self.dvol_stat.quantile(self.q_dvol))
        self._thr_dturn = max(1e-9, self.dturn_stat.quantile(self.q_dturn))

        self._thr_depth_low = max(float(self.depth_hard_min), self.depth_stat.quantile(self.q_depth_low))
        self._thr_dvol_high = self.dvol_stat.quantile(self.q_dvol_high)
        self._thr_lambda_high = max(1e-9, self.lam_stat.quantile(self.q_lambda_high))

        self._thr_spread_med = max(1.0, self.spread_stat.quantile(self.q_spread_med))
        self._thr_spread_p90 = max(self._thr_spread_med + 1.0, self.spread_stat.quantile(self.q_spread_p90))

    def _score_and_classify(self) -> tuple[int, int, float, float, float]:
        """
        返回：
        - event_type: 0/1/2
        - exec_tier: 0/1/2/3
        - event_score: 0~100
        - score_e1: 0~100
        - score_e2: 0~100
        """
        # 数据不足：不交易
        if self._thr_rv <= 0 or self._thr_mom <= 0:
            return 0, 0, 0.0, 0.0, 0.0

        # 不可交易硬约束：spread爆/深度塌 => 直接无事件（并且持仓时会强平）
        if self.spread_ticks >= float(self.spread_hard_stop) or self.depth5 < float(self.depth_hard_min):
            return 0, 0, 0.0, 0.0, 0.0

        # -------- event_score（总触发）--------
        price_norm = max(
            self.rv6 / max(1e-9, self._thr_rv),
            abs(self.mom6) / max(1e-9, self._thr_mom),
        )
        flow_norm = max(
            max(0.0, self.dvol6) / max(1e-9, self._thr_dvol),
            max(0.0, self.dturn6) / max(1e-9, self._thr_dturn),
        )

        # 必须同时超过各自“1.0倍阈值”
        if price_norm < 1.0 or flow_norm < 1.0:
            return 0, 0, 0.0, 0.0, 0.0

        s_price = _clip01((price_norm - 1.0) / max(1e-9, float(self.price_scale)))
        s_flow = _clip01((flow_norm - 1.0) / max(1e-9, float(self.flow_scale)))
        event_score = 100.0 * (float(self.w_price) * s_price + float(self.w_flow) * s_flow)

        if event_score < float(self.event_enter_score):
            return 0, 0, float(event_score), 0.0, 0.0

        # tier 映射（并限制最大档位，默认只到2：稳健）
        if event_score >= float(self.tier3_score):
            tier = 3
        elif event_score >= float(self.tier2_score):
            tier = 2
        else:
            tier = 1
        tier = min(tier, int(self.max_exec_tier))

        # -------- 分型打分 --------
        lam = abs(self.mom6) / (abs(self.ofi2) + 1.0)

        # spread_excess：相对中位数偏离，归一化到0~1
        spread_den = max(1.0, self._thr_spread_p90 - self._thr_spread_med)
        spread_excess = _clip01((self.spread_ticks - self._thr_spread_med) / spread_den)

        # depth_low：低于低分位阈值的程度
        depth_low = _clip01((self._thr_depth_low - self.depth5) / max(1e-9, self._thr_depth_low))

        # lambda_high：高于lambda高分位阈值的程度
        lambda_high = _clip01((lam - self._thr_lambda_high) / max(1e-9, self._thr_lambda_high))
        lambda_low = _clip01((self._thr_lambda_high - lam) / max(1e-9, self._thr_lambda_high))

        # flow_not_extreme：dvol没有非常高（Withdrawal常见：结构变差但成交不一定极端大）
        flow_not_extreme = _clip01((self._thr_dvol_high - max(0.0, self.dvol6)) / max(1e-9, self._thr_dvol_high))

        # flow_very_high：成交极高（Absorption）
        flow_very_high = _clip01((max(0.0, self.dvol6) - self._thr_dvol_high) / max(1e-9, self._thr_dvol_high))

        # spread_good：spread越小越好（相对中位数更好）
        spread_good = _clip01((self._thr_spread_p90 - self.spread_ticks) / max(1.0, self._thr_spread_p90))

        # depth_good：高于低阈值越多越好
        depth_good = _clip01((self.depth5 - self._thr_depth_low) / max(1e-9, self._thr_depth_low))

        # E1 Withdrawal score（偏结构恶化）
        score_e1 = 100.0 * (
            0.35 * spread_excess +
            0.35 * depth_low +
            0.20 * lambda_high +
            0.10 * flow_not_extreme
        )

        # E2 Absorption score（偏高参与承接）
        score_e2 = 100.0 * (
            0.45 * flow_very_high +
            0.25 * spread_good +
            0.20 * depth_good +
            0.10 * lambda_low
        )

        # 分型决策（要足够强、且领先）
        et = 0
        if score_e1 >= float(self.min_type_score) and (score_e1 - score_e2) >= float(self.type_score_diff):
            et = 1
        elif score_e2 >= float(self.min_type_score) and (score_e2 - score_e1) >= float(self.type_score_diff):
            et = 2
        else:
            et = 0

        # 非可交易事件：不做
        if et == 0:
            tier = 0

        return et, tier, float(event_score), float(score_e1), float(score_e2)

    # =========================
    # Profile：事件类型 + tier 的分档执行参数
    # =========================
    def _get_profile(self, event_type: int, tier: int) -> Dict[str, Any]:
        """
        所有可调点都集中在这里，后续回测调参建议先从这里入手。

        说明：下面数值是“稳健起点”（基于你给的IF样本：平时spread多在1~3ticks，事件P90多在<=4）
        你们若发现执行更快/更慢，主要调：
        - spread上限
        - chase_ticks
        - stop_loss/trail/max_hold
        - 入场确认门槛（mom/ofi/imb）
        - 锁盈比例与锁盈门槛（lock_extra_ticks + slippage_ticks + fee）
        """
        # ---- 默认基线（会被事件/档位覆盖）----
        p = {
            "spread_entry_max": 3,          # 入场最大spread
            "spread_add_max": 3,            # 加仓最大spread
            "pullback_max": 3,              # 回踩上限（ticks）
            "chase_ticks": 0,               # 吃对手价追价上限（ticks），稳健默认0
            "order_timeout_sec": 1.0,       # 订单超时撤单
            "stop_loss": 6,                 # 价格止损ticks
            "trail": 3,                     # 追踪回撤ticks
            "max_hold_sec": 25.0,           # 最长持仓
            "lock_ratio": 0.5,              # 锁盈先平比例
            "entry_mom": 2,                 # 入场mom阈值（ticks）
            "entry_ofi": 5,                 # 入场OFI阈值（量纲取决数据源，需回测调）
            "entry_imb": 0.03,              # imbalance阈值
            "entry_min_confirm": 2,         # mom/ofi/imb 三者至少满足2个
            "forbid_spread_widening": True, # 入场前spread不允许继续扩大（稳健）
            "add_max": 1,                   # 最多加仓次数
            "add_min_profit_ticks": 2,      # 至少盈利垫才加仓（会再与成本线取max）
            "add_cooldown_sec": 2.0,        # 加仓冷却
            "add_ratio_1": 0.5,
            "add_ratio_2": 0.3,
            "exit_ofi_flip": 8,             # OFI反转退出阈值
        }

        # ---- E1 Withdrawal：更严格、更快进快出 ----
        if event_type == 1:
            if tier == 1:
                p.update({
                    "spread_entry_max": 2,      # 建议 2~3
                    "spread_add_max": 2,
                    "pullback_max": 2,
                    "chase_ticks": 0,           # 不追价更稳
                    "order_timeout_sec": 0.8,   # 更短（0.5~1.0）
                    "stop_loss": 4,             # 建议 3~6
                    "trail": 1,                 # 建议 1~2
                    "max_hold_sec": 6.0,        # 建议 3~12
                    "lock_ratio": 0.80,         # 更快锁盈
                    "entry_mom": 3,             # 建议 3~6
                    "entry_ofi": 6,             # 建议 5~20
                    "entry_imb": 0.05,          # 建议 0.03~0.15
                    "add_max": 0,               # Withdrawal默认不加仓最稳
                    "forbid_spread_widening": True,
                })
            elif tier == 2:
                p.update({
                    "spread_entry_max": 3,
                    "spread_add_max": 3,
                    "pullback_max": 2,
                    "chase_ticks": 1,           # 允许追1跳（需要更严格的风控配套）
                    "order_timeout_sec": 1.0,
                    "stop_loss": 5,
                    "trail": 2,
                    "max_hold_sec": 8.0,
                    "lock_ratio": 0.70,
                    "entry_mom": 3,
                    "entry_ofi": 6,
                    "entry_imb": 0.05,
                    "add_max": 1,               # 只允许加1次
                    "forbid_spread_widening": True,
                })
            else:  # tier == 3（默认不启用，想启用请max_exec_tier=3）
                p.update({
                    "spread_entry_max": 3,
                    "spread_add_max": 3,
                    "pullback_max": 2,
                    "chase_ticks": 1,
                    "order_timeout_sec": 1.0,
                    "stop_loss": 6,
                    "trail": 2,
                    "max_hold_sec": 10.0,
                    "lock_ratio": 0.65,
                    "entry_mom": 4,
                    "entry_ofi": 8,
                    "entry_imb": 0.06,
                    "add_max": 1,
                    "forbid_spread_widening": True,
                })

        # ---- E2 Absorption：成交高、可交易性更好，允许稍放宽 ----
        elif event_type == 2:
            if tier == 1:
                p.update({
                    "spread_entry_max": 2,
                    "spread_add_max": 2,
                    "pullback_max": 3,
                    "chase_ticks": 0,
                    "order_timeout_sec": 1.0,
                    "stop_loss": 5,             # 建议 4~10
                    "trail": 2,                 # 建议 2~5
                    "max_hold_sec": 15.0,       # 建议 10~40
                    "lock_ratio": 0.60,
                    "entry_mom": 2,
                    "entry_ofi": 5,
                    "entry_imb": 0.03,
                    "add_max": 1,
                    "forbid_spread_widening": False,
                })
            elif tier == 2:
                p.update({
                    "spread_entry_max": 3,
                    "spread_add_max": 3,
                    "pullback_max": 3,
                    "chase_ticks": 1,
                    "order_timeout_sec": 1.2,
                    "stop_loss": 6,
                    "trail": 3,
                    "max_hold_sec": 25.0,
                    "lock_ratio": 0.50,
                    "entry_mom": 2,
                    "entry_ofi": 5,
                    "entry_imb": 0.03,
                    "add_max": 2,
                    "forbid_spread_widening": False,
                })
            else:  # tier == 3
                p.update({
                    "spread_entry_max": 4,
                    "spread_add_max": 4,
                    "pullback_max": 4,
                    "chase_ticks": 1,
                    "order_timeout_sec": 1.5,
                    "stop_loss": 8,
                    "trail": 4,
                    "max_hold_sec": 35.0,
                    "lock_ratio": 0.45,
                    "entry_mom": 3,
                    "entry_ofi": 6,
                    "entry_imb": 0.04,
                    "add_max": 2,
                    "forbid_spread_widening": False,
                })

        return p

    # =========================
    # 开仓：回踩后二次走强（稳健）
    # =========================
    def _try_open(self, tick: TickData) -> None:
        if self.event_type == 0 or self.exec_tier == 0:
            self.pending_dir = 0
            self.pending_peak_mid = 0.0
            return

        p = self._get_profile(self.event_type, self.exec_tier)

        # 入场可交易过滤
        if self.spread_ticks > float(p["spread_entry_max"]):
            self.pending_dir = 0
            return
        if self.depth5 < float(max(self.depth_hard_min, self._thr_depth_low)):
            self.pending_dir = 0
            return

        # spread 是否在扩大（Withdrawal更严格）
        if bool(p["forbid_spread_widening"]) and self._spread_is_widening(lookback=3):
            self.pending_dir = 0
            return

        # 方向提示（用mom）
        dir_hint = 1 if self.mom6 > 0 else (-1 if self.mom6 < 0 else 0)
        if dir_hint == 0:
            self.pending_dir = 0
            return

        # 确认：mom/ofi/imb 三者至少满足 entry_min_confirm
        cond_mom = abs(self.mom6) >= float(p["entry_mom"])
        cond_ofi = (self.ofi2 >= float(p["entry_ofi"])) if dir_hint > 0 else (self.ofi2 <= -float(p["entry_ofi"]))
        cond_imb = (self.imb5 >= float(p["entry_imb"])) if dir_hint > 0 else (self.imb5 <= -float(p["entry_imb"]))

        ok_count = int(cond_mom) + int(cond_ofi) + int(cond_imb)
        if ok_count < int(p["entry_min_confirm"]):
            self.pending_dir = 0
            self.pending_peak_mid = 0.0
            return

        # 回踩状态机：记录冲击方向的峰值，然后等待回踩到指定范围并“再次走强”
        mid = (tick.bid_price_1 + tick.ask_price_1) / 2.0
        if self.pending_dir != dir_hint:
            self.pending_dir = dir_hint
            self.pending_peak_mid = mid
        else:
            if dir_hint > 0:
                self.pending_peak_mid = max(self.pending_peak_mid, mid)
            else:
                self.pending_peak_mid = min(self.pending_peak_mid, mid)

        # pullback（ticks）
        if dir_hint > 0:
            pullback = (self.pending_peak_mid - mid) / self._pricetick
        else:
            pullback = (mid - self.pending_peak_mid) / self._pricetick

        if pullback < float(self.pullback_min_ticks) or pullback > float(p["pullback_max"]):
            return

        # 二次走强：用最近一跳收益确认（更严格可以改成OFI连续2跳同向）
        if not self.ret_ticks_q:
            return
        last_ret = self.ret_ticks_q[-1]
        if dir_hint > 0 and last_ret <= 0:
            return
        if dir_hint < 0 and last_ret >= 0:
            return

        # 下单：吃对手价，允许追 p["chase_ticks"]（稳健档通常0或1）
        vol = int(self.base_volume)
        if vol <= 0:
            return

        chase = int(p["chase_ticks"])
        if dir_hint > 0:
            price = tick.ask_price_1 + chase * self._pricetick
            self._send_buy(price, vol, float(p["order_timeout_sec"]))
        else:
            price = tick.bid_price_1 - chase * self._pricetick
            self._send_short(price, vol, float(p["order_timeout_sec"]))

        # 防止连发
        self.pending_dir = 0
        self.pending_peak_mid = 0.0

    # =========================
    # 持仓管理：平仓 + 稳妥加仓
    # =========================
    def _manage_position(self, tick: TickData) -> None:
        # 结构硬风险：强平
        if self.spread_ticks >= float(self.spread_hard_stop) or self.depth5 < float(self.depth_hard_min):
            self._close_all_aggressive("STRUCT_HARD")
            return

        if self.entry_dt is None or self.entry_price <= 0:
            self.entry_dt = tick.datetime
            self.entry_price = (tick.bid_price_1 + tick.ask_price_1) / 2.0
            self.entry_event_type = int(self.event_type)
            self.entry_exec_tier = int(self.exec_tier)

        et = int(self.entry_event_type) if self.entry_event_type in (1, 2) else int(self.event_type)
        tier = int(self.entry_exec_tier) if self.entry_exec_tier in (1, 2, 3) else int(self.exec_tier)
        if et == 0 or tier == 0:
            # 理论上不应发生：有持仓但无档位。这里采取保守处理：尽快平掉
            self._close_all_aggressive("NO_PROFILE")
            return

        p = self._get_profile(et, tier)

        # 当前浮盈（ticks）
        mid = (tick.bid_price_1 + tick.ask_price_1) / 2.0
        if self.pos > 0:
            cur_fav = (mid - self.entry_price) / self._pricetick
        else:
            cur_fav = (self.entry_price - mid) / self._pricetick

        # 峰值与回撤
        self.peak_fav_ticks = max(self.peak_fav_ticks, cur_fav)
        drawdown = self.peak_fav_ticks - cur_fav

        # 成本线（ticks）：手续费 + 预期滑点（往返）
        cost_ticks = self._estimate_roundtrip_cost_ticks()
        # 锁盈门槛：成本线 + 1tick（你也可以把这1tick变成参数）
        lock_ticks = cost_ticks + 1.0

        # 持仓时间
        hold_sec = (tick.datetime - self.entry_dt).total_seconds()

        # ---- 1) 价格止损（硬）----
        if cur_fav <= -float(p["stop_loss"]):
            self._close_all_aggressive("STOP_PRICE")
            return

        # ---- 2) OFI 反转退出（掉头就跑）----
        flip_thr = float(p["exit_ofi_flip"])
        ofi_flip = (self.ofi2 <= -flip_thr) if self.pos > 0 else (self.ofi2 >= flip_thr)
        if ofi_flip and drawdown >= 1.0 and cur_fav >= 0.0:
            self._close_all_aggressive("OFI_FLIP")
            return

        # ---- 3) 时间止损（防拖成震荡）----
        if hold_sec >= float(p["max_hold_sec"]):
            self._close_all_aggressive("TIME_STOP")
            return

        # ---- 4) 锁盈（分批）----
        if (not self.lock_done) and (cur_fav >= lock_ticks):
            ratio = float(p["lock_ratio"])
            self._lock_partial(tick, ratio, "LOCK_PROFIT", float(p["order_timeout_sec"]))
            self.lock_done = True
            return

        # ---- 5) 追踪止盈（锁盈后/过成本线后）----
        if cur_fav >= lock_ticks:
            if drawdown >= float(p["trail"]):
                self._close_all_aggressive("TRAIL_EXIT")
                return

        # ---- 6) 稳妥加仓（必须盈利垫 + 结构不坏 + 冷却 + 次数限制）----
        self._try_add(tick, p, cur_fav, lock_ticks)

    def _try_add(self, tick: TickData, p: Dict[str, Any], cur_fav: float, lock_ticks: float) -> None:
        if self.active_orderids:
            return

        # 次数限制
        if self.add_count >= int(p["add_max"]):
            return

        # 冷却
        if self.last_add_dt is not None:
            if (tick.datetime - self.last_add_dt).total_seconds() < float(p["add_cooldown_sec"]):
                return

        # 盈利垫：取 max(配置, 成本线)
        min_profit = max(float(p["add_min_profit_ticks"]), float(lock_ticks))
        if cur_fav < min_profit:
            return

        # 结构条件：加仓更严格的spread上限 + depth不能低
        if self.spread_ticks > float(p["spread_add_max"]):
            return
        if self.depth5 < float(max(self.depth_hard_min, self._thr_depth_low)):
            return

        # 方向仍要一致（避免在回吐段加仓）
        if self.pos > 0:
            if not (self.mom6 > 0 and self.ofi2 > 0):
                return
        else:
            if not (self.mom6 < 0 and self.ofi2 < 0):
                return

        # 简化：最近一跳收益同向，避免“刚开始回吐就加”
        if not self.ret_ticks_q:
            return
        last_ret = self.ret_ticks_q[-1]
        if self.pos > 0 and last_ret <= 0:
            return
        if self.pos < 0 and last_ret >= 0:
            return

        base = int(self.base_volume)
        if base <= 0:
            return

        if self.add_count == 0:
            add_vol = max(1, int(round(base * float(p["add_ratio_1"]))))
        else:
            add_vol = max(1, int(round(base * float(p["add_ratio_2"]))))

        chase = int(p["chase_ticks"])
        if self.pos > 0:
            price = tick.ask_price_1 + chase * self._pricetick
            self._send_buy(price, add_vol, float(p["order_timeout_sec"]))
        else:
            price = tick.bid_price_1 - chase * self._pricetick
            self._send_short(price, add_vol, float(p["order_timeout_sec"]))

        self.add_count += 1
        self.last_add_dt = tick.datetime

    # =========================
    # 下单封装 + 超时撤单
    # =========================
    def _send_buy(self, price: float, volume: int, timeout_sec: float) -> None:
        if not self.trading:
            return
        vt_orderid = self.buy(price=price, volume=volume)
        if vt_orderid:
            self.active_orderids.append(vt_orderid)
            self.last_order_dt = self._last_tick.datetime if self._last_tick else None
            # 将当前订单超时写入属性，便于动态不同profile
            self._cur_order_timeout = float(timeout_sec)

    def _send_short(self, price: float, volume: int, timeout_sec: float) -> None:
        if not self.trading:
            return
        vt_orderid = self.short(price=price, volume=volume)
        if vt_orderid:
            self.active_orderids.append(vt_orderid)
            self.last_order_dt = self._last_tick.datetime if self._last_tick else None
            self._cur_order_timeout = float(timeout_sec)

    def _send_sell(self, price: float, volume: int, timeout_sec: float) -> None:
        if not self.trading:
            return
        vt_orderid = self.sell(price=price, volume=volume)
        if vt_orderid:
            self.active_orderids.append(vt_orderid)
            self.last_order_dt = self._last_tick.datetime if self._last_tick else None
            self._cur_order_timeout = float(timeout_sec)

    def _send_cover(self, price: float, volume: int, timeout_sec: float) -> None:
        if not self.trading:
            return
        vt_orderid = self.cover(price=price, volume=volume)
        if vt_orderid:
            self.active_orderids.append(vt_orderid)
            self.last_order_dt = self._last_tick.datetime if self._last_tick else None
            self._cur_order_timeout = float(timeout_sec)

    def _check_order_timeout(self, now: datetime) -> None:
        if not self.active_orderids or self.last_order_dt is None:
            return
        timeout = float(getattr(self, "_cur_order_timeout", 1.0))
        if (now - self.last_order_dt).total_seconds() >= timeout:
            self.cancel_all()
            self.active_orderids.clear()
            self.last_order_dt = None

    # =========================
    # 平仓动作
    # =========================
    def _lock_partial(self, tick: TickData, ratio: float, reason: str, timeout_sec: float) -> None:
        if self.active_orderids:
            return
        vol = abs(self.pos)
        if vol <= 0:
            return
        close_vol = max(1, int(round(vol * ratio)))
        close_vol = min(close_vol, vol)

        if self.pos > 0:
            self._send_sell(tick.bid_price_1, close_vol, timeout_sec)
        else:
            self._send_cover(tick.ask_price_1, close_vol, timeout_sec)

    def _close_all_aggressive(self, reason: str) -> None:
        if self.active_orderids or self._last_tick is None:
            return
        tick = self._last_tick
        vol = abs(self.pos)
        if vol <= 0:
            return

        # 强平不追价：直接吃对手最优
        if self.pos > 0:
            self._send_sell(tick.bid_price_1, vol, 1.0)
        else:
            self._send_cover(tick.ask_price_1, vol, 1.0)

    # =========================
    # 成本折算 ticks：手续费分段 + 预期滑点
    # =========================
    def _estimate_roundtrip_cost_ticks(self) -> float:
        """
        往返成本（ticks）= (开仓费 + 平仓费)/ (size*pricetick) + 2*slippage_ticks

        说明：
        - 平仓费使用“每日累计已平仓手数 daily_close_volume”的分段近似：
          前 close_fee_first_n 手用 close_fee_first_rmb，之后用 close_fee_after_rmb
        - 若你们后续要更精细（例如不同方向、平今平昨），可在这里扩展
        """
        if self._size <= 0 or self._pricetick <= 0:
            return float(2 * self.slippage_ticks)

        # 当前预估：下一笔平仓使用哪个费率
        if self.daily_close_volume < int(self.close_fee_first_n):
            close_fee = float(self.close_fee_first_rmb)
        else:
            close_fee = float(self.close_fee_after_rmb)

        total_fee = float(self.open_fee_rmb) + close_fee
        fee_ticks = total_fee / (float(self._size) * float(self._pricetick))

        slip_ticks = float(self.slippage_ticks) * 2.0
        return float(fee_ticks + slip_ticks)

    # =========================
    # 计算函数
    # =========================
    def _calc_mom_ticks(self, n: int) -> float:
        if len(self.mid_q) <= n:
            return 0.0
        return (self.mid_q[-1] - self.mid_q[-1 - n]) / self._pricetick

    def _calc_rv_ticks(self, n: int) -> float:
        if len(self.ret_ticks_q) < n:
            return 0.0
        arr = np.asarray(list(self.ret_ticks_q)[-n:], dtype=float)
        return float(np.sqrt(np.sum(arr * arr)))

    @staticmethod
    def _calc_diff(q: Deque[float], n: int) -> float:
        if len(q) <= n:
            return 0.0
        return float(q[-1] - q[-1 - n])

    def _sum_depth5(self, tick: TickData) -> float:
        b = (
            tick.bid_volume_1 + tick.bid_volume_2 + tick.bid_volume_3 +
            tick.bid_volume_4 + tick.bid_volume_5
        )
        a = (
            tick.ask_volume_1 + tick.ask_volume_2 + tick.ask_volume_3 +
            tick.ask_volume_4 + tick.ask_volume_5
        )
        return float(b + a)

    def _calc_imbalance5(self, tick: TickData, depth5: float) -> float:
        if depth5 <= 0:
            return 0.0
        b = (
            tick.bid_volume_1 + tick.bid_volume_2 + tick.bid_volume_3 +
            tick.bid_volume_4 + tick.bid_volume_5
        )
        a = (
            tick.ask_volume_1 + tick.ask_volume_2 + tick.ask_volume_3 +
            tick.ask_volume_4 + tick.ask_volume_5
        )
        return float((b - a) / (b + a + 1e-9))

    def _calc_ofi1_best(self, tick: TickData) -> float:
        """
        最优档OFI（Cont/Kukanov/Stoikov思路）
        - bid: 价上升 => +新bid量；价下降 => -旧bid量；价不变 => +Δbid量
        - ask: 价上升 => -旧ask量；价下降 => +新ask量；价不变 => -Δask量
        """
        bid_p1 = float(tick.bid_price_1)
        ask_p1 = float(tick.ask_price_1)
        bid_v1 = float(tick.bid_volume_1)
        ask_v1 = float(tick.ask_volume_1)

        if self._prev_bid1 is None:
            self._prev_bid1, self._prev_ask1 = bid_p1, ask_p1
            self._prev_bidv1, self._prev_askv1 = bid_v1, ask_v1
            return 0.0

        bid_p0 = float(self._prev_bid1)
        ask_p0 = float(self._prev_ask1)
        bid_v0 = float(self._prev_bidv1)
        ask_v0 = float(self._prev_askv1)

        # bid
        if bid_p1 > bid_p0:
            bid_c = bid_v1
        elif bid_p1 < bid_p0:
            bid_c = -bid_v0
        else:
            bid_c = bid_v1 - bid_v0

        # ask
        if ask_p1 > ask_p0:
            ask_c = -ask_v0
        elif ask_p1 < ask_p0:
            ask_c = ask_v1
        else:
            ask_c = -(ask_v1 - ask_v0)

        ofi = bid_c + ask_c

        self._prev_bid1, self._prev_ask1 = bid_p1, ask_p1
        self._prev_bidv1, self._prev_askv1 = bid_v1, ask_v1
        return float(ofi)

    def _spread_is_widening(self, lookback: int = 3) -> bool:
        """
        判断spread是否在“扩大趋势中”
        - Withdrawal稳健档建议开启：避免在真空加速期追进
        """
        if len(self.spread_q) < lookback + 1:
            return False
        now = self.spread_q[-1]
        past = self.spread_q[-1 - lookback]
        return (now - past) >= 1.0  # 过去lookback步至少扩大1tick（可在回测中调）

    # =========================
    # 日内重置
    # =========================
    def _reset_daily(self, td: str) -> None:
        self.cur_trading_day = td
        self.daily_close_volume = 0
        self.daily_trade_count = 0
        self.daily_realized_pnl_rmb = 0.0
        self.daily_stop = False
