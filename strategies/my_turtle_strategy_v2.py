from typing import Tuple
import math
import numpy as np

from vnpy_ctastrategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    ArrayManager,
)


class MyTurtleStrategyV2(CtaTemplate):
    """
    优化的海龟策略 + 崩盘风险覆盖

    主要修改：
    1. 移除了所有内部费用模型代码
    2. 头寸计算不再包含预期费用
    3. 使用vnPy内置的手续费计算系统
    4. 保留崩盘风险覆盖和最小止损地板

    注意：回测时应在vnPy引擎中配置手续费率，避免重复计算
    """
    author = "jhd1993"

    # --- 核心指标 ---
    n_value: float = 0.0
    entry_high: float = 0.0
    entry_low: float = 0.0
    exit_high: float = 0.0
    exit_low: float = 0.0

    # --- 仓位状态 ---
    unit_size: int = 0
    entry_price: float = 0.0
    entry_n: float = 0.0
    last_add_price: float = 0.0
    adds_done: int = 0
    cooldown_count: int = 0

    # --- 崩盘风险覆盖状态 ---
    log_delta: float = 0.0
    lambda_long: float = 0.0
    lambda_short: float = 0.0
    risk_scale: float = 1.0
    _lambda_hist_long = []
    _lambda_hist_short = []

    # ======================
    # 合约/资金参数
    # ======================
    capital: float = 1_000_000.0
    contract_size: float = 300.0  # IF: 每点300元/手

    # ======================
    # 海龟策略参数
    # ======================
    entry_window: int = 20
    exit_window: int = 10
    atr_window: int = 20

    # 单位风险比例
    risk_percent: float = 0.003  # 每次入场风险0.4%

    # 安全限制
    atr_floor_ratio: float = 0.001  # ATR地板（收盘价的0.2%）
    max_unit_size: int = 2  # 单位头寸上限
    max_pos: int = 8  # 总持仓上限（包含加仓）
    max_total_risk_percent: float = 0.02  # 总风险上限（占资本的2%）

    # 加仓规则
    add_step_n: float = 0.5
    max_adds: int = 3
    add_one_per_bar: bool = True

    # 止损规则
    stop_n: float = 2.5
    conservative_exit_fill: bool = True  # 保守离场成交价模型

    # 入场信号
    use_close_for_breakout: bool = False  # 使用收盘价突破（否则使用最高/最低价）

    # 冷却期
    cooldown_bars: int = 5

    # 最小止损地板（保留，这是风险控制）
    avg_stop_loss_rmb_per_hand: float = 1200.0

    # ======================
    # 崩盘风险覆盖参数
    # ======================
    enable_crash_overlay: bool = True
    # enable_crash_overlay: bool = False

    # 错误定价EMA时间尺度
    # 原策略是（日线）：约1年
    # 目前的策略改为2000分钟，约8天
    tau_mispricing: int = 2000
    rbar: float = 0.0

    # lambda = sigmoid((log_delta - ref)/s)
    log_delta_ref: float = 0.0
    lambda_s: float = 0.08

    # 入场门控
    entry_lambda_max: float = 0.35

    # 风险缩放：risk_eff = risk_percent * max(min_risk_scale, 1 - max(lambda_long, lambda_short))
    min_risk_scale: float = 0.2

    # 加仓门控
    add_lambda_soft: float = 0.40
    add_lambda_hard: float = 0.55
    add_lambda_mid_max_adds: int = 1
    lambda_decreasing_bars: int = 10

    # 减仓/强制离场阈值
    delever_lambda: float = 0.60
    force_exit_lambda: float = 0.80

    # 高风险时收紧止损
    tighten_lambda: float = 0.60
    stop_n_tight: float = 1.5

    parameters = [
        # 合约/资金
        "capital", "contract_size",
        # 海龟策略

        "entry_window",
        "exit_window",
        "atr_window",
        "risk_percent",
        "atr_floor_ratio",
        "max_unit_size", "max_pos",
        "max_total_risk_percent",
        "add_step_n", "max_adds", "add_one_per_bar",
        "stop_n", "conservative_exit_fill",
        "use_close_for_breakout",
        "cooldown_bars",
        # 风险控制（保留最小止损地板）
        "avg_stop_loss_rmb_per_hand",
        # 崩盘风险覆盖
        "enable_crash_overlay",
        "tau_mispricing", "rbar",
        "log_delta_ref", "lambda_s",
        "entry_lambda_max",
        "min_risk_scale",
        "add_lambda_soft", "add_lambda_hard",
        "add_lambda_mid_max_adds",
        "lambda_decreasing_bars",
        "delever_lambda", "force_exit_lambda",
        "tighten_lambda", "stop_n_tight",
    ]

    variables = [
        "n_value",
        "entry_high", "entry_low",
        "exit_high", "exit_low",
        "unit_size",
        "entry_price",
        "entry_n",
        "last_add_price",
        "adds_done",
        "cooldown_count",
        # 崩盘风险覆盖状态
        "log_delta",
        "lambda_long",
        "lambda_short",
        "risk_scale",
    ]

    def __init__(self, cta_engine, strategy_name: str, vt_symbol: str, setting: dict):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        # 计算需要的数据量
        max_window = max(
            600,
            self.entry_window + self.atr_window + self.tau_mispricing + 120
        )
        self.am: ArrayManager = ArrayManager(size=max_window)

    def on_init(self) -> None:
        self.write_log("优化海龟策略初始化 (日线 + 风险覆盖)")

        # 加载足够的数据
        load_n = max(
            self.entry_window,
            self.atr_window,
            self.tau_mispricing
        ) + 100

        self.load_bar(10)

    def on_start(self) -> None:
        self.write_log("策略启动")
        self.put_event()

    def on_stop(self) -> None:
        self.write_log("策略停止")
        self.put_event()

    # ======================
    # 回调函数
    # ======================
    def on_tick(self, tick: TickData) -> None:
        # 仅使用日线
        return

    def on_bar(self, bar: BarData) -> None:
        ########### 1. 准备工作
        self.cancel_all()  # 取消所有未完成订单

        # 删除：self._roll_daily_fee_counter(bar.datetime.date())

        self.am.update_bar(bar)  # 更新数据
        if not self.am.inited:
            return

        ########## 2. 冷却期处理
        if self.pos == 0 and self.cooldown_count > 0:
            self.cooldown_count -= 1

        ########## 4. N值计算(ATR+地板)
        atr = float(self.am.atr(self.atr_window))
        if not np.isfinite(atr) or atr <= 0:
            return

        atr_floor = bar.close_price * self.atr_floor_ratio if self.atr_floor_ratio > 0 else 0.0
        self.n_value = max(atr, atr_floor)

        ########## 5. 计算唐奇安通道
        self.entry_high, self.entry_low = self._donchian_prev(self.entry_window)
        self.exit_high, self.exit_low = self._donchian_prev(self.exit_window)
        if self.entry_high <= 0 or self.entry_low <= 0 or self.exit_high <= 0 or self.exit_low <= 0:
            return

        ########## 6. 崩盘风险覆盖
        if self.enable_crash_overlay:
            self._update_crash_overlay(bar)

        ########## 7. 已有仓位管理
        if self.pos != 0:
            if self.pos > 0:
                self._manage_long(bar, self.exit_low)
            else:
                self._manage_short(bar, self.exit_high)
            self.put_event()
            return

        ########## 8. 空仓冷却检查
        if self.cooldown_count > 0:
            self.put_event()
            return

        ########## 9. 入场信号判断
        if self.use_close_for_breakout:
            long_break = bar.close_price >= self.entry_high
            short_break = bar.close_price <= self.entry_low
        else:
            long_break = bar.high_price >= self.entry_high
            short_break = bar.low_price <= self.entry_low

        ########## 10. 入场信号的处理
        if long_break:
            # 风险过滤
            if self.enable_crash_overlay and self.lambda_long >= self.entry_lambda_max:
                print(self.lambda_long)
                self.put_event()
                return

            unit = self._calc_unit_size_for_entry(n_value=self.n_value, direction=+1)
            if unit <= 0:
                self.put_event()
                return

            fill_price = self._entry_fill_price_long(bar, self.entry_high)
            self._enter_long(unit, fill_price, self.n_value)

        elif short_break:
            if self.enable_crash_overlay and self.lambda_short >= self.entry_lambda_max:
                self.put_event()
                return

            unit = self._calc_unit_size_for_entry(n_value=self.n_value, direction=-1)
            if unit <= 0:
                self.put_event()
                return

            fill_price = self._entry_fill_price_short(bar, self.entry_low)
            self._enter_short(unit, fill_price, self.n_value)

        self.put_event()

    def on_order(self, order: OrderData) -> None:
        return

    def on_trade(self, trade: TradeData) -> None:
        # 删除所有费用计算逻辑
        # vnPy会自动处理手续费

        # 平仓时重置状态
        if self.pos == 0:
            self._reset_trade_state_if_flat()

        self.put_event()

    def on_stop_order(self, stop_order: StopOrder) -> None:
        return

    # ======================
    # 崩盘风险覆盖
    # ======================
    def _update_crash_overlay(self, bar: BarData) -> None:
        """更新崩盘风险指标"""
        if self.am.count < 2:
            return

        prev_close = float(self.am.close[-2])
        if prev_close <= 0 or bar.close_price <= 0:
            return

        # 计算对数收益率
        r_t = math.log(bar.close_price / prev_close)
        excess = r_t - self.rbar

        # EMA平滑
        tau = max(2, int(self.tau_mispricing))
        a = 1.0 - 1.0 / tau
        self.log_delta = (1.0 - a) * excess + a * self.log_delta

        # 计算lambda值
        self.lambda_long = self._sigmoid((self.log_delta - self.log_delta_ref) / max(1e-6, self.lambda_s))
        self.lambda_short = self._sigmoid(((-self.log_delta) - self.log_delta_ref) / max(1e-6, self.lambda_s))

        # 计算风险缩放因子
        lam_for_scale = min(max(self.lambda_long, self.lambda_short), 1.0)
        self.risk_scale = max(self.min_risk_scale, 1.0 - lam_for_scale)

        # 保存历史lambda值
        self._lambda_hist_long.append(self.lambda_long)
        self._lambda_hist_short.append(self.lambda_short)

        # 保持历史长度
        keep = max(10, self.lambda_decreasing_bars + 2)
        if len(self._lambda_hist_long) > keep:
            self._lambda_hist_long = self._lambda_hist_long[-keep:]
        if len(self._lambda_hist_short) > keep:
            self._lambda_hist_short = self._lambda_hist_short[-keep:]

    @staticmethod
    def _sigmoid(x: float) -> float:
        x = max(-40.0, min(40.0, x))
        return 1.0 / (1.0 + math.exp(-x))

    def _lambda_is_decreasing(self, direction: int) -> bool:
        """检查lambda是否在下降"""
        n = max(1, int(self.lambda_decreasing_bars))
        hist = self._lambda_hist_long if direction > 0 else self._lambda_hist_short
        if len(hist) < n + 1:
            return False
        seq = hist[-(n + 1):]
        return all(seq[i] > seq[i + 1] for i in range(len(seq) - 1))

    def _allowed_adds_by_lambda(self, lam: float) -> int:
        """根据lambda值确定允许的加仓次数"""
        if lam >= self.add_lambda_hard:
            return 0
        if lam >= self.add_lambda_soft:
            return max(0, min(self.max_adds, int(self.add_lambda_mid_max_adds)))
        return int(self.max_adds)

    # ======================
    # 入场逻辑
    # ======================
    def _enter_long(self, unit: int, price: float, n: float) -> None:
        self.unit_size = unit
        self.entry_price = price
        self.entry_n = n
        self.last_add_price = price
        self.adds_done = 0
        self.buy(price=price, volume=unit)

    def _enter_short(self, unit: int, price: float, n: float) -> None:
        self.unit_size = unit
        self.entry_price = price
        self.entry_n = n
        self.last_add_price = price
        self.adds_done = 0
        self.short(price=price, volume=unit)

    def _entry_fill_price_long(self, bar: BarData, trigger: float) -> float:
        """入场成交价：如果开盘价已突破，用开盘价；否则用突破价"""
        return bar.open_price if bar.open_price >= trigger else trigger

    def _entry_fill_price_short(self, bar: BarData, trigger: float) -> float:
        return bar.open_price if bar.open_price <= trigger else trigger

    # ======================
    # 仓位管理
    # ======================
    def _manage_long(self, bar: BarData, exit_low: float) -> None:
        pos_abs = abs(self.pos)
        lam = self.lambda_long if self.enable_crash_overlay else 0.0

        # 1. 极端风险强制离场
        if self.enable_crash_overlay and lam >= self.force_exit_lambda:
            self.sell(price=self._exit_fill_price_forced_long(bar), volume=pos_abs)
            self._arm_cooldown()
            return

        # 2. 高风险减仓到1个单位
        if self.enable_crash_overlay and lam >= self.delever_lambda and self.unit_size > 0:
            if pos_abs > self.unit_size:
                reduce_vol = pos_abs - self.unit_size
                self.sell(price=self._exit_fill_price_forced_long(bar), volume=reduce_vol)
                return

        # 3. 唐奇安离场
        if bar.low_price <= exit_low:
            self.sell(price=self._exit_fill_price_long(bar, exit_low), volume=pos_abs)
            self._arm_cooldown()
            return

        # 4. 止损（考虑最小止损地板）
        stop_n_eff = self.stop_n
        if self.enable_crash_overlay and lam >= self.tighten_lambda:
            stop_n_eff = min(self.stop_n, self.stop_n_tight)

        stop_dist_points = max(
            stop_n_eff * self.entry_n,
            self.avg_stop_loss_rmb_per_hand / max(1e-9, self.contract_size)
        )
        stop_price = self.entry_price - stop_dist_points

        if bar.low_price <= stop_price:
            self.sell(price=self._exit_fill_price_long(bar, stop_price), volume=pos_abs)
            self._arm_cooldown()
            return

        # 5. 加仓
        self._try_add_long(bar)

    def _manage_short(self, bar: BarData, exit_high: float) -> None:
        pos_abs = abs(self.pos)
        lam = self.lambda_short if self.enable_crash_overlay else 0.0

        # 强制离场
        if self.enable_crash_overlay and lam >= self.force_exit_lambda:
            self.cover(price=self._exit_fill_price_forced_short(bar), volume=pos_abs)
            self._arm_cooldown()
            return

        # 减仓
        if self.enable_crash_overlay and lam >= self.delever_lambda and self.unit_size > 0:
            if pos_abs > self.unit_size:
                reduce_vol = pos_abs - self.unit_size
                self.cover(price=self._exit_fill_price_forced_short(bar), volume=reduce_vol)
                return

        # 唐奇安离场
        if bar.high_price >= exit_high:
            self.cover(price=self._exit_fill_price_short(bar, exit_high), volume=pos_abs)
            self._arm_cooldown()
            return

        # 止损
        stop_n_eff = self.stop_n
        if self.enable_crash_overlay and lam >= self.tighten_lambda:
            stop_n_eff = min(self.stop_n, self.stop_n_tight)

        stop_dist_points = max(
            stop_n_eff * self.entry_n,
            self.avg_stop_loss_rmb_per_hand / max(1e-9, self.contract_size)
        )
        stop_price = self.entry_price + stop_dist_points

        if bar.high_price >= stop_price:
            self.cover(price=self._exit_fill_price_short(bar, stop_price), volume=pos_abs)
            self._arm_cooldown()
            return

        # 加仓
        self._try_add_short(bar)

    # ======================
    # 加仓逻辑（金字塔加仓）
    # ======================
    def _try_add_long(self, bar: BarData) -> None:
        if self.unit_size <= 0 or self.entry_n <= 0:
            return

        lam = self.lambda_long if self.enable_crash_overlay else 0.0
        max_adds_eff = self.max_adds

        # 风险门控
        if self.enable_crash_overlay:
            max_adds_eff = self._allowed_adds_by_lambda(lam)
            if self.adds_done >= max_adds_eff:
                return
            if not self._lambda_is_decreasing(direction=+1):
                return

        if self.adds_done >= self.max_adds:
            return

        # 计算加仓触发价
        step = self.add_step_n * self.entry_n
        trigger = self.last_add_price + step

        # 检查是否触发
        hit = (bar.close_price >= trigger) if self.use_close_for_breakout else (bar.high_price >= trigger)
        if not hit:
            return

        # 仓位上限检查
        if abs(self.pos) + self.unit_size > self.max_pos:
            return

        # 总风险上限检查
        stop_dist_points = max(
            self.stop_n * self.entry_n,
            self.avg_stop_loss_rmb_per_hand / max(1e-9, self.contract_size)
        )
        stop_rmb_per_hand = stop_dist_points * self.contract_size
        pos_after = abs(self.pos) + self.unit_size
        total_risk_value = pos_after * stop_rmb_per_hand
        if total_risk_value > self.capital * self.max_total_risk_percent:
            return

        # 执行加仓
        add_price = bar.open_price if bar.open_price >= trigger else trigger
        self.buy(price=add_price, volume=self.unit_size)
        self.adds_done += 1
        self.last_add_price = trigger

        # 允许一根K线内多次加仓
        if not self.add_one_per_bar:
            while self.adds_done < min(self.max_adds, max_adds_eff):
                trigger2 = self.last_add_price + step
                hit2 = (bar.close_price >= trigger2) if self.use_close_for_breakout else (bar.high_price >= trigger2)
                if not hit2:
                    break
                if abs(self.pos) + self.unit_size > self.max_pos:
                    break
                pos_after2 = abs(self.pos) + self.unit_size
                total_risk_value2 = pos_after2 * stop_rmb_per_hand
                if total_risk_value2 > self.capital * self.max_total_risk_percent:
                    break
                add_price2 = bar.open_price if bar.open_price >= trigger2 else trigger2
                self.buy(price=add_price2, volume=self.unit_size)
                self.adds_done += 1
                self.last_add_price = trigger2

    def _try_add_short(self, bar: BarData) -> None:
        if self.unit_size <= 0 or self.entry_n <= 0:
            return

        lam = self.lambda_short if self.enable_crash_overlay else 0.0
        max_adds_eff = self.max_adds

        if self.enable_crash_overlay:
            max_adds_eff = self._allowed_adds_by_lambda(lam)
            if self.adds_done >= max_adds_eff:
                return
            if not self._lambda_is_decreasing(direction=-1):
                return

        if self.adds_done >= self.max_adds:
            return

        step = self.add_step_n * self.entry_n
        trigger = self.last_add_price - step

        hit = (bar.close_price <= trigger) if self.use_close_for_breakout else (bar.low_price <= trigger)
        if not hit:
            return

        if abs(self.pos) + self.unit_size > self.max_pos:
            return

        stop_dist_points = max(
            self.stop_n * self.entry_n,
            self.avg_stop_loss_rmb_per_hand / max(1e-9, self.contract_size)
        )
        stop_rmb_per_hand = stop_dist_points * self.contract_size
        pos_after = abs(self.pos) + self.unit_size
        total_risk_value = pos_after * stop_rmb_per_hand
        if total_risk_value > self.capital * self.max_total_risk_percent:
            return

        add_price = bar.open_price if bar.open_price <= trigger else trigger
        self.short(price=add_price, volume=self.unit_size)
        self.adds_done += 1
        self.last_add_price = trigger

        if not self.add_one_per_bar:
            while self.adds_done < min(self.max_adds, max_adds_eff):
                trigger2 = self.last_add_price - step
                hit2 = (bar.close_price <= trigger2) if self.use_close_for_breakout else (bar.low_price <= trigger2)
                if not hit2:
                    break
                if abs(self.pos) + self.unit_size > self.max_pos:
                    break
                pos_after2 = abs(self.pos) + self.unit_size
                total_risk_value2 = pos_after2 * stop_rmb_per_hand
                if total_risk_value2 > self.capital * self.max_total_risk_percent:
                    break
                add_price2 = bar.open_price if bar.open_price <= trigger2 else trigger2
                self.short(price=add_price2, volume=self.unit_size)
                self.adds_done += 1
                self.last_add_price = trigger2

    # ======================
    # 头寸计算（已简化，移除费用）
    # ======================
    def _calc_unit_size_for_entry(self, n_value: float, direction: int) -> int:
        """
        计算单位头寸大小（基于海龟公式）
        修改：移除了费用考虑，只计算止损风险
        """
        if n_value <= 0 or self.contract_size <= 0:
            return 0

        # 风险缩放因子（来自崩盘风险覆盖）
        risk_eff = self.risk_percent
        if self.enable_crash_overlay:
            risk_eff = self.risk_percent * float(self.risk_scale)

        # 止损距离（考虑最小止损地板）
        stop_dist_points = max(
            self.stop_n * n_value,
            self.avg_stop_loss_rmb_per_hand / self.contract_size
        )

        # 每手止损风险（元）
        stop_rmb = stop_dist_points * self.contract_size

        # 修改：移除费用计算
        risk_per_hand = stop_rmb  # 只考虑止损风险

        if risk_per_hand <= 0:
            return 0

        # 有效资本
        capital_eff = self.capital  # 修改：不再扣除累计费用

        # 计算头寸
        raw = (capital_eff * risk_eff) / risk_per_hand
        if not np.isfinite(raw) or raw < 1.0:
            return 0

        unit = int(math.floor(raw))

        # 应用上限
        unit = min(unit, int(self.max_unit_size))
        max_unit_by_pos = max(1, int(self.max_pos // max(1, (1 + self.max_adds))))
        unit = min(unit, max_unit_by_pos)

        return max(1, unit)

    # ======================
    # 保守离场成交价模型
    # ======================
    def _exit_fill_price_long(self, bar: BarData, trigger: float) -> float:
        if not self.conservative_exit_fill:
            return bar.close_price
        if bar.open_price <= trigger:
            return bar.open_price
        return min(trigger, bar.close_price)

    def _exit_fill_price_short(self, bar: BarData, trigger: float) -> float:
        if not self.conservative_exit_fill:
            return bar.close_price
        if bar.open_price >= trigger:
            return bar.open_price
        return max(trigger, bar.close_price)

    def _exit_fill_price_forced_long(self, bar: BarData) -> float:
        return min(bar.open_price, bar.close_price) if self.conservative_exit_fill else bar.close_price

    def _exit_fill_price_forced_short(self, bar: BarData) -> float:
        return max(bar.open_price, bar.close_price) if self.conservative_exit_fill else bar.close_price

    # ======================
    # 唐奇安通道计算
    # ======================
    def _donchian_prev(self, window: int) -> Tuple[float, float]:
        """
        计算唐奇安通道（只使用当前K线之前的数据）
        """
        if window <= 0:
            return 0.0, 0.0
        if self.am.count < window + 1:
            return 0.0, 0.0

        highs = self.am.high
        lows = self.am.low
        hh = float(np.max(highs[-window - 1: -1]))
        ll = float(np.min(lows[-window - 1: -1]))

        return hh, ll

    # ======================
    # 辅助函数
    # ======================
    def _arm_cooldown(self) -> None:
        """启动冷却期"""
        if self.cooldown_bars > 0:
            self.cooldown_count = int(self.cooldown_bars)

    def _reset_trade_state_if_flat(self) -> None:
        """平仓时重置交易状态"""
        self.unit_size = 0
        self.entry_price = 0.0
        self.entry_n = 0.0
        self.last_add_price = 0.0
        self.adds_done = 0