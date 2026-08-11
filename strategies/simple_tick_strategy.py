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
from vnpy.trader.constant import Direction, Offset, OrderType, Interval, Status
from vnpy.trader.object import BarData
from datetime import datetime
import numpy as np


class SimpleTickStrategy(CtaTemplate):
    """
    简单的Tick级别交易策略
    策略逻辑：
    1. 使用买卖价差作为入场信号
    2. 当最新价突破最近N个Tick的平均价时入场
    3. 固定止损止盈
    """

    author = "Tick_Strategy_Developer"

    # 策略参数
    tick_window = 50  # 用于计算平均价格的Tick数量
    spread_threshold = 2.0  # 买卖价差阈值（指数点）
    stop_loss = 10.0  # 止损点数
    take_profit = 20.0  # 止盈点数
    fixed_size = 1  # 固定交易手数

    # 策略变量
    avg_price = 0.0  # 平均价格
    last_price = 0.0  # 上一个Tick的最新价
    spread = 0.0  # 当前买卖价差
    tick_count = 0  # Tick计数器

    parameters = ["tick_window", "spread_threshold", "stop_loss", "take_profit", "fixed_size"]
    variables = ["avg_price", "last_price", "spread", "tick_count"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """
        初始化策略
        """
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        # 用于存储最近N个Tick的价格
        self.price_buffer = []

        # 入场价格
        self.entry_price = 0.0

        # 订单跟踪
        self.long_orderid = ""
        self.short_orderid = ""

        # 状态标志
        self.is_trading_hours = False

    def on_init(self) -> None:
        """
        策略初始化回调
        """
        self.write_log(f"策略初始化: {self.strategy_name}")
        self.write_log(f"交易品种: {self.vt_symbol}")
        self.write_log(f"参数设置: tick_window={self.tick_window}, spread_threshold={self.spread_threshold}")

        # 初始化价格缓冲区
        self.price_buffer = []

        # 加载Tick数据（如果需要的话）
        # 注意：vn.py 4.2可能需要通过特定方式加载Tick数据
        self.load_tick(10)  # 加载最近10天的Tick数据

    def on_start(self) -> None:
        """
        策略启动回调
        """
        self.write_log(f"策略启动: {self.strategy_name}")
        self.put_event()

    def on_stop(self) -> None:
        """
        策略停止回调
        """
        self.write_log(f"策略停止: {self.strategy_name}")

        # 取消所有未完成订单
        if self.long_orderid:
            self.cancel_order(self.long_orderid)
        if self.short_orderid:
            self.cancel_order(self.short_orderid)

        self.put_event()

    def check_trading_hours(self, tick: TickData) -> bool:
        """
        检查是否为交易时间
        对于CFFEX股指期货：9:30-11:30, 13:00-15:00
        """
        time_now = tick.datetime.time()

        # 上午交易时间
        morning_start = datetime.strptime("09:30:00", "%H:%M:%S").time()
        morning_end = datetime.strptime("11:30:00", "%H:%M:%S").time()

        # 下午交易时间
        afternoon_start = datetime.strptime("13:00:00", "%H:%M:%S").time()
        afternoon_end = datetime.strptime("15:00:00", "%H:%M:%S").time()

        # 检查是否在交易时间内
        if ((morning_start <= time_now < morning_end) or
                (afternoon_start <= time_now < afternoon_end)):
            return True

        return False

    def update_price_buffer(self, price: float) -> None:
        """
        更新价格缓冲区
        """
        # 添加新价格
        self.price_buffer.append(price)

        # 保持缓冲区大小不超过设定值
        if len(self.price_buffer) > self.tick_window:
            self.price_buffer.pop(0)

    def calculate_avg_price(self) -> float:
        """
        计算平均价格
        """
        if len(self.price_buffer) < self.tick_window:
            return 0.0

        return np.mean(self.price_buffer)

    def calculate_spread(self, tick: TickData) -> float:
        """
        计算买卖价差
        """
        if tick.bid_price_1 and tick.ask_price_1:
            return tick.ask_price_1 - tick.bid_price_1
        return 0.0

    def should_open_long(self, tick: TickData) -> bool:
        """
        判断是否应该开多仓
        条件：最新价 > 平均价 + 价差阈值 且 当前无持仓
        """
        # 检查是否在交易时间
        if not self.is_trading_hours:
            return False

        # 检查是否有持仓
        if self.pos != 0:
            return False

        # 检查价格数据是否有效
        if self.avg_price <= 0 or tick.last_price <= 0:
            return False

        # 检查买卖价差是否过大
        if self.spread > self.spread_threshold:
            self.write_log(f"价差过大: {self.spread} > {self.spread_threshold}")
            return False

        # 开多条件：最新价高于平均价
        if tick.last_price > self.avg_price:
            return True

        return False

    def should_open_short(self, tick: TickData) -> bool:
        """
        判断是否应该开空仓
        条件：最新价 < 平均价 - 价差阈值 且 当前无持仓
        """
        # 检查是否在交易时间
        if not self.is_trading_hours:
            return False

        # 检查是否有持仓
        if self.pos != 0:
            return False

        # 检查价格数据是否有效
        if self.avg_price <= 0 or tick.last_price <= 0:
            return False

        # 检查买卖价差是否过大
        if self.spread > self.spread_threshold:
            return False

        # 开空条件：最新价低于平均价
        if tick.last_price < self.avg_price:
            return True

        return False

    def should_close_long(self, tick: TickData) -> bool:
        """
        判断是否应该平多仓
        条件：达到止损或止盈
        """
        if self.pos <= 0:
            return False

        # 计算盈亏点数
        if self.entry_price > 0:
            profit = tick.last_price - self.entry_price

            # 止损条件
            if profit <= -self.stop_loss:
                self.write_log(f"多仓触发止损: 入场价={self.entry_price}, 当前价={tick.last_price}, 亏损={profit}")
                return True

            # 止盈条件
            if profit >= self.take_profit:
                self.write_log(f"多仓触发止盈: 入场价={self.entry_price}, 当前价={tick.last_price}, 盈利={profit}")
                return True

        return False

    def should_close_short(self, tick: TickData) -> bool:
        """
        判断是否应该平空仓
        条件：达到止损或止盈
        """
        if self.pos >= 0:
            return False

        # 计算盈亏点数
        if self.entry_price > 0:
            profit = self.entry_price - tick.last_price

            # 止损条件
            if profit <= -self.stop_loss:
                self.write_log(f"空仓触发止损: 入场价={self.entry_price}, 当前价={tick.last_price}, 亏损={profit}")
                return True

            # 止盈条件
            if profit >= self.take_profit:
                self.write_log(f"空仓触发止盈: 入场价={self.entry_price}, 当前价={tick.last_price}, 盈利={profit}")
                return True

        return False

    def open_long(self, tick: TickData) -> None:
        """
        开多仓
        """
        if self.pos != 0:
            return

        # 以卖一价买入
        price = tick.ask_price_1 if tick.ask_price_1 > 0 else tick.last_price

        # 发送买入订单
        orderids = self.buy(price, self.fixed_size)
        if orderids:
            self.long_orderid = orderids[0]
            self.write_log(f"发送买入订单: 价格={price}, 手数={self.fixed_size}")

    def open_short(self, tick: TickData) -> None:
        """
        开空仓
        """
        if self.pos != 0:
            return

        # 以买一价卖出
        price = tick.bid_price_1 if tick.bid_price_1 > 0 else tick.last_price

        # 发送卖出订单
        orderids = self.short(price, self.fixed_size)
        if orderids:
            self.short_orderid = orderids[0]
            self.write_log(f"发送卖出订单: 价格={price}, 手数={self.fixed_size}")

    def close_long(self, tick: TickData) -> None:
        """
        平多仓
        """
        if self.pos <= 0:
            return

        # 以买一价卖出平仓
        price = tick.bid_price_1 if tick.bid_price_1 > 0 else tick.last_price

        # 发送卖出平仓订单
        orderids = self.sell(price, abs(self.pos))
        if orderids:
            self.write_log(f"发送平多订单: 价格={price}, 手数={abs(self.pos)}")

    def close_short(self, tick: TickData) -> None:
        """
        平空仓
        """
        if self.pos >= 0:
            return

        # 以卖一价买入平仓
        price = tick.ask_price_1 if tick.ask_price_1 > 0 else tick.last_price

        # 发送买入平仓订单
        orderids = self.cover(price, abs(self.pos))
        if orderids:
            self.write_log(f"发送平空订单: 价格={price}, 手数={abs(self.pos)}")

    def on_tick(self, tick: TickData) -> None:
        """
        Tick数据更新回调
        这是策略的核心逻辑
        """
        # 更新Tick计数器
        self.tick_count += 1

        # 检查交易时间
        self.is_trading_hours = self.check_trading_hours(tick)

        # 更新价格缓冲区
        if tick.last_price > 0:
            self.update_price_buffer(tick.last_price)
            self.last_price = tick.last_price

        # 计算平均价格
        self.avg_price = self.calculate_avg_price()

        # 计算买卖价差
        self.spread = self.calculate_spread(tick)

        # 只在有足够数据时进行交易
        if len(self.price_buffer) < self.tick_window:
            if self.tick_count % 100 == 0:  # 每100个Tick显示一次状态
                self.write_log(f"数据积累中: {len(self.price_buffer)}/{self.tick_window}")
            return

        # 显示策略状态（每100个Tick显示一次）
        if self.tick_count % 100 == 0:
            self.write_log(
                f"状态: 持仓={self.pos}, 最新价={tick.last_price}, 平均价={self.avg_price:.2f}, 价差={self.spread:.2f}")

        # 交易逻辑
        if self.pos == 0:  # 无持仓状态
            # 检查开多信号
            if self.should_open_long(tick):
                self.open_long(tick)

            # 检查开空信号
            elif self.should_open_short(tick):
                self.open_short(tick)

        elif self.pos > 0:  # 持有多仓
            # 检查平多信号
            if self.should_close_long(tick):
                self.close_long(tick)

        elif self.pos < 0:  # 持有空仓
            # 检查平空信号
            if self.should_close_short(tick):
                self.close_short(tick)

        # 更新策略状态
        self.put_event()

    def on_bar(self, bar: BarData) -> None:
        """
        Bar数据更新回调
        本策略主要使用Tick数据，但也可以处理Bar数据
        """
        # 如果需要，可以在这里处理Bar数据
        # 例如：记录每日最高最低价等
        pass

    def on_order(self, order: OrderData) -> None:
        """
        订单更新回调
        """
        # 如果订单完全成交
        if order.status == Status.ALLTRADED:
            if order.direction == Direction.LONG:
                # 买入订单成交
                if order.offset == Offset.OPEN:
                    self.write_log(f"买入开仓成交: 价格={order.price}, 数量={order.volume}")
                    self.entry_price = order.price
                elif order.offset == Offset.CLOSE:
                    self.write_log(f"买入平仓成交: 价格={order.price}, 数量={order.volume}")
                    self.entry_price = 0.0

            elif order.direction == Direction.SHORT:
                # 卖出订单成交
                if order.offset == Offset.OPEN:
                    self.write_log(f"卖出开仓成交: 价格={order.price}, 数量={order.volume}")
                    self.entry_price = order.price
                elif order.offset == Offset.CLOSE:
                    self.write_log(f"卖出平仓成交: 价格={order.price}, 数量={order.volume}")
                    self.entry_price = 0.0

        # 如果订单被拒绝
        if order.status == Status.REJECTED:
            self.write_log(f"订单被拒绝: {order.orderid}, 原因={order.status}")

            # 重置订单ID
            if order.orderid == self.long_orderid:
                self.long_orderid = ""
            elif order.orderid == self.short_orderid:
                self.short_orderid = ""

        # 更新策略状态
        self.put_event()

    def on_trade(self, trade: TradeData) -> None:
        """
        成交更新回调
        """
        # 记录成交
        if trade.direction == Direction.LONG:
            if trade.offset == Offset.OPEN:
                self.write_log(f"=== 开多仓成交 ===")
            else:
                self.write_log(f"=== 平空仓成交 ===")
        else:
            if trade.offset == Offset.OPEN:
                self.write_log(f"=== 开空仓成交 ===")
            else:
                self.write_log(f"=== 平多仓成交 ===")

        self.write_log(f"成交详情: 价格={trade.price}, 数量={trade.volume}, 时间={trade.datetime}")

        # 更新策略状态
        self.put_event()

    def on_stop_order(self, stop_order: StopOrder) -> None:
        """
        停止单更新回调
        """
        pass