from vnpy_ctastrategy import (
    CtaTemplate,
    StopOrder,
    Direction,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
)

import numpy as np

class MyTurtleStrategy(CtaTemplate):
    """"""
    author = "用Python的交易员"

    entry_window: int = 20  # 入场唐奇安通道窗口（突破前entry_window日最高价做多）
    exit_window: int = 10   # 出场唐奇安通道窗口（跌破前exit_window日最低价平多仓）
    atr_window: int = 20    # ATR计算窗口（用于计算止损和加仓间距）
    fixed_size: int = 1     # 每次交易的单位数量

    entry_up: float = 0     # 入场通道的上下轨，即唐奇安通道的上下轨。
    entry_down: float = 0
    exit_up: float = 0      # 出场通道的上下轨
    exit_down: float = 0
    atr_value: float = 0    # ATR值，用于衡量市场波动性。
    long_entry: float = 0   # 多头的入场价格。
    short_entry: float = 0  # 空头的入场价格。
    long_stop: float = 0    # 多头的止损价格
    short_stop: float = 0   # 空头的止损价格

    parameters = ["entry_window",
                  "exit_window",
                  "atr_window",
                  "fixed_size"]

    variables = ["entry_up", "entry_down", "exit_up", "exit_down", "atr_value"]

    def on_init(self) -> None:
        """
        Callback when strategy is inited.
        """
        self.write_log("策略初始化")
        # K线生成器
        self.bg: BarGenerator = BarGenerator(self.on_bar)
        # 数组管理器
        max_window = max(self.entry_window, self.exit_window, self.atr_window, 100)+20
        self.am: ArrayManager = ArrayManager(size=max_window)
        # self.am: ArrayManager = ArrayManager()
        # 请求加载20根K线
        # TODO 这里我有疑问，我以为这里加载的是回测日开始之前的最近20条K线，
        # 这样回测日第一条数据进来时，就有指导策略的统计指标可以用，但是这对吗
        self.load_bar(20)
        # self.load_bar(max_window)

    # 2. 数据引擎响应请求
    # - 回测模式：从本地数据库加载指定数量的历史K线
    # - 实盘模式：从交易所/数据源获取最近的历史数据

    # 3. 加载的数据会通过on_bar逐条推送给策略
    # 策略会收到20条历史K线，然后才开始接收实时数据

    def on_start(self) -> None:
        """
        Callback when strategy is started.
        """
        self.write_log("策略启动")

    def on_stop(self) -> None:
        """
        Callback when strategy is stopped.
        """
        self.write_log("策略停止")

        # 事件触发，当一个新的tick信息进来之后
    def on_tick(self, tick: TickData) -> None:
        """
        Callback of new tick data update.
        """
        # 更新当前的tick数据
        self.bg.update_tick(tick)

    # 被动接受外部数据源提供的K线，具体可以是日线，分钟线等等
    def on_bar(self, bar: BarData) -> None:
        """
        Callback of new bar data update.
        """
        # 1. 先取消所有未成交订单（清空旧状态）
        self.cancel_all()

        # 2. 更新数据计算新指标
        self.am.update_bar(bar)
        #TODO 注意这里的逻辑，仅当am加载满了预先设定好的close_array数量(例如100条)之后，self.am.inited才会为True，这时这里的return逻辑才不会被触发
        if not self.am.inited:
            return

        # 仅当没有持仓时，计算入场通道
        # 注意: 这里的donchian只是机械地计算过去20根K线的情况
        if not self.pos:
            self.entry_up, self.entry_down = self.am.donchian(
                self.entry_window
            )
        # 无论有无持仓，都计算出场通道，对应海龟策略的出场信号计算。
        self.exit_up, self.exit_down = self.am.donchian(self.exit_window)
        # 如果没有持仓，并重置入场和止损价格，然后分别在上轨挂买入订单，在下轨挂卖出订单。对应海龟策略的初始入场准备。
        if not self.pos:
            # 计算ATR值
            self.atr_value = self.am.atr(self.atr_window)

            self.long_entry = 0
            self.short_entry = 0
            self.long_stop = 0
            self.short_stop = 0
            # 在上轨挂买入订单
            self.send_buy_orders(self.entry_up)
            # 在下轨挂卖出订单
            self.send_short_orders(self.entry_down)
        # 如果持有多头仓位，继续在突破上轨时加仓（金字塔加仓），
        # 并计算出场价格：取初始止损（基于ATR）和出场通道下轨的最大值，然后挂出卖单。对应海龟策略的多头加仓和出场逻辑。
        elif self.pos > 0:
            self.send_buy_orders(self.entry_up)

            sell_price: float = max(self.long_stop, self.exit_down)
            self.sell(sell_price, abs(self.pos), True)
        # 如果持有空头仓位，继续在突破下轨时加仓，
        # 并计算出场价格：取初始止损（基于ATR）和出场通道上轨的最小值，然后挂出平仓单。对应海龟策略的空头加仓和出场逻辑。
        elif self.pos < 0:
            self.send_short_orders(self.entry_down)

            cover_price: float = min(self.short_stop, self.exit_up)
            self.cover(cover_price, abs(self.pos), True)

        self.put_event()

    def on_trade(self, trade: TradeData) -> None:
        """
        Callback of new trade data update.
        """
        # 当有成交时，记录入场价格，并设置初始止损。对于多头，止损为入场价减去2倍ATR；
        if trade.direction == Direction.LONG:
            self.long_entry = trade.price
            self.long_stop = self.long_entry - 2 * self.atr_value
        # 对于空头，止损为入场价加上2倍ATR。对应海龟策略的初始止损设置。
        else:
            self.short_entry = trade.price
            self.short_stop = self.short_entry + 2 * self.atr_value

    def on_order(self, order: OrderData) -> None:
        """
        Callback of new order data update.
        """
        pass

    def on_stop_order(self, stop_order: StopOrder) -> None:
        """
        Callback of stop order update.
        """
        pass

    def send_buy_orders(self, price: float) -> None:
        """"""
        # 挂出多头的加仓订单。海龟策略允许最多4个单位的多头仓位，每个单位之间间隔0.5倍ATR。对应海龟策略的金字塔加仓规则。
        t: float = self.pos / self.fixed_size
        # 在没有持仓的时候，直接按照当前的上轨去买入
        if t < 1:
            self.buy(price, self.fixed_size, True)

        if t < 2:
            self.buy(price + self.atr_value * 0.5, self.fixed_size, True)

        if t < 3:
            self.buy(price + self.atr_value, self.fixed_size, True)

        if t < 4:
            self.buy(price + self.atr_value * 1.5, self.fixed_size, True)

    def send_short_orders(self, price: float) -> None:
        """"""
        # 挂出空头的加仓订单。同样最多4个单位，每个单位间隔0.5倍ATR，但方向相反。
        t: float = self.pos / self.fixed_size

        if t > -1:
            self.short(price, self.fixed_size, True)

        if t > -2:
            self.short(price - self.atr_value * 0.5, self.fixed_size, True)

        if t > -3:
            self.short(price - self.atr_value, self.fixed_size, True)

        if t > -4:
            self.short(price - self.atr_value * 1.5, self.fixed_size, True)
