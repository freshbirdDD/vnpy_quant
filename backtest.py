"""
backtest_jhd_strategy_v4.py
vn.py 4.2+ 版本的回测脚本
"""
import os
from vnpy.trader.setting import SETTINGS

print(f"当前工作目录: {os.getcwd()}")
print(f"数据库配置: {SETTINGS.get('database', '未配置')}")

# 检查默认数据库路径
db_path = os.path.join(os.path.expanduser("~"), ".vntrader", "database.db")
print(f"默认数据库路径: {db_path}")
print(f"数据库文件存在: {os.path.exists(db_path)}")


import pandas as pd
from datetime import datetime, timedelta
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.object import HistoryRequest
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import get_database
# from vnpy_ctp import CtpGateway
from vnpy_ctastrategy import CtaStrategyApp
from vnpy_ctastrategy.backtesting import BacktestingEngine, OptimizationSetting

# TODO 在这里import 你的策略，例如MyTurtleStrategy
from vnpy_ctastrategy.strategies.my_turtle_strategy import MyTurtleStrategy as MyStrategy  # 修改为你的策略路径


class BacktestRunner:
    """vn.py 4.2版本的回测运行器"""

    def __init__(self):
        # 创建事件引擎和主引擎
        self.event_engine = EventEngine()
        self.main_engine = MainEngine(self.event_engine)

        # 添加CTA策略应用
        self.main_engine.add_app(CtaStrategyApp)

        # 获取CTA策略引擎（用于回测）
        self.cta_engine = self.main_engine.get_engine("CtaStrategy")

        # 创建独立的回测引擎
        self.backtesting_engine = BacktestingEngine()

    def configure_backtest(self, start_date=None, end_date=None, vt_symbol="IF888.CFFEX",
                           interval=Interval.MINUTE, rate=0.0003, slippage=0.2,
                           size=300, pricetick=0.2, capital=1_000_000):
        """配置回测参数，明确指定时间范围"""
        print("配置回测参数...")

        # 明确指定要回测的时间范围，如果没有指定，默认为从30天前到昨天
        if start_date is None:
            start_date = datetime.now() - timedelta(days=30)
            print(f"未指定开始时间，默认为{start_date}")
        if end_date is None:
            end_date = datetime.now() - timedelta(days=1)
            print(f"未指定结束时间，默认为{end_date}")



        # 设置回测参数
        self.backtesting_engine.set_parameters(
            vt_symbol=vt_symbol,
            interval=interval,
            start=start_date,  # 明确指定开始时间
            end=end_date,  # 明确指定结束时间
            rate=rate,
            slippage=slippage,
            size=size,
            pricetick=pricetick,
            capital=capital,
        )

        print(f"✅ 回测时间范围明确指定为:")
        print(f"   开始: {start_date}")
        print(f"   结束: {end_date}")

    def load_data_from_database(self):
        """从数据库加载指定时间范围的数据"""
        print("\n查询指定时间范围的数据...")

        try:
            symbol = self.backtesting_engine.symbol
            exchange = self.backtesting_engine.exchange

            # 获取回测引擎配置的时间范围
            start_time = self.backtesting_engine.start
            end_time = self.backtesting_engine.end

            print(f"查询条件:")
            print(f"  合约: {symbol}.{exchange.value}")
            print(f"  时间: {start_time} 到 {end_time}")
            print(f"  周期: 1分钟")

            # 查询数据库，使用明确的时间范围
            database = get_database()
            bars = database.load_bar_data(
                symbol=symbol,
                exchange=exchange,
                interval=Interval.MINUTE,
                start=start_time,  # 使用明确的开始时间
                end=end_time  # 使用明确的结束时间
            )

            if not bars:
                print("❌ 错误：数据库中没有指定时间范围的数据！")
                print("\n可能的原因：")
                print(f"1. 数据库中没有任何 {symbol} 的数据")
                print(f"2. 数据时间不匹配（你需要 {start_time} 到 {end_time} 的数据）")

                # 查询数据库实际有哪些数据，给出明确提示
                print("\n📊 数据库现状检查：")
                all_bars = database.load_bar_data(
                    symbol=symbol,
                    exchange=exchange,
                    interval=Interval.MINUTE,
                    start=None,
                    end=None
                )

                if all_bars:
                    print(f"数据库中有 {len(all_bars)} 条 {symbol} 数据")
                    print(f"实际时间范围: {all_bars[0].datetime} 到 {all_bars[-1].datetime}")
                    print(f"\n💡 建议：将回测时间调整为以上实际范围")
                else:
                    print(f"数据库中没有 {symbol} 的任何数据")
                    print("请先运行数据生成脚本")

                return False

            print(f"✅ 成功加载 {len(bars)} 条K线数据")

            # 验证数据时间范围是否匹配
            actual_start = bars[0].datetime
            actual_end = bars[-1].datetime

            # 将时间对象转换为日期对象进行比较
            actual_start_date = actual_start.date()  # 只取日期部分
            actual_end_date = actual_end.date()
            start_date_need = start_time.date()
            end_date_need = end_time.date()

            if actual_start_date > start_date_need or actual_end_date < end_date_need:
                print("⚠️  警告：数据日期范围不完全覆盖回测需求")
                print(f"   需要日期: {start_date_need} 到 {end_date_need}")
                print(f"   实际日期: {actual_start_date} 到 {actual_end_date}")
            else:
                print("✅ 数据日期范围满足回测需求")
                # 即使具体时间不完全匹配，只要日期覆盖就足够了

            # 将数据添加到回测引擎
            self.backtesting_engine.history_data.extend(bars)
            self.backtesting_engine.loaded_data = True

            return True

        except Exception as e:
            print(f"❌ 数据加载失败: {e}")
            import traceback
            traceback.print_exc()
            return False

    def run_backtest(self, strategy_class, strategy_params=None):
        """运行回测"""
        if strategy_params is None:
            strategy_params = {}

        print(f"\n开始回测策略: {strategy_class.__name__}")
        print(f"策略参数: {strategy_params}")

        try:
            # 添加策略到回测引擎
            self.backtesting_engine.add_strategy(
                strategy_class=strategy_class,
                setting=strategy_params
            )

            # 运行回测
            print("运行回测计算...")
            self.backtesting_engine.run_backtesting()

            # 计算统计结果
            self.backtesting_engine.calculate_result()
            statistics = self.backtesting_engine.calculate_statistics()

            print("✅ 回测计算完成")
            return statistics

        except Exception as e:
            print(f"❌ 回测运行失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    def show_detailed_results(self, statistics):
        """显示详细的回测结果"""
        if statistics:
            print("\n" + "=" * 70)
            print("📈 关键绩效指标")
            print("=" * 70)

            # 显示关键指标
            key_metrics = {
                "总收益率": "total_return",
                "年化收益率": "annual_return",
                "夏普比率": "sharpe_ratio",
                "最大回撤": "max_drawdown",
                "收益回撤比": "return_drawdown_ratio",
                "总成交次数": "total_trade_count",
                "盈利次数": "winning_trade_count",
                "亏损次数": "losing_trade_count",
                "胜率": "winning_rate",
                "总盈亏": "total_net_pnl",
                "日均盈亏": "daily_net_pnl",
            }

            for label, key in key_metrics.items():
                if key in statistics and statistics[key] is not None:
                    value = statistics[key]
                    formatted_value = _format_percentage_value(value, key)
                    print(f"{label:>15}: {formatted_value}")

        # 获取交易记录（不带盈亏）
        trades = self.backtesting_engine.trades

        if trades:
            trade_count = len(trades) if isinstance(trades, dict) else len(trades)
            print(f"\n📈 交易次数: {trade_count} 笔")

            # 显示基本信息
            if isinstance(trades, dict):
                trade_list = list(trades.values())[:10]  # 只显示前10笔
            else:
                trade_list = trades[:10]

            print("最近10笔交易:")
            for i, trade in enumerate(trade_list):
                print(f"  {i + 1}. {trade.datetime} {trade.direction.value}{trade.offset.value} "
                      f"@{trade.price} x{trade.volume}")

        # 显示回测统计结果（包含盈亏）
        if hasattr(self.backtesting_engine, 'statistics'):
            stats = self.backtesting_engine.statistics
            print("\n📊 回测统计结果:")
            if stats:
                for key, value in stats.items():
                    if value is not None:
                        print(f"  {key}: {value}")

    def _export_statistics_summary(self):
        """导出统计摘要"""
        try:
            # 获取统计结果
            stats = {}

            # 从回测引擎获取统计
            if hasattr(self.backtesting_engine, 'calculate_statistics'):
                stats = self.backtesting_engine.calculate_statistics()
            elif hasattr(self.backtesting_engine, 'statistics'):
                stats = self.backtesting_engine.statistics

            if stats:
                # 创建统计摘要
                summary = {
                    '回测开始时间': self.backtesting_engine.start.strftime('%Y-%m-%d %H:%M:%S'),
                    '回测结束时间': self.backtesting_engine.end.strftime('%Y-%m-%d %H:%M:%S'),
                    '合约代码': getattr(self.backtesting_engine, 'vt_symbol', 'N/A'),
                    'K线周期': getattr(self.backtesting_engine, 'interval', 'N/A'),
                    '初始资金': getattr(self.backtesting_engine, 'capital', 0),
                }

                # 添加关键指标
                key_metrics = {
                    '总收益率': 'total_return',
                    '年化收益率': 'annual_return',
                    '夏普比率': 'sharpe_ratio',
                    '最大回撤': 'max_drawdown',
                    '最大回撤比率': 'max_ddpercent',
                    '总成交次数': 'total_trade_count',
                    '盈利次数': 'winning_trade_count',
                    '亏损次数': 'losing_trade_count',
                    '胜率': 'winning_rate',
                    '总盈亏': 'total_net_pnl',
                    '日均盈亏': 'daily_net_pnl',
                }

                for label, key in key_metrics.items():
                    if key in stats and stats[key] is not None:
                        summary[label] = stats[key]

                # 保存到CSV
                summary_df = pd.DataFrame([summary])
                summary_df.to_csv('backtest_summary.csv', index=False, encoding='utf-8-sig')
                print("✅ 回测摘要已导出到 backtest_summary.csv")

        except Exception as e:
            print(f"警告: 统计摘要导出失败 - {e}")

    def _export_backtest_config(self):
        """导出回测配置"""
        try:
            config = {
                '策略名称': self.backtesting_engine.strategy.__class__.__name__,
                '策略参数': str(self.backtesting_engine.strategy.get_parameters()),
                '手续费率': getattr(self.backtesting_engine, 'rate', 0),
                '滑点': getattr(self.backtesting_engine, 'slippage', 0),
                '合约乘数': getattr(self.backtesting_engine, 'size', 0),
                '价格跳动': getattr(self.backtesting_engine, 'pricetick', 0),
                '数据源': '模拟数据',
                '生成时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            config_df = pd.DataFrame([config])
            config_df.to_csv('backtest_config.csv', index=False, encoding='utf-8-sig')
            print("✅ 回测配置已导出到 backtest_config.csv")

        except Exception as e:
            print(f"警告: 配置导出失败 - {e}")

    def export_results(self):
        """导出回测结果到CSV"""
        try:
            print("\n💾 导出回测结果...")

            # 1. 导出交易记录（注意trades可能是字典）
            trades = self.backtesting_engine.trades

            if trades:
                trade_list = []

                # 检查trades类型并处理
                if isinstance(trades, dict):
                    # 字典类型：键是交易ID，值是TradeData对象
                    for trade_id, trade_obj in trades.items():
                        trade_list.append({
                            'trade_id': trade_id,
                            'datetime': trade_obj.datetime,
                            'symbol': trade_obj.symbol,
                            'exchange': trade_obj.exchange.value,
                            'direction': trade_obj.direction.value,
                            'offset': trade_obj.offset.value,
                            'price': trade_obj.price,
                            'volume': trade_obj.volume,
                            # 注意：TradeData对象没有pnl属性
                            # 'pnl': trade_obj.pnl,  # ❌ 删除这行
                            'commission': getattr(trade_obj, 'commission', 0)
                        })
                elif isinstance(trades, list):
                    # 列表类型：直接包含TradeData对象
                    for trade in trades:
                        trade_list.append({
                            'datetime': trade.datetime,
                            'symbol': trade.symbol,
                            'exchange': trade.exchange.value,
                            'direction': trade.direction.value,
                            'offset': trade.offset.value,
                            'price': trade.price,
                            'volume': trade.volume,
                            # 'pnl': trade.pnl,  # ❌ 删除这行
                            'commission': getattr(trade, 'commission', 0)
                        })

                if trade_list:
                    trade_df = pd.DataFrame(trade_list)
                    # 按时间排序
                    if 'datetime' in trade_df.columns:
                        trade_df = trade_df.sort_values('datetime')
                    trade_df.to_csv('backtest_trades.csv', index=False, encoding='utf-8-sig')
                    print(f"✅ 交易记录已导出到 backtest_trades.csv ({len(trade_list)}笔)")

            # 2. 导出每日资金曲线（注意date是索引）
            df = self.backtesting_engine.daily_df

            if df is not None and not df.empty:
                # 重置索引，让date变成普通列
                df_export = df.reset_index()

                # 确保列名存在
                if 'date' in df_export.columns:
                    df_export = df_export.sort_values('date')

                # 添加额外的计算列（如果需要）
                if 'balance' in df_export.columns and 'return' in df_export.columns:
                    df_export['cumulative_return'] = (1 + df_export['return']).cumprod() - 1

                df_export.to_csv('backtest_daily.csv', index=False, encoding='utf-8-sig')
                print(f"✅ 每日资金曲线已导出到 backtest_daily.csv ({len(df_export)}天)")

                # 导出关键统计摘要
                self._export_statistics_summary()

            # 3. 导出回测配置和参数
            self._export_backtest_config()

            print("🎉 所有结果导出完成！")

        except Exception as e:
            print(f"❌ 结果导出失败: {e}")
            import traceback
            traceback.print_exc()


def _format_percentage_value(value, key):
    """格式化百分比值，处理vn.py不同版本返回值的差异"""
    if value is None:
        return "N/A"

    # 处理可能的数据格式
    value_str = str(value)

    # 情况1: 已经是百分比字符串 (如 "-14.87%")
    if '%' in value_str:
        return value_str

    # 情况2: 是小数 (如 -0.1487)
    try:
        num_value = float(value)

        # 判断是否应该是百分比
        if key in ["total_return", "annual_return", "max_ddpercent", "winning_rate",
                   "max_drawdown", "return_drawdown_ratio"]:

            # 修复异常大的值（如-1486.52%应该是-14.87%）
            if abs(num_value) > 100 and key != "sharpe_ratio":
                num_value = num_value / 100.0

            # 格式化为百分比
            return f"{num_value * 100:>8.2f}%"

        # 其他数值类型
        return f"{num_value:>10.2f}"
    except:
        return str(value)

def main():
    """主函数"""
    print("="*70)
    print("vn.py 4.2 策略回测系统")
    print("="*70)

    # 创建回测运行器
    runner = BacktestRunner()

    try:
        # TODO 配置回测参数
        # 开始日期
        start_date = datetime(2010, 4, 20)
        # 结束日期
        end_date = datetime(2010, 5, 15)
        # 本地代码
        vt_symbol = "IF1005.CFFEX"
        # K线周期
        interval = Interval.MINUTE
        # 手续费律
        rate = 0.000025
        # 交易滑点
        slippage = 0.2
        # 合约乘数
        size = 300
        # 价格跳动
        pricetick = 0.2
        # 回测资金
        capital = 1_000_000

        runner.configure_backtest(start_date, end_date, vt_symbol=vt_symbol,
                                  interval=interval, rate=rate, slippage=slippage,
                                  size=size, pricetick=pricetick, capital=capital)


        # 2. 加载指定时间范围的数据
        if not runner.load_data_from_database():
            print("\n💡 解决方案：")
            print("1. 运行数据生成脚本，并记录生成数据的时间范围")
            print("2. 将上面的 start_date 和 end_date 调整为数据实际时间")
            return

        # TODO 设置策略参数
        strategy_params = {
            "entry_window": 20,
            "exit_window": 10,
            "atr_window": 20,
            "fixed_size": 1
        }

        # 4. 运行回测（使用你的JhdStrategy类）
        print("\n" + "-"*70)
        statistics = runner.run_backtest(MyStrategy, strategy_params)

        # 5. 显示详细结果
        # runner.show_detailed_results(statistics)
        #
        # # 6. 导出结果
        # runner.export_results()

        print("\n" + "="*70)
        print("🎉 回测完成！")
        print("="*70)

    except Exception as e:
        print(f"\n❌ 回测过程出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":

    main()