"""
convert_ctp_tick_to_vnpy.py
将CTP格式的五档Tick数据转换为vn.py格式
"""
import pandas as pd
from datetime import datetime, time
from vnpy.trader.constant import Exchange
from vnpy.trader.object import TickData
from vnpy.trader.database import BaseDatabase, get_database


class CtpTickConverter:
    """CTP Tick数据转换器"""

    def __init__(self):
        # 初始化数据库
        self.database: BaseDatabase = get_database()

        # CTP与vn.py字段映射
        self.field_mapping = {
            'InstrumentID': 'symbol',
            'LastPrice': 'last_price',
            'Volume': 'volume',
            'Turnover': 'turnover',
            'OpenInterest': 'open_interest',
            'UpdateTime': 'time_str',
            'ActionDay': 'date_str',
            'TradingDay': 'trading_day',
            'UpperLimitPrice': 'limit_up',
            'LowerLimitPrice': 'limit_down',
            'OpenPrice': 'open_price',
            'HighPrice': 'high_price',
            'LowPrice': 'low_price',
            'ClosePrice': 'close_price',
            'PreClosePrice': 'pre_close_price',
            'PreSettlementPrice': 'pre_settlement_price',
            'SettlementPrice': 'settlement_price',
            'AveragePrice': 'average_price',
            'PreOpenInterest': 'pre_open_interest',
            'CurrDelta': 'curr_delta',
            'PreDelta': 'pre_delta',
        }

        # 五档买卖盘映射
        self.bid_mapping = {
            'BidPrice1': 'bid_price_1',
            'BidVolume1': 'bid_volume_1',
            'BidPrice2': 'bid_price_2',
            'BidVolume2': 'bid_volume_2',
            'BidPrice3': 'bid_price_3',
            'BidVolume3': 'bid_volume_3',
            'BidPrice4': 'bid_price_4',
            'BidVolume4': 'bid_volume_4',
            'BidPrice5': 'bid_price_5',
            'BidVolume5': 'bid_volume_5',
        }

        self.ask_mapping = {
            'AskPrice1': 'ask_price_1',
            'AskVolume1': 'ask_volume_1',
            'AskPrice2': 'ask_price_2',
            'AskVolume2': 'ask_volume_2',
            'AskPrice3': 'ask_price_3',
            'AskVolume3': 'ask_volume_3',
            'AskPrice4': 'ask_price_4',
            'AskVolume4': 'ask_volume_4',
            'AskPrice5': 'ask_price_5',
            'AskVolume5': 'ask_volume_5',
        }

    def parse_ctp_time(self, date_str: str, time_str: str) -> datetime:
        """
        解析CTP的时间格式（直接解析版）

        规则：
        1. "03:06.1" → 03:06:00.100 (小时:分钟.秒)
        2. "29:00.1" → 05:00:00.100 (29-24=5小时)
        3. "30:00.6" → 06:00:00.600 (30-24=6小时)
        """
        try:
            # 1. 解析日期
            if len(date_str) == 8:  # YYYYMMDD
                year = int(date_str[:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
            else:
                # 尝试其他格式
                for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"]:
                    try:
                        dt = datetime.strptime(date_str, fmt)
                        year, month, day = dt.year, dt.month, dt.day
                        break
                    except:
                        continue
                else:
                    year, month, day = datetime.now().year, datetime.now().month, datetime.now().day

            # 2. 解析时间
            time_str = str(time_str).strip()

            # 初始化时间组件
            hours = 0
            minutes = 0
            seconds = 0
            milliseconds = 0

            if time_str:
                # 情况1: "HH:MM.SSS" 格式（如 "03:06.1"）
                if '.' in time_str and time_str.count(':') == 1:
                    # 分割小时:分钟 和 秒.毫秒
                    time_part, sec_part = time_str.split('.')

                    # 解析小时和分钟
                    if ':' in time_part:
                        h_m = time_part.split(':')
                        hours = int(h_m[0]) if h_m[0] else 0
                        minutes = int(h_m[1]) if len(h_m) > 1 and h_m[1] else 0
                    else:
                        hours = int(time_part)

                    # 解析秒和毫秒
                    sec_part = sec_part.ljust(3, '0')
                    seconds = int(sec_part[0]) if len(sec_part) > 0 else 0
                    milliseconds = int(sec_part[1:3]) if len(sec_part) > 2 else 0

                # 情况2: 其他格式，尝试直接解析
                else:
                    # 移除所有非数字字符，只保留数字
                    numbers = []
                    current_num = ''
                    for char in time_str:
                        if char.isdigit():
                            current_num += char
                        elif current_num:
                            numbers.append(int(current_num))
                            current_num = ''
                    if current_num:
                        numbers.append(int(current_num))

                    # 根据数字个数分配
                    if len(numbers) >= 1:
                        hours = numbers[0]
                    if len(numbers) >= 2:
                        minutes = numbers[1]
                    if len(numbers) >= 3:
                        seconds = numbers[2]
                    if len(numbers) >= 4:
                        # 毫秒可能是1-3位
                        ms_str = str(numbers[3])
                        if len(ms_str) == 1:
                            milliseconds = int(ms_str) * 100
                        elif len(ms_str) == 2:
                            milliseconds = int(ms_str) * 10
                        else:
                            milliseconds = int(ms_str[:3])

                # 3. 处理跨日情况（小时≥24）
                extra_days = 0
                if hours >= 24:
                    extra_days = hours // 24
                    hours = hours % 24

                # 4. 调整日期
                if extra_days > 0:
                    from datetime import timedelta
                    base_date = datetime(year, month, day)
                    adjusted_date = base_date + timedelta(days=extra_days)
                    year, month, day = adjusted_date.year, adjusted_date.month, adjusted_date.day

            # 5. 创建datetime对象
            return datetime(year, month, day, hours, minutes, seconds, milliseconds * 1000)

        except Exception as e:
            print(f"时间解析错误: date={date_str}, time={time_str}, error={e}")
            import traceback
            traceback.print_exc()
            return datetime.now()

    def convert_tick_row(self, row: pd.Series, exchange: Exchange = Exchange.CFFEX) -> TickData:
        """转换单行数据为TickData对象 - vn.py 4.2版本"""

        # 解析基础字段
        symbol = str(row.get('InstrumentID', ''))
        date_str = str(row.get('ActionDay', row.get('TradingDay', '')))
        time_str = str(row.get('UpdateTime', '00:00:00'))

        # 调试输出原始时间
        if hasattr(self, 'debug_mode') and self.debug_mode:
            print(f"原始: date={date_str}, time={time_str}")

        datetime_obj = self.parse_ctp_time(date_str, time_str)

        # 调试输出解析结果
        if hasattr(self, 'debug_mode') and self.debug_mode:
            print(f"解析后: {datetime_obj}")

        # 辅助函数：安全获取数值
        def get_float(field, default=0):
            value = row.get(field)
            if pd.notna(value) and str(value).strip() != '':
                try:
                    return float(value)
                except:
                    return default
            return default

        # 注意：根据你的检查，vn.py 4.2使用pre_close而不是pre_close_price
        # 使用正确的字段名创建TickData
        tick = TickData(
            gateway_name="CTP",
            symbol=symbol,
            exchange=exchange,
            datetime=datetime_obj,
            name="",

            # 基础字段
            volume=get_float('Volume'),
            turnover=get_float('Turnover'),
            open_interest=get_float('OpenInterest'),
            last_price=get_float('LastPrice'),
            last_volume=0,  # CTP数据没有这个字段，设为0

            # 价格限制
            limit_up=get_float('UpperLimitPrice'),
            limit_down=get_float('LowerLimitPrice'),

            # OHLC价格
            open_price=get_float('OpenPrice'),
            high_price=get_float('HighPrice'),
            low_price=get_float('LowPrice'),
            pre_close=get_float('PreClosePrice'),  # 注意：字段名是pre_close

            # 五档买价
            bid_price_1=get_float('BidPrice1'),
            bid_price_2=get_float('BidPrice2'),
            bid_price_3=get_float('BidPrice3'),
            bid_price_4=get_float('BidPrice4'),
            bid_price_5=get_float('BidPrice5'),

            # 五档卖价
            ask_price_1=get_float('AskPrice1'),
            ask_price_2=get_float('AskPrice2'),
            ask_price_3=get_float('AskPrice3'),
            ask_price_4=get_float('AskPrice4'),
            ask_price_5=get_float('AskPrice5'),

            # 五档买量
            bid_volume_1=get_float('BidVolume1'),
            bid_volume_2=get_float('BidVolume2'),
            bid_volume_3=get_float('BidVolume3'),
            bid_volume_4=get_float('BidVolume4'),
            bid_volume_5=get_float('BidVolume5'),

            # 五档卖量
            ask_volume_1=get_float('AskVolume1'),
            ask_volume_2=get_float('AskVolume2'),
            ask_volume_3=get_float('AskVolume3'),
            ask_volume_4=get_float('AskVolume4'),
            ask_volume_5=get_float('AskVolume5'),

            localtime=None,  # 本地时间，设为None
        )

        # 设置vt_symbol
        tick.vt_symbol = f"{tick.symbol}.{exchange.value}"

        # 设置可选字段（通过setattr，因为这些不在构造函数中）
        optional_fields = {
            'ClosePrice': 'close_price',  # 注意：vn.py没有这个字段，但我们可以添加
            'SettlementPrice': 'settlement_price',
            'PreSettlementPrice': 'pre_settlement_price',
            'AveragePrice': 'average_price',
            'PreOpenInterest': 'pre_open_interest',
            'CurrDelta': 'curr_delta',
            'PreDelta': 'pre_delta',
        }

        for ctp_field, attr_name in optional_fields.items():
            value = get_float(ctp_field)
            if value != 0:
                try:
                    setattr(tick, attr_name, value)
                except AttributeError:
                    # 如果字段不存在，动态添加
                    setattr(tick, attr_name, value)

        return tick

    def convert_csv_file(self, file_path: str, symbol_filter: str = None,
                         exchange: Exchange = Exchange.CFFEX,
                         save_to_db: bool = True) -> list:
        """
        转换整个CSV文件

        Args:
            file_path: CSV文件路径
            symbol_filter: 只转换特定合约（如"IF2401"），None表示所有
            exchange: 交易所
            save_to_db: 是否保存到数据库
        """
        print(f"读取文件: {file_path}")

        # 读取CSV文件
        try:
            df = pd.read_csv(file_path, dtype=str)  # 全部以字符串读取，避免类型问题
        except Exception as e:
            print(f"读取文件失败: {e}")
            return []

        print(f"原始数据行数: {len(df)}")

        # 过滤特定合约
        if symbol_filter:
            df = df[df['InstrumentID'] == symbol_filter]
            print(f"过滤后数据行数 ({symbol_filter}): {len(df)}")

        if len(df) == 0:
            print("没有符合条件的数据")
            return []

        # 转换数据
        ticks = []
        errors = []

        print("开始转换数据...")
        for idx, row in df.iterrows():
            try:
                tick = self.convert_tick_row(row, exchange)
                ticks.append(tick)

                # 进度显示
                if (idx + 1) % 10000 == 0:
                    print(f"已转换 {idx + 1}/{len(df)} 行")

            except Exception as e:
                errors.append((idx, str(e)))
                if len(errors) <= 10:  # 只显示前10个错误
                    print(f"行 {idx} 转换失败: {e}")

        print(f"转换完成: 成功 {len(ticks)} 条，失败 {len(errors)} 条")

        if errors:
            print(f"前10个错误: {errors[:10]}")

        # 保存到数据库
        if save_to_db and ticks:
            print("保存到数据库...")
            try:
                # 分批保存，避免内存问题
                batch_size = 10000
                for i in range(0, len(ticks), batch_size):
                    batch = ticks[i:i + batch_size]
                    self.database.save_tick_data(batch)
                    print(f"已保存 {min(i + batch_size, len(ticks))}/{len(ticks)} 条")

                print(f"✅ 成功保存 {len(ticks)} 条Tick数据到数据库")

            except Exception as e:
                print(f"❌ 保存到数据库失败: {e}")
                import traceback
                traceback.print_exc()

        return ticks

    def preview_conversion(self, file_path: str, n_rows: int = 5):
        """预览转换结果"""
        print(f"预览前{n_rows}行转换结果:")

        df = pd.read_csv(file_path, nrows=n_rows)

        for idx, row in df.iterrows():
            try:
                tick = self.convert_tick_row(row)
                print(f"\n行 {idx}: {tick.symbol} @ {tick.datetime}")
                print(f"  最新价: {tick.last_price}, 成交量: {tick.volume}")
                print(f"  买一档: {tick.bid_price_1} x {tick.bid_volume_1}")
                print(f"  卖一档: {tick.ask_price_1} x {tick.ask_volume_1}")
            except Exception as e:
                print(f"行 {idx} 转换失败: {e}")

def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='转换CTP Tick数据为vn.py格式')
    parser.add_argument('--file', type=str, required=True, help='CSV文件路径')
    parser.add_argument('--symbol', type=str, default='IF2401', help='合约代码（如IF2401）')
    parser.add_argument('--exchange', type=str, default='CFFEX', help='交易所（默认CFFEX）')
    parser.add_argument('--preview', action='store_true', help='只预览不保存')
    parser.add_argument('--no-save', action='store_true', help='不保存到数据库')

    args = parser.parse_args()

    # 创建转换器
    converter = CtpTickConverter()

    # 获取交易所
    try:
        exchange = Exchange(args.exchange)
    except:
        print(f"交易所 {args.exchange} 无效，使用默认CFFEX")
        exchange = Exchange.CFFEX

    if args.preview:
        # 预览模式
        converter.preview_conversion(args.file)
    else:
        # 转换模式
        ticks = converter.convert_csv_file(
            file_path=args.file,
            symbol_filter=args.symbol,
            exchange=exchange,
            save_to_db=not args.no_save
        )

        if ticks:
            # 显示统计信息
            print(f"\n📊 转换统计:")
            print(f"  开始时间: {ticks[0].datetime}")
            print(f"  结束时间: {ticks[-1].datetime}")
            print(f"  数据条数: {len(ticks)}")
            print(f"  合约代码: {ticks[0].symbol}")

            # 检查数据质量
            symbols = set(t.symbol for t in ticks)
            print(f"  包含合约: {list(symbols)}")

            # 时间间隔分析
            if len(ticks) > 1:
                intervals = []
                for i in range(1, min(100, len(ticks))):
                    interval = (ticks[i].datetime - ticks[i - 1].datetime).total_seconds()
                    intervals.append(interval)

                print(f"  平均间隔: {sum(intervals) / len(intervals):.3f}秒")
                print(f"  最小间隔: {min(intervals):.3f}秒")
                print(f"  最大间隔: {max(intervals):.3f}秒")



if __name__ == "__main__":
    # 示例用法
    # python convert_ctp_tick_to_vnpy.py --file your_data.csv --symbol IF2401
    # python convert_ctp_tick_to_vnpy.py --file your_data.csv --preview

    main()