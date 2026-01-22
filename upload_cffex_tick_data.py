"""
import_cffex_tick_data_fixed.py
vn.py 4.2版本 - 将包含多个CFFEX合约的Tick数据CSV导入数据库

修复版：正确使用TICK_FIELDS映射关系
"""
import pandas as pd
import numpy as np
from datetime import datetime, time
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
from vnpy.trader.constant import Exchange, Direction, Offset
from vnpy.trader.object import TickData
from vnpy.trader.database import BaseDatabase, get_database


class CFFEXTickDataImporterFixed:
    """CFFEX交易所多合约Tick数据导入器 (修复版)"""

    # CSV列名到TickData属性名的映射
    TICK_FIELDS = {
        # 必需字段
        'UpdateTime': 'datetime',           # 映射到TickData.datetime (需要转换为datetime对象)
        'InstrumentID': 'symbol',           # 映射到TickData.symbol

        # 价格和成交量字段
        'LastPrice': 'last_price',
        'Volume': 'volume',
        'Turnover': 'turnover',
        'OpenInterest': 'open_interest',

        # 买卖盘口字段
        'BidPrice1': 'bid_price_1',
        'BidVolume1': 'bid_volume_1',
        'AskPrice1': 'ask_price_1',
        'AskVolume1': 'ask_volume_1',

        # 其他价格档位
        'BidPrice2': 'bid_price_2',
        'BidVolume2': 'bid_volume_2',
        'AskPrice2': 'ask_price_2',
        'AskVolume2': 'ask_volume_2',

        'BidPrice3': 'bid_price_3',
        'BidVolume3': 'bid_volume_3',
        'AskPrice3': 'ask_price_3',
        'AskVolume3': 'ask_volume_3',

        'BidPrice4': 'bid_price_4',
        'BidVolume4': 'bid_volume_4',
        'AskPrice4': 'ask_price_4',
        'AskVolume4': 'ask_volume_4',

        'BidPrice5': 'bid_price_5',
        'BidVolume5': 'bid_volume_5',
        'AskPrice5': 'ask_price_5',
        'AskVolume5': 'ask_volume_5',

        # 其他价格字段
        'UpperLimitPrice': 'limit_up',
        'LowerLimitPrice': 'limit_down',
        'PreClosePrice': 'pre_close',
        # ToDo 暂时先注释掉，DB tick data里没有这个字段
        # 'PreSettlementPrice': 'pre_settlement',
        'OpenPrice': 'open_price',
        'HighPrice': 'high_price',
        'LowPrice': 'low_price',
        'SettlementPrice': 'settlement_price',
    }

    def __init__(self, file_path: str, custom_field_mapping: Dict[str, str] = None):
        """
        初始化导入器

        Args:
            file_path: CSV文件路径
            custom_field_mapping: 自定义字段映射，用于覆盖默认映射
        """
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(f"文件不存在: {self.file_path}")

        self.exchange = Exchange.CFFEX
        self.gateway_name = "TICK_CSV_IMPORT"
        self.database: BaseDatabase = get_database()

        # 更新字段映射（如果提供了自定义映射）
        if custom_field_mapping:
            self.TICK_FIELDS.update(custom_field_mapping)

        # 创建反向映射：TickData属性名 -> CSV列名
        self.reverse_mapping = {v: k for k, v in self.TICK_FIELDS.items()}

        # 统计信息
        self.stats = {
            'total_rows': 0,
            'valid_rows': 0,
            'invalid_rows': 0,
            'unique_symbols': set(),
            'time_range': {'start': None, 'end': None},
            'saved_ticks': 0,
            'missing_bid_ask': 0,
            'missing_last_price': 0,
            'field_mapping_used': self.TICK_FIELDS.copy()
        }

    def parse_datetime(self, dt_str: str) -> Optional[datetime]:
        """
        解析时间字符串为datetime对象
        """
        if pd.isna(dt_str) or not isinstance(dt_str, str):
            return None

        dt_str = str(dt_str).strip()
        if not dt_str:
            return None

        # 尝试多种时间格式
        date_formats = [
            '%Y-%m-%d %H:%M:%S.%f',
            '%Y/%m/%d %H:%M:%S.%f',
            '%Y%m%d %H:%M:%S.%f',
            '%Y-%m-%d %H:%M:%S',
            '%Y/%m/%d %H:%M:%S',
            '%Y%m%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y/%m/%d %H:%M',
            '%Y%m%d %H:%M',
        ]

        for fmt in date_formats:
            try:
                return datetime.strptime(dt_str, fmt)
            except ValueError:
                continue

        # 如果都不成功，尝试只解析日期部分
        try:
            date_part = dt_str.split()[0]
            dt = datetime.strptime(date_part, '%Y-%m-%d')
            return dt.replace(hour=9, minute=30, second=0, microsecond=0)
        except:
            return None

    def validate_symbol(self, symbol: str) -> Optional[str]:
        """
        验证并清理合约代码
        """
        if pd.isna(symbol) or not isinstance(symbol, str):
            return None

        symbol = str(symbol).strip().upper()

        # 移除可能包含的交易所后缀
        if '.' in symbol:
            parts = symbol.split('.')
            symbol = parts[0]  # 取第一个部分作为合约代码

        # 验证基本格式（至少2个字母+数字）
        if len(symbol) >= 2 and symbol[:2].isalpha():
            return symbol
        return None

    def get_csv_column_name(self, tick_attribute: str) -> Optional[str]:
        """
        根据TickData属性名获取CSV列名

        Args:
            tick_attribute: TickData属性名，如 'last_price', 'volume' 等

        Returns:
            CSV列名，如果未找到映射则返回None
        """
        return self.reverse_mapping.get(tick_attribute)

    def parse_row_to_tick(self, row: pd.Series, index: int) -> Optional[TickData]:
        """
        将一行数据解析为TickData对象，使用TICK_FIELDS映射关系
        """
        try:
            # 1. 获取合约代码和时间（必需字段）
            symbol_csv_col = self.get_csv_column_name('symbol')
            datetime_csv_col = self.get_csv_column_name('datetime')

            if not symbol_csv_col or not datetime_csv_col:
                print(f"行 {index}: 缺少必要的字段映射 (symbol或datetime)")
                return None

            # 2. 解析合约代码
            raw_symbol = row.get(symbol_csv_col)
            symbol = self.validate_symbol(raw_symbol)
            if not symbol:
                print(f"行 {index}: 无效的合约代码 '{raw_symbol}'")
                return None

            # 3. 解析时间
            raw_time = row.get(datetime_csv_col)
            dt = self.parse_datetime(raw_time)
            if not dt:
                print(f"行 {index}: 无效的时间格式 '{raw_time}'")
                return None

            # 4. 创建TickData对象
            tick = TickData(
                gateway_name=self.gateway_name,
                symbol=symbol,
                exchange=self.exchange,
                datetime=dt,
                name="",  # 可选的合约名称
            )

            # 5. 使用映射关系设置所有字段
            for csv_col, tick_attr in self.TICK_FIELDS.items():
                # 跳过已经处理的字段
                if tick_attr in ['symbol', 'datetime']:
                    continue

                # 检查CSV中是否有该列
                if csv_col not in row:
                    continue

                value = row[csv_col]

                # 检查是否为有效值
                if pd.isna(value) or (isinstance(value, (int, float)) and value == 0):
                    continue

                try:
                    # 尝试转换为浮点数（价格和成交量字段）
                    float_value = float(value)
                    setattr(tick, tick_attr, float_value)
                except (ValueError, TypeError):
                    # 如果不是数值，跳过（可能是字符串或其他类型）
                    pass

            # 6. 检查必需的价格字段
            last_price_csv_col = self.get_csv_column_name('last_price')
            if last_price_csv_col and last_price_csv_col in row:
                try:
                    tick.last_price = float(row[last_price_csv_col])
                except:
                    self.stats['missing_last_price'] += 1

            # 如果最新价缺失，尝试从买卖价推算
            if not tick.last_price or tick.last_price == 0:
                if tick.bid_price_1 and tick.bid_price_1 > 0:
                    tick.last_price = tick.bid_price_1
                elif tick.ask_price_1 and tick.ask_price_1 > 0:
                    tick.last_price = tick.ask_price_1
                else:
                    print(f"行 {index}: 缺少价格信息")
                    self.stats['invalid_rows'] += 1
                    return None

            # 7. 确保买卖盘口有有效值
            if not tick.bid_price_1 or tick.bid_price_1 == 0:
                tick.bid_price_1 = tick.last_price
                self.stats['missing_bid_ask'] += 1

            if not tick.ask_price_1 or tick.ask_price_1 == 0:
                tick.ask_price_1 = tick.last_price
                self.stats['missing_bid_ask'] += 1

            if not tick.bid_volume_1 or tick.bid_volume_1 == 0:
                tick.bid_volume_1 = 1

            if not tick.ask_volume_1 or tick.ask_volume_1 == 0:
                tick.ask_volume_1 = 1

            # 8. 更新统计信息
            self.stats['valid_rows'] += 1
            self.stats['unique_symbols'].add(symbol)

            if not self.stats['time_range']['start'] or dt < self.stats['time_range']['start']:
                self.stats['time_range']['start'] = dt
            if not self.stats['time_range']['end'] or dt > self.stats['time_range']['end']:
                self.stats['time_range']['end'] = dt

            return tick

        except Exception as e:
            print(f"行 {index}: 解析Tick数据错误: {e}")
            import traceback
            traceback.print_exc()
            self.stats['invalid_rows'] += 1
            return None

    def detect_csv_fields(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        自动检测CSV字段并尝试匹配映射

        Args:
            df: DataFrame对象

        Returns:
            检测到的字段映射
        """
        detected_mapping = {}
        available_columns = list(df.columns)

        print(f"CSV可用列: {available_columns}")

        # 常见的中文字段名映射（如果CSV使用中文列名）
        chinese_mapping = {
            '时间': 'UpdateTime',
            '合约代码': 'InstrumentID',
            '最新价': 'LastPrice',
            '成交量': 'Volume',
            '成交额': 'Turnover',
            '持仓量': 'OpenInterest',
            '买一价': 'BidPrice1',
            '买一量': 'BidVolume1',
            '卖一价': 'AskPrice1',
            '卖一量': 'AskVolume1',
            '涨停价': 'UpperLimitPrice',
            '跌停价': 'LowerLimitPrice',
            '昨收': 'PreClosePrice',
            '昨结': 'PreSettlementPrice',
            '开盘价': 'OpenPrice',
            '最高价': 'HighPrice',
            '最低价': 'LowPrice',
            '结算价': 'SettlementPrice',
        }

        # 首先尝试中文映射
        for csv_col in available_columns:
            if csv_col in chinese_mapping:
                standard_col = chinese_mapping[csv_col]
                if standard_col in self.TICK_FIELDS:
                    detected_mapping[standard_col] = self.TICK_FIELDS[standard_col]
                    print(f"  检测到映射: '{csv_col}' -> {standard_col} -> {self.TICK_FIELDS[standard_col]}")

        # 然后尝试直接匹配标准列名
        for csv_col in available_columns:
            if csv_col in self.TICK_FIELDS:
                detected_mapping[csv_col] = self.TICK_FIELDS[csv_col]
                print(f"  直接匹配: '{csv_col}' -> {self.TICK_FIELDS[csv_col]}")

        return detected_mapping

    def load_and_validate_csv(self) -> pd.DataFrame:
        """
        加载CSV文件并进行基本验证
        """
        print(f"加载CSV文件: {self.file_path}")

        try:
            # 读取CSV，尝试自动检测编码
            encodings = ['utf-8', 'gbk', 'gb2312', 'utf-8-sig']
            df = None

            for encoding in encodings:
                try:
                    df = pd.read_csv(self.file_path, encoding=encoding)
                    print(f"使用编码: {encoding}")
                    break
                except UnicodeDecodeError:
                    continue

            if df is None:
                raise ValueError("无法识别文件编码，请尝试UTF-8或GBK编码")

            self.stats['total_rows'] = len(df)
            print(f"总行数: {self.stats['total_rows']}")

            # 显示前几行数据
            print("\n前3行数据预览:")
            print(df.head(3).to_string())

            # 显示列信息
            print(f"\nCSV列信息:")
            for i, col in enumerate(df.columns):
                sample_value = df[col].iloc[0] if len(df) > 0 else 'N/A'
                print(f"  {i+1:2d}. {col:20s} (示例: {str(sample_value)[:30]}...)")

            # 自动检测字段映射
            print(f"\n🔍 自动检测字段映射...")
            detected_mapping = self.detect_csv_fields(df)

            # 检查必需字段
            required_mappings = ['symbol', 'datetime', 'last_price']
            missing_required = []

            for req_attr in required_mappings:
                csv_col = self.get_csv_column_name(req_attr)
                if not csv_col or csv_col not in df.columns:
                    missing_required.append(req_attr)

            if missing_required:
                print(f"⚠️  警告: 缺少以下必需字段的映射: {missing_required}")
                print("   请检查CSV文件列名或提供自定义字段映射")

                # 尝试从检测到的映射中查找
                for req_attr in missing_required:
                    for csv_col, tick_attr in self.TICK_FIELDS.items():
                        if tick_attr == req_attr and csv_col in df.columns:
                            print(f"   找到替代: '{csv_col}' 作为 {req_attr}")
                            break

            return df

        except Exception as e:
            print(f"加载CSV文件失败: {e}")
            raise

    def check_duplicate_ticks(self, symbol: str, ticks: List[TickData]) -> List[TickData]:
        """
        检查并移除重复的Tick数据（相同symbol和datetime）
        """
        if not ticks:
            return ticks

        # 按时间排序
        ticks.sort(key=lambda x: x.datetime)

        # 去重
        unique_ticks = []
        seen_datetimes = set()

        for tick in ticks:
            key = (tick.symbol, tick.datetime)
            if key not in seen_datetimes:
                seen_datetimes.add(key)
                unique_ticks.append(tick)

        removed = len(ticks) - len(unique_ticks)
        if removed > 0:
            print(f"  移除 {removed} 条重复Tick数据")

        return unique_ticks

    def import_data(self, batch_size: int = 10000, skip_existing: bool = True) -> Dict:
        """
        导入Tick数据
        """
        print(f"\n开始导入Tick数据...")
        print(f"批处理大小: {batch_size}")
        print(f"跳过已存在数据: {skip_existing}")
        print(f"使用的字段映射: {self.TICK_FIELDS}")

        # 1. 加载CSV
        df = self.load_and_validate_csv()

        # 检查必需字段是否存在
        symbol_csv_col = self.get_csv_column_name('symbol')
        datetime_csv_col = self.get_csv_column_name('datetime')

        if not symbol_csv_col or symbol_csv_col not in df.columns:
            raise ValueError(f"CSV文件必须包含合约代码列，映射为: {self.get_csv_column_name('symbol')}")

        if not datetime_csv_col or datetime_csv_col not in df.columns:
            raise ValueError(f"CSV文件必须包含时间列，映射为: {self.get_csv_column_name('datetime')}")

        # 2. 按合约分组解析Tick数据
        contract_ticks: Dict[str, List[TickData]] = {}

        print(f"\n解析数据并分组...")
        for idx, row in df.iterrows():
            # 显示进度
            if idx % 10000 == 0 and idx > 0:
                print(f"  已解析 {idx} 行...")

            tick = self.parse_row_to_tick(row, idx)
            if tick:
                # 按symbol分组
                if tick.symbol not in contract_ticks:
                    contract_ticks[tick.symbol] = []
                contract_ticks[tick.symbol].append(tick)

        print(f"解析完成，共 {len(contract_ticks)} 个合约")

        # 3. 对每个合约单独处理
        total_saved = 0

        for symbol, ticks in contract_ticks.items():
            print(f"\n处理合约: {symbol}")
            print(f"  原始Tick数: {len(ticks)}")

            # 去重
            ticks = self.check_duplicate_ticks(symbol, ticks)

            if not ticks:
                print(f"  ⚠️  没有有效Tick数据")
                continue

            # 跳过已存在数据（如果需要）
            # 注意：Tick数据去重通常由数据库的唯一约束处理

            # 4. 分批保存
            print(f"  准备保存 {len(ticks)} 条Tick数据...")
            contract_saved = 0

            for i in range(0, len(ticks), batch_size):
                batch = ticks[i:i + batch_size]

                try:
                    # 保存Tick数据
                    self.database.save_tick_data(batch)
                    contract_saved += len(batch)

                    if (i // batch_size) % 10 == 0 and (i // batch_size) > 0:
                        print(f"    批次 {i // batch_size + 1}: 已保存 {min(i + batch_size, len(ticks))}/{len(ticks)}")

                except Exception as e:
                    print(f"    ❌ 批次 {i // batch_size + 1} 保存失败: {e}")
                    # 尝试逐条保存以找出问题Tick
                    error_count = 0
                    for j, tick in enumerate(batch):
                        try:
                            self.database.save_tick_data([tick])
                            contract_saved += 1
                        except Exception as single_error:
                            error_count += 1
                            if error_count <= 5:  # 只显示前5个错误
                                print(f"      行 {i + j} 失败: {single_error}")
                                print(f"      失败Tick: {tick.symbol} {tick.datetime} {tick.last_price}")

                    if error_count > 5:
                        print(f"      还有 {error_count - 5} 个错误未显示...")

            total_saved += contract_saved
            print(f"  ✅ 合约 {symbol} 保存完成: {contract_saved} 条")

        # 5. 更新统计信息
        self.stats['saved_ticks'] = total_saved
        self.stats['unique_symbols'] = set(contract_ticks.keys())

        print(f"\n✅ Tick数据导入完成")
        print(f"   总保存Tick数: {total_saved} 条")
        print(f"   涉及合约数: {len(contract_ticks)} 个")

        self.print_statistics()

        return self.stats

    def print_statistics(self):
        """打印导入统计信息"""
        print("\n" + "=" * 60)
        print("📊 Tick数据导入统计信息")
        print("=" * 60)

        print(f"文件路径: {self.file_path}")
        print(f"总行数: {self.stats['total_rows']}")
        print(f"有效Tick数: {self.stats['valid_rows']}")
        print(f"无效行数: {self.stats['invalid_rows']}")
        print(f"缺少买卖盘口数: {self.stats['missing_bid_ask']}")
        print(f"缺少最新价数: {self.stats['missing_last_price']}")

        if self.stats['unique_symbols']:
            print(f"合约数量: {len(self.stats['unique_symbols'])}")
            print(f"合约列表: {sorted(self.stats['unique_symbols'])}")

        if self.stats['time_range']['start'] and self.stats['time_range']['end']:
            print(f"时间范围: {self.stats['time_range']['start']} 到 {self.stats['time_range']['end']}")

        print(f"保存Tick数: {self.stats['saved_ticks']}")

        # 显示使用的字段映射
        print(f"\n使用的字段映射:")
        for csv_col, tick_attr in self.stats['field_mapping_used'].items():
            print(f"  {csv_col:20s} -> {tick_attr}")

        print("=" * 60)


def main():
    """主函数"""
    import argparse
    import sys
    import json

    parser = argparse.ArgumentParser(description='导入CFFEX多合约Tick数据到vn.py数据库')
    parser.add_argument('--file', type=str, required=True, help='CSV文件路径')
    parser.add_argument('--batch-size', type=int, default=10000, help='批处理大小')
    parser.add_argument('--no-skip', action='store_true', help='不跳过已存在的数据（默认跳过）')
    parser.add_argument('--verify', action='store_true', help='导入后验证数据')
    parser.add_argument('--mapping-file', type=str, help='自定义字段映射JSON文件')

    args = parser.parse_args()

    try:
        # 加载自定义字段映射（如果有）
        custom_mapping = {}
        if args.mapping_file:
            with open(args.mapping_file, 'r', encoding='utf-8') as f:
                custom_mapping = json.load(f)
            print(f"加载自定义字段映射: {custom_mapping}")

        # 创建导入器
        importer = CFFEXTickDataImporterFixed(
            file_path=args.file,
            custom_field_mapping=custom_mapping
        )

        # 导入数据
        stats = importer.import_data(
            batch_size=args.batch_size,
            skip_existing=not args.no_skip
        )

        # 验证数据（可选）
        if args.verify and stats['saved_ticks'] > 0:
            # 查询数据库验证
            from vnpy.trader.database import get_database
            database = get_database()

            # 查询第一个合约的数据作为验证
            if stats['unique_symbols']:
                sample_symbol = list(stats['unique_symbols'])[0]
                try:
                    ticks = database.load_tick_data(
                        symbol=sample_symbol,
                        exchange=importer.exchange,
                        start=stats['time_range']['start'],
                        end=stats['time_range']['end'],
                    )

                    ticks = ticks[:3]

                    if ticks:
                        print(f"\n✅ 验证成功: 查询到 {sample_symbol} 的 {len(ticks)} 条Tick数据")
                        for i, tick in enumerate(ticks):
                            print(f"  {i+1}. {tick.datetime}: 最新价:{tick.last_price:.2f}")
                    else:
                        print(f"⚠️  验证警告: 未查询到 {sample_symbol} 的数据")

                except Exception as e:
                    print(f"⚠️  验证时出错: {e}")

        print(f"\n🎉 Tick数据导入完成!")

    except Exception as e:
        print(f"❌ 导入失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    # 示例用法:
    # python import_cffex_tick_data_fixed.py --file your_tick_data.csv
    # python import_cffex_tick_data_fixed.py --file your_tick_data.csv --batch-size 5000 --verify
    # python import_cffex_tick_data_fixed.py --file your_tick_data.csv --mapping-file custom_mapping.json

    main()