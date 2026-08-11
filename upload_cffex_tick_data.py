"""
import_cffex_tick_data_fixed.py
vn.py 4.2版本

- 将包含多个CFFEX合约的Tick数据CSV导入数据库， 这里的Tick数据原始格式是网上购买的版本，经过自行日期处理
- 注意这里实质上有三套字段映射
  - 输入文件的原始字段
  - 中间正式字段
  - vnpy内置字段

修复版：支持单文件或文件夹批量导入
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
    """韩喆给的数据"""


    TICK_FIELDS = {
        # 必需字段
        'datetime': 'datetime',  # 映射到TickData.datetime (需要转换为datetime对象)
        'instrumentID': 'symbol',  # 映射到TickData.symbol

        # 价格和成交量字段
        'lastPrice': 'last_price',
        'volume': 'volume',
        'turnover': 'turnover',
        'openInterest': 'open_interest',

        # 买卖盘口字段
        'bidPrice1': 'bid_price_1',
        'bidVolume1': 'bid_volume_1',
        'askPrice1': 'ask_price_1',
        'askVolume1': 'ask_volume_1',

        # 其他价格档位
        'bidPrice2': 'bid_price_2',
        'bidVolume2': 'bid_volume_2',
        'askPrice2': 'ask_price_2',
        'askVolume2': 'ask_volume_2',

        'bidPrice3': 'bid_price_3',
        'bidVolume3': 'bid_volume_3',
        'askPrice3': 'ask_price_3',
        'askVolume3': 'ask_volume_3',

        'bidPrice4': 'bid_price_4',
        'bidVolume4': 'bid_volume_4',
        'askPrice4': 'ask_price_4',
        'askVolume4': 'ask_volume_4',

        'bidPrice5': 'bid_price_5',
        'bidVolume5': 'bid_volume_5',
        'askPrice5': 'ask_price_5',
        'askVolume5': 'ask_volume_5',

        # 其他价格字段
        'upperLimitPrice': 'limit_up',
        'lowerLimitPrice': 'limit_down',
        'preClosePrice': 'pre_close',
        # ToDo 暂时先注释掉，DB tick data里没有这个字段
        # 'settlement_price': 'pre_settlement',
        'openPrice': 'open_price',
        'highestPrice': 'high_price',
        'lowestPrice': 'low_price',
        'settlementPrice': 'settlement_price',
    }


    # CSV列名到TickData属性名的映射
    # TICK_FIELDS = {
    #     # 必需字段
    #     'UpdateTime': 'datetime',           # 映射到TickData.datetime (需要转换为datetime对象)
    #     'InstrumentID': 'symbol',           # 映射到TickData.symbol
    #
    #     # 价格和成交量字段
    #     'LastPrice': 'last_price',
    #     'Volume': 'volume',
    #     'Turnover': 'turnover',
    #     'OpenInterest': 'open_interest',
    #
    #     # 买卖盘口字段
    #     'BidPrice1': 'bid_price_1',
    #     'BidVolume1': 'bid_volume_1',
    #     'AskPrice1': 'ask_price_1',
    #     'AskVolume1': 'ask_volume_1',
    #
    #     # 其他价格档位
    #     'BidPrice2': 'bid_price_2',
    #     'BidVolume2': 'bid_volume_2',
    #     'AskPrice2': 'ask_price_2',
    #     'AskVolume2': 'ask_volume_2',
    #
    #     'BidPrice3': 'bid_price_3',
    #     'BidVolume3': 'bid_volume_3',
    #     'AskPrice3': 'ask_price_3',
    #     'AskVolume3': 'ask_volume_3',
    #
    #     'BidPrice4': 'bid_price_4',
    #     'BidVolume4': 'bid_volume_4',
    #     'AskPrice4': 'ask_price_4',
    #     'AskVolume4': 'ask_volume_4',
    #
    #     'BidPrice5': 'bid_price_5',
    #     'BidVolume5': 'bid_volume_5',
    #     'AskPrice5': 'ask_price_5',
    #     'AskVolume5': 'ask_volume_5',
    #
    #     # 其他价格字段
    #     'UpperLimitPrice': 'limit_up',
    #     'LowerLimitPrice': 'limit_down',
    #     'PreClosePrice': 'pre_close',
    #     # ToDo 暂时先注释掉，DB tick data里没有这个字段
    #     # 'PreSettlementPrice': 'pre_settlement',
    #     'OpenPrice': 'open_price',
    #     'HighPrice': 'high_price',
    #     'LowPrice': 'low_price',
    #     'SettlementPrice': 'settlement_price',
    # }

    def __init__(self):
        """初始化导入器"""
        self.exchange = Exchange.CFFEX
        self.gateway_name = "TICK_CSV_IMPORT"
        self.database: BaseDatabase = get_database()

    def parse_datetime(self, dt_str: str) -> Optional[datetime]:
        """解析时间字符串为datetime对象"""
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
        """验证并清理合约代码"""
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

    def parse_row_to_tick(self, row: pd.Series, index: int, field_mapping: Dict) -> Optional[TickData]:
        """
        将一行数据解析为TickData对象
        注意：如果时间列为空，会返回None，该条tick数据不会被上传
        """
        try:
            # 1. 解析合约代码和时间（必需字段）
            # TODO hardcode
            symbol = self.validate_symbol(row.get('instrumentID'))
            if not symbol:
                return None

            # 2. 解析时间 - 如果为空或无效，返回None，该条tick不上传
            # TODO hardcode
            raw_time = row.get('datetime')
            dt = self.parse_datetime(raw_time)
            if not dt:  # 时间列为空，返回None，不上传该条tick
                return None

            # 3. 创建TickData对象
            tick = TickData(
                gateway_name=self.gateway_name,
                symbol=symbol,
                exchange=self.exchange,
                datetime=dt,
                name="",
            )

            # 4. 使用映射关系设置所有字段
            for csv_col, tick_attr in field_mapping.items():
                if csv_col not in row:
                    continue

                value = row[csv_col]
                if pd.isna(value) or (isinstance(value, (int, float)) and value == 0):
                    continue

                try:
                    float_value = float(value)
                    setattr(tick, tick_attr, float_value)
                except (ValueError, TypeError):
                    pass

            # 5. 检查必需的价格字段
            if not tick.last_price or tick.last_price == 0:
                if tick.bid_price_1 and tick.bid_price_1 > 0:
                    tick.last_price = tick.bid_price_1
                elif tick.ask_price_1 and tick.ask_price_1 > 0:
                    tick.last_price = tick.ask_price_1
                else:
                    return None

            # 6. 确保买卖盘口有有效值
            if not tick.bid_price_1 or tick.bid_price_1 == 0:
                tick.bid_price_1 = tick.last_price

            if not tick.ask_price_1 or tick.ask_price_1 == 0:
                tick.ask_price_1 = tick.last_price

            if not tick.bid_volume_1 or tick.bid_volume_1 == 0:
                tick.bid_volume_1 = 1

            if not tick.ask_volume_1 or tick.ask_volume_1 == 0:
                tick.ask_volume_1 = 1

            return tick

        except Exception:
            return None

    def detect_field_mapping(self, df: pd.DataFrame) -> Dict[str, str]:
        """检测CSV字段并返回映射"""
        detected_mapping = {}

        # 常见的中文字段名映射
        # TODO 这里有bug， 原来的映射表改了之后，这里没对应上
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
            '开盘价': 'OpenPrice',
            '最高价': 'HighPrice',
            '最低价': 'LowPrice',
            '结算价': 'SettlementPrice',
        }

        # 首先尝试中文映射
        for csv_col in df.columns:
            if csv_col in chinese_mapping:
                standard_col = chinese_mapping[csv_col]
                if standard_col in self.TICK_FIELDS:
                    detected_mapping[standard_col] = self.TICK_FIELDS[standard_col]

        # 然后尝试直接匹配自定义标准列名
        for csv_col in df.columns:
            if csv_col in self.TICK_FIELDS:
                detected_mapping[csv_col] = self.TICK_FIELDS[csv_col]

        return detected_mapping or self.TICK_FIELDS.copy()

    def import_file(self, file_path: Path, batch_size: int = 10000) -> Dict:
        """导入单个文件"""
        if not file_path.exists():
            return {'error': f"文件不存在: {file_path}"}

        # 统计信息
        stats = {
            'file': str(file_path),
            'total_rows': 0,
            'valid_rows': 0,
            'invalid_rows': 0,
            'unique_symbols': set(),
            'saved_ticks': 0,
        }

        try:
            # 读取CSV
            encodings = ['utf-8', 'gbk', 'gb2312', 'utf-8-sig']
            df = None

            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    break
                except UnicodeDecodeError:
                    continue

            if df is None:
                stats['error'] = "无法识别文件编码"
                return stats

            stats['total_rows'] = len(df)

            # 检测字段映射
            field_mapping = self.detect_field_mapping(df)

            # 检查必需字段
            # if 'InstrumentID' not in df.columns or 'UpdateTime' not in df.columns:
            #     stats['error'] = "CSV缺少必需字段(InstrumentID或UpdateTime)"
            #     return stats

            # 解析数据
            contract_ticks: Dict[str, List[TickData]] = {}

            for idx, row in df.iterrows():
                tick = self.parse_row_to_tick(row, idx, field_mapping)
                if tick:
                    stats['valid_rows'] += 1
                    stats['unique_symbols'].add(tick.symbol)

                    if tick.symbol not in contract_ticks:
                        contract_ticks[tick.symbol] = []
                    contract_ticks[tick.symbol].append(tick)
                else:
                    stats['invalid_rows'] += 1

            # 保存数据
            total_saved = 0
            for symbol, ticks in contract_ticks.items():
                # 去重
                unique_ticks = []
                seen = set()
                for tick in ticks:
                    key = (tick.symbol, tick.datetime)
                    if key not in seen:
                        seen.add(key)
                        unique_ticks.append(tick)

                # 分批保存
                for i in range(0, len(unique_ticks), batch_size):
                    batch = unique_ticks[i:i + batch_size]
                    try:
                        self.database.save_tick_data(batch)
                        total_saved += len(batch)
                    except Exception:
                        # 尝试逐条保存
                        for tick in batch:
                            try:
                                self.database.save_tick_data([tick])
                                total_saved += 1
                            except Exception:
                                pass

            stats['saved_ticks'] = total_saved
            stats['unique_symbols'] = list(stats['unique_symbols'])

        except Exception as e:
            stats['error'] = str(e)

        return stats


def main():
    """主函数"""
    import argparse
    import sys

    parser = argparse.ArgumentParser(description='导入CFFEX多合约Tick数据到vn.py数据库')
    parser.add_argument('--path', type=str, required=True, help='CSV文件路径或包含CSV文件的文件夹路径')
    parser.add_argument('--batch-size', type=int, default=10000, help='批处理大小')

    args = parser.parse_args()

    try:
        # 创建导入器
        importer = CFFEXTickDataImporterFixed()

        path = Path(args.path)
        all_stats = []

        if path.is_file():
            # 处理单个文件
            print(f"处理文件: {path}")
            stats = importer.import_file(path, batch_size=args.batch_size)
            all_stats.append(stats)

        elif path.is_dir():
            # 处理文件夹下所有CSV文件
            print(f"处理文件夹: {path}")
            csv_files = list(path.glob("*.csv"))
            if not csv_files:
                print(f"文件夹中没有CSV文件: {path}")
                sys.exit(1)

            print(f"找到 {len(csv_files)} 个CSV文件")

            for i, csv_file in enumerate(csv_files, 1):
                print(f"\n[{i}/{len(csv_files)}] 处理文件: {csv_file.name}")
                stats = importer.import_file(csv_file, batch_size=args.batch_size)
                all_stats.append(stats)

        else:
            print(f"路径不存在: {path}")
            sys.exit(1)

        # 打印汇总统计
        print("\n" + "=" * 60)
        print("📊 导入汇总统计")
        print("=" * 60)

        total_files = len(all_stats)
        successful_files = 0
        total_rows = 0
        total_valid = 0
        total_invalid = 0
        total_saved = 0
        all_symbols = set()

        for stats in all_stats:
            if 'error' in stats:
                print(f"❌ {stats['file']}: {stats['error']}")
            else:
                successful_files += 1
                total_rows += stats['total_rows']
                total_valid += stats['valid_rows']
                total_invalid += stats['invalid_rows']
                total_saved += stats['saved_ticks']
                all_symbols.update(stats['unique_symbols'])

                print(f"✅ {Path(stats['file']).name}: "
                      f"行数:{stats['total_rows']}, "
                      f"有效:{stats['valid_rows']}, "
                      f"保存:{stats['saved_ticks']}, "
                      f"合约:{len(stats['unique_symbols'])}")

        print(f"\n总计: {successful_files}/{total_files} 个文件成功")
        print(f"总行数: {total_rows}")
        print(f"有效Tick数: {total_valid}")
        print(f"无效行数: {total_invalid}")
        print(f"保存Tick数: {total_saved}")
        print(f"合约列表: {sorted(all_symbols)}")
        print("=" * 60)

    except Exception as e:
        print(f"❌ 导入失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    # 示例用法:
    # python import_cffex_tick_data_fixed.py --path tick_data.csv (单文件)
    # python import_cffex_tick_data_fixed.py --path ./tick_folder (文件夹)
    main()