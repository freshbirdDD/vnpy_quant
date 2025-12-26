"""
import_cffex_minute_bars_v4.py
vn.py 4.2版本 - 将包含多个CFFEX合约的分钟Bar数据CSV导入数据库

CSV格式要求：
    时间,开盘价,最高价,最低价,收盘价,成交量,成交额,持仓量,合约代码
示例：
    2024-01-02 09:30:00,3439.4,3440.2,3439.0,3440.0,1000,3440000,105704,IF1005
"""
import pandas as pd
import numpy as np
from datetime import datetime, time
from pathlib import Path
from typing import List, Dict, Set, Optional
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData
from vnpy.trader.database import BaseDatabase, get_database


class CFFEXMinuteBarImporter:
    """CFFEX交易所多合约分钟Bar数据导入器 (vn.py 4.2版本)"""

    # ToDo 必填字段映射，如果后续导入数据的字段名有修改，需要在这里同步
    REQUIRED_FIELDS = {
        '时间': 'datetime_str',
        '开盘价': 'open_price',
        '最高价': 'high_price',
        '最低价': 'low_price',
        '收盘价': 'close_price',
        '成交量': 'volume',
        '成交额': 'turnover',
        '持仓量': 'open_interest',
        '合约代码': 'symbol'
    }

    def __init__(self, file_path: str):
        """
        初始化导入器

        Args:
            file_path: CSV文件路径
        """
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(f"文件不存在: {self.file_path}")

        self.exchange = Exchange.CFFEX
        self.interval = Interval.MINUTE
        self.gateway_name = "CSV_IMPORT"
        self.database: BaseDatabase = get_database()

        # 统计信息
        self.stats = {
            'total_rows': 0,
            'valid_rows': 0,
            'invalid_rows': 0,
            'unique_symbols': set(),
            'time_range': {'start': None, 'end': None},
            'saved_bars': 0
        }

    def parse_datetime(self, dt_str: str) -> Optional[datetime]:
        """
        解析时间字符串为datetime对象
        支持多种格式：'2024-01-02 09:30:00' 或 '2024/01/02 09:30:00'等
        """
        if pd.isna(dt_str) or not isinstance(dt_str, str):
            return None

        dt_str = str(dt_str).strip()
        if not dt_str:
            return None

        # 尝试多种时间格式
        date_formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y/%m/%d %H:%M:%S',
            '%Y%m%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y/%m/%d %H:%M',
            '%Y%m%d %H:%M',
        ]

        for fmt in date_formats:
            try:
                dt = datetime.strptime(dt_str, fmt)
                # 确保秒数为0（分钟数据特性）
                if dt.second != 0:
                    dt = dt.replace(second=0)
                return dt
            except ValueError:
                continue

        # 如果都不成功，尝试只解析日期部分
        try:
            # 只取日期部分，时间设为9:30（交易日开始）
            date_part = dt_str.split()[0]
            dt = datetime.strptime(date_part, '%Y-%m-%d')
            return dt.replace(hour=9, minute=30, second=0)
        except:
            return None

    def validate_symbol(self, symbol: str) -> Optional[str]:
        """
        验证并清理合约代码
        支持：IF1005, IF888, IF.CFFEX 等格式
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

    def parse_row_to_bar(self, row: pd.Series, index: int) -> Optional[BarData]:
        """
        将一行数据解析为BarData对象 (vn.py 4.2版本)
        """
        try:
            # 1. 解析合约代码
            raw_symbol = row.get('合约代码')
            symbol = self.validate_symbol(raw_symbol)
            if not symbol:
                print(f"行 {index}: 无效的合约代码 '{raw_symbol}'")
                return None

            # 2. 解析时间
            raw_time = row.get('时间')
            dt = self.parse_datetime(raw_time)
            if not dt:
                print(f"行 {index}: 无效的时间格式 '{raw_time}'")
                return None

            # 3. 解析价格和成交量（必需字段）
            # TODO 这里的风险在于，如果这行的该字段缺失，会静默地填成0
            try:
                open_price = float(row.get('开盘价', 0))
                high_price = float(row.get('最高价', 0))
                low_price = float(row.get('最低价', 0))
                close_price = float(row.get('收盘价', 0))
                volume = float(row.get('成交量', 0))
            except (ValueError, TypeError) as e:
                print(f"行 {index}: 数值转换错误: {e}")
                return None

            # 4. 解析可选字段
            turnover = 0.0
            open_interest = 0.0

            if '成交额' in row and pd.notna(row['成交额']):
                try:
                    turnover = float(row['成交额'])
                except:
                    turnover = volume * close_price  # 估算成交额

            if '持仓量' in row and pd.notna(row['持仓量']):
                try:
                    open_interest = float(row['持仓量'])
                except:
                    open_interest = 0.0

            # 5. 创建BarData对象 (vn.py 4.2版本)
            # 注意：vn.py 4.2的BarData构造函数参数
            bar = BarData(
                gateway_name=self.gateway_name,
                symbol=symbol,
                exchange=self.exchange,
                datetime=dt,
                interval=self.interval,
                volume=volume,
                turnover=turnover,
                open_interest=open_interest,
                open_price=open_price,
                high_price=high_price,
                low_price=low_price,
                close_price=close_price,
            )

            # 更新统计信息
            self.stats['valid_rows'] += 1
            self.stats['unique_symbols'].add(symbol)

            if not self.stats['time_range']['start'] or dt < self.stats['time_range']['start']:
                self.stats['time_range']['start'] = dt
            if not self.stats['time_range']['end'] or dt > self.stats['time_range']['end']:
                self.stats['time_range']['end'] = dt

            return bar

        except Exception as e:
            print(f"行 {index}: 解析错误: {e}")
            self.stats['invalid_rows'] += 1
            return None

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

            # 检查必需字段
            missing_fields = []
            for field in self.REQUIRED_FIELDS.keys():
                if field not in df.columns:
                    missing_fields.append(field)

            if missing_fields:
                print(f"警告: 缺少以下字段: {missing_fields}")
                print(f"可用字段: {list(df.columns)}")

            # 显示前几行数据
            print("\n前3行数据预览:")
            print(df.head(3).to_string())

            # 基本数据统计
            print(f"\n合约代码分布:")
            if '合约代码' in df.columns:
                symbol_counts = df['合约代码'].value_counts().head(10)
                for symbol, count in symbol_counts.items():
                    print(f"  {symbol}: {count} 行")

            return df

        except Exception as e:
            print(f"加载CSV文件失败: {e}")
            raise

    def import_data(self, batch_size: int = 10000, skip_existing: bool = True) -> Dict:
        """
        修复版：按合约分组后再分批导入数据
        """
        print(f"\n开始导入数据...")
        print(f"批处理大小: {batch_size}")
        print(f"跳过已存在数据: {skip_existing}")

        # 1. 加载CSV
        df = self.load_and_validate_csv()

        if '合约代码' not in df.columns:
            raise ValueError("CSV文件必须包含'合约代码'列")

        # 2. 按合约分组解析Bar数据
        contract_bars: Dict[str, List[BarData]] = {}

        print(f"\n解析数据并分组...")
        for idx, row in df.iterrows():
            # 显示进度
            if idx % 10000 == 0 and idx > 0:
                print(f"  已解析 {idx} 行...")

            bar = self.parse_row_to_bar(row, idx)
            if bar:
                # 按symbol分组
                if bar.symbol not in contract_bars:
                    contract_bars[bar.symbol] = []
                contract_bars[bar.symbol].append(bar)

        print(f"解析完成，共 {len(contract_bars)} 个合约")

        # 3. 对每个合约单独处理
        total_saved = 0

        for symbol, bars in contract_bars.items():
            print(f"\n处理合约: {symbol}")
            print(f"  原始Bar数: {len(bars)}")

            # 按时间排序
            bars.sort(key=lambda x: x.datetime)

            # 去重（相同datetime的Bar）
            unique_bars = []
            seen_times = set()

            for bar in bars:
                if bar.datetime not in seen_times:
                    seen_times.add(bar.datetime)
                    unique_bars.append(bar)

            if len(unique_bars) < len(bars):
                print(f"  去重后: {len(unique_bars)} 条（移除 {len(bars) - len(unique_bars)} 条重复）")

            bars = unique_bars

            # 跳过已存在数据（如果需要）
            if skip_existing and bars:
                # 查询该合约的现有数据时间范围
                existing_bars = self.database.load_bar_data(
                    symbol=symbol,
                    exchange=self.exchange,
                    interval=self.interval,
                    start=bars[0].datetime,
                    end=bars[-1].datetime
                )

                if existing_bars:
                    existing_times = {b.datetime for b in existing_bars}
                    new_bars = [b for b in bars if b.datetime not in existing_times]
                    print(f"  已存在: {len(existing_bars)} 条，新增: {len(new_bars)} 条")
                    bars = new_bars

            if not bars:
                print(f"  ⚠️  没有需要导入的新数据")
                continue

            # 4. 按合约分批保存
            print(f"  准备保存 {len(bars)} 条Bar数据...")
            contract_saved = 0

            for i in range(0, len(bars), batch_size):
                batch = bars[i:i + batch_size]

                try:
                    # ✅ 关键修复：每个批次只包含同一个合约的数据
                    self.database.save_bar_data(batch)
                    contract_saved += len(batch)

                    if (i // batch_size) % 10 == 0:  # 每10批显示一次进度
                        print(f"    批次 {i // batch_size + 1}: 已保存 {min(i + batch_size, len(bars))}/{len(bars)}")

                except Exception as e:
                    print(f"    ❌ 批次 {i // batch_size + 1} 保存失败: {e}")
                    # 尝试逐条保存以找出问题Bar
                    for j, bar in enumerate(batch):
                        try:
                            self.database.save_bar_data([bar])
                            contract_saved += 1
                        except Exception as single_error:
                            print(f"      行 {i + j} 失败: {single_error}")
                            print(f"      失败Bar: {bar.symbol} {bar.datetime} {bar.close_price}")

            total_saved += contract_saved
            print(f"  ✅ 合约 {symbol} 保存完成: {contract_saved} 条")

            # 验证保存的数据
            # self._verify_saved_data(symbol, contract_saved)

        # 5. 更新统计信息
        self.stats['saved_bars'] = total_saved
        self.stats['unique_symbols'] = set(contract_bars.keys())

        print(f"\n✅ 数据导入完成")
        print(f"   总保存Bar数: {total_saved} 条")
        print(f"   涉及合约数: {len(contract_bars)} 个")

        self.print_statistics()

        return self.stats

    def _verify_saved_data(self, symbol: str, expected_count: int):
        """验证保存的数据"""
        try:
            # 查询刚刚保存的数据
            saved_bars = self.database.load_bar_data(
                symbol=symbol,
                exchange=self.exchange,
                interval=self.interval,
                limit=min(5, expected_count)
            )

            if saved_bars:
                print(f"    验证: 数据库中有 {len(saved_bars)} 条 {symbol} 数据")
                if expected_count > 0 and len(saved_bars) < min(5, expected_count):
                    print(f"    ⚠️  预期至少 {min(5, expected_count)} 条，实际 {len(saved_bars)} 条")
            else:
                print(f"    ⚠️  验证失败: 未找到 {symbol} 的数据")

        except Exception as e:
            print(f"    验证错误: {e}")


        # 4. 打印统计信息
        self.print_statistics()

        return self.stats

    def print_statistics(self):
        """打印导入统计信息"""
        print("\n" + "=" * 60)
        print("📊 导入统计信息")
        print("=" * 60)

        print(f"文件路径: {self.file_path}")
        print(f"总行数: {self.stats['total_rows']}")
        print(f"有效行数: {self.stats['valid_rows']}")
        print(f"无效行数: {self.stats['invalid_rows']}")

        if self.stats['unique_symbols']:
            print(f"合约数量: {len(self.stats['unique_symbols'])}")
            print(f"合约列表: {sorted(self.stats['unique_symbols'])}")

        if self.stats['time_range']['start'] and self.stats['time_range']['end']:
            print(f"时间范围: {self.stats['time_range']['start']} 到 {self.stats['time_range']['end']}")

        print(f"保存Bar数: {self.stats['saved_bars']}")
        print("=" * 60)

    def verify_import(self, sample_symbol: str = None) -> List[BarData]:
        """
        验证导入的数据

        Args:
            sample_symbol: 示例合约代码，用于验证

        Returns:
            查询到的Bar数据
        """
        print(f"\n🔍 验证导入的数据...")

        if not self.stats['unique_symbols']:
            print("没有可验证的合约")
            return []

        # 使用第一个合约或指定合约进行验证
        if sample_symbol and sample_symbol in self.stats['unique_symbols']:
            verify_symbol = sample_symbol
        else:
            verify_symbol = next(iter(self.stats['unique_symbols']))

        # 查询数据库
        bars = self.database.load_bar_data(
            symbol=verify_symbol,
            exchange=self.exchange,
            interval=self.interval,
            start=self.stats['time_range']['start'],
            end=self.stats['time_range']['end'],
            limit=5
        )

        if bars:
            print(f"合约 {verify_symbol} 的数据验证成功:")
            for i, bar in enumerate(bars):
                print(f"  {i + 1}. {bar.datetime}: "
                      f"O:{bar.open_price:.2f} H:{bar.high_price:.2f} "
                      f"L:{bar.low_price:.2f} C:{bar.close_price:.2f} "
                      f"V:{bar.volume}")
        else:
            print(f"⚠️  未找到合约 {verify_symbol} 的数据")

        return bars


def main():
    """主函数"""
    import argparse
    import sys

    parser = argparse.ArgumentParser(description='导入CFFEX多合约分钟Bar数据到vn.py数据库')
    parser.add_argument('--file', type=str, required=True, help='CSV文件路径')
    parser.add_argument('--batch-size', type=int, default=10000, help='批处理大小')
    parser.add_argument('--no-skip', action='store_true', help='不跳过已存在的数据（默认跳过）')
    parser.add_argument('--verify', action='store_true', help='导入后验证数据')

    args = parser.parse_args()

    try:
        # 创建导入器
        importer = CFFEXMinuteBarImporter(args.file)

        # 导入数据
        stats = importer.import_data(
            batch_size=args.batch_size,
            skip_existing=not args.no_skip
        )

        # 验证数据（可选）
        if args.verify and stats['saved_bars'] > 0:
            importer.verify_import()

        print(f"\n🎉 导入完成!")

    except Exception as e:
        print(f"❌ 导入失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    # 示例用法:
    # python import_cffex_minute_bars_v4.py --file your_data.csv
    # python import_cffex_minute_bars_v4.py --file your_data.csv --batch-size 5000 --verify
    # python import_cffex_minute_bars_v4.py --file your_data.csv --no-skip

    main()