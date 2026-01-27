import pandas as pd
import numpy as np
import os
from pathlib import Path
import re


def safe_datetime_conversion(date_str, time_str):
    """安全的日期时间转换"""
    try:
        # 格式化日期为YYYY-MM-DD
        date_str = str(date_str).strip()
        if len(date_str) == 8 and date_str.isdigit():  # 20231101
            date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

        # 格式化时间为HH:MM:SS
        time_str = str(time_str).strip()

        # 处理各种时间格式
        if ':' in time_str:
            # 已经是标准格式
            parts = time_str.split(':')
            if len(parts) == 2:
                time_str = f"{parts[0].zfill(2)}:{parts[1].zfill(2)}:00"
        elif len(time_str) in [3, 4] and time_str.isdigit():
            # 紧凑格式：930或0930
            time_str = time_str.zfill(4)  # 确保4位
            time_str = f"{time_str[:2]}:{time_str[2:]}:00"

        return pd.to_datetime(f"{date_str} {time_str}")
    except Exception as e:
        print(f"转换错误: date={date_str}, time={time_str}, error={e}")
        return pd.NaT


def process_file(input_path, output_dir, date_str):
    """处理单个文件"""
    try:
        # 读取文件
        df = pd.read_csv(input_path)

        # 确保必要的列存在
        required_cols = ['TradingDay', 'UpdateTime']
        if not all(col in df.columns for col in required_cols):
            print(f"文件 {input_path} 缺少必要的列，跳过处理")
            return False

        # 应用转换
        df['UpdateTime'] = df.apply(
            lambda row: safe_datetime_conversion(row['TradingDay'], row['UpdateTime']),
            axis=1
        )

        # 保存文件
        df.to_csv(output_dir, index=False)
        return True

    except Exception as e:
        print(f"处理文件 {input_path} 时出错: {e}")
        return False


def batch_process_files(input_base_path, output_base_path):
    """
    批量处理文件夹下的所有CSV文件

    参数:
    input_base_path: 输入文件夹的基础路径 (例如: "./data/ticks")
    output_base_path: 输出文件夹的基础路径 (例如: "./data/processed")
    """
    # 将路径转换为Path对象
    input_path = Path(input_base_path)
    output_path = Path(output_base_path)

    # 确保输出目录存在
    output_path.mkdir(parents=True, exist_ok=True)

    # 统计信息
    processed_count = 0
    error_count = 0

    # 遍历输入目录
    for year_month_dir in input_path.iterdir():
        if not year_month_dir.is_dir():
            continue

        # 检查目录名是否是年月格式 (如: 202311)
        year_month = year_month_dir.name
        if not re.match(r'^\d{6}$', year_month):
            print(f"跳过非年月格式的目录: {year_month_dir}")
            continue

        print(f"处理月份: {year_month}")

        # 遍历日期目录
        for date_dir in year_month_dir.iterdir():
            if not date_dir.is_dir():
                continue

            # 检查目录名是否是日期格式 (如: 20231101)
            date_str = date_dir.name
            if not re.match(r'^\d{8}$', date_str):
                print(f"跳过非日期格式的目录: {date_dir}")
                continue

            print(f"  处理日期: {date_str}")

            # 遍历该日期目录下的所有CSV文件
            for csv_file in date_dir.glob("*.csv"):
                if not csv_file.is_file():
                    continue

                # 提取产品名称 (去掉.csv扩展名)
                product_name = csv_file.stem

                # 构建输出路径: SAVEPATH/产品名称/日期.csv
                product_output_dir = output_path / product_name
                product_output_dir.mkdir(parents=True, exist_ok=True)

                output_file_path = product_output_dir / f"{date_str}.csv"

                print(f"    处理: {product_name}.csv -> {product_name}/{date_str}.csv")

                # 处理文件
                if process_file(csv_file, output_file_path, date_str):
                    processed_count += 1
                else:
                    error_count += 1

    # 输出统计信息
    print("\n" + "=" * 50)
    print(f"处理完成!")
    print(f"成功处理: {processed_count} 个文件")
    print(f"处理失败: {error_count} 个文件")
    print(f"输出目录: {output_path.absolute()}")

    return processed_count, error_count


# 使用示例
if __name__ == "__main__":
    # 设置输入和输出路径
    INPUT_PATH = "./data/ticks"  # 根据实际情况修改
    OUTPUT_PATH = "./data/processed_ticks"  # 根据实际情况修改

    # 执行批量处理
    processed, errors = batch_process_files(INPUT_PATH, OUTPUT_PATH)