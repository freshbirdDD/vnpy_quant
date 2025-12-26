from datetime import datetime

from vnpy.trader.constant import Interval,Exchange
from vnpy.trader.database import BaseDatabase, get_database

db = get_database()

bar1 = db.load_bar_data(symbol='IF1005', exchange=Exchange.CFFEX, interval=Interval.MINUTE, start=datetime(2010, 4, 16), end=datetime(2010, 6, 9))
print(len(bar1))

print(len(bar1))


# import os
# from vnpy.trader.setting import SETTINGS
#
# print("=" * 60)
# print("当前数据库配置信息")
# print("=" * 60)
#
# # 1. 查看SETTINGS中的数据库配置
# database_config = SETTINGS.get("database", {})
# print(f"数据库配置: {database_config}")
#
# # 2. 检查可能的数据库文件位置
# possible_paths = [
#     # 默认路径（用户目录）
#     os.path.join(os.path.expanduser("~"), ".vntrader", "database.db"),
#
#     # 项目目录路径
#     os.path.join(os.getcwd(), ".vntrader", "database.db"),
#
#     # 当前目录
#     "database.db",
#
#     # 如果配置了特定路径
#     database_config.get("database", "") if database_config else ""
# ]
#
# print("\n可能的数据库文件位置:")
# for i, path in enumerate(possible_paths, 1):
#     exists = os.path.exists(path) if path else False
#     print(f"{i}. {path}")
#     print(f"   存在: {exists}")
#     if exists:
#         size = os.path.getsize(path) / (1024 * 1024)  # MB
#         print(f"   大小: {size:.2f} MB")
#         print(f"   修改时间: {os.path.getmtime(path)}")
#
# # 3. 检查环境变量
# print("\n环境变量检查:")
# env_vars = ["VNPY_HOME", "VNPY_ROOT", "VNPY_DATA"]
# for var in env_vars:
#     value = os.environ.get(var)
#     print(f"{var}: {value}")

# database = get_database()
# # 删除数据库中k线数据
# database.delete_bar_data(
#     symbol='IF1005',
#     exchange=Exchange.CFFEX,
#     interval=Interval.MINUTE,
# )