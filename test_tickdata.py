from datetime import datetime
from vnpy.trader.constant import Exchange

def check_tickdata_fields():
    """检查TickData类的实际字段"""
    from vnpy.trader.object import TickData
    import inspect

    print("TickData类的字段:")

    # 方法1: 查看__init__签名
    sig = inspect.signature(TickData.__init__)
    print("构造函数参数:")
    for param_name, param in sig.parameters.items():
        if param_name != 'self':
            print(f"  {param_name}: {param}")

    # 方法2: 创建实例查看属性
    print("\n创建实例测试...")
    try:
        tick = TickData(
            gateway_name="TEST",
            symbol="TEST",
            exchange=Exchange.CFFEX,
            datetime=datetime.now(),
            name=""
        )
        print("实例创建成功")
        print("实例属性:", [attr for attr in dir(tick) if not attr.startswith('_')])
    except Exception as e:
        print(f"创建失败: {e}")

check_tickdata_fields()