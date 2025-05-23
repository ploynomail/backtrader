# 关键方法说明
# _check(forcedata=None)：这个方法在你的查询中特别提到，它的作用是检查是否有新数据可用，而不移动指针。当forcedata参数提供时，它会尝试查找与主数据时间匹配的数据点。

# next(ticks=True, datamaster=None)：获取并处理下一个数据点，返回True表示有新数据，None表示等待中，False表示没有更多数据。

# do_qcheck(newqcheck, elapsed_seconds)：执行轮询检查，可以根据经过的时间调整检查频率。

# _load_historical_data()：加载历史数据进行回填。

# _run()：在单独的线程中运行数据收集循环，模拟实时数据流。

# 通过这个完整示例，你可以将任何数据源（如股票API、加密货币交易所API等）连接到backtrader，实现实时交易策略测试和部署。

import sys
import os
# 添加backtrader所在的目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import backtrader as bt
import datetime
import time
import threading
import queue
import numpy as np
import pandas as pd
import random
from backtrader.utils.py3 import queue, with_metaclass
from backtrader.feed import DataBase
from backtrader import TimeFrame, date2num, num2date


class SimulatedRealTimeData(with_metaclass(bt.MetaParams, DataBase)):
    """
    模拟实时数据源，包含历史数据回填功能
    
    参数:
        - dataname: 用于历史数据的文件路径或DataFrame
        - historical_days: 回填的历史数据天数
        - timeframe: 时间帧
        - compression: 数据压缩率
        - ohlcv_fields: OHLCV字段名称映射
        - qcheck: 轮询间隔(秒)
        - latethrough: 是否允许晚到的数据通过
        - rtbar: 是否交付未完成的实时柱
        - simulated_generator: 模拟数据生成器类
        - generator_args: 传递给数据生成器的参数
    """
    params = (
        ('historical_days', 30),  # 回填历史数据的天数
        ('timeframe', TimeFrame.Minutes),  # 时间帧
        ('compression', 1),  # 压缩率
        ('ohlcv_fields', {
            'datetime': 0, 'open': 1, 'high': 2,
            'low': 3, 'close': 4, 'volume': 5,
            'openinterest': 6
        }),
        ('qcheck', 0.5),  # 轮询间隔(秒)
        ('latethrough', False),  # 是否允许晚到的数据通过
        ('rtbar', False),  # 是否交付未完成的实时柱
        ('simulated_generator', None),  # 模拟数据生成器类
        ('generator_args', {}),  # 传递给数据生成器的参数
    )

    _store = 1  # 标识为store连接的数据源

    # 自定义状态常量
    HISTORICALDATA = 10  # 历史数据状态
    RTDATA = 11  # 实时数据状态

    def islive(self):
        """标识这是一个实时数据源"""
        return True

    def __init__(self):
        """初始化实时数据源"""
        self.instart = False  # 是否已开始
        self._storedmsg = []  # 存储通知消息
        
        # 通信和控制变量
        self._data_q = queue.Queue()  # 实时数据队列
        self._notification_q = queue.Queue()  # 通知队列
        self._thread = None  # 数据生成线程
        self._running = False  # 运行标志
        self._historical_loaded = False  # 历史数据是否已加载
        self._in_history_phase = True  # 标记是否在历史数据阶段
        
        # 数据状态
        self._laststatus = self.CONNECTED  # 初始状态为已连接
        self._lastdatetime = None  # 最后数据的时间戳
        self._load_history_first = True  # 是否首先加载历史数据
        
        # 初始化父类并设置各种线
        DataBase.__init__(self)
        # 确保缓冲区被正确初始化
        self.fromdate = None
        self.todate = None
    
        # 创建模拟数据生成器
        self._data_generator = self.p.simulated_generator(**self.p.generator_args) if self.p.simulated_generator else DefaultSimulatedDataGenerator(**self.p.generator_args)

    def start(self):
        """启动数据源"""
        if not self._thread and not self._running:
            self.instart = True
            self._running = True
            self._thread = threading.Thread(target=self._run)
            self._thread.daemon = True
            self._thread.start()
            self._put_notification(self.CONNECTED)

    def stop(self):
        """停止数据源"""
        if self._running:
            self._running = False
            if self._thread:
                self._thread.join(timeout=2.0)
                self._thread = None
            self._put_notification(self.DISCONNECTED)

    def haslivedata(self) -> bool:
        """检查是否有实时数据可用"""
        return not self._data_q.empty()

    def _load_historical_data(self):
        """加载历史数据"""
        if self._historical_loaded:
            return
            
        try:
            # 通知进入历史数据阶段
            self._put_notification(self.HISTORICALDATA)
            
            # 从数据生成器获取历史数据
            hist_data = self._data_generator.get_historical_data(self.p.historical_days)
            
            # 记录数据已加载
            self._historical_loaded = True
            
            # 将历史数据加入队列
            for bar in hist_data:
                self._data_q.put(bar)
                
        except Exception as e:
            self._put_notification(self.ERROR, str(e))
            return False
            
        return True

    def _run(self):
        """在单独的线程中运行数据收集循环"""
        try:
            # 先加载历史数据
            if self._load_history_first and not self._historical_loaded:
                self._load_historical_data()
            
            # 标记历史数据阶段结束，开始实时数据
            self._in_history_phase = False
            
            # 通知从历史数据转为实时数据
            self._put_notification(self.RTDATA)
            
            # 设置为实时状态
            self._put_notification(self.LIVE)
            self._laststatus = self.LIVE
            
            # 主循环，生成实时数据
            last_bar_time = None
            while self._running:
                try:
                    # 从数据生成器获取下一个数据点
                    bar = self._data_generator.get_next_bar()
                    
                    # 确保时间是递增的
                    if last_bar_time and bar[0] <= last_bar_time:
                        time.sleep(self.p.qcheck)
                        continue
                    
                    last_bar_time = bar[0]
                    self._data_q.put(bar)
                    
                except Exception as e:
                    self._put_notification(self.ERROR, str(e))
                    
                # 控制数据生成速度
                time.sleep(self.p.qcheck)
                
        except Exception as e:
            self._put_notification(self.ERROR, str(e))
            self._put_notification(self.DISCONNECTED)
        finally:
            self._running = False

    def _check(self, forcedata=None):
        """
        检查是否有新数据可用，但不移动指针
        当forcedata提供时，尝试找到与该数据时间匹配的数据点
        """
        if forcedata is not None and self._lastdatetime is not None:
            # 尝试同步到forcedata的时间
            fddtnum = forcedata.datetime[0]
            if fddtnum > self._lastdatetime:
                # 如果强制数据的时间大于最后时间，则查找匹配的数据
                return self.haslivedata()
        
        # 默认只检查是否有数据
        return self.haslivedata()

    def _put_notification(self, status, *args, **kwargs):
        """添加通知到队列"""
        msg = (status, args, kwargs)
        self._notification_q.put(msg)

    def get_notifications(self):
        """获取队列中的所有通知"""
        while not self._notification_q.empty():
            yield self._notification_q.get()

    def next(self, ticks=True, datamaster=None):
        """
        获取并处理下一个数据点
        
        返回值:
          - True: 有新数据
          - None: 等待中
          - False: 没有更多数据
        """
        if not self.haslivedata():
            return None  # 等待新数据
        
        # 从队列获取数据
        bar = self._data_q.get()
        dt = bar[0]
        
        # 如果有datamaster，尝试与其同步
        if datamaster is not None:
            dtmaster = datamaster.datetime[0]
            if dt > dtmaster:  # 如果我们的数据时间大于主数据
                self._data_q.put(bar)  # 放回队列
                return None  # 等待主数据赶上
        
        # 更新最后时间
        self._lastdatetime = dt
        
        # 在赋值前移动缓冲区指针
        self.forward()
        
        # 填充行数据
        self.lines.datetime[0] = dt
        self.lines.open[0] = bar[1]
        self.lines.high[0] = bar[2]
        self.lines.low[0] = bar[3]
        self.lines.close[0] = bar[4]
        self.lines.volume[0] = bar[5]
        self.lines.openinterest[0] = bar[6] if len(bar) > 6 else 0
        
        return True  # 有新数据

    def rewind(self):
        """回退数据，用于时间不同步时"""
        # 由于我们是实时数据源，通常不回退数据
        # 但此方法需要存在以满足backtrader的API要求
        pass


class DefaultSimulatedDataGenerator:
    """
    默认模拟数据生成器，生成随机的OHLCV数据
    
    参数:
        - initial_price: 初始价格
        - volatility: 波动率 (每天的标准差百分比)
        - volume: 每bar的成交量基准
        - bar_interval_minutes: 每bar的时间间隔(分钟)
    """
    def __init__(self, initial_price=100, volatility=0.01, volume=10000, bar_interval_minutes=1):
        self.price: int = initial_price
        self.volatility: float = volatility
        self.volume: int = volume
        self.bar_interval_minutes: int = bar_interval_minutes
        self.last_dt: bt.datetime = datetime.datetime.now() - datetime.timedelta(minutes=bar_interval_minutes) # 初始化为当前时间前一个bar的时间
        
    def get_historical_data(self, days):
        """生成历史数据"""
        data = []
        end_dt = datetime.datetime.now().replace(second=0, microsecond=0) #datetime.datetime(2025, 5, 20, 18, 7)
        end_dt -= datetime.timedelta(minutes=end_dt.minute % self.bar_interval_minutes) # 向下取整到最近的bar时间
        
        # 计算开始日期
        start_dt = end_dt - datetime.timedelta(days=days)
        current_dt = start_dt
        
        # 创建交易时间的列表(假设每天9:30-16:00为交易时间)
        trading_times = []
        while current_dt <= end_dt:
            # 只考虑工作日
            if current_dt.weekday() < 5:  # 0-4是周一到周五
                # 9:30 - 16:00为交易时间
                day_start = current_dt.replace(hour=9, minute=30)
                day_end = current_dt.replace(hour=16, minute=0)
                
                bar_dt = day_start
                while bar_dt <= day_end:
                    trading_times.append(bar_dt)
                    bar_dt += datetime.timedelta(minutes=self.bar_interval_minutes)
            
            # 移动到下一天
            current_dt = current_dt.replace(hour=0, minute=0) + datetime.timedelta(days=1)
        
        # 生成价格序列
        prices = self._generate_price_sequence(len(trading_times))
        
        # 创建OHLCV数据
        for i, dt in enumerate(trading_times):
            price = prices[i]
            open_price = price
            high_price = price * (1 + random.uniform(0, self.volatility))
            low_price = price * (1 - random.uniform(0, self.volatility))
            close_price = price * (1 + random.uniform(-self.volatility, self.volatility))
            volume = int(self.volume * (1 + random.uniform(-0.3, 0.5)))
            
            # 转换为backtrader的日期格式
            dt_num = date2num(dt)
            
            # 添加bar(datetime, open, high, low, close, volume, openinterest)
            data.append((dt_num, open_price, high_price, low_price, close_price, volume, 0))
            
            # 更新最后时间
            self.last_dt = dt
            self.price = close_price
            
        return data
        
    def get_next_bar(self):
        """生成下一个数据点"""
        # 计算下一个bar的时间
        next_dt = self.last_dt + datetime.timedelta(minutes=self.bar_interval_minutes)
        
        # 如果不是交易时间，跳到下一个交易日的开盘
        while (next_dt.weekday() >= 5 or  # 周末
               next_dt.hour < 9 or  # 开盘前
               (next_dt.hour == 9 and next_dt.minute < 30) or  # 9:30前
               next_dt.hour > 16 or  # 收盘后
               (next_dt.hour == 16 and next_dt.minute > 0)):  # 16:00后
            
            if next_dt.weekday() >= 5:  # 周末
                # 跳到下周一
                days_to_add = 7 - next_dt.weekday()
                next_dt = next_dt.replace(hour=9, minute=30) + datetime.timedelta(days=days_to_add)
            elif next_dt.hour < 9 or (next_dt.hour == 9 and next_dt.minute < 30):
                # 早上开盘前
                next_dt = next_dt.replace(hour=9, minute=30)
            else:
                # 收盘后，跳到下一个交易日
                next_dt = (next_dt + datetime.timedelta(days=1)).replace(hour=9, minute=30)
        
        # 生成价格
        price_change = random.normalvariate(0, self.volatility)
        self.price *= (1 + price_change)
        
        open_price = self.price
        high_price = self.price * (1 + random.uniform(0, self.volatility))
        low_price = self.price * (1 - random.uniform(0, self.volatility))
        close_price = self.price * (1 + random.uniform(-self.volatility, self.volatility))
        volume = int(self.volume * (1 + random.uniform(-0.3, 0.5)))
        
        # 更新最后时间
        self.last_dt = next_dt
        self.price = close_price
        
        # 转换为backtrader的日期格式
        dt_num = date2num(next_dt)
        
        # 返回bar(datetime, open, high, low, close, volume, openinterest)
        return (dt_num, open_price, high_price, low_price, close_price, volume, 0)
        
    def _generate_price_sequence(self, length):
        """生成随机价格序列，模拟价格走势"""
        # 使用几何布朗运动模拟价格
        daily_returns = np.random.normal(0, self.volatility, length)
        price_path = self.price * np.cumprod(1 + daily_returns)
        return price_path


# 创建自定义策略
class MyStrategy(bt.Strategy):
    params = (
        ('sma_period', 20),
    )
    
    def __init__(self):
        self.sma = bt.indicators.SMA(self.data.close, period=self.params.sma_period)
        self.dataclose = self.data.close
        self.order = None
        self.live_trading = False  # 标记是否处于实时交易阶段
        
    def next(self):
        # 只有在实时交易阶段才执行交易操作
        if not self.live_trading:
            return
            
        if not self.position:
            if self.dataclose[0] > self.sma[0]:
                self.order = self.buy()
                print(f"买入信号: 价格={self.dataclose[0]:.2f}, 时间={bt.num2date(self.data.datetime[0])}")
        else:
            if self.dataclose[0] < self.sma[0]:
                self.order = self.sell()
                print(f"卖出信号: 价格={self.dataclose[0]:.2f}, 时间={bt.num2date(self.data.datetime[0])}")
    
    def notify_data(self, data, status, *args, **kwargs):
        """数据状态通知"""
        print(f'数据状态: {data._name} - {status}')
        
        # SimulatedRealTimeData的自定义状态处理
        if hasattr(data, 'RTDATA') and status == data.RTDATA:
            self.live_trading = True
            print("数据源已从历史数据进入实时交易状态，开始执行交易信号!")
        elif hasattr(data, 'HISTORICALDATA') and status == data.HISTORICALDATA:
            self.live_trading = False
            print("数据源正在加载历史数据，此阶段不执行交易!")
        elif status == data.LIVE:
            print("数据源已进入实时状态!")
            
    def notify_order(self, order):
        """订单状态通知"""
        if order.status in [order.Submitted, order.Accepted]:
            return
            
        if order.status in [order.Completed]:
            if order.isbuy():
                print(f"买入执行: 价格={order.executed.price:.2f}, 成本={order.executed.value:.2f}, 佣金={order.executed.comm:.2f}")
            else:
                print(f"卖出执行: 价格={order.executed.price:.2f}, 成本={order.executed.value:.2f}, 佣金={order.executed.comm:.2f}")
        
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            print(f"订单未执行: 状态={order.getstatusname()}")
            
        self.order = None


if __name__ == '__main__':
    # 创建cerebro实例
    cerebro = bt.Cerebro()
    
    # 添加模拟实时数据源
    data = SimulatedRealTimeData(
        historical_days=30,                # 回填30天历史数据
        timeframe=bt.TimeFrame.Minutes,    # 分钟级数据
        compression=5,                     # 5分钟压缩
        qcheck=3,                        # 每3秒检查新数据
        rtbar=True,                        # 交付未完成的实时柱
        simulated_generator=DefaultSimulatedDataGenerator,
        generator_args={
            'initial_price': 100.0,        # 初始价格
            'volatility': 0.01,            # 波动率
            'volume': 10000,               # 成交量基准
            'bar_interval_minutes': 5      # 每柱5分钟
        }
    )
    
    # 添加数据到cerebro
    cerebro.adddata(data, name="5分钟实时数据")
    
    # 添加策略
    cerebro.addstrategy(MyStrategy)
    
    # 设置初始资金
    cerebro.broker.setcash(100000.0)
    
    # 设置佣金
    cerebro.broker.setcommission(commission=0.001)
    
    # 设置交易下单量
    cerebro.addsizer(bt.sizers.FixedSize, stake=1000)
    
    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    
    # 启用实时模式和快速通知
    cerebro.run(live=True, quicknotify=True)
    
    # 输出结果
    print("策略执行完毕!")
    print(f"最终资金: {cerebro.broker.getvalue():.2f}")
    
    # 绘制图表
    cerebro.plot(style='candle', volume=True)