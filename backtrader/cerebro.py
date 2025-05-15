from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime
import collections
import itertools
import multiprocessing

try:  # For new Python versions
    collectionsAbc = collections.abc  # collections.Iterable -> collections.abc.Iterable
except AttributeError:  # For old Python versions
    collectionsAbc = collections  # Используем collections.Iterable

import backtrader as bt
from .utils.py3 import (map, range, zip, with_metaclass, string_types,
                        integer_types)

from . import linebuffer
from . import indicator
from .brokers import BackBroker
from .metabase import MetaParams
from . import observers
from .writer import WriterFile
from .utils import OrderedDict, tzparse, num2date, date2num
from .strategy import Strategy, SignalStrategy
from .tradingcal import (TradingCalendarBase, TradingCalendar,
                         PandasMarketCalendar)
from .timer import Timer

# Defined here to make it pickable. Ideally it could be defined inside Cerebro


class OptReturn(object):
    def __init__(self, params, **kwargs):
        self.p = self.params = params
        for k, v in kwargs.items():
            setattr(self, k, v)


class Cerebro(with_metaclass(MetaParams, object)):
    '''参数:

      - preload (默认值: ``True``)

        是否预加载传递给cerebro的不同"数据源"，以供策略使用

      - runonce (默认值: ``True``)

        以向量化模式运行"指标"以加速整个系统,策略和观察器将始终基于事件方式运行

      - live (默认值: ``False``)

        如果没有数据通过数据的"islive"方法将自己报告为"实时"，但最终用户仍希望在"实时"模式下运行，可以将此参数设置为true
        这将同时停用"preload"和"runonce"。它对内存节省方案没有影响。

      - maxcpus (默认值: None -> 使用所有可用核心) 优化过程中同时使用多少个CPU核心

      - stdstats (默认值: ``True``)

        如果为True，将添加默认观察器：Broker(Cash和Value)、Trades和BuySell

      - oldbuysell (默认值: ``False``)
        如果"stdstats"为"True"且观察器被自动添加，此开关控制"BuySell",观察器的主要行为
        - ``False``: 使用现代行为，买入/卖出信号分别绘制在低/高价格的下方/上方，以避免图表混乱
        - ``True``: 使用已弃用的行为，买入/卖出信号绘制在订单执行的平均价格处。这当然会出现在OHLC柱或Close线上，使图表识别变得困难。

      - ``oldtrades`` (默认值: ``False``)
        如果"stdstats"为"True"且观察器被自动添加，此开关控制"Trades",观察器的主要行为
        - ``False``: 使用现代行为，所有数据的交易使用不同的标记绘制
        - ``True``: 使用旧的Trades观察器，用相同的标记绘制交易，只区分交易是正面还是负面

      - ``exactbars`` (默认值: ``False``)

        使用默认值时，存储在line中的每个值都保留在内存中

        可能的值:
          - ``True`` 或 ``1``: 所有"线"对象将内存使用减少到自动计算的最小周期。
            如果简单移动平均线周期为30，则底层数据将始终保持30条数据的运行缓冲区，以便计算简单移动平均线
            - 此设置将停用"preload"和"runonce"
            - 使用此设置还会停用**绘图**功能

          - ``-1``: 策略级别的数据源和指标/操作将在内存中保留所有数据。

            例如：``RSI``内部使用指标``UpDay``进行计算。此子指标不会在内存中,保留所有数据
            - 这允许保持"绘图"和"预加载"功能处于活动状态。
            - "runonce"将被停用

          - ``-2``: 作为策略属性保留的数据源和指标将在内存中保留所有点。

            例如：``RSI``内部使用指标``UpDay``进行计算。此子指标不会在内存中,保留所有数据

            如果在``__init__``中定义了类似``a = self.data.close - self.data.high`` 这样的表达式，那么``a``将不会在内存中保留所有数据
            - 这允许保持"绘图"和"预加载"功能处于活动状态。
            - "runonce"将被停用

      - ``objcache`` (默认值: ``False``)

        实验性选项，实现线对象缓存以减少它们的数量。
        例如，来自UltimateOscillator的示例代码::

          bp = self.data.close - TrueLow(self.data)
          tr = TrueRange(self.data)  # -> creates another TrueLow(self.data)

        如果设置为``True``，``TrueRange``中的第二个``TrueLow(self.data)``
        与``bp``计算中的相同，因此会被重用。

        在某些极端情况下，这可能会导致线对象偏离其最小周期并导致问题，
        因此默认禁用。

      - ``writer`` (默认值: ``False``)

        如果设置为``True``，将创建一个默认的WriterFile，它将打印到标准输出。
        它将被添加到策略中（除了用户代码添加的任何其他写入器）

      - ``tradehistory`` (默认值: ``False``)

        如果设置为``True``，它将为所有策略激活每笔交易的事件日志记录。
        这也可以通过策略的``set_tradehistory``方法在每个策略的基础上实现

      - ``optdatas`` (默认值: ``True``)

        如果在优化过程中设置为``True``（且系统可以``preload``和使用``runonce``），
        数据预加载将仅在主进程中执行一次，以节省时间和资源。

        测试表明，执行时间从样本的``83``秒减少到``66``秒，提高了大约``20%``

      - ``optreturn`` (默认值: ``True``)

        如果为``True``，优化结果将不是完整的``Strategy``对象（及其所有*数据*、
        *指标*、*观察器*...），而是具有以下属性的对象（与``Strategy``中相同）：

          - 该策略执行时的``params``（或``p``）
          - 该策略已执行的``analyzers``

        在大多数情况下，评估策略性能只需要*分析器*和使用哪些*参数*。如果需要详细分析（例如）*指标*的生成值，请关闭此选项

        测试表明执行时间提高了``13% - 15%``。与``optdatas``结合使用，
        优化运行的总提速增加到``32%``。

      - ``oldsync`` (默认值: ``False``)

        从1.9.0.99版本开始，多个数据（相同或不同时间帧）的同步已更改，以允许不同长度的数据。
        如果希望使用data0作为系统主数据的旧行为，请将此参数设置为true

      - ``tz`` (默认值: ``None``)
        为策略添加全局时区。参数``tz``可以是：
          - ``None``：在这种情况下，策略显示的日期时间将采用UTC格式，这直接是标准行为
          - ``pytz``实例。它将用于将UTC时间转换为所选时区
          - ``字符串``。将尝试实例化一个``pytz``实例。
          - ``整数``。对于策略，使用与``self.datas``迭代器中相应的``data``相同的时区
            （``0``将使用``data0``的时区）

      - ``cheat_on_open`` (默认值: ``False``)
        将调用策略的``next_open``方法。这发生在``next``之前，在经纪人有机会评估订单之前。
        指标尚未重新计算。这允许发出考虑前一天指标但使用``open``价格进行持仓计算的订单。

        对于cheat_on_open订单执行，还需要调用``cerebro.broker.set_coo(True)``或
        实例化一个带有``BackBroker(coo=True)``的经纪人（其中*coo*代表cheat-on-open），
        或将``broker_coo``参数设置为``True``。除非下面禁用，否则Cerebro将自动执行此操作。

      - ``broker_coo`` (默认值: ``True``)

        这将自动调用经纪人的``set_coo``方法，并传入``True``以激活``cheat_on_open``执行。
        仅当``cheat_on_open``也为``True``时才会执行此操作。

      - ``quicknotify`` (默认值: ``False``)

        经纪人通知在下一个价格交付之前传递。对于回测，这没有影响，但对于实时经纪人，
        通知可能发生在交付柱状图之前很长时间。设置为``True``时，通知将尽快传递
        （参见实时数据中的``qcheck``）设置为``False``以保持兼容性。将来可能更改为``True``

      - 实际使用案例：
        
        基本使用：
        ```python
        cerebro = bt.Cerebro()
        cerebro.adddata(data)            # 添加数据源
        cerebro.addstrategy(MyStrategy)  # 添加策略类
        cerebro.run()                    # 运行回测
        cerebro.plot()                   # 绘制结果
        ```
        
        实时交易设置：
        ```python
        # 创建实时交易环境
        cerebro = bt.Cerebro(live=True, quicknotify=True)
        cerebro.adddata(live_data_feed)
        cerebro.run()
        ```
        
        优化策略参数：
        ```python
        # 优化SMA策略的参数
        cerebro = bt.Cerebro(optreturn=True, maxcpus=4)
        cerebro.optstrategy(SMAStrategy, period=range(10, 30))
        results = cerebro.run()
        # 分析结果
        for r in results:
            print(f"期间: {r.params.period}, 收益: {r.analyzers.returns.get_analysis()['returns']}")
        ```
    '''
    params = (  # 定义Cerebro类的参数，使用MetaParams元类进行处理
        ('preload', True),       # 是否预加载数据到内存中
        ('runonce', True),       # 是否使用向量化模式运行指标计算
        ('maxcpus', None),       # 优化过程中使用的CPU核心数
        ('stdstats', True),      # 是否添加标准观察器
        ('oldbuysell', False),   # 是否使用旧的买卖信号绘图样式
        ('oldtrades', False),    # 是否使用旧的交易绘图样式
        ('lookahead', 0),        # 提前查看的bar数量，应为0以避免未来函数偏差
        ('exactbars', False),    # 内存优化级别
        ('optdatas', True),      # 优化时是否只预加载一次数据
        ('optreturn', True),     # 优化时是否返回轻量级结果对象
        ('objcache', False),     # 是否启用对象缓存
        ('live', False),         # 是否在实时模式下运行
        ('writer', False),       # 是否添加默认的输出写入器
        ('tradehistory', False), # 是否记录详细的交易历史
        ('oldsync', False),      # 是否使用旧的数据同步方式
        ('tz', None),            # 全局时区设置
        ('cheat_on_open', False),# 是否允许在开盘时"作弊"
        ('broker_coo', True),    # 是否自动设置经纪人的cheat_on_open
        ('quicknotify', False),  # 是否快速传递通知，在实时交易中有用
    )

    def __init__(self):
        """
        初始化Cerebro引擎，设置所有必要的内部变量
        Cerebro是回测/交易系统的核心引擎，管理数据、策略、经纪人等组件
        """
        self._dolive = False             # 是否有实时数据源的标志
        self._doreplay = False           # 是否有重放数据源的标志
        self._dooptimize = False         # 是否执行优化模式的标志
        self.stores = list()             # 存储商店(Store)实例的列表
        self.feeds = list()              # 存储数据馈送(Feed)实例的列表
        self.datas = list()              # 存储数据源(Data)实例的列表
        self.datasbyname = collections.OrderedDict()  # 按名称索引的数据源字典
        self.strats = list()             # 存储策略配置的列表
        self.optcbs = list()             # 存储优化回调函数的列表
        self.observers = list()          # 存储观察器配置的列表
        self.analyzers = list()          # 存储分析器配置的列表
        self.indicators = list()         # 存储指标配置的列表
        self.sizers = dict()             # 存储仓位管理器的字典
        self.writers = list()            # 存储输出写入器的列表
        self.storecbs = list()           # 存储商店回调的列表
        self.datacbs = list()            # 存储数据回调的列表
        self.signals = list()            # 存储信号的列表
        self._signal_strat = (None, None, None)  # 信号策略配置(类,参数,关键字参数)
        self._signal_concurrent = False  # 是否允许并发信号
        self._signal_accumulate = False  # 是否允许信号累积

        self._dataid = itertools.count(1)  # 数据ID生成器，从1开始

        self._broker = BackBroker()      # 创建默认的回测经纪人
        self._broker.cerebro = self      # 将经纪人与cerebro关联

        self._tradingcal = None          # 交易日历，默认为None

        self._pretimers = list()         # 预定时器列表
        self._ohistory = list()          # 订单历史列表
        self._fhistory = None            # 资金历史，用于性能评估

    @staticmethod
    def iterize(iterable):
        '''
        将输入转换为可迭代的格式，方便后续处理
        
        参数:
            iterable: 输入的对象或可迭代对象
            
        返回:
            list: 转换后的可迭代列表，每个元素都是可迭代的
        '''
        niterable = list()                          # 创建新的空列表存储结果
        for elem in iterable:                       # 遍历输入的每个元素
            if isinstance(elem, string_types):      # 如果元素是字符串类型
                elem = (elem,)                      # 将字符串转换为单元素元组
            elif not isinstance(elem, collectionsAbc.Iterable):  # 如果元素不是可迭代的
                elem = (elem,)                      # 将非可迭代对象转换为单元素元组

            niterable.append(elem)                  # 将处理后的元素添加到结果列表

        return niterable                            # 返回处理后的列表

    def set_fund_history(self, fund):
        '''
        添加资金历史记录，用于性能评估
        
        参数:
            fund: 可迭代对象，每个元素包含 [日期时间, 份额值, 净资产值]
            
        说明:
            - 每个元素必须包含日期时间、份额值和净资产值
            - 数据必须按日期时间升序排序
        '''
        self._fhistory = fund                       # 保存资金历史数据

    def add_order_history(self, orders, notify=True):
        '''
        添加订单历史记录，用于性能评估
        
        参数:
            orders: 可迭代对象，包含订单信息，格式为:
                   [日期时间, 数量, 价格] 或 [日期时间, 数量, 价格, 数据源]
            notify: 是否通知第一个策略关于这些人工订单
            
        说明:
            - 数据必须按日期时间升序排序
            - 数量为正表示买入，为负表示卖出
        '''
        self._ohistory.append((orders, notify))     # 将订单历史和通知标志添加到历史记录列表

    def notify_timer(self, timer, when, *args, **kwargs):
        '''
        接收定时器通知的方法，可在子类中重写
        
        参数:
            timer: 由add_timer返回的定时器对象
            when: 调用时间
            *args, **kwargs: 传递给add_timer的额外参数
        '''
        pass                                        # 默认实现为空，子类可重写

    def _add_timer(self, owner, when,
                   offset=datetime.timedelta(), repeat=datetime.timedelta(),
                   weekdays=[], weekcarry=False,
                   monthdays=[], monthcarry=True,
                   allow=None,
                   tzdata=None, strats=False, cheat=False,
                   *args, **kwargs):
        '''
        内部方法，创建定时器但不启动它
        
        参数:
            owner: 定时器的所有者对象
            when: 定时器触发时间
            offset: 时间偏移量
            repeat: 重复间隔
            weekdays: 允许触发的星期几列表
            weekcarry: 星期几未出现时是否顺延
            monthdays: 允许触发的每月日期列表
            monthcarry: 月日未出现时是否顺延
            allow: 自定义允许函数
            tzdata: 时区数据
            strats: 是否通知策略
            cheat: 是否在经纪人评估订单前调用
            *args, **kwargs: 额外参数
            
        返回:
            Timer: 创建的定时器对象
        '''
        timer = Timer(                              # 创建新的定时器对象
            tid=len(self._pretimers),               # 定时器ID为当前列表长度
            owner=owner, strats=strats,             # 设置所有者和策略通知标志
            when=when, offset=offset, repeat=repeat, # 设置时间相关参数
            weekdays=weekdays, weekcarry=weekcarry,  # 设置每周触发参数
            monthdays=monthdays, monthcarry=monthcarry, # 设置每月触发参数
            allow=allow,                            # 设置自定义允许函数
            tzdata=tzdata, cheat=cheat,             # 设置时区和是否作弊
            *args, **kwargs                         # 传递额外参数
        )

        self._pretimers.append(timer)               # 将定时器添加到预定时器列表
        return timer                                # 返回创建的定时器

    def add_timer(self, when,
                  offset=datetime.timedelta(), repeat=datetime.timedelta(),
                  weekdays=[], weekcarry=False,
                  monthdays=[], monthcarry=True,
                  allow=None,
                  tzdata=None, strats=False, cheat=False,
                  *args, **kwargs):
        '''
        安排定时器以调用notify_timer方法
        
        参数:
            when: 定时器触发时间，可以是:
                - datetime.time实例
                - bt.timer.SESSION_START 表示交易会话开始
                - bt.timer.SESSION_END 表示交易会话结束
            offset: 时间偏移量，用于偏移when值
            repeat: 首次调用后的重复间隔
            weekdays: 定时器可以在一周中的哪些天被调用
            weekcarry: 如果星期几未出现，是否在下一天执行
            monthdays: 定时器在每月的哪些天执行
            monthcarry: 如果月日未出现，是否在下一可用日执行
            allow: 日期允许执行的回调函数
            tzdata: 时区数据，决定了when的解释方式
            strats: 是否也调用策略的notify_timer方法
            cheat: 是否在经纪人评估订单前调用定时器
            *args, **kwargs: 传递给notify_timer的额外参数
            
        返回:
            Timer: 创建的定时器对象
        '''
        # 调用内部方法创建定时器，设置当前cerebro实例为所有者
        return self._add_timer(
            owner=self, when=when, offset=offset, repeat=repeat, 
            weekdays=weekdays, weekcarry=weekcarry,
            monthdays=monthdays, monthcarry=monthcarry,
            allow=allow,
            tzdata=tzdata, strats=strats, cheat=cheat,
            *args, **kwargs)

    def addtz(self, tz):
        '''
        为策略添加全局时区，也可以通过tz参数设置
        
        参数``tz``可以是：
          - ``None``：策略显示的日期时间将采用UTC格式
          - ``pytz``实例：用于将UTC时间转换为选定时区
          - ``字符串``：尝试实例化pytz实例
          - ``整数``：使用self.datas中对应数据的时区(0使用data0的时区)
        '''
        self.p.tz = tz  # 将参数tz保存到cerebro实例的参数中

    def addcalendar(self, cal):
        '''
        添加全局交易日历到系统。各数据源可能有单独的日历覆盖全局日历
        
        ``cal``可以是``TradingCalendar``实例、字符串或``pandas_market_calendars``实例。
        字符串将实例化为``PandasMarketCalendar``(需要安装pandas_market_calendar模块)。
        
        如果传入`TradingCalendarBase`的子类(非实例)，将被实例化
        '''
        if isinstance(cal, string_types):  # 如果cal是字符串类型
            cal = PandasMarketCalendar(calendar=cal)  # 创建PandasMarketCalendar实例
        elif hasattr(cal, 'valid_days'):  # 如果cal有valid_days属性(可能是pandas_market_calendars对象)
            cal = PandasMarketCalendar(calendar=cal)  # 创建PandasMarketCalendar实例

        else:
            try:
                if issubclass(cal, TradingCalendarBase):  # 如果cal是TradingCalendarBase的子类
                    cal = cal()  # 实例化该类
            except TypeError:  # 捕获TypeError异常，通常在cal已经是实例时发生
                pass  # 不做任何处理，保持cal不变

        self._tradingcal = cal  # 设置交易日历为处理后的cal

    def add_signal(self, sigtype, sigcls, *sigargs, **sigkwargs):
        '''
        添加信号到系统，之后会添加到SignalStrategy中
        
        参数:
            sigtype: 信号类型(如买入、卖出等)
            sigcls: 信号类
            *sigargs, **sigkwargs: 传递给信号类的参数
        '''
        self.signals.append((sigtype, sigcls, sigargs, sigkwargs))  # 将信号配置添加到信号列表

    def signal_strategy(self, stratcls, *args, **kwargs):
        '''
        添加SignalStrategy子类，用于接收信号
        
        参数:
            stratcls: SignalStrategy子类
            *args, **kwargs: 策略参数
        '''
        self._signal_strat = (stratcls, args, kwargs)  # 存储信号策略类及其参数

    def signal_concurrent(self, onoff):
        '''
        如果系统添加了信号且concurrent值为True，则允许并发订单
        
        参数:
            onoff: 布尔值，控制是否允许并发订单
        '''
        self._signal_concurrent = onoff  # 设置信号并发标志

    def signal_accumulate(self, onoff):
        '''
        如果系统添加了信号且accumulate值为True，则允许在已有市场仓位的情况下
        继续进入市场，增加仓位
        
        参数:
            onoff: 布尔值，控制是否允许累积仓位
        '''
        self._signal_accumulate = onoff  # 设置信号累积标志

    def addstore(self, store):
        '''
        添加Store实例到系统(如果尚未存在)
        
        参数:
            store: Store实例，用于连接外部数据源或经纪商
        '''
        if store not in self.stores:  # 检查store是否已经在stores列表中
            self.stores.append(store)  # 如果不在，则添加到stores列表

    def addwriter(self, wrtcls, *args, **kwargs):
        '''
        添加Writer类到系统。实例化将在run时进行
        
        参数:
            wrtcls: Writer类
            *args, **kwargs: 实例化Writer类的参数
        '''
        self.writers.append((wrtcls, args, kwargs))  # 将Writer类及其参数添加到writers列表

    def addsizer(self, sizercls, *args, **kwargs):
        '''
        添加Sizer类(及参数)作为cerebro中任何策略的默认仓位管理器
        
        参数:
            sizercls: Sizer类
            *args, **kwargs: 实例化Sizer类的参数
        '''
        self.sizers[None] = (sizercls, args, kwargs)  # 将Sizer类及其参数存储为默认仓位管理器

    def addsizer_byidx(self, idx, sizercls, *args, **kwargs):
        '''
        通过索引添加Sizer类。该索引与addstrategy返回的索引兼容，
        只有由idx引用的策略会接收此仓位管理器
        
        参数:
            idx: 策略索引
            sizercls: Sizer类
            *args, **kwargs: 实例化Sizer类的参数
        '''
        self.sizers[idx] = (sizercls, args, kwargs)  # 将Sizer类及其参数存储到指定索引的策略中

    def addindicator(self, indcls, *args, **kwargs):
        '''
        添加Indicator类到系统。实例化将在run时在传递的策略中完成
        
        参数:
            indcls: Indicator类
            *args, **kwargs: 实例化Indicator类的参数
        '''
        self.indicators.append((indcls, args, kwargs))  # 将Indicator类及其参数添加到indicators列表

    def addanalyzer(self, ancls, *args, **kwargs):
        '''
        添加Analyzer类到系统。实例化将在run时完成
        
        参数:
            ancls: Analyzer类
            *args, **kwargs: 实例化Analyzer类的参数
        '''
        self.analyzers.append((ancls, args, kwargs))  # 将Analyzer类及其参数添加到analyzers列表

    def addobserver(self, obscls, *args, **kwargs):
        '''
        添加Observer类到系统。实例化将在run时完成
        
        参数:
            obscls: Observer类
            *args, **kwargs: 实例化Observer类的参数
        '''
        self.observers.append((False, obscls, args, kwargs))  # 添加Observer，False表示不是多数据观察器

    def addobservermulti(self, obscls, *args, **kwargs):
        '''
        添加Observer类到系统，实例化将在run时完成
        
        该观察器将为系统中的每个"数据"添加一次。用例是买卖观察器，观察单个数据。
        
        相反的例子是CashValue，它观察系统范围的值
        '''
        self.observers.append((True, obscls, args, kwargs))  # 添加Observer，True表示是多数据观察器

    def addstorecb(self, callback):
        '''
        添加回调以获取通常由notify_store方法处理的消息
        
        回调的签名必须支持以下格式:
          - callback(msg, \*args, \*\*kwargs)
        
        接收的实际msg、*args和**kwargs取决于具体实现(完全依赖于data/broker/store)，
        但通常可以期望它们是可打印的，以便于接收和测试。
        '''
        self.storecbs.append(callback)  # 将回调函数添加到store回调列表

    def _notify_store(self, msg, *args, **kwargs):
        """
        内部方法，用于通知所有store回调和notify_store方法
        
        参数:
            msg: 消息内容
            *args, **kwargs: 额外参数
        """
        for callback in self.storecbs:  # 遍历所有store回调
            callback(msg, *args, **kwargs)  # 调用每个回调函数

        self.notify_store(msg, *args, **kwargs)  # 调用通知方法

    def notify_store(self, msg, *args, **kwargs):
        '''
        在cerebro中接收store通知
        
        此方法可在Cerebro子类中重写
        
        接收的实际msg、*args和**kwargs取决于具体实现(完全依赖于data/broker/store)，
        但通常可以期望它们是可打印的，以便于接收和测试。
        '''
        pass  # 默认实现为空，子类可重写

    def _storenotify(self):
        """
        内部方法，处理所有store的通知并分发给cerebro和策略
        """
        for store in self.stores:  # 遍历所有store
            for notif in store.get_notifications():  # 获取每个store的所有通知
                msg, args, kwargs = notif  # 解析通知消息

                self._notify_store(msg, *args, **kwargs)  # 通知cerebro
                for strat in self.runningstrats:  # 遍历所有运行中的策略
                    strat.notify_store(msg, *args, **kwargs)  # 通知每个策略

    def adddatacb(self, callback):
        '''
        添加回调以获取通常由notify_data方法处理的消息
        
        回调的签名必须支持以下格式:
          - callback(data, status, \*args, \*\*kwargs)
        
        接收的实际*args和**kwargs取决于具体实现(完全依赖于data/broker/store)，
        但通常可以期望它们是可打印的，以便于接收和测试。
        '''
        self.datacbs.append(callback)  # 将回调函数添加到数据回调列表

    def _datanotify(self):
        """
        内部方法，处理所有数据的通知并分发给cerebro和策略
        """
        for data in self.datas:  # 遍历所有数据
            for notif in data.get_notifications():  # 获取每个数据的所有通知
                status, args, kwargs = notif  # 解析通知消息
                self._notify_data(data, status, *args, **kwargs)  # 通知cerebro
                for strat in self.runningstrats:  # 遍历所有运行中的策略
                    strat.notify_data(data, status, *args, **kwargs)  # 通知每个策略

    def _notify_data(self, data, status, *args, **kwargs):
        """
        内部方法，用于通知所有数据回调和notify_data方法
        
        参数:
            data: 数据源对象
            status: 状态消息
            *args, **kwargs: 额外参数
        """
        for callback in self.datacbs:  # 遍历所有数据回调
            callback(data, status, *args, **kwargs)  # 调用每个回调函数

        self.notify_data(data, status, *args, **kwargs)  # 调用通知方法

    def notify_data(self, data, status, *args, **kwargs):
        '''
        在cerebro中接收数据通知
        
        此方法可在Cerebro子类中重写
        
        接收的实际*args和**kwargs取决于具体实现(完全依赖于data/broker/store)，
        但通常可以期望它们是可打印的，以便于接收和测试。
        '''
        pass  # 默认实现为空，子类可重写

    def adddata(self, data, name=None):
        '''
        添加数据源实例到系统
        
        参数:
            data: 数据源实例
            name: 如果不为None，将存入data._name，用于装饰/绘图目的
            
        返回:
            data: 添加的数据源实例
        '''
        if name is not None:  # 如果提供了名称
            data._name = name  # 设置数据源的名称

        data._id = next(self._dataid)  # 为数据源分配唯一ID
        data.setenvironment(self)  # 设置数据源的环境为当前cerebro

        self.datas.append(data)  # 将数据添加到数据列表
        self.datasbyname[data._name] = data  # 将数据添加到按名称索引的字典
        feed = data.getfeed()  # 获取数据源的feed
        if feed and feed not in self.feeds:  # 如果feed存在且不在feeds列表中
            self.feeds.append(feed)  # 添加feed到feeds列表

        if data.islive():  # 如果是实时数据
            self._dolive = True  # 设置实时数据标志

        return data  # 返回添加的数据源

    def chaindata(self, *args, **kwargs):
        '''
        将多个数据源链接成一个
        
        参数:
            *args: 要链接的数据源
            **kwargs: 命名参数，其中name用于装饰/绘图目的
            
        如果name作为命名参数传递且不为None，将存入data._name。
        如果为None，则使用第一个数据源的名称。
        
        返回:
            链接后的数据源实例
        '''
        dname = kwargs.pop('name', None)  # 获取name参数，默认为None
        if dname is None:  # 如果没有指定名称
            dname = args[0]._dataname  # 使用第一个数据源的名称
        d = bt.feeds.Chainer(dataname=dname, *args)  # 创建Chainer实例链接数据源
        self.adddata(d, name=dname)  # 添加链接的数据源到cerebro

        return d  # 返回链接的数据源

    def rolloverdata(self, *args, **kwargs):
        '''
        将多个数据源链接成一个(用于期货合约滚动)
        
        参数:
            *args: 要链接的数据源
            **kwargs: 命名参数，其中name用于装饰/绘图目的，其他参数传递给RollOver类
            
        如果name作为命名参数传递且不为None，将存入data._name。
        如果为None，则使用第一个数据源的名称。
        
        返回:
            滚动链接后的数据源实例
        '''
        dname = kwargs.pop('name', None)  # 获取name参数，默认为None
        if dname is None:  # 如果没有指定名称
            dname = args[0]._dataname  # 使用第一个数据源的名称
        d = bt.feeds.RollOver(dataname=dname, *args, **kwargs)  # 创建RollOver实例链接数据源
        self.adddata(d, name=dname)  # 添加滚动链接的数据源到cerebro

        return d  # 返回滚动链接的数据源

    def replaydata(self, dataname, name=None, **kwargs):
        '''
        添加数据源以在系统中进行重放
        
        参数:
            dataname: 数据源对象或名称
            name: 如果不为None，将存入data._name，用于装饰/绘图目的
            **kwargs: 其他参数如timeframe、compression、todate等，
                     将透明传递给replay过滤器
                     
        返回:
            配置为重放的数据源实例
        '''
        if any(dataname is x for x in self.datas):  # 如果数据源已在datas列表中
            dataname = dataname.clone()  # 克隆数据源以避免修改原始数据

        dataname.replay(**kwargs)  # 配置数据源为重放模式
        self.adddata(dataname, name=name)  # 添加重放数据源到cerebro
        self._doreplay = True  # 设置重放模式标志

        return dataname  # 返回重放数据源

    def resampledata(self, dataname, name=None, **kwargs):
        '''
        添加数据源以在系统中进行重采样
        
        参数:
            dataname: 数据源对象或名称
            name: 如果不为None，将存入data._name，用于装饰/绘图目的
            **kwargs: 其他参数如timeframe、compression、todate等，
                     将透明传递给resample过滤器
                     
        返回:
            配置为重采样的数据源实例
        '''
        if any(dataname is x for x in self.datas):  # 如果数据源已在datas列表中
            dataname = dataname.clone()  # 克隆数据源以避免修改原始数据

        dataname.resample(**kwargs)  # 配置数据源为重采样模式
        self.adddata(dataname, name=name)  # 添加重采样数据源到cerebro
        self._doreplay = True  # 设置重放模式标志(重采样内部使用重放机制)

        return dataname  # 返回重采样数据源

    def optcallback(self, cb):
        '''
        添加回调到优化回调列表，当每个策略运行完成后调用
        
        参数:
            cb: 回调函数，签名为cb(strategy)
        '''
        self.optcbs.append(cb)  # 将回调函数添加到优化回调列表

    def optstrategy(self, strategy, *args, **kwargs):
        '''
        添加Strategy类进行优化。实例化将在运行时进行。
        
        参数:
            strategy: 策略类
            *args, **kwargs: 必须是可迭代对象，包含要检查的值
        
        args和kwargs必须是可迭代对象，包含要检查的值。
        
        示例: 如果Strategy接受参数"period"，优化调用如下:
        
          - cerebro.optstrategy(MyStrategy, period=(15, 25))
        
        这将为值15和25执行优化。而
        
          - cerebro.optstrategy(MyStrategy, period=range(15, 25))
        
        将用period值15到24(不包括25，因为Python的range是半开区间)执行MyStrategy
        
        如果某个参数需要传递但不需要优化，调用如下:
        
          - cerebro.optstrategy(MyStrategy, period=(15,))
        
        注意period仍作为可迭代对象传递...只是只有1个元素
        
        backtrader会尝试识别以下情况:
        
          - cerebro.optstrategy(MyStrategy, period=15)
        
        并在可能的情况下创建内部伪可迭代对象
        '''
        self._dooptimize = True  # 设置优化模式标志
        args = self.iterize(args)  # 确保args是可迭代的格式
        optargs = itertools.product(*args)  # 计算位置参数的所有组合

        optkeys = list(kwargs)  # 获取关键字参数的键列表

        vals = self.iterize(kwargs.values())  # 确保kwargs值是可迭代的格式
        optvals = itertools.product(*vals)  # 计算关键字参数值的所有组合

        okwargs1 = map(zip, itertools.repeat(optkeys), optvals)  # 将键和值组合在一起

        optkwargs = map(dict, okwargs1)  # 将组合转换为字典列表

        it = itertools.product([strategy], optargs, optkwargs)  # 组合策略类、位置参数和关键字参数
        self.strats.append(it)  # 将优化配置添加到策略列表

    def addstrategy(self, strategy, *args, **kwargs):
        '''
        添加一个策略类到系统中以进行单次运行。实例化将在运行时进行。
    
        参数:
            strategy: 策略类
            *args, **kwargs: 实例化策略时传递的参数
    
        返回:
            int: 策略在系统中的索引，以便后续添加其他对象（如仓位管理器）时引用
        '''
        self.strats.append([(strategy, args, kwargs)])
        return len(self.strats) - 1

    def setbroker(self, broker):
        '''
        为该策略设置特定的经纪人实例，替换从cerebro继承的经纪人实例。
        '''
        self._broker = broker
        broker.cerebro = self
        return broker

    def getbroker(self):
        '''
        返回经纪人实例。

        这也可以通过名为``broker``的``property``获得

        '''
        return self._broker

    broker = property(getbroker, setbroker)  # 定义broker属性，使用getbroker和setbroker方法作为读取和设置器

    def plot(self, plotter=None, numfigs=1, iplot=True, start=None, end=None,
             width=16, height=9, dpi=300, tight=True, use=None,
             **kwargs):
        '''
        绘制cerebro中的策略图表
        
        参数:
            plotter: 绘图器实例，如果为None则创建默认Plot实例
            numfigs: 将图表拆分成指定数量，以减少图表密度
            iplot: 如果为True且在notebook中运行，图表将内联显示
            use: 设置为所需matplotlib后端的名称，优先于iplot
            start: 策略datetime行数组的索引或datetime.date/datetime.datetime实例，表示绘图开始点
            end: 策略datetime行数组的索引或datetime.date/datetime.datetime实例，表示绘图结束点
            width: 保存图形的宽度(英寸)
            height: 保存图形的高度(英寸)
            dpi: 保存图形的质量(每英寸点数)
            tight: 是否只保存实际内容而非图形的整个框架
        '''
        if self._exactbars > 0:  # 如果启用了exactbars，则无法绘图(内存优化模式)
            return  # 直接返回，不执行绘图

        if not plotter:  # 如果没有提供绘图器
            from . import plot  # 导入plot模块
            if self.p.oldsync:  # 如果使用旧同步模式
                plotter = plot.Plot_OldSync(**kwargs)  # 创建旧同步模式的绘图器
            else:
                plotter = plot.Plot(**kwargs)  # 创建标准绘图器

        # pfillers = {self.datas[i]: self._plotfillers[i]
        # for i, x in enumerate(self._plotfillers)}  # 注释掉的代码，填充绘图用

        # pfillers2 = {self.datas[i]: self._plotfillers2[i]
        # for i, x in enumerate(self._plotfillers2)}  # 注释掉的代码，填充绘图用

        figs = []  # 创建空列表存储图形对象
        for stratlist in self.runstrats:  # 遍历运行的策略列表
            for si, strat in enumerate(stratlist):  # 遍历每个策略
                rfig = plotter.plot(strat, figid=si * 100,  # 绘制策略图表
                                    numfigs=numfigs, iplot=iplot,
                                    start=start, end=end, use=use)
                # pfillers=pfillers2)  # 注释掉的代码

                figs.append(rfig)  # 将图形对象添加到列表

            plotter.show()  # 显示所有图表

        return figs  # 返回图形对象列表

    def __call__(self, iterstrat):
        '''
        使Cerebro实例可调用，在优化过程中通过多处理模块传递cerebro
        
        参数:
            iterstrat: 迭代策略设置
            
        返回:
            策略运行结果
        '''
        # 确定是否需要预先加载数据(取决于optdatas参数和其他条件)
        predata = self.p.optdatas and self._dopreload and self._dorunonce
        return self.runstrategies(iterstrat, predata=predata)  # 运行策略并返回结果

    def __getstate__(self):
        '''
        在优化过程中防止优化结果runstrats被pickle到子进程
        
        返回:
            cerebro实例状态的副本，移除了runstrats属性
        '''
        rv = vars(self).copy()  # 复制实例变量字典
        if 'runstrats' in rv:  # 如果包含runstrats
            del(rv['runstrats'])  # 删除runstrats，减少进程间传输数据量
        return rv  # 返回修改后的状态

    def runstop(self):
        '''
        如果从策略内部或任何其他地方(包括其他线程)调用，执行将尽快停止
        设置停止事件标志，多个地方会检查此标志以中断执行
        '''
        self._event_stop = True  # 设置停止事件标志

    def run(self, **kwargs):
        '''
        执行回测的核心方法，传递的kwargs会影响Cerebro实例化时设置的标准参数值
        
        参数:
            **kwargs: 可选的关键字参数，用于覆盖Cerebro的标准参数
        
        返回:
            list: 包含策略实例的列表，具体取决于是否启用优化
            - 对于非优化：包含通过addstrategy添加的策略实例列表
            - 对于优化：包含通过addstrategy添加的策略实例列表的列表
        '''
        self._event_stop = False  # 重置停止事件标志

        if not self.datas:  # 如果没有数据
            return []  # 直接返回空列表，无法运行

        pkeys = self.params._getkeys()  # 获取参数键列表
        for key, val in kwargs.items():  # 遍历传入的关键字参数
            if key in pkeys:  # 如果是已知参数
                setattr(self.params, key, val)  # 更新参数值

        # 管理对象缓存的启用/禁用
        linebuffer.LineActions.cleancache()  # 清除行缓存
        indicator.Indicator.cleancache()  # 清除指标缓存

        linebuffer.LineActions.usecache(self.p.objcache)  # 设置是否使用对象缓存
        indicator.Indicator.usecache(self.p.objcache)  # 设置指标是否使用缓存

        self._dorunonce = self.p.runonce  # 设置是否使用runonce模式
        self._dopreload = self.p.preload  # 设置是否预加载数据
        self._exactbars = int(self.p.exactbars)  # 设置精确柱模式

        if self._exactbars:  # 如果启用了精确柱模式
            self._dorunonce = False  # 禁用runonce(内存优化模式下不能使用向量化)
            self._dopreload = self._dopreload and self._exactbars < 1  # 仅当exactbars < 1时预加载

        # 检查是否有任何数据使用重放模式
        self._doreplay = self._doreplay or any(x.replaying for x in self.datas)
        if self._doreplay:  # 如果使用重放模式
            # 重放模式下不支持预加载，框架柱在实时构建
            self._dopreload = False  # 禁用预加载

        if self._dolive or self.p.live:  # 如果使用实时模式
            # 实时模式下，预加载和runonce都必须关闭
            self._dorunonce = False  # 禁用runonce
            self._dopreload = False  # 禁用预加载

        self.runwriters = list()  # 初始化运行写入器列表

        # 如果请求添加系统默认写入器
        if self.p.writer is True:  # 如果启用了writer参数
            wr = WriterFile()  # 创建默认文件写入器
            self.runwriters.append(wr)  # 添加到运行写入器列表

        # 实例化其他写入器
        for wrcls, wrargs, wrkwargs in self.writers:  # 遍历所有写入器配置
            wr = wrcls(*wrargs, **wrkwargs)  # 实例化写入器
            self.runwriters.append(wr)  # 添加到运行写入器列表

        # 记录是否有写入器需要完整的CSV输出
        self.writers_csv = any(map(lambda x: x.p.csv, self.runwriters))

        self.runstrats = list()  # 初始化运行策略结果列表

        if self.signals:  # 如果有信号，处理信号逻辑
            signalst, sargs, skwargs = self._signal_strat  # 获取信号策略配置
            if signalst is None:  # 如果信号策略未设置
                # 尝试检查第一个普通策略是否为信号策略
                try:
                    signalst, sargs, skwargs = self.strats.pop(0)  # 取出第一个策略
                except IndexError:
                    pass  # 如果没有策略，不做任何处理
                else:
                    if not isinstance(signalst, SignalStrategy):  # 如果不是信号策略
                        # 不是信号策略，重新插入到开头
                        self.strats.insert(0, (signalst, sargs, skwargs))
                        signalst = None  # 标记为不存在

            if signalst is None:  # 再次检查
                # 仍然为None，创建默认信号策略
                signalst, sargs, skwargs = SignalStrategy, tuple(), dict()

            # 添加信号策略
            self.addstrategy(signalst,  # 添加信号策略
                             _accumulate=self._signal_accumulate,  # 设置是否累积信号
                             _concurrent=self._signal_concurrent,  # 设置是否允许并发信号
                             signals=self.signals,  # 传递信号列表
                             *sargs,  # 传递位置参数
                             **skwargs)  # 传递关键字参数

        if not self.strats:  # 如果没有策略但有数据，添加默认策略
            self.addstrategy(Strategy)  # 添加默认策略类

        iterstrats = itertools.product(*self.strats)  # 计算所有策略参数组合
        if not self._dooptimize or self.p.maxcpus == 1:  # 如果不进行优化或只使用1个CPU
            # 跳过进程"生成"，直接运行
            for iterstrat in iterstrats:  # 遍历每个策略参数组合
                runstrat = self.runstrategies(iterstrat)  # 运行策略
                self.runstrats.append(runstrat)  # 添加结果到列表
                if self._dooptimize:  # 如果进行优化
                    for cb in self.optcbs:  # 遍历优化回调
                        cb(runstrat)  # 调用回调处理完成的策略
        else:  # 使用多进程进行优化
            if self.p.optdatas and self._dopreload and self._dorunonce:  # 如果优化数据选项开启
                for data in self.datas:  # 遍历所有数据
                    data.reset()  # 重置数据
                    if self._exactbars < 1:  # 如果不是精确柱模式
                        data.extend(size=self.params.lookahead)  # 扩展数据大小
                    data._start()  # 启动数据
                    if self._dopreload:  # 如果预加载
                        data.preload()  # 预加载数据

            # 创建进程池进行并行优化
            pool = multiprocessing.Pool(self.p.maxcpus or None)  # 创建进程池
            for r in pool.imap(self, iterstrats):  # 将策略参数组合分配给进程
                self.runstrats.append(r)  # 收集结果
                for cb in self.optcbs:  # 遍历优化回调
                    cb(r)  # 调用回调处理完成的策略

            pool.close()  # 关闭进程池

            if self.p.optdatas and self._dopreload and self._dorunonce:  # 清理资源
                for data in self.datas:  # 遍历所有数据
                    data.stop()  # 停止数据

        if not self._dooptimize:  # 如果不是优化模式
            # 避免常规情况下的嵌套列表，直接返回第一个结果
            return self.runstrats[0]  # 返回第一个策略结果

        return self.runstrats  # 返回所有策略结果列表

    def _init_stcount(self):
        """
        初始化策略计数器，用于为策略分配唯一ID
        """
        self.stcount = itertools.count(0)  # 创建从0开始的计数器

    def _next_stid(self):
        """
        获取下一个策略ID
        
        返回:
            下一个唯一的策略ID
        """
        return next(self.stcount)  # 返回计数器的下一个值

    def runstrategies(self, iterstrat, predata=False):
        '''
        内部方法，由run调用来运行一组策略
        
        参数:
            iterstrat: 策略迭代器配置
            predata: 是否已经预处理了数据
            
        返回:
            运行策略的结果
        '''
        self._init_stcount()  # 初始化策略计数器

        self.runningstrats = runstrats = list()  # 创建运行策略列表
        for store in self.stores:  # 启动所有商店
            store.start()  # 启动商店

        if self.p.cheat_on_open and self.p.broker_coo:  # 如果启用了开盘作弊
            # 尝试在经纪人中激活开盘作弊
            if hasattr(self._broker, 'set_coo'):
                self._broker.set_coo(True)  # 设置经纪人的开盘作弊选项

        if self._fhistory is not None:  # 如果有资金历史
            self._broker.set_fund_history(self._fhistory)  # 设置经纪人的资金历史

        for orders, onotify in self._ohistory:  # 遍历订单历史
            self._broker.add_order_history(orders, onotify)  # 添加订单历史到经纪人

        self._broker.start()  # 启动经纪人

        for feed in self.feeds:  # 启动所有数据馈送
            feed.start()  # 启动数据馈送

        if self.writers_csv:  # 如果需要CSV输出
            wheaders = list()  # 创建CSV头部列表
            for data in self.datas:  # 遍历数据源
                if data.csv:  # 如果数据源支持CSV
                    wheaders.extend(data.getwriterheaders())  # 获取CSV头部

            for writer in self.runwriters:  # 遍历写入器
                if writer.p.csv:  # 如果写入器支持CSV
                    writer.addheaders(wheaders)  # 添加CSV头部

        # self._plotfillers = [list() for d in self.datas]  # 注释掉的代码
        # self._plotfillers2 = [list() for d in self.datas]  # 注释掉的代码

        if not predata:  # 如果数据未预处理
            for data in self.datas:  # 遍历所有数据
                data.reset()  # 重置数据
                if self._exactbars < 1:  # 如果不是精确柱模式
                    data.extend(size=self.params.lookahead)  # 扩展数据大小
                data._start()  # 启动数据
                if self._dopreload:  # 如果预加载
                    data.preload()  # 预加载数据

        for stratcls, sargs, skwargs in iterstrat:  # 遍历策略类和参数
            sargs = self.datas + list(sargs)  # 将数据添加到策略参数前
            try:
                strat = stratcls(*sargs, **skwargs)  # 创建策略实例
            except bt.errors.StrategySkipError:
                continue  # 跳过添加策略到集合

            if self.p.oldsync:  # 如果使用旧同步模式
                strat._oldsync = True  # 告诉策略使用旧时钟更新
            if self.p.tradehistory:  # 如果记录交易历史
                strat.set_tradehistory()  # 设置交易历史
            runstrats.append(strat)  # 将策略添加到运行列表

        tz = self.p.tz  # 获取时区设置
        if isinstance(tz, integer_types):  # 如果时区是整数
            tz = self.datas[tz]._tz  # 使用指定数据的时区
        else:
            tz = tzparse(tz)  # 解析时区字符串

        if runstrats:  # 如果有策略要运行
            # 分离循环以提高清晰度
            defaultsizer = self.sizers.get(None, (None, None, None))  # 获取默认仓位管理器
            for idx, strat in enumerate(runstrats):  # 遍历所有策略
                if self.p.stdstats:  # 如果使用标准统计
                    strat._addobserver(False, observers.Broker)  # 添加Broker观察器
                    if self.p.oldbuysell:  # 如果使用旧买卖显示
                        strat._addobserver(True, observers.BuySell)  # 添加旧BuySell观察器
                    else:
                        strat._addobserver(True, observers.BuySell,  # 添加新BuySell观察器
                                           barplot=True)

                    if self.p.oldtrades or len(self.datas) == 1:  # 如果使用旧交易显示或只有一个数据
                        strat._addobserver(False, observers.Trades)  # 添加Trades观察器
                    else:
                        strat._addobserver(False, observers.DataTrades)  # 添加DataTrades观察器

                for multi, obscls, obsargs, obskwargs in self.observers:  # 添加自定义观察器
                    strat._addobserver(multi, obscls, *obsargs, **obskwargs)

                for indcls, indargs, indkwargs in self.indicators:  # 添加指标
                    strat._addindicator(indcls, *indargs, **indkwargs)

                for ancls, anargs, ankwargs in self.analyzers:  # 添加分析器
                    strat._addanalyzer(ancls, *anargs, **ankwargs)

                # 获取策略的仓位管理器，如没有则使用默认
                sizer, sargs, skwargs = self.sizers.get(idx, defaultsizer)
                if sizer is not None:  # 如果有仓位管理器
                    strat._addsizer(sizer, *sargs, **skwargs)  # 添加仓位管理器

                strat._settz(tz)  # 设置策略时区
                strat._start()  # 启动策略

                for writer in self.runwriters:  # 遍历写入器
                    if writer.p.csv:  # 如果写入器支持CSV
                        writer.addheaders(strat.getwriterheaders())  # 添加策略CSV头部

            if not predata:  # 如果数据未预处理
                for strat in runstrats:  # 遍历所有策略
                    strat.qbuffer(self._exactbars, replaying=self._doreplay)  # 设置策略缓冲区

            for writer in self.runwriters:  # 启动所有写入器
                writer.start()

            # 准备定时器
            self._timers = []  # 初始化定时器列表
            self._timerscheat = []  # 初始化作弊定时器列表
            for timer in self._pretimers:  # 遍历预定时器
                # 预处理时区数据如需要
                timer.start(self.datas[0])  # 启动定时器

                if timer.params.cheat:  # 如果是作弊定时器
                    self._timerscheat.append(timer)  # 添加到作弊定时器列表
                else:
                    self._timers.append(timer)  # 添加到普通定时器列表

            # 根据预加载和runonce设置选择运行模式
            if self._dopreload and self._dorunonce:  # 如果预加载和runonce
                if self.p.oldsync:  # 如果使用旧同步
                    self._runonce_old(runstrats)  # 使用旧runonce模式
                else:
                    self._runonce(runstrats)  # 使用新runonce模式
            else:  # 否则使用next模式
                if self.p.oldsync:  # 如果使用旧同步
                    self._runnext_old(runstrats)  # 使用旧next模式
                else:
                    self._runnext(runstrats)  # 使用新next模式

            for strat in runstrats:  # 停止所有策略
                strat._stop()  # 停止策略

        self._broker.stop()  # 停止经纪人

        if not predata:  # 如果数据未预处理
            for data in self.datas:  # 停止所有数据
                data.stop()  # 停止数据

        for feed in self.feeds:  # 停止所有馈送
            feed.stop()  # 停止馈送

        for store in self.stores:  # 停止所有商店
            store.stop()  # 停止商店

        self.stop_writers(runstrats)  # 停止写入器

        if self._dooptimize and self.p.optreturn:  # 如果优化且返回优化结果
            # 可以优化结果
            results = list()  # 创建结果列表
            for strat in runstrats:  # 遍历所有策略
                for a in strat.analyzers:  # 处理每个分析器
                    a.strategy = None  # 清除分析器对策略的引用
                    a._parent = None  # 清除分析器的父引用
                    for attrname in dir(a):  # 遍历分析器的属性
                        if attrname.startswith('data'):  # 如果是数据属性
                            setattr(a, attrname, None)  # 清除数据引用

                # 创建优化返回对象，包含参数和分析器
                oreturn = OptReturn(strat.params, analyzers=strat.analyzers, strategycls=type(strat))
                results.append(oreturn)  # 添加到结果列表

            return results  # 返回优化结果列表

        return runstrats  # 返回运行策略列表

    def stop_writers(self, runstrats):
        """
        停止所有写入器并提供完整的cerebro信息
        
        参数:
            runstrats: 运行的策略列表
        """
        cerebroinfo = OrderedDict()  # 创建cerebro信息有序字典
        datainfos = OrderedDict()  # 创建数据信息有序字典

        for i, data in enumerate(self.datas):  # 遍历所有数据
            datainfos['Data%d' % i] = data.getwriterinfo()  # 获取数据写入信息

        cerebroinfo['Datas'] = datainfos  # 添加数据信息到cerebro信息

        stratinfos = dict()  # 创建策略信息字典
        for strat in runstrats:  # 遍历所有策略
            stname = strat.__class__.__name__  # 获取策略类名
            stratinfos[stname] = strat.getwriterinfo()  # 获取策略写入信息

        cerebroinfo['Strategies'] = stratinfos  # 添加策略信息到cerebro信息

        for writer in self.runwriters:  # 遍历所有写入器
            writer.writedict(dict(Cerebro=cerebroinfo))  # 写入cerebro信息
            writer.stop()  # 停止写入器

    def _brokernotify(self):
        '''
        内部方法，通知经纪人并将经纪人通知传递给策略
        处理订单通知流程
        '''
        self._broker.next()  # 调用经纪人的next方法
        while True:  # 循环处理所有通知
            order = self._broker.get_notification()  # 获取通知
            if order is None:  # 如果没有更多通知
                break  # 退出循环

            owner = order.owner  # 获取订单所有者
            if owner is None:  # 如果没有所有者
                owner = self.runningstrats[0]  # 默认使用第一个运行策略
            
            # 向策略添加通知，根据quicknotify参数决定是否快速通知
            owner._addnotification(order, quicknotify=self.p.quicknotify)

    def _runnext_old(self, runstrats):
        '''
        运行的实际实现，使用完整next模式
        所有对象的next方法在每个数据到达时被调用
        这是旧版实现，仅处理线性数据
        '''
        data0 = self.datas[0]  # 获取第一个数据
        d0ret = True  # 设置初始状态为True
        while d0ret or d0ret is None:  # 当data0返回True或None时继续
            lastret = False  # 重置lastret
            # 在移动数据前通知商店
            # 因为数据可能由于商店报告的错误而无法移动
            self._storenotify()  # 处理商店通知
            if self._event_stop:  # 如果请求停止
                return
            self._datanotify()  # 处理数据通知
            if self._event_stop:  # 如果请求停止
                return

            d0ret = data0.next()  # 调用data0的next方法
            if d0ret:  # 如果data0返回True
                for data in self.datas[1:]:  # 遍历其他数据
                    if not data.next(datamaster=data0):  # 如果数据未传递
                        data._check(forcedata=data0)  # 强制检查输出
                        data.next(datamaster=data0)  # 重试next方法

            elif d0ret is None:  # 如果data0返回None
                # 用于实时馈送可能不会立即产生柱状图
                # 但需要运行循环以处理通知和获取重采样等
                data0._check()  # 检查data0
                for data in self.datas[1:]:  # 遍历其他数据
                    data._check()  # 检查数据
            else:  # 如果data0返回False
                lastret = data0._last()  # 调用data0的_last方法
                for data in self.datas[1:]:  # 遍历其他数据
                    lastret += data._last(datamaster=data0)  # 调用_last方法并累加结果

                if not lastret:  # 如果没有由"lasts"改变
                    # 只有当"lasts"改变了某些内容时才进行额外回合
                    break

            # 数据可能在next后生成新通知
            self._datanotify()  # 处理数据通知
            if self._event_stop:  # 如果请求停止
                return

            self._brokernotify()  # 处理经纪人通知
            if self._event_stop:  # 如果请求停止
                return

            if d0ret or lastret:  # 如果由数据或过滤器产生了柱状图
                for strat in runstrats:  # 遍历所有策略
                    strat._next()  # 调用策略的_next方法
                    if self._event_stop:  # 如果请求停止
                        return

                    self._next_writers(runstrats)  # 通知写入器

        # 停止前的最后通知机会
        self._datanotify()  # 处理数据通知
        if self._event_stop:  # 如果请求停止
            return
        self._storenotify()  # 处理商店通知
        if self._event_stop:  # 如果请求停止
            return

    def _runonce_old(self, runstrats):
        '''
        运行的实际实现，使用向量模式
        策略仍然以伪事件模式在每个数据到达时调用``next``
        这是旧版实现，用于向量化计算
        '''
        for strat in runstrats:  # 遍历所有策略
            strat._once()  # 调用策略的_once方法

        # 策略的默认_once方法不做任何事情
        # 因此没有向前移动all datas/indicators/observers
        # 在调用_once之前已经安置好了，因此这里不需要
        # 因为指针在0位置
        data0 = self.datas[0]  # 获取第一个数据
        datas = self.datas[1:]  # 获取其他数据
        for i in range(data0.buflen()):  # 遍历数据长度
            data0.advance()  # 推进data0
            for data in datas:  # 遍历其他数据
                data.advance(datamaster=data0)  # 推进数据，以data0为主

            self._brokernotify()  # 处理经纪人通知
            if self._event_stop:  # 如果请求停止
                return

            for strat in runstrats:  # 遍历所有策略
                # data0.datetime[0]用于兼容新策略的oncepost
                strat._oncepost(data0.datetime[0])  # 调用策略的_oncepost方法
                if self._event_stop:  # 如果请求停止
                    return

                self._next_writers(runstrats)  # 通知写入器

    def _next_writers(self, runstrats):
        """
        通知写入器处理新数据
        
        参数:
            runstrats: 运行的策略列表
        """
        if not self.runwriters:  # 如果没有运行写入器
            return  # 直接返回

        if self.writers_csv:  # 如果需要CSV输出
            wvalues = list()  # 创建值列表
            for data in self.datas:  # 遍历所有数据
                if data.csv:  # 如果数据支持CSV
                    wvalues.extend(data.getwritervalues())  # 获取数据值

            for strat in runstrats:  # 遍历所有策略
                wvalues.extend(strat.getwritervalues())  # 获取策略值

            for writer in self.runwriters:  # 遍历所有写入器
                if writer.p.csv:  # 如果写入器支持CSV
                    writer.addvalues(wvalues)  # 添加值

                    writer.next()  # 调用写入器的next方法

    def _disable_runonce(self):
        '''
        lineiterators的API，用于禁用runonce(在HeikinAshi等特殊情况使用)
        '''
        self._dorunonce = False  # 禁用runonce模式

    def _runnext(self, runstrats):
        '''
        运行的实际实现，使用完整next模式
        所有对象的next方法在每个数据到达时被调用
        这是新版实现，处理多时间框架数据
        
        数据流实例:
        假设有两个数据源: data0(日K线)和data1(小时K线)
        1. 排序后，先处理小时K线，后处理日K线
        2. 系统找到最早的时间点(dt0)，例如 2023-01-01 09:00:00
        3. 所有这个时间的数据都会被交付给策略
        4. 可能data0没有这个时间点，系统会尝试通过_check和next获取
        5. 如果获取成功，数据点存储到dts[i]
        6. 如果数据时间大于dt0，会被回退(rewind)不交付
        7. 最后调用策略的_next方法处理这个时间点的所有数据
        
        交易系统流程实例:
        1. 数据进入 -> 检查时间点 -> 找到最小时间dt0 -> 确保所有数据同步到dt0
        2. 执行cheat_on_open(如有开启) -> 处理经纪人通知(如订单成交)
        3. 执行策略next方法 -> 可能生成新订单 -> 写入数据
        4. 循环继续直到没有数据
        '''
        # 按时间框架和压缩排序数据
        # 例如: [(5min,1)最小时间框架, (30min,1), (1day,1)最大时间框架]
        datas = sorted(self.datas,
                       key=lambda x: (x._timeframe, x._compression))
        datas1 = datas[1:]  # 获取第一个数据之后的所有数据
        data0 = datas[0]  # 获取第一个数据
        d0ret = True  # 设置初始状态为True

        # 识别各种数据类型的索引
        # 实例: 假设datas有3个元素[data0, data1, data2]，其中data1是重采样数据
        # 那么rs=[1], rp=[], rsonly=[1]
        rs = [i for i, x in enumerate(datas) if x.resampling]  # 重采样数据索引
        rp = [i for i, x in enumerate(datas) if x.replaying]  # 重放数据索引
        rsonly = [i for i, x in enumerate(datas)  # 仅重采样数据索引
                  if x.resampling and not x.replaying]
        onlyresample = len(datas) == len(rsonly)  # 是否所有数据都是重采样
        noresample = not rsonly  # 是否没有重采样数据

        # 处理克隆计数和其他初始化
        # 例如: 如果datas中有一个克隆数据，clonecount=1, ldatas=3, ldatas_noclones=2
        clonecount = sum(d._clone for d in datas)  # 计算克隆数据数量
        ldatas = len(datas)  # 数据总数
        ldatas_noclones = ldatas - clonecount  # 非克隆数据数
        lastqcheck = False  # 重置lastqcheck
        dt0 = date2num(datetime.datetime.max) - 2  # 默认为最大值

        while d0ret or d0ret is None:  # 当data0返回True或None时继续
            # 如果任何数据在缓冲区中有实时数据，则不需要等待
            # 实例: 在实时交易中，如果data0有新数据进入缓冲区，haslivedata()返回True
            newqcheck: bool = not any(d.haslivedata() for d in datas)  # 检查是否需要新的qcheck
            if not newqcheck:  # 如果不需要新的qcheck
                # 如果没有数据达到实时状态或全部达到，等待下一个数据
                # 实例: 实时交易中所有数据都已同步到最新点，livecount=ldatas_noclones，newqcheck=True
                livecount = sum(d._laststatus == d.LIVE for d in datas)  # 计算实时数据数
                newqcheck = not livecount or livecount == ldatas_noclones  # 更新newqcheck

            lastret = False  # 重置lastret
            # 在移动数据前通知商店
            # 例如: 连接到Interactive Brokers的商店可能报告连接断开错误
            self._storenotify()  # 处理商店通知
            if self._event_stop:  # 如果请求停止
                return
            self._datanotify()  # 处理数据通知
            if self._event_stop:  # 如果请求停止
                return

            # 记录开始时间并告诉馈送从qcheck值中折扣经过的时间
            drets = []  # 创建数据返回值列表
            qstart = datetime.datetime.utcnow()  # 记录开始时间
            for d in datas:  # 遍历所有数据
                qlapse = datetime.datetime.utcnow() - qstart  # 计算经过的时间
                d.do_qcheck(newqcheck, qlapse.total_seconds())  # 执行qcheck
                # 实例: 调用data0.next()可能返回True表示有新数据，None表示等待中，False表示没有更多数据
                drets.append(d.next(ticks=False))  # 调用next方法并记录返回值

            # 确定是否有数据返回了True或None
            # 实例: 如果drets=[True, False, None]，d0ret=True表示至少有一个数据有新bar
            d0ret = any((dret for dret in drets))  # 检查是否有True返回
            if not d0ret and any((dret is None for dret in drets)):  # 如果没有True但有None
                d0ret = None  # 设置d0ret为None

            if d0ret:  # 如果有数据返回True
                dts = []  # 创建日期时间列表
                for i, ret in enumerate(drets):  # 遍历返回值
                    # 实例: 如果drets=[True, False, None]，dts会包含[data0时间, None, None]
                    dts.append(datas[i].datetime[0] if ret else None)  # 添加日期时间或None

                # 获取最小日期时间的索引
                # 实例: 如果data0时间是10:00，data1时间是09:00，dt0将是09:00
                if onlyresample or noresample:  # 如果只有重采样或没有重采样
                    dt0 = min((d for d in dts if d is not None))  # 获取最小日期时间
                else:  # 包含重采样的混合数据情况
                    dt0 = min((d for i, d in enumerate(dts)  # 获取非仅重采样的最小日期时间
                               if d is not None and i not in rsonly))

                # 例如: 如果dt0=09:00来自data1，dmaster=data1
                dmaster = datas[dts.index(dt0)]  # 获取时间主数据
                self._dtmaster = dmaster.num2date(dt0)  # 转换为日期时间
                self._udtmaster = num2date(dt0)  # 转换为用户日期时间

                # slen = len(runstrats[0])  # 注释掉的代码
                # 尝试为那些没有返回的数据获取数据
                for i, ret in enumerate(drets):  # 遍历返回值
                    if ret:  # 如果已经有有效值
                        continue  # 跳过处理

                    # 实例: 如果data0没有09:00的数据，尝试强制检查并获取，
                    # 如果data0是日K线而dmaster是小时K线，它可能不会有这个时间点
                    d = datas[i]  # 获取数据对象
                    d._check(forcedata=dmaster)  # 强制检查
                    if d.next(datamaster=dmaster, ticks=False):  # 尝试再次调用next
                        dts[i] = d.datetime[0]  # 存储日期时间
                    else:
                        pass  # 不做任何处理

                # 确保只有dmaster级别的数据最终交付
                # 实例: 假设dt0=09:00，data0有09:30的数据，data1有09:00的数据
                # data0的数据会被回退，因为它时间大于主时间
                for i, dti in enumerate(dts):  # 遍历日期时间
                    if dti is not None:  # 如果有日期时间
                        di = datas[i]  # 获取数据对象
                        rpi = False and di.replaying   # 检查行为的标志
                        if dti > dt0:  # 如果日期时间大于主时间
                            if not rpi:  # 如果不是重放
                                di.rewind()  # 回退，无法交付
                        elif not di.replaying:  # 如果不是重放
                            # 重放强制tick填充，否则在这里强制
                            # 例如: 确保所有OHLCV值都已正确填充
                            di._tick_fill(force=True)  # 强制tick填充

            elif d0ret is None:  # 如果没有True但有None
                # 用于实时馈送可能不会立即产生柱状图
                # 实例: 实时交易中等待新数据但尚未收到，d0ret=None
                for data in datas:  # 遍历所有数据
                    data._check()  # 检查数据
            else:  # 如果没有True也没有None
                # 实例: 回测结束时，数据源没有更多数据，但可能还有过滤器需要处理
                lastret = data0._last()  # 调用data0的_last方法
                for data in datas1:  # 遍历其他数据
                    lastret += data._last(datamaster=data0)  # 调用_last方法并累加结果

                if not lastret:  # 如果没有由"lasts"改变
                    # 只有当"lasts"改变了某些内容时才进行额外回合
                    break  # 退出循环

            # 数据可能在next后生成新通知
            # 例如: 在实时交易中，数据源可能在获取新数据后通知连接状态变化
            self._datanotify()  # 处理数据通知
            if self._event_stop:  # 如果请求停止
                return

            if d0ret or lastret:  # 如果由数据或过滤器产生了柱状图
                # 实例: 在开盘前(cheat=True)检查是否有定时器需要触发
                self._check_timers(runstrats, dt0, cheat=True)  # 检查作弊定时器
                if self.p.cheat_on_open:  # 如果开启了开盘作弊
                    # 实例: 运行策略的next_open方法，可以在开盘价基础上创建订单
                    # 例如在09:00时刻，策略可以根据09:00的开盘价决定下单
                    for strat in runstrats:  # 遍历所有策略
                        strat._next_open()  # 调用策略的_next_open方法
                        if self._event_stop:  # 如果请求停止
                            return

            # 实例: 经纪人通知策略订单已执行，如"买入100股AAPL，成交价150.5"
            self._brokernotify()  # 处理经纪人通知
            if self._event_stop:  # 如果请求停止
                return

            if d0ret or lastret:  # 如果由数据或过滤器产生了柱状图
                # 实例: 在正常时间(cheat=False)检查是否有定时器需要触发
                self._check_timers(runstrats, dt0, cheat=False)  # 检查常规定时器
                # 实例: 策略在09:00时间点执行next，根据指标计算结果可能产生新订单
                for strat in runstrats:  # 遍历所有策略
                    strat._next()  # 调用策略的_next方法
                    if self._event_stop:  # 如果请求停止
                        return

                    self._next_writers(runstrats)  # 通知写入器

        # 停止前的最后通知机会
        self._datanotify()  # 处理数据通知
        if self._event_stop:  # 如果请求停止
            return
        self._storenotify()  # 处理商店通知
        if self._event_stop:  # 如果请求停止
            return

    def _runonce(self, runstrats):
        '''
        运行的实际实现，使用向量模式
        
        策略仍以伪事件模式调用，即每个数据到达时调用next
        这是新版实现，优化了向量化计算
        
        数据流实例:
        假设有日K线数据(250天):
        1. 在_once阶段，所有指标一次性计算完所有250天数据(向量化)
        2. 然后进入遍历循环，找出所有数据中最早的时间点
        3. 对应这个时间点的数据点被推进(advance)
        4. 策略的_oncepost方法被调用，处理这个时间点的逻辑
        5. 然后找下一个最早时间点，重复直到所有数据处理完
        
        性能对比实例:
        - 传统next模式: 每个数据点调用一次指标计算函数(250次)
        - runonce模式: 指标计算函数只调用1次，处理全部250天数据
        - 结果: runonce模式计算速度可能提高10-100倍
        '''
        for strat in runstrats:  # 遍历所有策略
            # 实例: 策略的_once方法会一次性初始化所有指标，如SMA(data.close, 20)会计算全部数据
            strat._once()  # 调用策略的_once方法
            strat.reset()  # 重置策略线 - 由next调用next

        # 策略的默认_once方法不做任何事情
        # 因此没有向前移动all datas/indicators/observers
        # 在调用_once之前已经安置好了，因此这里不需要
        # 因为指针在0位置
        datas = sorted(self.datas,  # 按时间框架和压缩排序数据
                       key=lambda x: (x._timeframe, x._compression))

        while True:  # 持续循环直到没有数据
            # 检查数据中的下一个日期
            # 实例: data0可能下一个是2023-01-02，data1是2023-01-01，返回的dts会包含这两个日期
            dts = [d.advance_peek() for d in datas]  # 获取所有数据的下一个日期
            dt0 = min(dts)  # 获取最小日期
            if dt0 == float('inf'):  # 如果没有更多数据
                break  # 退出循环

            # 如果需要时间主
            # dmaster = datas[dts.index(dt0)]  # 注释掉的代码
            slen = len(runstrats[0])  # 获取策略长度
            for i, dti in enumerate(dts):  # 遍历日期
                if dti <= dt0:  # 如果日期小于等于最小日期
                    # 实例: 如果dt0=2023-01-01，data0的下一个日期是2023-01-01，这里会推进data0
                    datas[i].advance()  # 推进数据
                else:
                    # 实例: 如果dt0=2023-01-01，data1的下一个日期是2023-01-02，这里不推进data1
                    pass  # 不做处理

            # 实例: 在开盘前(cheat=True)检查是否有定时器需要触发
            self._check_timers(runstrats, dt0, cheat=True)  # 检查作弊定时器

            if self.p.cheat_on_open:  # 如果开启了开盘作弊
                # 实例: 策略可以在开盘价确定后立即执行订单，而不是等到当前bar结束
                # 例如基于2023-01-01开盘价生成订单，而不是等到收盘
                for strat in runstrats:  # 遍历所有策略
                    strat._oncepost_open()  # 调用策略的_oncepost_open方法
                    if self._event_stop:  # 如果请求停止
                        return

            # 实例: 处理经纪人通知，如订单成交或订单拒绝
            self._brokernotify()  # 处理经纪人通知
            if self._event_stop:  # 如果请求停止
                return

            # 实例: 在正常时间(cheat=False)检查是否有定时器需要触发
            self._check_timers(runstrats, dt0, cheat=False)  # 检查常规定时器

            # 实例: 策略处理时间点dt0(如2023-01-01)的所有逻辑，包括计算指标、生成信号和下单
            for strat in runstrats:  # 遍历所有策略
                strat._oncepost(dt0)  # 调用策略的_oncepost方法，dt0是当前处理的时间
                if self._event_stop:  # 如果请求停止
                    return

                self._next_writers(runstrats)  # 通知写入器

    def _check_timers(self, runstrats, dt0, cheat=False):
        """
        检查定时器是否需要触发
        
        参数:
            runstrats: 运行的策略列表
            dt0: 当前日期时间
            cheat: 是否是作弊模式(在broker前运行)
        """
        timers = self._timers if not cheat else self._timerscheat  # 选择定时器列表
        for t in timers:  # 遍历定时器
            if not t.check(dt0):  # 检查定时器是否需要触发
                continue  # 不需要触发，继续下一个

            # 通知定时器所有者
            t.params.owner.notify_timer(t, t.lastwhen, *t.args, **t.kwargs)

            if t.params.strats:  # 如果需要通知策略
                for strat in runstrats:  # 遍历所有策略
                    strat.notify_timer(t, t.lastwhen, *t.args, **t.kwargs)  # 通知策略
