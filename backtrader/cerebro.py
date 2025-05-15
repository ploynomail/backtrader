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

        在大多数情况下，评估策略性能只需要*分析器*和使用哪些*参数*。如果需要详细
        分析（例如）*指标*的生成值，请关闭此选项

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
        （参见实时数据中的``qcheck``）

        设置为``False``以保持兼容性。将来可能更改为``True``

        实际使用案例：
        
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
    params = (
        ('preload', True),
        ('runonce', True),
        ('maxcpus', None),
        ('stdstats', True),
        ('oldbuysell', False),
        ('oldtrades', False),
        ('lookahead', 0),
        ('exactbars', False),
        ('optdatas', True),
        ('optreturn', True),
        ('objcache', False),
        ('live', False),
        ('writer', False),
        ('tradehistory', False),
        ('oldsync', False),
        ('tz', None),
        ('cheat_on_open', False),
        ('broker_coo', True),
        ('quicknotify', False),
    )

    def __init__(self):
        self._dolive = False
        self._doreplay = False
        self._dooptimize = False
        self.stores = list()
        self.feeds = list()
        self.datas = list()
        self.datasbyname = collections.OrderedDict()
        self.strats = list()
        self.optcbs = list()  # holds a list of callbacks for opt strategies
        self.observers = list()
        self.analyzers = list()
        self.indicators = list()
        self.sizers = dict()
        self.writers = list()
        self.storecbs = list()
        self.datacbs = list()
        self.signals = list()
        self._signal_strat = (None, None, None)
        self._signal_concurrent = False
        self._signal_accumulate = False

        self._dataid = itertools.count(1)

        self._broker = BackBroker()
        self._broker.cerebro = self

        self._tradingcal = None  # TradingCalendar()

        self._pretimers = list()
        self._ohistory = list()
        self._fhistory = None

    @staticmethod
    def iterize(iterable):
        '''Handy function which turns things into things that can be iterated upon
        including iterables
        '''
        niterable = list()
        for elem in iterable:
            if isinstance(elem, string_types):
                elem = (elem,)
            elif not isinstance(elem, collectionsAbc.Iterable):  # Different functions will be called for different Python versions
                elem = (elem,)

            niterable.append(elem)

        return niterable

    def set_fund_history(self, fund):
        '''
        Add a history of orders to be directly executed in the broker for
        performance evaluation

          - ``fund``: is an iterable (ex: list, tuple, iterator, generator)
            in which each element will be also an iterable (with length) with
            the following sub-elements (2 formats are possible)

            ``[datetime, share_value, net asset value]``

            **Note**: it must be sorted (or produce sorted elements) by
              datetime ascending

            where:

              - ``datetime`` is a python ``date/datetime`` instance or a string
                with format YYYY-MM-DD[THH:MM:SS[.us]] where the elements in
                brackets are optional
              - ``share_value`` is an float/integer
              - ``net_asset_value`` is a float/integer
        '''
        self._fhistory = fund

    def add_order_history(self, orders, notify=True):
        '''
        Add a history of orders to be directly executed in the broker for
        performance evaluation

          - ``orders``: is an iterable (ex: list, tuple, iterator, generator)
            in which each element will be also an iterable (with length) with
            the following sub-elements (2 formats are possible)

            ``[datetime, size, price]`` or ``[datetime, size, price, data]``

            **Note**: it must be sorted (or produce sorted elements) by
              datetime ascending

            where:

              - ``datetime`` is a python ``date/datetime`` instance or a string
                with format YYYY-MM-DD[THH:MM:SS[.us]] where the elements in
                brackets are optional
              - ``size`` is an integer (positive to *buy*, negative to *sell*)
              - ``price`` is a float/integer
              - ``data`` if present can take any of the following values

                - *None* - The 1st data feed will be used as target
                - *integer* - The data with that index (insertion order in
                  **Cerebro**) will be used
                - *string* - a data with that name, assigned for example with
                  ``cerebro.addata(data, name=value)``, will be the target

          - ``notify`` (default: *True*)

            If ``True`` the 1st strategy inserted in the system will be
            notified of the artificial orders created following the information
            from each order in ``orders``

        **Note**: Implicit in the description is the need to add a data feed
          which is the target of the orders. This is for example needed by
          analyzers which track for example the returns
        '''
        self._ohistory.append((orders, notify))

    def notify_timer(self, timer, when, *args, **kwargs):
        '''Receives a timer notification where ``timer`` is the timer which was
        returned by ``add_timer``, and ``when`` is the calling time. ``args``
        and ``kwargs`` are any additional arguments passed to ``add_timer``

        The actual ``when`` time can be later, but the system may have not be
        able to call the timer before. This value is the timer value and no the
        system time.
        '''
        pass

    def _add_timer(self, owner, when,
                   offset=datetime.timedelta(), repeat=datetime.timedelta(),
                   weekdays=[], weekcarry=False,
                   monthdays=[], monthcarry=True,
                   allow=None,
                   tzdata=None, strats=False, cheat=False,
                   *args, **kwargs):
        '''Internal method to really create the timer (not started yet) which
        can be called by cerebro instances or other objects which can access
        cerebro'''

        timer = Timer(
            tid=len(self._pretimers),
            owner=owner, strats=strats,
            when=when, offset=offset, repeat=repeat,
            weekdays=weekdays, weekcarry=weekcarry,
            monthdays=monthdays, monthcarry=monthcarry,
            allow=allow,
            tzdata=tzdata, cheat=cheat,
            *args, **kwargs
        )

        self._pretimers.append(timer)
        return timer

    def add_timer(self, when,
                  offset=datetime.timedelta(), repeat=datetime.timedelta(),
                  weekdays=[], weekcarry=False,
                  monthdays=[], monthcarry=True,
                  allow=None,
                  tzdata=None, strats=False, cheat=False,
                  *args, **kwargs):
        '''
        Schedules a timer to invoke ``notify_timer``

        Arguments:

          - ``when``: can be

            - ``datetime.time`` instance (see below ``tzdata``)
            - ``bt.timer.SESSION_START`` to reference a session start
            - ``bt.timer.SESSION_END`` to reference a session end

         - ``offset`` which must be a ``datetime.timedelta`` instance

           Used to offset the value ``when``. It has a meaningful use in
           combination with ``SESSION_START`` and ``SESSION_END``, to indicated
           things like a timer being called ``15 minutes`` after the session
           start.

          - ``repeat`` which must be a ``datetime.timedelta`` instance

            Indicates if after a 1st call, further calls will be scheduled
            within the same session at the scheduled ``repeat`` delta

            Once the timer goes over the end of the session it is reset to the
            original value for ``when``

          - ``weekdays``: a **sorted** iterable with integers indicating on
            which days (iso codes, Monday is 1, Sunday is 7) the timers can
            be actually invoked

            If not specified, the timer will be active on all days

          - ``weekcarry`` (default: ``False``). If ``True`` and the weekday was
            not seen (ex: trading holiday), the timer will be executed on the
            next day (even if in a new week)

          - ``monthdays``: a **sorted** iterable with integers indicating on
            which days of the month a timer has to be executed. For example
            always on day *15* of the month

            If not specified, the timer will be active on all days

          - ``monthcarry`` (default: ``True``). If the day was not seen
            (weekend, trading holiday), the timer will be executed on the next
            available day.

          - ``allow`` (default: ``None``). A callback which receives a
            `datetime.date`` instance and returns ``True`` if the date is
            allowed for timers or else returns ``False``

          - ``tzdata`` which can be either ``None`` (default), a ``pytz``
            instance or a ``data feed`` instance.

            ``None``: ``when`` is interpreted at face value (which translates
            to handling it as if it where UTC even if it's not)

            ``pytz`` instance: ``when`` will be interpreted as being specified
            in the local time specified by the timezone instance.

            ``data feed`` instance: ``when`` will be interpreted as being
            specified in the local time specified by the ``tz`` parameter of
            the data feed instance.

            **Note**: If ``when`` is either ``SESSION_START`` or
              ``SESSION_END`` and ``tzdata`` is ``None``, the 1st *data feed*
              in the system (aka ``self.data0``) will be used as the reference
              to find out the session times.

          - ``strats`` (default: ``False``) call also the ``notify_timer`` of
            strategies

          - ``cheat`` (default ``False``) if ``True`` the timer will be called
            before the broker has a chance to evaluate the orders. This opens
            the chance to issue orders based on opening price for example right
            before the session starts
          - ``*args``: any extra args will be passed to ``notify_timer``

          - ``**kwargs``: any extra kwargs will be passed to ``notify_timer``

        Return Value:

          - The created timer

        '''
        return self._add_timer(
            owner=self, when=when, offset=offset, repeat=repeat,
            weekdays=weekdays, weekcarry=weekcarry,
            monthdays=monthdays, monthcarry=monthcarry,
            allow=allow,
            tzdata=tzdata, strats=strats, cheat=cheat,
            *args, **kwargs)

    def addtz(self, tz):
        '''
        This can also be done with the parameter ``tz``

        Adds a global timezone for strategies. The argument ``tz`` can be

          - ``None``: in this case the datetime displayed by strategies will be
            in UTC, which has been always the standard behavior

          - ``pytz`` instance. It will be used as such to convert UTC times to
            the chosen timezone

          - ``string``. Instantiating a ``pytz`` instance will be attempted.

          - ``integer``. Use, for the strategy, the same timezone as the
            corresponding ``data`` in the ``self.datas`` iterable (``0`` would
            use the timezone from ``data0``)

        '''
        self.p.tz = tz

    def addcalendar(self, cal):
        '''Adds a global trading calendar to the system. Individual data feeds
        may have separate calendars which override the global one

        ``cal`` can be an instance of ``TradingCalendar`` a string or an
        instance of ``pandas_market_calendars``. A string will be will be
        instantiated as a ``PandasMarketCalendar`` (which needs the module
        ``pandas_market_calendar`` installed in the system.

        If a subclass of `TradingCalendarBase` is passed (not an instance) it
        will be instantiated
        '''
        if isinstance(cal, string_types):
            cal = PandasMarketCalendar(calendar=cal)
        elif hasattr(cal, 'valid_days'):
            cal = PandasMarketCalendar(calendar=cal)

        else:
            try:
                if issubclass(cal, TradingCalendarBase):
                    cal = cal()
            except TypeError:  # already an instance
                pass

        self._tradingcal = cal

    def add_signal(self, sigtype, sigcls, *sigargs, **sigkwargs):
        '''Adds a signal to the system which will be later added to a
        ``SignalStrategy``'''
        self.signals.append((sigtype, sigcls, sigargs, sigkwargs))

    def signal_strategy(self, stratcls, *args, **kwargs):
        '''Adds a SignalStrategy subclass which can accept signals'''
        self._signal_strat = (stratcls, args, kwargs)

    def signal_concurrent(self, onoff):
        '''If signals are added to the system and the ``concurrent`` value is
        set to True, concurrent orders will be allowed'''
        self._signal_concurrent = onoff

    def signal_accumulate(self, onoff):
        '''If signals are added to the system and the ``accumulate`` value is
        set to True, entering the market when already in the market, will be
        allowed to increase a position'''
        self._signal_accumulate = onoff

    def addstore(self, store):
        '''Adds an ``Store`` instance to the if not already present'''
        if store not in self.stores:
            self.stores.append(store)

    def addwriter(self, wrtcls, *args, **kwargs):
        '''Adds an ``Writer`` class to the mix. Instantiation will be done at
        ``run`` time in cerebro
        '''
        self.writers.append((wrtcls, args, kwargs))

    def addsizer(self, sizercls, *args, **kwargs):
        '''Adds a ``Sizer`` class (and args) which is the default sizer for any
        strategy added to cerebro
        '''
        self.sizers[None] = (sizercls, args, kwargs)

    def addsizer_byidx(self, idx, sizercls, *args, **kwargs):
        '''Adds a ``Sizer`` class by idx. This idx is a reference compatible to
        the one returned by ``addstrategy``. Only the strategy referenced by
        ``idx`` will receive this size
        '''
        self.sizers[idx] = (sizercls, args, kwargs)

    def addindicator(self, indcls, *args, **kwargs):
        '''
        Adds an ``Indicator`` class to the mix. Instantiation will be done at
        ``run`` time in the passed strategies
        '''
        self.indicators.append((indcls, args, kwargs))

    def addanalyzer(self, ancls, *args, **kwargs):
        '''
        Adds an ``Analyzer`` class to the mix. Instantiation will be done at
        ``run`` time
        '''
        self.analyzers.append((ancls, args, kwargs))

    def addobserver(self, obscls, *args, **kwargs):
        '''
        Adds an ``Observer`` class to the mix. Instantiation will be done at
        ``run`` time
        '''
        self.observers.append((False, obscls, args, kwargs))

    def addobservermulti(self, obscls, *args, **kwargs):
        '''
        Adds an ``Observer`` class to the mix. Instantiation will be done at
        ``run`` time

        It will be added once per "data" in the system. A use case is a
        buy/sell observer which observes individual datas.

        A counter-example is the CashValue, which observes system-wide values
        '''
        self.observers.append((True, obscls, args, kwargs))

    def addstorecb(self, callback):
        '''Adds a callback to get messages which would be handled by the
        notify_store method

        The signature of the callback must support the following:

          - callback(msg, \*args, \*\*kwargs)

        The actual ``msg``, ``*args`` and ``**kwargs`` received are
        implementation defined (depend entirely on the *data/broker/store*) but
        in general one should expect them to be *printable* to allow for
        reception and experimentation.
        '''
        self.storecbs.append(callback)

    def _notify_store(self, msg, *args, **kwargs):
        for callback in self.storecbs:
            callback(msg, *args, **kwargs)

        self.notify_store(msg, *args, **kwargs)

    def notify_store(self, msg, *args, **kwargs):
        '''Receive store notifications in cerebro

        This method can be overridden in ``Cerebro`` subclasses

        The actual ``msg``, ``*args`` and ``**kwargs`` received are
        implementation defined (depend entirely on the *data/broker/store*) but
        in general one should expect them to be *printable* to allow for
        reception and experimentation.
        '''
        pass

    def _storenotify(self):
        for store in self.stores:
            for notif in store.get_notifications():
                msg, args, kwargs = notif

                self._notify_store(msg, *args, **kwargs)
                for strat in self.runningstrats:
                    strat.notify_store(msg, *args, **kwargs)

    def adddatacb(self, callback):
        '''Adds a callback to get messages which would be handled by the
        notify_data method

        The signature of the callback must support the following:

          - callback(data, status, \*args, \*\*kwargs)

        The actual ``*args`` and ``**kwargs`` received are implementation
        defined (depend entirely on the *data/broker/store*) but in general one
        should expect them to be *printable* to allow for reception and
        experimentation.
        '''
        self.datacbs.append(callback)

    def _datanotify(self):
        for data in self.datas:
            for notif in data.get_notifications():
                status, args, kwargs = notif
                self._notify_data(data, status, *args, **kwargs)
                for strat in self.runningstrats:
                    strat.notify_data(data, status, *args, **kwargs)

    def _notify_data(self, data, status, *args, **kwargs):
        for callback in self.datacbs:
            callback(data, status, *args, **kwargs)

        self.notify_data(data, status, *args, **kwargs)

    def notify_data(self, data, status, *args, **kwargs):
        '''Receive data notifications in cerebro

        This method can be overridden in ``Cerebro`` subclasses

        The actual ``*args`` and ``**kwargs`` received are
        implementation defined (depend entirely on the *data/broker/store*) but
        in general one should expect them to be *printable* to allow for
        reception and experimentation.
        '''
        pass

    def adddata(self, data, name=None):
        '''
        Adds a ``Data Feed`` instance to the mix.

        If ``name`` is not None it will be put into ``data._name`` which is
        meant for decoration/plotting purposes.
        '''
        if name is not None:
            data._name = name

        data._id = next(self._dataid)
        data.setenvironment(self)

        self.datas.append(data)
        self.datasbyname[data._name] = data
        feed = data.getfeed()
        if feed and feed not in self.feeds:
            self.feeds.append(feed)

        if data.islive():
            self._dolive = True

        return data

    def chaindata(self, *args, **kwargs):
        '''
        Chains several data feeds into one

        If ``name`` is passed as named argument and is not None it will be put
        into ``data._name`` which is meant for decoration/plotting purposes.

        If ``None``, then the name of the 1st data will be used
        '''
        dname = kwargs.pop('name', None)
        if dname is None:
            dname = args[0]._dataname
        d = bt.feeds.Chainer(dataname=dname, *args)
        self.adddata(d, name=dname)

        return d

    def rolloverdata(self, *args, **kwargs):
        '''Chains several data feeds into one

        If ``name`` is passed as named argument and is not None it will be put
        into ``data._name`` which is meant for decoration/plotting purposes.

        If ``None``, then the name of the 1st data will be used

        Any other kwargs will be passed to the RollOver class

        '''
        dname = kwargs.pop('name', None)
        if dname is None:
            dname = args[0]._dataname
        d = bt.feeds.RollOver(dataname=dname, *args, **kwargs)
        self.adddata(d, name=dname)

        return d

    def replaydata(self, dataname, name=None, **kwargs):
        '''
        Adds a ``Data Feed`` to be replayed by the system

        If ``name`` is not None it will be put into ``data._name`` which is
        meant for decoration/plotting purposes.

        Any other kwargs like ``timeframe``, ``compression``, ``todate`` which
        are supported by the replay filter will be passed transparently
        '''
        if any(dataname is x for x in self.datas):
            dataname = dataname.clone()

        dataname.replay(**kwargs)
        self.adddata(dataname, name=name)
        self._doreplay = True

        return dataname

    def resampledata(self, dataname, name=None, **kwargs):
        '''
        Adds a ``Data Feed`` to be resample by the system

        If ``name`` is not None it will be put into ``data._name`` which is
        meant for decoration/plotting purposes.

        Any other kwargs like ``timeframe``, ``compression``, ``todate`` which
        are supported by the resample filter will be passed transparently
        '''
        if any(dataname is x for x in self.datas):
            dataname = dataname.clone()

        dataname.resample(**kwargs)
        self.adddata(dataname, name=name)
        self._doreplay = True

        return dataname

    def optcallback(self, cb):
        '''
        Adds a *callback* to the list of callbacks that will be called with the
        optimizations when each of the strategies has been run

        The signature: cb(strategy)
        '''
        self.optcbs.append(cb)

    def optstrategy(self, strategy, *args, **kwargs):
        '''
        Adds a ``Strategy`` class to the mix for optimization. Instantiation
        will happen during ``run`` time.

        args and kwargs MUST BE iterables which hold the values to check.

        Example: if a Strategy accepts a parameter ``period``, for optimization
        purposes the call to ``optstrategy`` looks like:

          - cerebro.optstrategy(MyStrategy, period=(15, 25))

        This will execute an optimization for values 15 and 25. Whereas

          - cerebro.optstrategy(MyStrategy, period=range(15, 25))

        will execute MyStrategy with ``period`` values 15 -> 25 (25 not
        included, because ranges are semi-open in Python)

        If a parameter is passed but shall not be optimized the call looks
        like:

          - cerebro.optstrategy(MyStrategy, period=(15,))

        Notice that ``period`` is still passed as an iterable ... of just 1
        element

        ``backtrader`` will anyhow try to identify situations like:

          - cerebro.optstrategy(MyStrategy, period=15)

        and will create an internal pseudo-iterable if possible
        '''
        self._dooptimize = True
        args = self.iterize(args)
        optargs = itertools.product(*args)

        optkeys = list(kwargs)

        vals = self.iterize(kwargs.values())
        optvals = itertools.product(*vals)

        okwargs1 = map(zip, itertools.repeat(optkeys), optvals)

        optkwargs = map(dict, okwargs1)

        it = itertools.product([strategy], optargs, optkwargs)
        self.strats.append(it)

    def addstrategy(self, strategy, *args, **kwargs):
        '''
        Adds a ``Strategy`` class to the mix for a single pass run.
        Instantiation will happen during ``run`` time.

        args and kwargs will be passed to the strategy as they are during
        instantiation.

        Returns the index with which addition of other objects (like sizers)
        can be referenced
        '''
        self.strats.append([(strategy, args, kwargs)])
        return len(self.strats) - 1

    def setbroker(self, broker):
        '''
        Sets a specific ``broker`` instance for this strategy, replacing the
        one inherited from cerebro.
        '''
        self._broker = broker
        broker.cerebro = self
        return broker

    def getbroker(self):
        '''
        Returns the broker instance.

        This is also available as a ``property`` by the name ``broker``
        '''
        return self._broker

    broker = property(getbroker, setbroker)

    def plot(self, plotter=None, numfigs=1, iplot=True, start=None, end=None,
             width=16, height=9, dpi=300, tight=True, use=None,
             **kwargs):
        '''
        Plots the strategies inside cerebro

        If ``plotter`` is None a default ``Plot`` instance is created and
        ``kwargs`` are passed to it during instantiation.

        ``numfigs`` split the plot in the indicated number of charts reducing
        chart density if wished

        ``iplot``: if ``True`` and running in a ``notebook`` the charts will be
        displayed inline

        ``use``: set it to the name of the desired matplotlib backend. It will
        take precedence over ``iplot``

        ``start``: An index to the datetime line array of the strategy or a
        ``datetime.date``, ``datetime.datetime`` instance indicating the start
        of the plot

        ``end``: An index to the datetime line array of the strategy or a
        ``datetime.date``, ``datetime.datetime`` instance indicating the end
        of the plot

        ``width``: in inches of the saved figure

        ``height``: in inches of the saved figure

        ``dpi``: quality in dots per inches of the saved figure

        ``tight``: only save actual content and not the frame of the figure
        '''
        if self._exactbars > 0:
            return

        if not plotter:
            from . import plot
            if self.p.oldsync:
                plotter = plot.Plot_OldSync(**kwargs)
            else:
                plotter = plot.Plot(**kwargs)

        # pfillers = {self.datas[i]: self._plotfillers[i]
        # for i, x in enumerate(self._plotfillers)}

        # pfillers2 = {self.datas[i]: self._plotfillers2[i]
        # for i, x in enumerate(self._plotfillers2)}

        figs = []
        for stratlist in self.runstrats:
            for si, strat in enumerate(stratlist):
                rfig = plotter.plot(strat, figid=si * 100,
                                    numfigs=numfigs, iplot=iplot,
                                    start=start, end=end, use=use)
                # pfillers=pfillers2)

                figs.append(rfig)

            plotter.show()

        return figs

    def __call__(self, iterstrat):
        '''
        Used during optimization to pass the cerebro over the multiprocesing
        module without complains
        '''

        predata = self.p.optdatas and self._dopreload and self._dorunonce
        return self.runstrategies(iterstrat, predata=predata)

    def __getstate__(self):
        '''
        Used during optimization to prevent optimization result `runstrats`
        from being pickled to subprocesses
        '''

        rv = vars(self).copy()
        if 'runstrats' in rv:
            del(rv['runstrats'])
        return rv

    def runstop(self):
        '''If invoked from inside a strategy or anywhere else, including other
        threads the execution will stop as soon as possible.'''
        self._event_stop = True  # signal a stop has been requested

    def run(self, **kwargs):
        '''The core method to perform backtesting. Any ``kwargs`` passed to it
        will affect the value of the standard parameters ``Cerebro`` was
        instantiated with.

        If ``cerebro`` has not datas the method will immediately bail out.

        It has different return values:

          - For No Optimization: a list contanining instances of the Strategy
            classes added with ``addstrategy``

          - For Optimization: a list of lists which contain instances of the
            Strategy classes added with ``addstrategy``
        '''
        self._event_stop = False  # Stop is requested

        if not self.datas:
            return []  # nothing can be run

        pkeys = self.params._getkeys()
        for key, val in kwargs.items():
            if key in pkeys:
                setattr(self.params, key, val)

        # Manage activate/deactivate object cache
        linebuffer.LineActions.cleancache()  # clean cache
        indicator.Indicator.cleancache()  # clean cache

        linebuffer.LineActions.usecache(self.p.objcache)
        indicator.Indicator.usecache(self.p.objcache)

        self._dorunonce = self.p.runonce
        self._dopreload = self.p.preload
        self._exactbars = int(self.p.exactbars)

        if self._exactbars:
            self._dorunonce = False  # something is saving memory, no runonce
            self._dopreload = self._dopreload and self._exactbars < 1

        self._doreplay = self._doreplay or any(x.replaying for x in self.datas)
        if self._doreplay:
            # preloading is not supported with replay. full timeframe bars
            # are constructed in realtime
            self._dopreload = False

        if self._dolive or self.p.live:
            # in this case both preload and runonce must be off
            self._dorunonce = False
            self._dopreload = False

        self.runwriters = list()

        # Add the system default writer if requested
        if self.p.writer is True:
            wr = WriterFile()
            self.runwriters.append(wr)

        # Instantiate any other writers
        for wrcls, wrargs, wrkwargs in self.writers:
            wr = wrcls(*wrargs, **wrkwargs)
            self.runwriters.append(wr)

        # Write down if any writer wants the full csv output
        self.writers_csv = any(map(lambda x: x.p.csv, self.runwriters))

        self.runstrats = list()

        if self.signals:  # allow processing of signals
            signalst, sargs, skwargs = self._signal_strat
            if signalst is None:
                # Try to see if the 1st regular strategy is a signal strategy
                try:
                    signalst, sargs, skwargs = self.strats.pop(0)
                except IndexError:
                    pass  # Nothing there
                else:
                    if not isinstance(signalst, SignalStrategy):
                        # no signal ... reinsert at the beginning
                        self.strats.insert(0, (signalst, sargs, skwargs))
                        signalst = None  # flag as not presetn

            if signalst is None:  # recheck
                # Still None, create a default one
                signalst, sargs, skwargs = SignalStrategy, tuple(), dict()

            # Add the signal strategy
            self.addstrategy(signalst,
                             _accumulate=self._signal_accumulate,
                             _concurrent=self._signal_concurrent,
                             signals=self.signals,
                             *sargs,
                             **skwargs)

        if not self.strats:  # Datas are present, add a strategy
            self.addstrategy(Strategy)

        iterstrats = itertools.product(*self.strats)
        if not self._dooptimize or self.p.maxcpus == 1:
            # If no optimmization is wished ... or 1 core is to be used
            # let's skip process "spawning"
            for iterstrat in iterstrats:
                runstrat = self.runstrategies(iterstrat)
                self.runstrats.append(runstrat)
                if self._dooptimize:
                    for cb in self.optcbs:
                        cb(runstrat)  # callback receives finished strategy
        else:
            if self.p.optdatas and self._dopreload and self._dorunonce:
                for data in self.datas:
                    data.reset()
                    if self._exactbars < 1:  # datas can be full length
                        data.extend(size=self.params.lookahead)
                    data._start()
                    if self._dopreload:
                        data.preload()

            pool = multiprocessing.Pool(self.p.maxcpus or None)
            for r in pool.imap(self, iterstrats):
                self.runstrats.append(r)
                for cb in self.optcbs:
                    cb(r)  # callback receives finished strategy

            pool.close()

            if self.p.optdatas and self._dopreload and self._dorunonce:
                for data in self.datas:
                    data.stop()

        if not self._dooptimize:
            # avoid a list of list for regular cases
            return self.runstrats[0]

        return self.runstrats

    def _init_stcount(self):
        self.stcount = itertools.count(0)

    def _next_stid(self):
        return next(self.stcount)

    def runstrategies(self, iterstrat, predata=False):
        '''
        Internal method invoked by ``run``` to run a set of strategies
        '''
        self._init_stcount()

        self.runningstrats = runstrats = list()
        for store in self.stores:
            store.start()

        if self.p.cheat_on_open and self.p.broker_coo:
            # try to activate in broker
            if hasattr(self._broker, 'set_coo'):
                self._broker.set_coo(True)

        if self._fhistory is not None:
            self._broker.set_fund_history(self._fhistory)

        for orders, onotify in self._ohistory:
            self._broker.add_order_history(orders, onotify)

        self._broker.start()

        for feed in self.feeds:
            feed.start()

        if self.writers_csv:
            wheaders = list()
            for data in self.datas:
                if data.csv:
                    wheaders.extend(data.getwriterheaders())

            for writer in self.runwriters:
                if writer.p.csv:
                    writer.addheaders(wheaders)

        # self._plotfillers = [list() for d in self.datas]
        # self._plotfillers2 = [list() for d in self.datas]

        if not predata:
            for data in self.datas:
                data.reset()
                if self._exactbars < 1:  # datas can be full length
                    data.extend(size=self.params.lookahead)
                data._start()
                if self._dopreload:
                    data.preload()

        for stratcls, sargs, skwargs in iterstrat:
            sargs = self.datas + list(sargs)
            try:
                strat = stratcls(*sargs, **skwargs)
            except bt.errors.StrategySkipError:
                continue  # do not add strategy to the mix

            if self.p.oldsync:
                strat._oldsync = True  # tell strategy to use old clock update
            if self.p.tradehistory:
                strat.set_tradehistory()
            runstrats.append(strat)

        tz = self.p.tz
        if isinstance(tz, integer_types):
            tz = self.datas[tz]._tz
        else:
            tz = tzparse(tz)

        if runstrats:
            # loop separated for clarity
            defaultsizer = self.sizers.get(None, (None, None, None))
            for idx, strat in enumerate(runstrats):
                if self.p.stdstats:
                    strat._addobserver(False, observers.Broker)
                    if self.p.oldbuysell:
                        strat._addobserver(True, observers.BuySell)
                    else:
                        strat._addobserver(True, observers.BuySell,
                                           barplot=True)

                    if self.p.oldtrades or len(self.datas) == 1:
                        strat._addobserver(False, observers.Trades)
                    else:
                        strat._addobserver(False, observers.DataTrades)

                for multi, obscls, obsargs, obskwargs in self.observers:
                    strat._addobserver(multi, obscls, *obsargs, **obskwargs)

                for indcls, indargs, indkwargs in self.indicators:
                    strat._addindicator(indcls, *indargs, **indkwargs)

                for ancls, anargs, ankwargs in self.analyzers:
                    strat._addanalyzer(ancls, *anargs, **ankwargs)

                sizer, sargs, skwargs = self.sizers.get(idx, defaultsizer)
                if sizer is not None:
                    strat._addsizer(sizer, *sargs, **skwargs)

                strat._settz(tz)
                strat._start()

                for writer in self.runwriters:
                    if writer.p.csv:
                        writer.addheaders(strat.getwriterheaders())

            if not predata:
                for strat in runstrats:
                    strat.qbuffer(self._exactbars, replaying=self._doreplay)

            for writer in self.runwriters:
                writer.start()

            # Prepare timers
            self._timers = []
            self._timerscheat = []
            for timer in self._pretimers:
                # preprocess tzdata if needed
                timer.start(self.datas[0])

                if timer.params.cheat:
                    self._timerscheat.append(timer)
                else:
                    self._timers.append(timer)

            if self._dopreload and self._dorunonce:
                if self.p.oldsync:
                    self._runonce_old(runstrats)
                else:
                    self._runonce(runstrats)
            else:
                if self.p.oldsync:
                    self._runnext_old(runstrats)
                else:
                    self._runnext(runstrats)

            for strat in runstrats:
                strat._stop()

        self._broker.stop()

        if not predata:
            for data in self.datas:
                data.stop()

        for feed in self.feeds:
            feed.stop()

        for store in self.stores:
            store.stop()

        self.stop_writers(runstrats)

        if self._dooptimize and self.p.optreturn:
            # Results can be optimized
            results = list()
            for strat in runstrats:
                for a in strat.analyzers:
                    a.strategy = None
                    a._parent = None
                    for attrname in dir(a):
                        if attrname.startswith('data'):
                            setattr(a, attrname, None)

                oreturn = OptReturn(strat.params, analyzers=strat.analyzers, strategycls=type(strat))
                results.append(oreturn)

            return results

        return runstrats

    def stop_writers(self, runstrats):
        cerebroinfo = OrderedDict()
        datainfos = OrderedDict()

        for i, data in enumerate(self.datas):
            datainfos['Data%d' % i] = data.getwriterinfo()

        cerebroinfo['Datas'] = datainfos

        stratinfos = dict()
        for strat in runstrats:
            stname = strat.__class__.__name__
            stratinfos[stname] = strat.getwriterinfo()

        cerebroinfo['Strategies'] = stratinfos

        for writer in self.runwriters:
            writer.writedict(dict(Cerebro=cerebroinfo))
            writer.stop()

    def _brokernotify(self):
        '''
        Internal method which kicks the broker and delivers any broker
        notification to the strategy
        '''
        self._broker.next()
        while True:
            order = self._broker.get_notification()
            if order is None:
                break

            owner = order.owner
            if owner is None:
                owner = self.runningstrats[0]  # default

            owner._addnotification(order, quicknotify=self.p.quicknotify)

    def _runnext_old(self, runstrats):
        '''
        Actual implementation of run in full next mode. All objects have its
        ``next`` method invoke on each data arrival
        '''
        data0 = self.datas[0]
        d0ret = True
        while d0ret or d0ret is None:
            lastret = False
            # Notify anything from the store even before moving datas
            # because datas may not move due to an error reported by the store
            self._storenotify()
            if self._event_stop:  # stop if requested
                return
            self._datanotify()
            if self._event_stop:  # stop if requested
                return

            d0ret = data0.next()
            if d0ret:
                for data in self.datas[1:]:
                    if not data.next(datamaster=data0):  # no delivery
                        data._check(forcedata=data0)  # check forcing output
                        data.next(datamaster=data0)  # retry

            elif d0ret is None:
                # meant for things like live feeds which may not produce a bar
                # at the moment but need the loop to run for notifications and
                # getting resample and others to produce timely bars
                data0._check()
                for data in self.datas[1:]:
                    data._check()
            else:
                lastret = data0._last()
                for data in self.datas[1:]:
                    lastret += data._last(datamaster=data0)

                if not lastret:
                    # Only go extra round if something was changed by "lasts"
                    break

            # Datas may have generated a new notification after next
            self._datanotify()
            if self._event_stop:  # stop if requested
                return

            self._brokernotify()
            if self._event_stop:  # stop if requested
                return

            if d0ret or lastret:  # bars produced by data or filters
                for strat in runstrats:
                    strat._next()
                    if self._event_stop:  # stop if requested
                        return

                    self._next_writers(runstrats)

        # Last notification chance before stopping
        self._datanotify()
        if self._event_stop:  # stop if requested
            return
        self._storenotify()
        if self._event_stop:  # stop if requested
            return

    def _runonce_old(self, runstrats):
        '''
        Actual implementation of run in vector mode.
        Strategies are still invoked on a pseudo-event mode in which ``next``
        is called for each data arrival
        '''
        for strat in runstrats:
            strat._once()

        # The default once for strategies does nothing and therefore
        # has not moved forward all datas/indicators/observers that
        # were homed before calling once, Hence no "need" to do it
        # here again, because pointers are at 0
        data0 = self.datas[0]
        datas = self.datas[1:]
        for i in range(data0.buflen()):
            data0.advance()
            for data in datas:
                data.advance(datamaster=data0)

            self._brokernotify()
            if self._event_stop:  # stop if requested
                return

            for strat in runstrats:
                # data0.datetime[0] for compat. w/ new strategy's oncepost
                strat._oncepost(data0.datetime[0])
                if self._event_stop:  # stop if requested
                    return

                self._next_writers(runstrats)

    def _next_writers(self, runstrats):
        if not self.runwriters:
            return

        if self.writers_csv:
            wvalues = list()
            for data in self.datas:
                if data.csv:
                    wvalues.extend(data.getwritervalues())

            for strat in runstrats:
                wvalues.extend(strat.getwritervalues())

            for writer in self.runwriters:
                if writer.p.csv:
                    writer.addvalues(wvalues)

                    writer.next()

    def _disable_runonce(self):
        '''API for lineiterators to disable runonce (see HeikinAshi)'''
        self._dorunonce = False

    def _runnext(self, runstrats):
        '''
        Actual implementation of run in full next mode. All objects have its
        ``next`` method invoke on each data arrival
        '''
        datas = sorted(self.datas,
                       key=lambda x: (x._timeframe, x._compression))
        datas1 = datas[1:]
        data0 = datas[0]
        d0ret = True

        rs = [i for i, x in enumerate(datas) if x.resampling]
        rp = [i for i, x in enumerate(datas) if x.replaying]
        rsonly = [i for i, x in enumerate(datas)
                  if x.resampling and not x.replaying]
        onlyresample = len(datas) == len(rsonly)
        noresample = not rsonly

        clonecount = sum(d._clone for d in datas)
        ldatas = len(datas)
        ldatas_noclones = ldatas - clonecount
        lastqcheck = False
        dt0 = date2num(datetime.datetime.max) - 2  # default at max
        while d0ret or d0ret is None:
            # if any has live data in the buffer, no data will wait anything
            newqcheck = not any(d.haslivedata() for d in datas)
            if not newqcheck:
                # If no data has reached the live status or all, wait for
                # the next incoming data
                livecount = sum(d._laststatus == d.LIVE for d in datas)
                newqcheck = not livecount or livecount == ldatas_noclones

            lastret = False
            # Notify anything from the store even before moving datas
            # because datas may not move due to an error reported by the store
            self._storenotify()
            if self._event_stop:  # stop if requested
                return
            self._datanotify()
            if self._event_stop:  # stop if requested
                return

            # record starting time and tell feeds to discount the elapsed time
            # from the qcheck value
            drets = []
            qstart = datetime.datetime.utcnow()
            for d in datas:
                qlapse = datetime.datetime.utcnow() - qstart
                d.do_qcheck(newqcheck, qlapse.total_seconds())
                drets.append(d.next(ticks=False))

            d0ret = any((dret for dret in drets))
            if not d0ret and any((dret is None for dret in drets)):
                d0ret = None

            if d0ret:
                dts = []
                for i, ret in enumerate(drets):
                    dts.append(datas[i].datetime[0] if ret else None)

                # Get index to minimum datetime
                if onlyresample or noresample:
                    dt0 = min((d for d in dts if d is not None))
                else:
                    dt0 = min((d for i, d in enumerate(dts)
                               if d is not None and i not in rsonly))

                dmaster = datas[dts.index(dt0)]  # and timemaster
                self._dtmaster = dmaster.num2date(dt0)
                self._udtmaster = num2date(dt0)

                # slen = len(runstrats[0])
                # Try to get something for those that didn't return
                for i, ret in enumerate(drets):
                    if ret:  # dts already contains a valid datetime for this i
                        continue

                    # try to get a data by checking with a master
                    d = datas[i]
                    d._check(forcedata=dmaster)  # check to force output
                    if d.next(datamaster=dmaster, ticks=False):  # retry
                        dts[i] = d.datetime[0]  # good -> store
                        # self._plotfillers2[i].append(slen)  # mark as fill
                    else:
                        # self._plotfillers[i].append(slen)  # mark as empty
                        pass

                # make sure only those at dmaster level end up delivering
                for i, dti in enumerate(dts):
                    if dti is not None:
                        di = datas[i]
                        rpi = False and di.replaying   # to check behavior
                        if dti > dt0:
                            if not rpi:  # must see all ticks ...
                                di.rewind()  # cannot deliver yet
                            # self._plotfillers[i].append(slen)
                        elif not di.replaying:
                            # Replay forces tick fill, else force here
                            di._tick_fill(force=True)

                        # self._plotfillers2[i].append(slen)  # mark as fill

            elif d0ret is None:
                # meant for things like live feeds which may not produce a bar
                # at the moment but need the loop to run for notifications and
                # getting resample and others to produce timely bars
                for data in datas:
                    data._check()
            else:
                lastret = data0._last()
                for data in datas1:
                    lastret += data._last(datamaster=data0)

                if not lastret:
                    # Only go extra round if something was changed by "lasts"
                    break

            # Datas may have generated a new notification after next
            self._datanotify()
            if self._event_stop:  # stop if requested
                return

            if d0ret or lastret:  # if any bar, check timers before broker
                self._check_timers(runstrats, dt0, cheat=True)
                if self.p.cheat_on_open:
                    for strat in runstrats:
                        strat._next_open()
                        if self._event_stop:  # stop if requested
                            return

            self._brokernotify()
            if self._event_stop:  # stop if requested
                return

            if d0ret or lastret:  # bars produced by data or filters
                self._check_timers(runstrats, dt0, cheat=False)
                for strat in runstrats:
                    strat._next()
                    if self._event_stop:  # stop if requested
                        return

                    self._next_writers(runstrats)

        # Last notification chance before stopping
        self._datanotify()
        if self._event_stop:  # stop if requested
            return
        self._storenotify()
        if self._event_stop:  # stop if requested
            return

    def _runonce(self, runstrats):
        '''
        Actual implementation of run in vector mode.

        Strategies are still invoked on a pseudo-event mode in which ``next``
        is called for each data arrival
        '''
        for strat in runstrats:
            strat._once()
            strat.reset()  # strat called next by next - reset lines

        # The default once for strategies does nothing and therefore
        # has not moved forward all datas/indicators/observers that
        # were homed before calling once, Hence no "need" to do it
        # here again, because pointers are at 0
        datas = sorted(self.datas,
                       key=lambda x: (x._timeframe, x._compression))

        while True:
            # Check next incoming date in the datas
            dts = [d.advance_peek() for d in datas]
            dt0 = min(dts)
            if dt0 == float('inf'):
                break  # no data delivers anything

            # Timemaster if needed be
            # dmaster = datas[dts.index(dt0)]  # and timemaster
            slen = len(runstrats[0])
            for i, dti in enumerate(dts):
                if dti <= dt0:
                    datas[i].advance()
                    # self._plotfillers2[i].append(slen)  # mark as fill
                else:
                    # self._plotfillers[i].append(slen)
                    pass

            self._check_timers(runstrats, dt0, cheat=True)

            if self.p.cheat_on_open:
                for strat in runstrats:
                    strat._oncepost_open()
                    if self._event_stop:  # stop if requested
                        return

            self._brokernotify()
            if self._event_stop:  # stop if requested
                return

            self._check_timers(runstrats, dt0, cheat=False)

            for strat in runstrats:
                strat._oncepost(dt0)
                if self._event_stop:  # stop if requested
                    return

                self._next_writers(runstrats)

    def _check_timers(self, runstrats, dt0, cheat=False):
        timers = self._timers if not cheat else self._timerscheat
        for t in timers:
            if not t.check(dt0):
                continue

            t.params.owner.notify_timer(t, t.lastwhen, *t.args, **t.kwargs)

            if t.params.strats:
                for strat in runstrats:
                    strat.notify_timer(t, t.lastwhen, *t.args, **t.kwargs)
