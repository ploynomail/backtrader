#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015-2023 Daniel Rodriguez
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import collections
import datetime
import inspect
import io
import os.path

import backtrader as bt
from backtrader import (date2num, num2date, time2num, TimeFrame, dataseries,
                        metabase)

from backtrader.utils.py3 import with_metaclass, zip, range, string_types
from backtrader.utils import tzparse
from .dataseries import SimpleFilterWrapper
from .resamplerfilter import Resampler, Replayer
from .tradingcal import PandasMarketCalendar


class MetaAbstractDataBase(dataseries.OHLCDateTime.__class__):
    """
    DataFeed元类，用于自动注册所有非别名、非基类的数据源子类
    功能:
        1. 维护_indcol字典存储所有数据源子类
        2. 在类创建时自动完成注册
        3. 处理数据源的预处理和后期初始化
    """
    _indcol = dict()  # 存储所有数据源子类的注册表，键为子类名，值为子类本身

    def __init__(cls, name, bases, dct):
        '''
        元类初始化方法，在子类创建时被调用
        Args:
            name (str): 类名
            bases (tuple): 基类元组
            dct (dict): 类属性字典
        '''
        # 调用父类元类的初始化，确保元类的基础功能正常工作
        super(MetaAbstractDataBase, cls).__init__(name, bases, dct)

        # 注册非别名、非基类、非内部类(_开头)的子类
        # cls.aliased: 检查类是否为别名类
        # name != 'DataBase': 排除基类本身
        # not name.startswith('_'): 排除内部类（以_开头）
        if not cls.aliased and \
           name != 'DataBase' and not name.startswith('_'):
            cls._indcol[name] = cls  # 将子类添加到注册表

    def dopreinit(cls, _obj, *args, **kwargs):
        '''
        数据源的预初始化方法，通常在实例化对象时调用
        Args:
            _obj: 数据源实例
            *args: 位置参数
            **kwargs: 关键字参数
        Returns:
            _obj: 经过预初始化的对象
            args: 处理后的位置参数
            kwargs: 处理后的关键字参数
        '''
        # 调用父类的预初始化方法，确保继承链中的其他逻辑被执行
        _obj, args, kwargs = \
            super(MetaAbstractDataBase, cls).dopreinit(_obj, *args, **kwargs)

        # 查找数据源的所有者（通常是FeedBase类型的实例）并存储
        _obj._feed = metabase.findowner(_obj, FeedBase)

        # 初始化通知队列，用于存储通知消息（如连接状态变化）
        _obj.notifs = collections.deque()

        # 设置数据源的名称和数据名称
        _obj._dataname = _obj.p.dataname  # 从参数中获取数据名称
        _obj._name = ''  # 初始化名称为空字符串
        return _obj, args, kwargs

    def dopostinit(cls, _obj, *args, **kwargs):
        '''
        数据源的后期初始化方法，通常在实例化对象后调用
        Args:
            _obj: 数据源实例
            *args: 位置参数
            **kwargs: 关键字参数
        Returns:
            _obj: 经过后期初始化的对象
            args: 处理后的位置参数
            kwargs: 处理后的关键字参数
        '''
        # 调用父类的后期初始化方法，确保继承链中的其他逻辑被执行
        _obj, args, kwargs = \
            super(MetaAbstractDataBase, cls).dopostinit(_obj, *args, **kwargs)

        # 设置数据源的名称，优先级为：子类设置的名称 > 参数中的名称 > 数据名称
        _obj._name = _obj._name or _obj.p.name
        if not _obj._name and isinstance(_obj.p.dataname, string_types):
            _obj._name = _obj.p.dataname

        # 设置数据的压缩级别和时间框架
        _obj._compression = _obj.p.compression
        _obj._timeframe = _obj.p.timeframe

        # 处理交易会话的开始时间
        if isinstance(_obj.p.sessionstart, datetime.datetime):
            # 如果是datetime类型，提取时间部分
            _obj.p.sessionstart = _obj.p.sessionstart.time()
        elif _obj.p.sessionstart is None:
            # 如果未设置，默认设置为一天的最小时间
            _obj.p.sessionstart = datetime.time.min

        # 处理交易会话的结束时间
        if isinstance(_obj.p.sessionend, datetime.datetime):
            # 如果是datetime类型，提取时间部分
            _obj.p.sessionend = _obj.p.sessionend.time()
        elif _obj.p.sessionend is None:
            # 如果未设置，默认设置为一天的最大时间（减去一点点以避免精度问题）
            _obj.p.sessionend = datetime.time(23, 59, 59, 999990)

        # 处理数据的起始日期
        if isinstance(_obj.p.fromdate, datetime.date):
            # 如果是date类型，将其转换为datetime类型，并结合会话开始时间
            if not hasattr(_obj.p.fromdate, 'hour'):
                _obj.p.fromdate = datetime.datetime.combine(
                    _obj.p.fromdate, _obj.p.sessionstart)

        # 处理数据的结束日期
        if isinstance(_obj.p.todate, datetime.date):
            # 如果是date类型，将其转换为datetime类型，并结合会话结束时间
            if not hasattr(_obj.p.todate, 'hour'):
                _obj.p.todate = datetime.datetime.combine(
                    _obj.p.todate, _obj.p.sessionend)

        # 初始化两个双端队列，用于存储过滤操作的中间结果
        _obj._barstack = collections.deque()  # 用于过滤操作的堆栈
        _obj._barstash = collections.deque()  # 用于过滤操作的临时存储

        # 初始化过滤器列表
        _obj._filters = list()  # 普通过滤器
        _obj._ffilters = list()  # 带有last方法的过滤器
        for fp in _obj.p.filters:
            if inspect.isclass(fp):  # 如果过滤器是类
                fp = fp(_obj)  # 实例化过滤器
                if hasattr(fp, 'last'):  # 如果过滤器有last方法
                    _obj._ffilters.append((fp, [], {}))  # 添加到ffilters列表

            _obj._filters.append((fp, [], {}))  # 添加到filters列表

        return _obj, args, kwargs


class AbstractDataBase(with_metaclass(MetaAbstractDataBase,
                                      dataseries.OHLCDateTime)):
    """
    抽象数据基类(AbstractDataBase)
    作用:
        - 提供数据源的基础功能和接口
        - 支持数据的加载、过滤、时间处理等功能
        - 作为所有数据源类的基类
    使用方法:
        - 继承此类并实现 `_load` 方法以定义具体的数据加载逻辑
        - 可通过添加过滤器、设置时间范围等方式定制数据行为
    """

    # 参数定义，控制数据源的行为
    params = (
        ('dataname', None),  # 数据源名称或路径
        ('name', ''),  # 数据源的名称（可选）
        ('compression', 1),  # 数据压缩级别
        ('timeframe', TimeFrame.Days),  # 时间框架（如天、分钟等）
        ('fromdate', None),  # 数据起始日期
        ('todate', None),  # 数据结束日期
        ('sessionstart', None),  # 交易会话开始时间
        ('sessionend', None),  # 交易会话结束时间
        ('filters', []),  # 数据过滤器列表
        ('tz', None),  # 数据的时区
        ('tzinput', None),  # 输入数据的时区
        ('qcheck', 0.0),  # 检查事件的超时时间（秒）
        ('calendar', None),  # 交易日历
    )

    # 数据状态常量，用于通知和状态管理
    (CONNECTED, DISCONNECTED, CONNBROKEN, DELAYED,
     LIVE, NOTSUBSCRIBED, NOTSUPPORTED_TF, UNKNOWN) = range(8)

    # 状态名称列表，用于将状态值映射为可读名称
    _NOTIFNAMES = [
        'CONNECTED', 'DISCONNECTED', 'CONNBROKEN', 'DELAYED',
        'LIVE', 'NOTSUBSCRIBED', 'NOTSUPPORTED_TIMEFRAME', 'UNKNOWN']

    @classmethod
    def _getstatusname(cls, status):
        """
        根据状态值返回状态名称
        Args:
            status (int): 状态值
        Returns:
            str: 状态名称
        """
        return cls._NOTIFNAMES[status]

    # 内部属性，用于存储补偿信息、数据源、存储对象等
    _compensate = None  # 补偿对象，用于处理相关资产的补偿操作
    _feed = None  # 数据源对象
    _store = None  # 存储对象

    _clone = False  # 是否为克隆数据
    _qcheck = 0.0  # 检查事件的超时时间

    _tmoffset = datetime.timedelta()  # 时间偏移量

    # 标志是否正在进行重采样或重放
    resampling = 0
    replaying = 0

    _started = False  # 数据源是否已启动

    def _start_finish(self):
        """
        数据启动的最终阶段，处理时区和时间相关的初始化
        """
        # 获取输出时区
        self._tz = self._gettz()
        # 设置时区到 datetime 行
        self.lines.datetime._settz(self._tz)

        # 初始化输入时区本地化器
        self._tzinput = bt.utils.date.Localizer(self._gettzinput())

        # 转换用户输入的时间范围到输出时区
        if self.p.fromdate is None:
            self.fromdate = float('-inf')  # 如果未设置起始日期，设置为负无穷
        else:
            self.fromdate = self.date2num(self.p.fromdate)  # 转换为数值

        if self.p.todate is None:
            self.todate = float('inf')  # 如果未设置结束日期，设置为正无穷
        else:
            self.todate = self.date2num(self.p.todate)  # 转换为数值

        # FIXME: 这两个属性未使用，可以移除
        self.sessionstart = time2num(self.p.sessionstart)
        self.sessionend = time2num(self.p.sessionend)

        # 初始化交易日历
        self._calendar = cal = self.p.calendar
        if cal is None:
            self._calendar = self._env._tradingcal  # 使用环境中的默认交易日历
        elif isinstance(cal, string_types):
            self._calendar = PandasMarketCalendar(calendar=cal)  # 使用指定的交易日历

        self._started = True  # 标记数据源已启动

    def _start(self):
        """
        数据启动方法，调用用户定义的 start 方法并完成启动流程
        """
        self.start()  # 调用用户定义的 start 方法

        if not self._started:
            self._start_finish()  # 如果未完成启动，调用启动的最终阶段

    def _timeoffset(self):
        """
        返回时间偏移量
        Returns:
            datetime.timedelta: 时间偏移量
        """
        return self._tmoffset

    def _getnexteos(self):
        """
        获取下一个交易会话结束时间（End of Session, EOS）
        Returns:
            tuple: (结束时间的 datetime 对象, 数值形式的结束时间)
        """
        if self._clone:
            return self.data._getnexteos()  # 如果是克隆数据，调用原始数据的方法

        if not len(self):
            return datetime.datetime.min, 0.0  # 如果数据为空，返回最小时间

        dt = self.lines.datetime[0]  # 当前时间
        dtime = num2date(dt)  # 转换为 datetime 对象
        if self._calendar is None:
            # 如果没有交易日历，计算默认的会话结束时间
            nexteos = datetime.datetime.combine(dtime, self.p.sessionend)
            nextdteos = self.date2num(nexteos)  # 转换为数值形式
            nexteos = num2date(nextdteos)  # 转换回 datetime 对象
            while dtime > nexteos:
                nexteos += datetime.timedelta(days=1)  # 如果当前时间超过结束时间，增加一天

            nextdteos = date2num(nexteos)  # 转换为数值形式
        else:
            # 如果有交易日历，使用日历计算结束时间
            _, nexteos = self._calendar.schedule(dtime, self._tz)
            nextdteos = date2num(nexteos)  # 转换为数值形式

        return nexteos, nextdteos

    def _gettzinput(self):
        """
        获取输入数据的时区
        Returns:
            tzinfo: 输入数据的时区
        """
        return tzparse(self.p.tzinput)

    def _gettz(self):
        """
        获取输出数据的时区
        Returns:
            tzinfo: 输出数据的时区
        """
        return tzparse(self.p.tz)

    def date2num(self, dt):
        """
        将 datetime 对象转换为数值形式
        Args:
            dt (datetime): datetime 对象
        Returns:
            float: 数值形式的时间
        """
        if self._tz is not None:
            return date2num(self._tz.localize(dt))  # 如果有时区，进行本地化

        return date2num(dt)

    def num2date(self, dt=None, tz=None, naive=True):
        """
        将数值形式的时间转换为 datetime 对象
        Args:
            dt (float): 数值形式的时间
            tz (tzinfo): 时区
            naive (bool): 是否返回 naive 的 datetime 对象
        Returns:
            datetime: datetime 对象
        """
        if dt is None:
            return num2date(self.lines.datetime[0], tz or self._tz, naive)

        return num2date(dt, tz or self._tz, naive)

    def haslivedata(self):
        """
        检查数据源是否支持实时数据
        Returns:
            bool: 是否支持实时数据
        """
        return False  # 必须由支持实时数据的子类重写

    def do_qcheck(self, onoff, qlapse):
        """
        设置检查事件的超时时间
        Args:
            onoff (bool): 是否启用检查
            qlapse (float): 已经过的时间
        """
        qwait = self.p.qcheck if onoff else 0.0  # 如果启用检查，使用参数中的超时时间
        qwait = max(0.0, qwait - qlapse)  # 减去已经过的时间
        self._qcheck = qwait

    def islive(self):
        """
        检查数据源是否为实时数据
        Returns:
            bool: 是否为实时数据
        """
        return False  # 必须由支持实时数据的子类重写

    def put_notification(self, status, *args, **kwargs):
        """
        添加通知到通知队列
        Args:
            status (int): 状态值
            *args: 额外参数
            **kwargs: 额外关键字参数
        """
        if self._laststatus != status:  # 如果状态发生变化
            self.notifs.append((status, args, kwargs))  # 添加通知到队列
            self._laststatus = status  # 更新最后的状态

    def get_notifications(self):
        """
        获取所有待处理的通知
        Returns:
            list: 通知列表
        """
        self.notifs.append(None)  # 添加标记，表示通知结束
        notifs = list()
        while True:
            notif = self.notifs.popleft()  # 从队列中取出通知
            if notif is None:  # 如果遇到标记，结束
                break
            notifs.append(notif)

        return notifs

    def getfeed(self):
        """
        获取数据源对象
        Returns:
            数据源对象
        """
        return self._feed

    def qbuffer(self, savemem=0, replaying=False):
        """
        设置缓冲区
        Args:
            savemem (int): 保存的内存大小
            replaying (bool): 是否为重放模式
        """
        extrasize = self.resampling or replaying
        for line in self.lines:
            line.qbuffer(savemem=savemem, extrasize=extrasize)

    def start(self):
        """
        启动数据源，初始化内部状态
        """
        self._barstack = collections.deque()
        self._barstash = collections.deque()
        self._laststatus = self.CONNECTED

    def stop(self):
        """
        停止数据源，清理资源
        """
        pass

    def clone(self, **kwargs):
        """
        克隆数据源，返回一个新的数据源实例
        Returns:
            DataClone: 数据源克隆对象
        """
        return DataClone(dataname=self, **kwargs)

    def copyas(self, _dataname, **kwargs):
        """
        以指定名称复制数据源
        Args:
            _dataname (str): 新的数据源名称
        Returns:
            DataClone: 数据源克隆对象
        """
        d = DataClone(dataname=self, **kwargs)
        d._dataname = _dataname
        d._name = _dataname
        return d

    def setenvironment(self, env):
        """
        设置环境对象
        Args:
            env: 环境对象
        """
        self._env = env

    def getenvironment(self):
        """
        获取环境对象
        Returns:
            环境对象
        """
        return self._env

    def addfilter_simple(self, f, *args, **kwargs):
        """
        添加简单过滤器
        Args:
            f: 过滤器函数
        """
        fp = SimpleFilterWrapper(self, f, *args, **kwargs)
        self._filters.append((fp, fp.args, fp.kwargs))

    def addfilter(self, p, *args, **kwargs):
        """
        添加过滤器
        Args:
            p: 过滤器类或实例
        """
        if inspect.isclass(p):
            pobj = p(self, *args, **kwargs)
            self._filters.append((pobj, [], {}))

            if hasattr(pobj, 'last'):
                self._ffilters.append((pobj, [], {}))

        else:
            self._filters.append((p, args, kwargs))

    def compensate(self, other):
        """
        设置补偿对象
        Args:
            other: 相关资产的数据源对象
        """
        self._compensate = other

    def _tick_nullify(self):
        # These are the updating prices in case the new bar is "updated"
        # and the length doesn't change like if a replay is happening or
        # a real-time data feed is in use and 1 minutes bars are being
        # constructed with 5 seconds updates
        for lalias in self.getlinealiases():
            if lalias != 'datetime':
                setattr(self, 'tick_' + lalias, None)

        self.tick_last = None

    def _tick_fill(self, force=False):
        # If nothing filled the tick_xxx attributes, the bar is the tick
        alias0 = self._getlinealias(0)
        if force or getattr(self, 'tick_' + alias0, None) is None:
            for lalias in self.getlinealiases():
                if lalias != 'datetime':
                    setattr(self, 'tick_' + lalias,
                            getattr(self.lines, lalias)[0])

            self.tick_last = getattr(self.lines, alias0)[0]

    def advance_peek(self):
        if len(self) < self.buflen():
            return self.lines.datetime[1]  # return the future

        return float('inf')  # max date else

    def advance(self, size=1, datamaster=None, ticks=True):
        if ticks:
            self._tick_nullify()

        # Need intercepting this call to support datas with
        # different lengths (timeframes)
        self.lines.advance(size)

        if datamaster is not None:
            if len(self) > self.buflen():
                # if no bar can be delivered, fill with an empty bar
                self.rewind()
                self.lines.forward()
                return

            if self.lines.datetime[0] > datamaster.lines.datetime[0]:
                self.lines.rewind()
            else:
                if ticks:
                    self._tick_fill()
        elif len(self) < self.buflen():
            # a resampler may have advance us past the last point
            if ticks:
                self._tick_fill()

    def next(self, datamaster=None, ticks=True):

        if len(self) >= self.buflen():
            if ticks:
                self._tick_nullify()

            # not preloaded - request next bar
            ret = self.load()
            if not ret:
                # if load cannot produce bars - forward the result
                return ret

            if datamaster is None:
                # bar is there and no master ... return load's result
                if ticks:
                    self._tick_fill()
                return ret
        else:
            self.advance(ticks=ticks)

        # a bar is "loaded" or was preloaded - index has been moved to it
        if datamaster is not None:
            # there is a time reference to check against
            if self.lines.datetime[0] > datamaster.lines.datetime[0]:
                # can't deliver new bar, too early, go back
                self.rewind()
                return False
            else:
                if ticks:
                    self._tick_fill()

        else:
            if ticks:
                self._tick_fill()

        # tell the world there is a bar (either the new or the previous
        return True

    def preload(self):
        """
        预加载数据，直到加载完所有可用数据
        """
        while self.load():
            pass

        self._last()
        self.home()

    def _last(self, datamaster=None):
        # Last chance for filters to deliver something
        ret = 0
        for ff, fargs, fkwargs in self._ffilters:
            ret += ff.last(self, *fargs, **fkwargs)

        doticks = False
        if datamaster is not None and self._barstack:
            doticks = True

        while self._fromstack(forward=True):
            # consume bar(s) produced by "last"s - adding room
            pass

        if doticks:
            self._tick_fill()

        return bool(ret)

    def _check(self, forcedata=None):
        ret = 0
        for ff, fargs, fkwargs in self._filters:
            if not hasattr(ff, 'check'):
                continue
            ff.check(self, _forcedata=forcedata, *fargs, **fkwargs)

    def load(self):
        """
        加载下一条数据
        Returns:
            bool: 是否成功加载数据
        """
        while True:
            # move data pointer forward for new bar
            self.forward()

            if self._fromstack():  # bar is available
                return True

            if not self._fromstack(stash=True):
                _loadret = self._load()
                if not _loadret:  # no bar use force to make sure in exactbars
                    # the pointer is undone this covers especially (but not
                    # uniquely) the case in which the last bar has been seen
                    # and a backwards would ruin pointer accounting in the
                    # "stop" method of the strategy
                    self.backwards(force=True)  # undo data pointer

                    # return the actual returned value which may be None to
                    # signal no bar is available, but the data feed is not
                    # done. False means game over
                    return _loadret

            # Get a reference to current loaded time
            dt = self.lines.datetime[0]

            # A bar has been loaded, adapt the time
            if self._tzinput:
                # Input has been converted at face value but it's not UTC in
                # the input stream
                dtime = num2date(dt)  # get it in a naive datetime
                # localize it
                dtime = self._tzinput.localize(dtime)  # pytz compatible-ized
                self.lines.datetime[0] = dt = date2num(dtime)  # keep UTC val

            # Check standard date from/to filters
            if dt < self.fromdate:
                # discard loaded bar and carry on
                self.backwards()
                continue
            if dt > self.todate:
                # discard loaded bar and break out
                self.backwards(force=True)
                break

            # Pass through filters
            retff = False
            for ff, fargs, fkwargs in self._filters:
                # previous filter may have put things onto the stack
                if self._barstack:
                    for i in range(len(self._barstack)):
                        self._fromstack(forward=True)
                        retff = ff(self, *fargs, **fkwargs)
                else:
                    retff = ff(self, *fargs, **fkwargs)

                if retff:  # bar removed from systemn
                    break  # out of the inner loop

            if retff:  # bar removed from system - loop to get new bar
                continue  # in the greater loop

            # Checks let the bar through ... notify it
            return True

        # Out of the loop ... no more bars or past todate
        return False

    def _load(self):
        """
        子类实现此方法以定义具体的数据加载逻辑
        Returns:
            bool: 是否成功加载数据
        """
        return False

    def _add2stack(self, bar, stash=False):
        '''Saves given bar (list of values) to the stack for later retrieval'''
        if not stash:
            self._barstack.append(bar)
        else:
            self._barstash.append(bar)

    def _save2stack(self, erase=False, force=False, stash=False):
        '''将当前的bar保存到堆栈中以供后续检索

        参数:
            erase (bool): 是否从数据流中移除当前bar
            force (bool): 是否强制移除bar
            stash (bool): 是否将bar保存到临时堆栈
        '''
        bar = [line[0] for line in self.itersize()]  # 获取当前bar的所有行数据
        if not stash:  # 如果不是临时堆栈
            self._barstack.append(bar)  # 将bar添加到主堆栈
        else:  # 如果是临时堆栈
            self._barstash.append(bar)  # 将bar添加到临时堆栈

        if erase:  # 如果需要移除当前bar
            self.backwards(force=force)  # 回退数据指针，移除bar

    def _updatebar(self, bar, forward=False, ago=0):
        '''将堆栈中的值加载到行数据中以形成新的bar

        参数:
            bar (list): 包含bar数据的列表
            forward (bool): 是否前进数据指针
            ago (int): 指定更新的时间偏移量
        返回:
            bool: 如果存在值则返回True，否则返回False
        '''
        if forward:  # 如果需要前进数据指针
            self.forward()  # 前进数据指针

        for line, val in zip(self.itersize(), bar):  # 遍历行数据和bar值
            line[0 + ago] = val  # 将bar值更新到行数据中

    def _fromstack(self, forward=False, stash=False):
        '''从堆栈中加载值到行数据中以形成新的bar

        参数:
            forward (bool): 是否前进数据指针
            stash (bool): 是否从临时堆栈加载
        返回:
            bool: 如果存在值则返回True，否则返回False
        '''
        coll = self._barstack if not stash else self._barstash  # 根据stash参数选择主堆栈或临时堆栈

        if coll:  # 如果堆栈中有数据
            if forward:  # 如果需要前进数据指针
                self.forward()  # 前进数据指针

            for line, val in zip(self.itersize(), coll.popleft()):  # 遍历行数据和堆栈中的值
                line[0] = val  # 将堆栈中的值更新到行数据中

            return True  # 返回True表示成功加载数据

        return False  # 返回False表示堆栈中没有数据

    def resample(self, **kwargs):
        """
        添加重采样过滤器
        """
        self.addfilter(Resampler, **kwargs)

    def replay(self, **kwargs):
        """
        添加重放过滤器
        """
        self.addfilter(Replayer, **kwargs)


class DataBase(AbstractDataBase):
    pass


class FeedBase(with_metaclass(metabase.MetaParams, object)):
    """
    FeedBase类 - 数据源基类
    作用：
        - 管理和协调多个数据源
        - 提供统一的数据接口
        - 作为所有Feed类的基类
    使用方法：
        - 继承此类并实现_getdata方法
        - 通过getdata方法添加并获取数据源
    """
    # 参数定义，合并空元组与DataBase类的所有参数
    params = () + DataBase.params._gettuple()  # 继承DataBase的所有参数设置

    def __init__(self):
        """
        初始化FeedBase对象
        功能：创建一个空的数据源列表
        """
        self.datas = list()  # 初始化数据源列表，用于存储所有添加的数据源对象

    def start(self):
        """
        启动所有数据源
        功能：遍历并启动所有已添加的数据源
        使用时机：在回测或实盘交易开始前调用
        """
        for data in self.datas:  # 遍历数据源列表中的每个数据源对象
            data.start()  # 调用每个数据源的start方法，初始化数据加载

    def stop(self):
        """
        停止所有数据源
        功能：遍历并停止所有已添加的数据源
        使用时机：在回测或实盘交易结束后调用，释放资源
        """
        for data in self.datas:  # 遍历数据源列表中的每个数据源对象
            data.stop()  # 调用每个数据源的stop方法，关闭资源

    def getdata(self, dataname, name=None, **kwargs):
        """
        获取数据源对象并添加到数据源列表
        功能：
            - 创建新的数据源对象
            - 设置数据源名称
            - 将数据源添加到列表中
        参数:
            dataname (str): 数据源名称或路径，指定数据位置
            name (str): 数据源的自定义名称（可选）
            **kwargs: 其他参数，用于覆盖默认参数
        返回:
            数据源对象：创建并添加到列表的数据源实例
        """
        # 遍历当前Feed实例的所有参数
        for pname, pvalue in self.p._getitems():  # 获取所有参数项
            kwargs.setdefault(pname, getattr(self.p, pname))  # 如果kwargs中未提供该参数，则使用实例默认值

        kwargs['dataname'] = dataname  # 将数据源名称添加到kwargs中
        data = self._getdata(**kwargs)  # 调用子类实现的_getdata方法创建数据源对象

        data._name = name  # 设置数据源的自定义名称

        self.datas.append(data)  # 将创建的数据源对象添加到数据源列表中
        return data  # 返回创建的数据源对象

    def _getdata(self, dataname, **kwargs):
        """
        内部方法，创建数据源对象
        功能：
            - 创建特定类型的数据源对象
            - 子类必须重写此方法以定义具体的数据获取逻辑
        参数:
            dataname (str): 数据源名称或路径
            **kwargs: 其他参数，用于初始化数据源对象
        返回:
            数据源对象：根据参数创建的数据源实例
        """
        for pname, pvalue in self.p._getitems():
            kwargs.setdefault(pname, getattr(self.p, pname))

        kwargs['dataname'] = dataname
        return self.DataCls(**kwargs)


class MetaCSVDataBase(DataBase.__class__):
    def dopostinit(cls, _obj, *args, **kwargs):
        # Before going to the base class to make sure it overrides the default
        if not _obj.p.name and not _obj._name:
            _obj._name, _ = os.path.splitext(os.path.basename(_obj.p.dataname))

        _obj, args, kwargs = \
            super(MetaCSVDataBase, cls).dopostinit(_obj, *args, **kwargs)

        return _obj, args, kwargs


class CSVDataBase(with_metaclass(MetaCSVDataBase, DataBase)):
    '''
    Base class for classes implementing CSV DataFeeds

    The class takes care of opening the file, reading the lines and
    tokenizing them.

    Subclasses do only need to override:

      - _loadline(tokens)

    The return value of ``_loadline`` (True/False) will be the return value
    of ``_load`` which has been overriden by this base class
    '''

    f = None
    params = (('headers', True), ('separator', ','),)

    def start(self):
        super(CSVDataBase, self).start()

        if self.f is None:
            if hasattr(self.p.dataname, 'readline'):
                self.f = self.p.dataname
            else:
                # Let an exception propagate to let the caller know
                self.f = io.open(self.p.dataname, 'r')

        if self.p.headers:
            self.f.readline()  # skip the headers

        self.separator = self.p.separator

    def stop(self):
        super(CSVDataBase, self).stop()
        if self.f is not None:
            self.f.close()
            self.f = None

    def preload(self):
        while self.load():
            pass

        self._last()
        self.home()

        # preloaded - no need to keep the object around - breaks multip in 3.x
        self.f.close()
        self.f = None

    def _load(self):
        if self.f is None:
            return False

        # Let an exception propagate to let the caller know
        line = self.f.readline()

        if not line:
            return False

        line = line.rstrip('\n')
        linetokens = line.split(self.separator)
        return self._loadline(linetokens)

    def _getnextline(self):
        if self.f is None:
            return None

        # Let an exception propagate to let the caller know
        line = self.f.readline()

        if not line:
            return None

        line = line.rstrip('\n')
        linetokens = line.split(self.separator)
        return linetokens


class CSVFeedBase(FeedBase):
    params = (('basepath', ''),) + CSVDataBase.params._gettuple()

    def _getdata(self, dataname, **kwargs):
        return self.DataCls(dataname=self.p.basepath + dataname,
                            **self.p._getkwargs())


class DataClone(AbstractDataBase):
    """
    DataClone类 - 数据克隆类
    作用:
        - 创建现有数据源的副本
        - 允许同一数据在策略中以不同方式使用
        - 提供数据复制而不是重新加载的机制
    使用方法:
        - 通过AbstractDataBase的clone或copyas方法创建
    """
    _clone = True  # 标记此数据对象为克隆对象，用于区分原始数据源和克隆数据源

    def __init__(self):
        """
        初始化数据克隆对象
        功能：复制原始数据源的关键参数和设置
        """
        self.data = self.p.dataname  # 存储原始数据源的引用，p.dataname指向原始数据对象
        self._dataname = self.data._dataname  # 复制原始数据源的名称

        # 复制原始数据源的日期/会话参数
        self.p.fromdate = self.p.fromdate  # 复制起始日期参数
        self.p.todate = self.p.todate  # 复制结束日期参数
        self.p.sessionstart = self.data.p.sessionstart  # 复制会话开始时间参数
        self.p.sessionend = self.data.p.sessionend  # 复制会话结束时间参数

        self.p.timeframe = self.data.p.timeframe  # 复制时间帧参数(如天、分钟等)
        self.p.compression = self.data.p.compression  # 复制数据压缩级别参数

    def _start(self):
        """
        启动数据克隆的内部过程
        功能：从原始数据源复制关键数据和设置
        """
        self.start()  # 调用自身的start方法初始化基本设置

        # 复制时区信息
        self._tz = self.data._tz  # 复制原始数据源的时区设置
        self.lines.datetime._settz(self._tz)  # 设置datetime行的时区

        self._calendar = self.data._calendar  # 复制交易日历

        # 原始数据已经处理了时区转换，克隆数据不需要再次转换
        self._tzinput = None  # 不需要进一步转换输入时区

        # 复制日期/会话信息
        self.fromdate = self.data.fromdate  # 复制处理后的起始日期
        self.todate = self.data.todate  # 复制处理后的结束日期

        # FIXME: 如果原始数据中移除了这些属性，这里也应该移除
        self.sessionstart = self.data.sessionstart  # 复制会话开始时间
        self.sessionend = self.data.sessionend  # 复制会话结束时间

    def start(self):
        """
        启动数据克隆对象
        功能：初始化内部状态和计数器
        """
        super(DataClone, self).start()  # 调用父类的start方法
        self._dlen = 0  # 初始化数据长度计数器，用于跟踪已处理的数据点数量
        self._preloading = False  # 初始化预加载标志为False

    def preload(self):
        """
        预加载数据
        功能：预先加载所有数据，提高后续访问效率
        """
        self._preloading = True  # 设置预加载标志为True
        super(DataClone, self).preload()  # 调用父类的preload方法
        self.data.home()  # 将原始数据源的指针重置到起始位置，因为预加载过程会前移指针
        self._preloading = False  # 预加载完成后，重置标志为False

    def _load(self):
        """
        加载数据的内部方法
        功能：从原始数据源复制当前数据点到克隆对象
        返回：
            bool: 是否成功加载数据
        """
        # 假设原始数据已经在系统中
        # 简单地复制行数据
        if self._preloading:  # 如果正在预加载
            # 数据已预加载，我们也在预加载，可以前进直到有完整的bar或数据源用尽
            self.data.advance()  # 前进原始数据源的指针
            if len(self.data) > self.data.buflen():  # 如果原始数据已超出缓冲区大小
                return False  # 返回False表示没有更多数据可加载

            for line, dline in zip(self.lines, self.data.lines):  # 遍历所有行
                line[0] = dline[0]  # 将原始数据行值复制到克隆数据行

            return True  # 返回True表示成功加载数据

        # 非预加载状态
        if not (len(self.data) > self._dlen):  # 如果原始数据长度未增加
            # 数据未超过最后看到的bar
            return False  # 返回False表示没有新数据加载

        self._dlen += 1  # 增加已见数据计数器

        for line, dline in zip(self.lines, self.data.lines):  # 遍历所有行
            line[0] = dline[0]  # 将原始数据行值复制到克隆数据行

        return True  # 返回True表示成功加载数据

    def advance(self, size=1, datamaster=None, ticks=True):
        """
        前进数据指针
        功能：将数据指针前移指定步数
        参数：
            size (int): 前进的步数
            datamaster: 主数据源，用于同步
            ticks (bool): 是否处理tick数据
        """
        self._dlen += size  # 增加已见数据计数器
        super(DataClone, self).advance(size, datamaster, ticks=ticks)  # 调用父类的advance方法
