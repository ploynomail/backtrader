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
import operator
import sys

from .utils.py3 import map, range, zip, with_metaclass, string_types
from .utils import DotDict

from .lineroot import LineRoot, LineSingle
from .linebuffer import LineActions, LineNum
from .lineseries import LineSeries, LineSeriesMaker
from .dataseries import DataSeries
from . import metabase


class MetaLineIterator(LineSeries.__class__):
    # MetaLineIterator类：LineIterator的元类，继承自LineSeries的元类
    # 负责管理LineIterator类的创建和实例化过程
    
    def donew(cls, *args, **kwargs):
        # donew方法：在实例创建期间被调用，负责处理参数和初始化实例
        # 处理来自基类的对象、参数和关键字参数
        _obj, args, kwargs = \
            super(MetaLineIterator, cls).donew(*args, **kwargs)

        # 准备保存需要计算并影响minperiod的子对象
        # 使用defaultdict存储不同类型的迭代器（指标、观察者等）
        _obj._lineiterators = collections.defaultdict(list)

        # 扫描参数以查找数据源... 如果没有找到，
        # 使用_owner（作为时钟源）
        mindatas = _obj._mindatas  # 获取最小数据源数量
        lastarg = 0  # 记录处理到的参数位置
        _obj.datas = []  # 初始化数据源列表
        
        # 遍历位置参数，查找数据源
        for arg in args:
            if isinstance(arg, LineRoot):
                # 如果参数是LineRoot类型，将其转换为LineSeries并添加到数据源列表
                _obj.datas.append(LineSeriesMaker(arg))

            elif not mindatas:
                # 如果已经找到所需的最小数据源数量，结束循环
                break  # found not data and must not be collected
            else:
                try:
                    # 尝试将参数转换为LineNum类型，然后添加到数据源列表
                    _obj.datas.append(LineSeriesMaker(LineNum(arg)))
                except:
                    # 如果转换失败，不是LineNum且不是LineSeries，结束循环
                    break

            # 更新最小数据源数量和处理过的参数位置
            mindatas = max(0, mindatas - 1)
            lastarg += 1

        # 保存剩余未处理的参数
        newargs = args[lastarg:]

        # 如果没有传递数据源到指标... 使用所有者的主数据源，
        # 简化添加"self.data"...
        if not _obj.datas and isinstance(_obj, (IndicatorBase, ObserverBase)):
            # 如果没有数据源且对象是指标或观察者，使用所有者的数据源
            _obj.datas = _obj._owner.datas[0:mindatas]

        # 创建字典以便能够检查存在性
        # python中的列表在使用"in"测试存在性时使用"=="运算符
        # 这实际上不是检查存在性而是检查相等性
        _obj.ddatas = {x: None for x in _obj.datas}

        # 为每个找到的数据源添加访问成员 -
        # 对于第一个数据源有2个（data和data0）
        if _obj.datas:
            # 将第一个数据源保存为data属性
            _obj.data = data = _obj.datas[0]

            # 为第一个数据源的每条线创建别名
            for l, line in enumerate(data.lines):
                linealias = data._getlinealias(l)
                if linealias:
                    # 如果有别名，创建data_别名形式的访问
                    setattr(_obj, 'data_%s' % linealias, line)
                # 创建data_索引形式的访问
                setattr(_obj, 'data_%d' % l, line)

            # 为每个数据源创建别名和访问方式
            for d, data in enumerate(_obj.datas):
                # 创建data索引形式的访问
                setattr(_obj, 'data%d' % d, data)

                # 为每个数据源的每条线创建别名
                for l, line in enumerate(data.lines):
                    linealias = data._getlinealias(l)
                    if linealias:
                        # 如果有别名，创建data索引_别名形式的访问
                        setattr(_obj, 'data%d_%s' % (d, linealias), line)
                    # 创建data索引_索引形式的访问
                    setattr(_obj, 'data%d_%d' % (d, l), line)

        # 参数值现在在__init__之前已经设置
        # 创建数据名称到数据对象的映射字典
        _obj.dnames = DotDict([(d._name, d)
                               for d in _obj.datas if getattr(d, '_name', '')])

        # 返回处理后的对象、参数和关键字参数
        return _obj, newargs, kwargs

    def dopreinit(cls, _obj, *args, **kwargs):
        # dopreinit方法：在初始化前调用，设置时钟和最小周期
        # 调用基类的dopreinit方法
        _obj, args, kwargs = \
            super(MetaLineIterator, cls).dopreinit(_obj, *args, **kwargs)

        # 如果没有找到数据源，使用_owner作为时钟
        _obj.datas = _obj.datas or [_obj._owner]

        # 第一个数据源是我们的计时时钟
        _obj._clock = _obj.datas[0]

        # 通过扫描发现的数据源来自动设置周期
        # 在所有数据源产生"数据"之前，无法进行计算
        # 一个数据源可能是一个指标，可能需要x个bar才能产生数据
        _obj._minperiod = \
            max([x._minperiod for x in _obj.datas] or [_obj._minperiod])

        # 线至少具有与数据源相同的最小周期
        for line in _obj.lines:
            # 为每条线添加最小周期
            line.addminperiod(_obj._minperiod)

        # 返回处理后的对象、参数和关键字参数
        return _obj, args, kwargs

    def dopostinit(cls, _obj, *args, **kwargs):
        # dopostinit方法：在初始化后调用，最终确定最小周期和注册指标
        # 调用基类的dopostinit方法
        _obj, args, kwargs = \
            super(MetaLineIterator, cls).dopostinit(_obj, *args, **kwargs)

        # 我的最小周期与我的线的最小周期一样大
        _obj._minperiod = max([x._minperiod for x in _obj.lines])

        # 重新计算周期
        _obj._periodrecalc()

        # 在_minperiod计算完成后将(my)self注册为所有者的指标
        if _obj._owner is not None:
            # 如果有所有者，将自己注册为所有者的指标
            _obj._owner.addindicator(_obj)

        # 返回处理后的对象、参数和关键字参数
        return _obj, args, kwargs


class LineIterator(with_metaclass(MetaLineIterator, LineSeries)):
    # LineIterator类：使用MetaLineIterator元类的LineSeries子类
    # 用于迭代处理数据行，是大多数指标和策略的基类
    
    _nextforce = False  # 强制cerebro在next模式下运行（runonce=False）

    _mindatas = 1  # 最小数据源数量
    _ltype = LineSeries.IndType  # 类型标识为指标类型

    # 绘图信息字典
    plotinfo = dict(plot=True,  # 是否绘制
                    subplot=True,  # 是否在子图中绘制
                    plotname='',  # 绘图名称
                    plotskip=False,  # 是否跳过绘图
                    plotabove=False,  # 是否绘制在上方
                    plotlinelabels=False,  # 是否绘制线标签
                    plotlinevalues=True,  # 是否绘制线的值
                    plotvaluetags=True,  # 是否绘制值标签
                    plotymargin=0.0,  # y轴边距
                    plotyhlines=[],  # 水平线的y值
                    plotyticks=[],  # y轴刻度
                    plothlines=[],  # 水平线
                    plotforce=False,  # 是否强制绘制
                    plotmaster=None,)  # 主绘图对象

    def _periodrecalc(self):
        # _periodrecalc方法：重新计算周期
        # 最后检查，以防并非所有lineiterators都被分配给
        # 线（在某些操作之后直接或间接）
        # 例如是Kaufman的自适应移动平均线
        
        # 获取指标类型的迭代器列表
        indicators = self._lineiterators[LineIterator.IndType]
        # 获取所有指标的最小周期
        indperiods = [ind._minperiod for ind in indicators]
        # 取最大的最小周期
        indminperiod = max(indperiods or [self._minperiod])
        # 更新最小周期
        self.updateminperiod(indminperiod)

    def _stage2(self):
        # _stage2方法：第二阶段准备
        # 调用基类的_stage2方法
        super(LineIterator, self)._stage2()

        # 为所有数据源调用_stage2
        for data in self.datas:
            data._stage2()

        # 为所有lineiterators调用_stage2
        for lineiterators in self._lineiterators.values():
            for lineiterator in lineiterators:
                lineiterator._stage2()

    def _stage1(self):
        # _stage1方法：第一阶段准备
        # 调用基类的_stage1方法
        super(LineIterator, self)._stage1()

        # 为所有数据源调用_stage1
        for data in self.datas:
            data._stage1()

        # 为所有lineiterators调用_stage1
        for lineiterators in self._lineiterators.values():
            for lineiterator in lineiterators:
                lineiterator._stage1()

    def getindicators(self):
        # 获取指标类型的迭代器列表
        return self._lineiterators[LineIterator.IndType]

    def getindicators_lines(self):
        # 获取具有getlinealiases属性的指标类型迭代器列表
        return [x for x in self._lineiterators[LineIterator.IndType]
                if hasattr(x.lines, 'getlinealiases')]

    def getobservers(self):
        # 获取观察者类型的迭代器列表
        return self._lineiterators[LineIterator.ObsType]

    def addindicator(self, indicator):
        # addindicator方法：添加一个指标到适当的队列中
        # 根据指标类型存储在正确的队列中
        self._lineiterators[indicator._ltype].append(indicator)

        # 使用getattr因为line缓冲区没有这个属性
        if getattr(indicator, '_nextforce', False):
            # 如果指标需要runonce=False
            o = self
            # 沿层次结构向上移动
            while o is not None:
                # 如果找到策略类型
                if o._ltype == LineIterator.StratType:
                    # 禁用runonce模式
                    o.cerebro._disable_runonce()
                    break

                # 向上移动到所有者
                o = o._owner  # move up the hierarchy

    def bindlines(self, owner=None, own=None):
        # bindlines方法：将线绑定到所有者的线
        if not owner:
            owner = 0  # 默认为第一条线

        # 处理owner参数，确保是可迭代的
        if isinstance(owner, string_types):
            owner = [owner]  # 如果是字符串，转换为列表
        elif not isinstance(owner, collections.Iterable):
            owner = [owner]  # 如果不是可迭代对象，转换为列表

        # 处理own参数，如果未提供，默认为owner的索引范围
        if not own:
            own = range(len(owner))

        # 处理own参数，确保是可迭代的
        if isinstance(own, string_types):
            own = [own]  # 如果是字符串，转换为列表
        elif not isinstance(own, collections.Iterable):
            own = [own]  # 如果不是可迭代对象，转换为列表

        # 遍历owner和own对，进行线绑定
        for lineowner, lineown in zip(owner, own):
            # 获取所有者的线引用
            if isinstance(lineowner, string_types):
                # 如果是字符串，通过名称获取
                lownerref = getattr(self._owner.lines, lineowner)
            else:
                # 否则，通过索引获取
                lownerref = self._owner.lines[lineowner]

            # 获取自己的线引用
            if isinstance(lineown, string_types):
                # 如果是字符串，通过名称获取
                lownref = getattr(self.lines, lineown)
            else:
                # 否则，通过索引获取
                lownref = self.lines[lineown]

            # 将自己的线绑定到所有者的线
            lownref.addbinding(lownerref)

        # 返回自身，以支持链式调用
        return self

    # 可能更易读的别名
    bind2lines = bindlines
    bind2line = bind2lines

    def _next(self):
        # _next方法：在next模式下处理下一个数据点
        # 更新时钟并获取长度
        clock_len = self._clk_update()

        # 调用所有指标的_next方法
        for indicator in self._lineiterators[LineIterator.IndType]:
            indicator._next()

        # 通知
        self._notify()

        # 根据对象类型和时钟长度决定调用哪个方法
        if self._ltype == LineIterator.StratType:
            # 支持具有不同长度的数据源
            # 获取最小周期状态
            minperstatus = self._getminperstatus()
            if minperstatus < 0:
                # 如果小于0，调用next方法
                self.next()
            elif minperstatus == 0:
                # 如果等于0，调用nextstart方法（仅对第一个值调用）
                self.nextstart()  # only called for the 1st value
            else:
                # 如果大于0，调用prenext方法
                self.prenext()
        else:
            # 假设指标和其他操作在相同长度的数据源上
            # 虽然上面的操作可以通用化
            if clock_len > self._minperiod:
                # 如果时钟长度大于最小周期，调用next方法
                self.next()
            elif clock_len == self._minperiod:
                # 如果时钟长度等于最小周期，调用nextstart方法（仅对第一个值调用）
                self.nextstart()  # only called for the 1st value
            elif clock_len:
                # 如果时钟长度大于0但小于最小周期，调用prenext方法
                self.prenext()

    def _clk_update(self):
        # _clk_update方法：更新时钟并同步长度
        # 获取时钟长度
        clock_len = len(self._clock)
        if clock_len != len(self):
            # 如果时钟长度与自身长度不同，向前移动以同步
            self.forward()

        # 返回时钟长度
        return clock_len

    def _once(self):
        # _once方法：在runonce模式下一次性处理所有数据
        # 向前移动到时钟缓冲区的长度
        self.forward(size=self._clock.buflen())

        # 为所有指标调用_once方法
        for indicator in self._lineiterators[LineIterator.IndType]:
            indicator._once()

        # 为所有观察者向前移动到缓冲区的长度
        for observer in self._lineiterators[LineIterator.ObsType]:
            observer.forward(size=self.buflen())

        # 将所有数据源、指标和观察者重置到初始位置
        for data in self.datas:
            data.home()

        for indicator in self._lineiterators[LineIterator.IndType]:
            indicator.home()

        for observer in self._lineiterators[LineIterator.ObsType]:
            observer.home()

        # 将自己重置到初始位置
        self.home()

        # 这3个方法对策略保持为空，因此不起作用
        # 因为策略总是在next的基础上执行
        # 指标各自以其最小周期调用
        # 调用preonce处理初始数据（0到最小周期-1）
        self.preonce(0, self._minperiod - 1)
        # 调用oncestart处理起始点（最小周期-1到最小周期）
        self.oncestart(self._minperiod - 1, self._minperiod)
        # 调用once处理剩余数据（最小周期到缓冲区长度）
        self.once(self._minperiod, self.buflen())

        # 为所有线应用oncebinding
        for line in self.lines:
            line.oncebinding()

    def preonce(self, start, end):
        # preonce方法：在runonce模式下预处理数据
        # 默认为空，由子类实现
        pass

    def oncestart(self, start, end):
        # oncestart方法：在runonce模式下开始处理数据
        # 默认调用once方法
        self.once(start, end)

    def once(self, start, end):
        # once方法：在runonce模式下处理数据
        # 默认为空，由子类实现
        pass

    def prenext(self):
        '''
        This method will be called before the minimum period of all
        datas/indicators have been meet for the strategy to start executing
        '''
        # prenext方法：在所有数据源/指标的最小周期满足之前被调用
        # 默认为空，由子类实现
        pass

    def nextstart(self):
        '''
        This method will be called once, exactly when the minimum period for
        all datas/indicators have been meet. The default behavior is to call
        next
        '''
        # nextstart方法：当所有数据源/指标的最小周期刚好满足时被调用一次
        # 默认行为是调用next方法
        # 为第一次完整计算调用一次 - 默认为常规next
        self.next()

    def next(self):
        '''
        This method will be called for all remaining data points when the
        minimum period for all datas/indicators have been meet.
        '''
        # next方法：当所有数据源/指标的最小周期满足后，为所有剩余数据点调用
        # 默认为空，由子类实现
        pass

    def _addnotification(self, *args, **kwargs):
        # _addnotification方法：添加通知
        # 默认为空，由子类实现
        pass

    def _notify(self):
        # _notify方法：通知
        # 默认为空，由子类实现
        pass

    def _plotinit(self):
        # _plotinit方法：初始化绘图
        # 默认为空，由子类实现
        pass

    def qbuffer(self, savemem=0):
        # qbuffer方法：优化内存使用的缓冲区
        if savemem:
            # 如果savemem为真，为所有线调用qbuffer
            for line in self.lines:
                line.qbuffer()

        # 如果被调用，其下的任何东西都必须保存
        # 为所有指标调用qbuffer，强制savemem=1
        for obj in self._lineiterators[self.IndType]:
            obj.qbuffer(savemem=1)

        # 告诉数据源调整缓冲区到最小周期
        for data in self.datas:
            data.minbuffer(self._minperiod)


# 这3个子类可以用于在LineIterator内部甚至外部（如在LineObservers中）
# 识别3个子分支的目的，而不会产生循环导入引用
# This 3 subclasses can be used for identification purposes within LineIterator
# or even outside (like in LineObservers)
# for the 3 subbranches without generating circular import references

class DataAccessor(LineIterator):
    # DataAccessor类：LineIterator的子类，用于访问数据
    # 定义数据价格的常量引用
    PriceClose = DataSeries.Close  # 收盘价
    PriceLow = DataSeries.Low  # 最低价
    PriceHigh = DataSeries.High  # 最高价
    PriceOpen = DataSeries.Open  # 开盘价
    PriceVolume = DataSeries.Volume  # 成交量
    PriceOpenInteres = DataSeries.OpenInterest  # 未平仓合约数
    PriceDateTime = DataSeries.DateTime  # 日期时间


class IndicatorBase(DataAccessor):
    # IndicatorBase类：DataAccessor的子类，作为所有指标的基类
    pass


class ObserverBase(DataAccessor):
    # ObserverBase类：DataAccessor的子类，作为所有观察者的基类
    pass


class StrategyBase(DataAccessor):
    # StrategyBase类：DataAccessor的子类，作为所有策略的基类
    pass


# 用于耦合可能具有不同长度的线/lineiterators的实用类
# 仅当runonce=False传递给Cerebro时才能工作
# Utility class to couple lines/lineiterators which may have different lengths
# Will only work when runonce=False is passed to Cerebro

class SingleCoupler(LineActions):
    # SingleCoupler类：LineActions的子类，用于耦合单条线
    
    def __init__(self, cdata, clock=None):
        # 初始化方法：设置数据源、时钟和初始值
        super(SingleCoupler, self).__init__()
        # 设置时钟，如果未提供则使用所有者
        self._clock = clock if clock is not None else self._owner

        # 保存数据源
        self.cdata = cdata
        # 初始化数据长度
        self.dlen = 0
        # 初始化值为NaN
        self.val = float('NaN')

    def next(self):
        # next方法：处理下一个数据点
        if len(self.cdata) > self.dlen:
            # 如果数据源长度大于当前长度，更新值和长度
            self.val = self.cdata[0]  # 获取当前值
            self.dlen += 1  # 增加长度计数

        # 将当前值写入自己的当前位置
        self[0] = self.val


class MultiCoupler(LineIterator):
    # MultiCoupler类：LineIterator的子类，用于耦合多条线
    _ltype = LineIterator.IndType  # 类型设置为指标

    def __init__(self):
        # 初始化方法：设置数据长度和值
        super(MultiCoupler, self).__init__()
        # 初始化数据长度
        self.dlen = 0
        # 获取线的数量（使用fullsize作为线数量的快捷方式）
        self.dsize = self.fullsize()  # shorcut for number of lines
        # 为每条线创建初始值为NaN的列表
        self.dvals = [float('NaN')] * self.dsize

    def next(self):
        # next方法：处理下一个数据点
        if len(self.data) > self.dlen:
            # 如果数据源长度大于当前长度，更新值和长度
            self.dlen += 1  # 增加长度计数

            # 获取每条线的当前值
            for i in range(self.dsize):
                self.dvals[i] = self.data.lines[i][0]

        # 将每条线的值写入自己的当前位置
        for i in range(self.dsize):
            self.lines[i][0] = self.dvals[i]


def LinesCoupler(cdata, clock=None, **kwargs):
    # LinesCoupler函数：根据数据类型创建适当的耦合器
    if isinstance(cdata, LineSingle):
        # 如果是单条线，返回SingleCoupler实例
        return SingleCoupler(cdata, clock)  # return for single line

    # 在创建之前复制重要结构
    cdatacls = cdata.__class__  # copy important structures before creation
    try:
        # 尝试增加计数器，用于生成唯一的类名
        LinesCoupler.counter += 1  # counter for unique class name
    except AttributeError:
        # 如果计数器不存在，初始化它
        LinesCoupler.counter = 0

    # 准备一个MultiCoupler子类
    # 创建唯一的类名
    nclsname = str('LinesCoupler_%d' % LinesCoupler.counter)
    # 使用type创建新的类
    ncls = type(nclsname, (MultiCoupler,), {})
    # 获取当前模块
    thismod = sys.modules[LinesCoupler.__module__]
    # 将新类添加到模块
    setattr(thismod, ncls.__name__, ncls)
    
    # 替换lines等属性，获得合理的克隆
    ncls.lines = cdatacls.lines  # 复制线定义
    ncls.params = cdatacls.params  # 复制参数
    ncls.plotinfo = cdatacls.plotinfo  # 复制绘图信息
    ncls.plotlines = cdatacls.plotlines  # 复制线绘图信息

    # 实例化
    obj = ncls(cdata, **kwargs)  # instantiate
    
    # 在这里设置时钟，以避免被LineIterator背景扫描代码解释为数据
    # The clock is set here to avoid it being interpreted as a data by the
    # LineIterator background scanning code
    if clock is None:
        # 如果没有提供时钟，尝试从cdata获取
        clock = getattr(cdata, '_clock', None)
        if clock is not None:
            # 如果有时钟，检查它是否有自己的时钟
            nclock = getattr(clock, '_clock', None)
            if nclock is not None:
                # 如果有，使用它的时钟
                clock = nclock
            else:
                # 否则，检查它是否有data属性
                nclock = getattr(clock, 'data', None)
                if nclock is not None:
                    # 如果有，使用data作为时钟
                    clock = nclock

        if clock is None:
            # 如果仍然没有时钟，使用obj的所有者
            clock = obj._owner

    # 设置时钟
    obj._clock = clock
    # 返回对象
    return obj


# 为"单线"线添加别名（看起来更合理）
# Add an alias (which seems a lot more sensible for "Single Line" lines
LineCoupler = LinesCoupler
