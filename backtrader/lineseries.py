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
'''

.. module:: lineroot

在其中定义 LineSeries 和 Descriptors，用于同时保存多条线的类。
.. moduleauthor:: Daniel Rodriguez

'''
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import sys

from .utils.py3 import map, range, string_types, with_metaclass

from .linebuffer import LineBuffer, LineActions, LinesOperation, LineDelay, NAN
from .lineroot import LineRoot, LineSingle, LineMultiple
from .metabase import AutoInfoClass
from . import metabase


class LineAlias(object):
    ''' Descriptor class that store a line reference and returns that line
    from the owner

    Keyword Args:
        line (int): reference to the line that will be returned from
        owner's *lines* buffer

    As a convenience the __set__ method of the descriptor is used not set
    the *line* reference because this is a constant along the live of the
    descriptor instance, but rather to set the value of the *line* at the
    instant '0' (the current one)
    '''
    # LineAlias类：描述符类，存储线的引用并从拥有者返回该线
    # 用于创建线的别名，使得可以通过属性访问线对象

    def __init__(self, line):
        # 初始化方法，存储线的索引
        # line参数：要引用的线在owner's lines buffer中的索引
        self.line = line

    def __get__(self, obj, cls=None):
        # 描述符的获取方法
        # 当通过实例访问此描述符时返回obj.lines[self.line]
        return obj.lines[self.line]

    def __set__(self, obj, value):
        '''
        A line cannot be "set" once it has been created. But the values
        inside the line can be "set". This is achieved by adding a binding
        to the line inside "value"
        '''
        # 描述符的设置方法，一旦创建，线本身不能被"设置"，但线内的值可以通过绑定来设置

        if isinstance(value, LineMultiple):
            # 如果value是LineMultiple类型，取其第一条线
            value = value.lines[0]

        # If the now for sure, LineBuffer 'value' is not a LineActions the
        # binding below could kick-in too early in the chain writing the value
        # into a not yet "forwarded" line, effectively writing the value 1
        # index too early and breaking the functionality (all in next mode)
        # Hence the need to transform it into a LineDelay object of null delay
        # 如果value不是LineActions类型，需要将其转换为延迟为0的LineDelay对象
        # 这是为了防止绑定过早触发，导致值被写入尚未"前进"的线中，从而导致值被写入提前1个索引
        if not isinstance(value, LineActions):
            value = value(0)

        # 将value绑定到目标线上，实现值的设置
        value.addbinding(obj.lines[self.line])


class Lines(object):
    '''
    Defines an "array" of lines which also has most of the interface of
    a LineBuffer class (forward, rewind, advance...).

    This interface operations are passed to the lines held by self

    The class can autosubclass itself (_derive) to hold new lines keeping them
    in the defined order.
    '''
    # Lines类：定义一个线的"数组"，具有LineBuffer类的大部分接口（forward, rewind, advance等）
    # 这些接口操作会传递给self持有的线
    # 该类可以自动子类化自身(_derive)以保持新线的定义顺序

    # 类方法，返回基础线的元组
    _getlinesbase = classmethod(lambda cls: ())
    # 类方法，返回所有线的元组
    _getlines = classmethod(lambda cls: ())
    # 类方法，返回额外线的数量
    _getlinesextra = classmethod(lambda cls: 0)
    # 类方法，返回基础额外线的数量
    _getlinesextrabase = classmethod(lambda cls: 0)

    @classmethod
    def _derive(cls, name, lines, extralines, otherbases, linesoverride=False,
                lalias=None):
        '''
        Creates a subclass of this class with the lines of this class as
        initial input for the subclass. It will include num "extralines" and
        lines present in "otherbases"

        "name" will be used as the suffix of the final class name

        "linesoverride": if True the lines of all bases will be discarded and
        the baseclass will be the topmost class "Lines". This is intended to
        create a new hierarchy
        '''
        # _derive方法：创建该类的子类，使用该类的线作为子类的初始输入
        # 它将包含"extralines"和"otherbases"中存在的线
        # "name"将用作最终类名的后缀
        # "linesoverride"：如果为True，所有基类的线将被丢弃，基类将是最顶层的"Lines"类
        
        # 初始化其他基类的线和额外线
        obaseslines = ()
        obasesextralines = 0

        # 遍历其他基类，收集它们的线和额外线
        for otherbase in otherbases:
            if isinstance(otherbase, tuple):
                # 如果otherbase是元组，直接添加到obaseslines
                obaseslines += otherbase
            else:
                # 否则，获取otherbase的线和额外线
                obaseslines += otherbase._getlines()
                obasesextralines += otherbase._getlinesextra()

        if not linesoverride:
            # 如果不覆盖线，使用当前类和其他基类的线和额外线
            baselines = cls._getlines() + obaseslines
            baseextralines = cls._getlinesextra() + obasesextralines
        else:  # overriding lines, skip anything from baseclasses
            # 如果覆盖线，跳过基类中的任何内容
            baselines = ()
            baseextralines = 0

        # 合并所有线和额外线
        clslines = baselines + lines
        clsextralines = baseextralines + extralines
        lines2add = obaseslines + lines

        # str for Python 2/3 compatibility
        # 确定基类，如果linesoverride为True，使用Lines作为基类
        basecls = cls if not linesoverride else Lines

        # 创建新的类
        newcls = type(str(cls.__name__ + '_' + name), (basecls,), {})
        # 获取当前模块
        clsmodule = sys.modules[cls.__module__]
        # 设置新类的模块
        newcls.__module__ = cls.__module__
        # 将新类添加到模块中
        setattr(clsmodule, str(cls.__name__ + '_' + name), newcls)

        # 设置新类的基础线和所有线
        setattr(newcls, '_getlinesbase', classmethod(lambda cls: baselines))
        setattr(newcls, '_getlines', classmethod(lambda cls: clslines))

        # 设置新类的基础额外线和所有额外线
        setattr(newcls, '_getlinesextrabase',
                classmethod(lambda cls: baseextralines))
        setattr(newcls, '_getlinesextra',
                classmethod(lambda cls: clsextralines))

        # 计算起始线索引
        l2start = len(cls._getlines()) if not linesoverride else 0
        # 生成要添加的线的枚举，从l2start开始
        l2add = enumerate(lines2add, start=l2start)
        # 获取线别名字典
        l2alias = {} if lalias is None else lalias._getkwargsdefault()
        # 为每条要添加的线创建LineAlias描述符
        for line, linealias in l2add:
            if not isinstance(linealias, string_types):
                # 如果linealias不是字符串类型，假定它是元组或列表，取第一个元素作为名称
                linealias = linealias[0]

            # 创建LineAlias描述符并设置到新类中
            desc = LineAlias(line)  # keep a reference below
            setattr(newcls, linealias, desc)

        # Create extra aliases for the given name, checking if the names is in
        # l2alias (which is from the argument lalias and comes from the
        # directive 'linealias', hence the confusion here (the LineAlias come
        # from the directive 'lines')
        # 为给定名称创建额外的别名，检查名称是否在l2alias中
        for line, linealias in enumerate(newcls._getlines()):
            if not isinstance(linealias, string_types):
                # 如果linealias不是字符串类型，假定它是元组或列表，取第一个元素作为名称
                linealias = linealias[0]

            # 创建LineAlias描述符
            desc = LineAlias(line)  # keep a reference below
            # 如果linealias在l2alias中，为其创建额外的别名
            if linealias in l2alias:
                extranames = l2alias[linealias]
                if isinstance(linealias, string_types):
                    extranames = [extranames]

                # 为每个额外名称设置描述符
                for ename in extranames:
                    setattr(newcls, ename, desc)

        # 返回新创建的类
        return newcls

    @classmethod
    def _getlinealias(cls, i):
        '''
        Return the alias for a line given the index
        '''
        # 根据索引返回线的别名
        lines = cls._getlines()
        if i >= len(lines):
            # 如果索引超出范围，返回空字符串
            return ''
        linealias = lines[i]
        return linealias

    @classmethod
    def getlinealiases(cls):
        # 获取所有线的别名
        return cls._getlines()

    def itersize(self):
        # 返回有效线大小的迭代器
        return iter(self.lines[0:self.size()])

    def __init__(self, initlines=None):
        '''
        Create the lines recording during "_derive" or else use the
        provided "initlines"
        '''
        # 初始化方法：创建在"_derive"期间记录的线，或者使用提供的"initlines"
        
        # 初始化lines列表
        self.lines = list()
        # 为每个线定义创建一个LineBuffer
        for line, linealias in enumerate(self._getlines()):
            kwargs = dict()
            self.lines.append(LineBuffer(**kwargs))

        # Add the required extralines
        # 添加额外的线
        for i in range(self._getlinesextra()):
            if not initlines:
                # 如果没有初始线，创建新的LineBuffer
                self.lines.append(LineBuffer())
            else:
                # 否则使用提供的初始线
                self.lines.append(initlines[i])

    def __len__(self):
        '''
        Proxy line operation
        '''
        # 返回第一条线的长度
        return len(self.lines[0])

    def size(self):
        # 返回常规线的数量（不包括额外线）
        return len(self.lines) - self._getlinesextra()

    def fullsize(self):
        # 返回所有线的总数（包括额外线）
        return len(self.lines)

    def extrasize(self):
        # 返回额外线的数量
        return self._getlinesextra()

    def __getitem__(self, line):
        '''
        Proxy line operation
        '''
        # 根据索引获取线
        return self.lines[line]

    def get(self, ago=0, size=1, line=0):
        '''
        Proxy line operation
        '''
        # 获取指定线在指定位置的值
        return self.lines[line].get(ago, size=size)

    def __setitem__(self, line, value):
        '''
        Proxy line operation
        '''
        # 设置线的值
        setattr(self, self._getlinealias(line), value)

    def forward(self, value=NAN, size=1):
        '''
        Proxy line operation
        '''
        # 将所有线向前移动
        for line in self.lines:
            line.forward(value, size=size)

    def backwards(self, size=1, force=False):
        '''
        Proxy line operation
        '''
        # 将所有线向后移动
        for line in self.lines:
            line.backwards(size, force=force)

    def rewind(self, size=1):
        '''
        Proxy line operation
        '''
        # 将所有线回滚
        for line in self.lines:
            line.rewind(size)

    def extend(self, value=NAN, size=0):
        '''
        Proxy line operation
        '''
        # 扩展所有线
        for line in self.lines:
            line.extend(value, size)

    def reset(self):
        '''
        Proxy line operation
        '''
        # 重置所有线
        for line in self.lines:
            line.reset()

    def home(self):
        '''
        Proxy line operation
        '''
        # 将所有线回到起始位置
        for line in self.lines:
            line.home()

    def advance(self, size=1):
        '''
        Proxy line operation
        '''
        # 将所有线前进
        for line in self.lines:
            line.advance(size)

    def buflen(self, line=0):
        '''
        Proxy line operation
        '''
        # 返回指定线的缓冲区长度
        return self.lines[line].buflen()


class MetaLineSeries(LineMultiple.__class__):
    '''
    Dirty job manager for a LineSeries

      - During __new__ (class creation), it reads "lines", "plotinfo",
        "plotlines" class variable definitions and turns them into
        Classes of type Lines or AutoClassInfo (plotinfo/plotlines)

      - During "new" (instance creation) the lines/plotinfo/plotlines
        classes are substituted in the instance with instances of the
        aforementioned classes and aliases are added for the "lines" held
        in the "lines" instance

        Additionally and for remaining kwargs, these are matched against
        args in plotinfo and if existent are set there and removed from kwargs

        Remember that this Metaclass has a MetaParams (from metabase)
        as root class and therefore "params" defined for the class have been
        removed from kwargs at an earlier state
    '''
    # MetaLineSeries：LineSeries的元类，负责管理LineSeries的"脏活"
    # 在__new__（类创建）期间，它读取"lines"、"plotinfo"、"plotlines"类变量定义
    # 并将它们转换为Lines或AutoClassInfo（plotinfo/plotlines）类型的类
    # 在"new"（实例创建）期间，lines/plotinfo/plotlines类在实例中被替换为上述类的实例
    # 并为"lines"实例中持有的"lines"添加别名

    def __new__(meta, name, bases, dct):
        '''
        Intercept class creation, identifiy lines/plotinfo/plotlines class
        attributes and create corresponding classes for them which take over
        the class attributes
        '''
        # 拦截类创建，识别lines/plotinfo/plotlines类属性并为它们创建相应的类

        # 获取别名，不要留给子类
        aliases = dct.setdefault('alias', ())
        aliased = dct.setdefault('aliased', '')

        # 从类创建中删除线定义（如果有）
        linesoverride = dct.pop('linesoverride', False)
        newlines = dct.pop('lines', ())
        extralines = dct.pop('extralines', 0)

        # 删除新的linealias定义（如果有）
        newlalias = dict(dct.pop('linealias', {}))

        # 删除新的plotinfo/plotlines定义（如果有）
        newplotinfo = dict(dct.pop('plotinfo', {}))
        newplotlines = dict(dct.pop('plotlines', {}))

        # 创建类 - 引入任何现有的"lines"
        cls = super(MetaLineSeries, meta).__new__(meta, name, bases, dct)

        # 在创建线之前检查线别名
        lalias = getattr(cls, 'linealias', AutoInfoClass)
        oblalias = [x.linealias for x in bases[1:] if hasattr(x, 'linealias')]
        cls.linealias = la = lalias._derive('la_' + name, newlalias, oblalias)

        # 获取实际的线或默认值
        lines = getattr(cls, 'lines', Lines)

        # 用我们的名称和新线创建线类的子类并将其放入类中
        morebaseslines = [x.lines for x in bases[1:] if hasattr(x, 'lines')]
        cls.lines = lines._derive(name, newlines, extralines, morebaseslines,
                                  linesoverride, lalias=la)

        # 从基类获取plotinfo/plotlines的副本（在类中创建或设置默认值）
        plotinfo = getattr(cls, 'plotinfo', AutoInfoClass)
        plotlines = getattr(cls, 'plotlines', AutoInfoClass)

        # 创建plotinfo/plotlines子类并将其设置在类中
        morebasesplotinfo = \
            [x.plotinfo for x in bases[1:] if hasattr(x, 'plotinfo')]
        cls.plotinfo = plotinfo._derive('pi_' + name, newplotinfo,
                                        morebasesplotinfo)

        # 在添加新线之前，如果没有plotlineinfo，添加默认值
        for line in newlines:
            newplotlines.setdefault(line, dict())

        # 创建plotlines子类
        morebasesplotlines = \
            [x.plotlines for x in bases[1:] if hasattr(x, 'plotlines')]
        cls.plotlines = plotlines._derive(
            'pl_' + name, newplotlines, morebasesplotlines, recurse=True)

        # 创建声明的类别名（没有修改的子类）
        for alias in aliases:
            # 创建新的类字典，包含文档、模块和别名指向的类
            newdct = {'__doc__': cls.__doc__,
                      '__module__': cls.__module__,
                      'aliased': cls.__name__}

            if not isinstance(alias, string_types):
                # 如果传递的是元组或列表，第1个是名称，第2个是plotname
                aliasplotname = alias[1]
                alias = alias[0]
                newdct['plotinfo'] = dict(plotname=aliasplotname)

            # 创建新的类
            newcls = type(str(alias), (cls,), newdct)
            clsmodule = sys.modules[cls.__module__]
            # 将新类添加到模块
            setattr(clsmodule, alias, newcls)

        # 返回类
        return cls

    def donew(cls, *args, **kwargs):
        '''
        Intercept instance creation, take over lines/plotinfo/plotlines
        class attributes by creating corresponding instance variables and add
        aliases for "lines" and the "lines" held within it
        '''
        # 拦截实例创建，通过创建相应的实例变量接管lines/plotinfo/plotlines类属性
        # 并为"lines"和其中包含的"lines"添加别名
        
        # _obj.plotinfo覆盖类中的plotinfo（类）定义
        plotinfo = cls.plotinfo()

        # 设置plotinfo属性
        for pname, pdef in cls.plotinfo._getitems():
            setattr(plotinfo, pname, kwargs.pop(pname, pdef))

        # 创建对象并设置参数
        _obj, args, kwargs = super(MetaLineSeries, cls).donew(*args, **kwargs)

        # 在类中设置plotinfo成员
        _obj.plotinfo = plotinfo

        # _obj.lines覆盖类中的lines（类）定义
        _obj.lines = cls.lines()

        # _obj.plotlines覆盖类中的plotinfo（类）定义
        _obj.plotlines = cls.plotlines()

        # 为lines和lines类本身添加别名
        _obj.l = _obj.lines
        if _obj.lines.fullsize():
            _obj.line = _obj.lines[0]

        # 为每条线添加别名
        for l, line in enumerate(_obj.lines):
            setattr(_obj, 'line_%s' % l, _obj._getlinealias(l))
            setattr(_obj, 'line_%d' % l, line)
            setattr(_obj, 'line%d' % l, line)

        # 在__init__之前已经设置了参数值
        return _obj, args, kwargs


class LineSeries(with_metaclass(MetaLineSeries, LineMultiple)):
    # LineSeries类：基础线系列类，使用MetaLineSeries作为元类
    
    # 绘图信息字典
    plotinfo = dict(
        plot=True,  # 是否绘制
        plotmaster=None,  # 绘图主图
        legendloc=None,  # 图例位置
    )

    # 是否支持CSV输出
    csv = True

    @property
    def array(self):
        # 返回第一条线的数组
        return self.lines[0].array

    def __getattr__(self, name):
        # 通过名称直接引用线，如果在此对象中找不到属性
        # 如果我们在此对象中设置了一个属性，它将在我们到达这里之前被找到
        return getattr(self.lines, name)

    def __len__(self):
        # 返回线的数量
        return len(self.lines)

    def __getitem__(self, key):
        # 获取第一条线的值
        return self.lines[0][key]

    def __setitem__(self, key, value):
        # 设置线的值
        setattr(self.lines, self.lines._getlinealias(key), value)

    def __init__(self, *args, **kwargs):
        # 如果有任何args, kwargs到达这里，说明有问题
        # 定义__init__保证在lineiterator中findbases中存在im_func
        # 因为object.__init__没有im_func（object有slots）
        super(LineSeries, self).__init__()
        pass

    def plotlabel(self):
        # 返回绘图标签
        label = self.plotinfo.plotname or self.__class__.__name__
        sublabels = self._plotlabel()
        if sublabels:
            for i, sublabel in enumerate(sublabels):
                # if isinstance(sublabel, LineSeries): ## DOESN'T WORK ???
                if hasattr(sublabel, 'plotinfo'):
                    try:
                        s = sublabel.plotinfo.plotname
                    except:
                        s = ''

                    sublabels[i] = s or sublabel.__name__

            # 将子标签添加到主标签中
            label += ' (%s)' % ', '.join(map(str, sublabels))
        return label

    def _plotlabel(self):
        # 获取参数值作为绘图子标签
        return self.params._getvalues()

    def _getline(self, line, minusall=False):
        # 获取指定的线对象
        if isinstance(line, string_types):
            # 如果line是字符串类型，通过名称获取
            lineobj = getattr(self.lines, line)
        else:
            if line == -1:  # restore original api behavior - default -> 0
                if minusall:  # minus means ... all lines
                    # 如果minusall为True，-1表示所有线
                    return None
                # 否则默认为第一条线
                line = 0
            lineobj = self.lines[line]

        return lineobj

    def __call__(self, ago=None, line=-1):
        '''Returns either a delayed verison of itself in the form of a
        LineDelay object or a timeframe adapting version with regards to a ago

        Param: ago (default: None)

          If ago is None or an instance of LineRoot (a lines object) the
          returned valued is a LineCoupler instance

          If ago is anything else, it is assumed to be an int and a LineDelay
          object will be returned

        Param: line (default: -1)
          If a LinesCoupler will be returned ``-1`` means to return a
          LinesCoupler which adapts all lines of the current LineMultiple
          object. Else the appropriate line (referenced by name or index) will
          be LineCoupled

          If a LineDelay object will be returned, ``-1`` is the same as ``0``
          (to retain compatibility with the previous default value of 0). This
          behavior will change to return all existing lines in a LineDelayed
          form

          The referenced line (index or name) will be LineDelayed
        '''
        # 返回延迟版本或时间框架适配版本
        from .lineiterator import LinesCoupler  # avoid circular import

        if ago is None or isinstance(ago, LineRoot):
            # 如果ago为None或LineRoot实例，返回LinesCoupler实例
            args = [self, ago]
            lineobj = self._getline(line, minusall=True)
            if lineobj is not None:
                args[0] = lineobj

            return LinesCoupler(*args, _ownerskip=self)

        # else -> assume type(ago) == int -> return LineDelay object
        # 否则假定ago是int类型，返回LineDelay对象
        return LineDelay(self._getline(line), ago, _ownerskip=self)

    # The operations below have to be overriden to make sure subclasses can
    # reach them using "super" which will not call __getattr__ and
    # LineSeriesStub (see below) already uses super
    # 下面的操作必须被重写，以确保子类可以使用"super"访问它们
    def forward(self, value=NAN, size=1):
        # 将线向前移动
        self.lines.forward(value, size)

    def backwards(self, size=1, force=False):
        # 将线向后移动
        self.lines.backwards(size, force=force)

    def rewind(self, size=1):
        # 回滚线
        self.lines.rewind(size)

    def extend(self, value=NAN, size=0):
        # 扩展线
        self.lines.extend(value, size)

    def reset(self):
        # 重置线
        self.lines.reset()

    def home(self):
        # 将线回到起始位置
        self.lines.home()

    def advance(self, size=1):
        # 将线前进
        self.lines.advance(size)


class LineSeriesStub(LineSeries):
    '''Simulates a LineMultiple object based on LineSeries from a single line

    The index management operations are overriden to take into account if the
    line is a slave, ie:

      - The line reference is a line from many in a LineMultiple object
      - Both the LineMultiple object and the Line are managed by the same
        object

    Were slave not to be taken into account, the individual line would for
    example be advanced twice:

      - Once under when the LineMultiple object is advanced (because it
        advances all lines it is holding
      - Again as part of the regular management of the object holding it
    '''
    # LineSeriesStub类：基于来自单线的LineSeries模拟LineMultiple对象
    # 索引管理操作被重写以考虑线是否为从属线
    # 如果不考虑从属关系，单个线会被前进两次：
    # - 一次是当LineMultiple对象前进时（因为它会前进它所持有的所有线）
    # - 另一次是作为持有它的对象的常规管理的一部分

    # 额外的线数量
    extralines = 1

    def __init__(self, line, slave=False):
        # 初始化方法，创建LineSeriesStub对象
        # 使用提供的线初始化lines
        self.lines = self.__class__.lines(initlines=[line])
        # 提供一个找到线所有者的机会（至少用于绘图）
        self.owner = self._owner = line._owner
        # 设置最小周期
        self._minperiod = line._minperiod
        # 设置从属状态
        self.slave = slave

    # 只有在对象不是从属对象时才执行以下操作
    def forward(self, value=NAN, size=1):
        if not self.slave:
            super(LineSeriesStub, self).forward(value, size)

    def backwards(self, size=1, force=False):
        if not self.slave:
            super(LineSeriesStub, self).backwards(size, force=force)

    def rewind(self, size=1):
        if not self.slave:
            super(LineSeriesStub, self).rewind(size)

    def extend(self, value=NAN, size=0):
        if not self.slave:
            super(LineSeriesStub, self).extend(value, size)

    def reset(self):
        if not self.slave:
            super(LineSeriesStub, self).reset()

    def home(self):
        if not self.slave:
            super(LineSeriesStub, self).home()

    def advance(self, size=1):
        if not self.slave:
            super(LineSeriesStub, self).advance(size)

    def qbuffer(self):
        if not self.slave:
            super(LineSeriesStub, self).qbuffer()

    def minbuffer(self, size):
        if not self.slave:
            super(LineSeriesStub, self).minbuffer(size)


def LineSeriesMaker(arg, slave=False):
    # LineSeriesMaker函数：根据输入参数创建LineSeriesStub或返回已有的LineSeries
    if isinstance(arg, LineSeries):
        # 如果arg已经是LineSeries实例，直接返回
        return arg

    # 否则，创建一个LineSeriesStub实例
    return LineSeriesStub(arg, slave=slave)
