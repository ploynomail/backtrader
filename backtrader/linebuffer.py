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

.. module:: linebuffer

Classes that hold the buffer for a *line* and can operate on it
with appends, forwarding, rewinding, resetting and other

.. moduleauthor:: Daniel Rodriguez

'''
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import array
import collections
import datetime
from itertools import islice
import math

from .utils.py3 import range, with_metaclass, string_types

from .lineroot import LineRoot, LineSingle, LineMultiple
from . import metabase
from .utils import num2date, time2num


NAN = float('NaN')


class LineBuffer(LineSingle):
    '''
    LineBuffer定义了一个接口，用于管理类似"array.array"(或列表)的数据结构，
    其中索引0指向当前活跃的输入和输出项。
    
    正向索引（如[1], [2]等）用于获取过去的值（左侧数据）
    负向索引（如[-1], [-2]等）用于获取未来的值（如果数组已在右侧扩展）
    
    通过这种行为，在实体之间传递当前值时无需传递索引，因为当前值
    总是可以通过[0]访问。
    
    同样，存储"自身"产生的当前值也总是在索引0处完成。
    
    此外，还提供了移动指针的其他操作（home, forward, extend, rewind, 
    advance, getzero等）
    
    该类还可以保存与其他LineBuffer的"绑定"。当在此类中设置值时，
    也会在绑定的对象中设置该值。
    '''

    # 缓冲区模式枚举：UnBounded(0)表示无界缓冲区，QBuffer(1)表示有限队列缓冲区
    UnBounded, QBuffer = (0, 1)

    def __init__(self):
        '''
        初始化LineBuffer实例。
        设置self.lines为包含自身的列表，
        设置模式为无界(UnBounded)，
        创建空绑定列表，
        重置缓冲区状态，
        初始化时区为None。
        '''
        self.lines = [self]
        self.mode = self.UnBounded
        self.bindings = list()
        self.reset()
        self._tz = None

    def get_idx(self):
        '''
        获取当前逻辑索引值。
        逻辑索引表示当前活跃数据在缓冲区中的位置。
        
        返回：
            当前逻辑索引值
        '''
        return self._idx

    def set_idx(self, idx, force=False):
        '''
        设置逻辑索引值。
        
        参数：
            idx (int): 要设置的索引值
            force (bool): 是否强制设置，即使超出了QBuffer模式下的限制
            
        说明：
            - 在QBuffer模式下，如果已经达到缓冲区最后位置，则保持索引0不变
              （除非force=True）。这允许重采样操作：
              - forward添加一个位置，但第一个被丢弃，位置0保持不变
            - force参数支持回放功能，因为回放需要额外的bar来前后浮动
        '''
        if self.mode == self.QBuffer:
            if force or self._idx < self.lenmark:
                self._idx = idx
        else:  # default: UnBounded
            self._idx = idx

    # idx属性，通过getter/setter访问和修改逻辑索引
    idx = property(get_idx, set_idx)

    def reset(self):
        ''' 
        重置内部缓冲区结构和索引。
        
        根据缓冲区模式(UnBounded或QBuffer)创建适当的数组：
        - QBuffer模式：使用collections.deque创建有限长度队列
        - UnBounded模式：使用array.array('d')创建无界浮点数组
        
        同时重置计数器、索引和扩展值。
        '''
        if self.mode == self.QBuffer:
            self.array = collections.deque(maxlen=self.maxlen + self.extrasize)
            self.useislice = True
        else:
            self.array = array.array(str('d'))
            self.useislice = False

        self.lencount = 0
        self.idx = -1
        self.extension = 0

    def qbuffer(self, savemem=0, extrasize=0):
        '''
        将缓冲区设置为有限队列模式(QBuffer)。
        
        参数：
            savemem (int): 是否节省内存的标志(未使用)
            extrasize (int): 在最小周期基础上增加的额外大小
            
        说明：
            QBuffer模式会限制缓冲区大小，当新数据进入时，最旧的数据会被丢弃，
            有助于控制内存使用。
        '''
        self.mode = self.QBuffer
        self.maxlen = self._minperiod
        self.extrasize = extrasize
        self.lenmark = self.maxlen - (not self.extrasize)
        self.reset()

    def getindicators(self):
        '''
        获取与此LineBuffer关联的指标列表。
        
        返回：
            空列表 - 基类中默认没有关联指标
        '''
        return []

    def minbuffer(self, size):
        '''
        确保LineBuffer能够提供请求的最小大小。
        
        参数：
            size (int): 请求的最小缓冲区大小
        
        说明：
            - 在非QBuffer模式下，这总是成立的(除了起始数据填充前)
            - 在QBuffer模式下，如果当前缓冲区小于请求的大小，则调整缓冲区
        '''
        if self.mode != self.QBuffer or self.maxlen >= size:
            return

        self.maxlen = size
        self.lenmark = self.maxlen - (not self.extrasize)
        self.reset()

    def __len__(self):
        '''
        返回LineBuffer的逻辑长度。
        
        返回：
            lencount (int): 表示已处理的数据点数量
        '''
        return self.lencount

    def buflen(self):
        ''' 
        返回内部缓冲区当前能够持有的实际数据量。
        
        说明：
            内部缓冲区可能比实际存储的数据更长，以允许"前瞻"操作。
            此方法返回实际持有/可持有的数据量。
            
        返回：
            int: 缓冲区实际数据长度(排除扩展部分)
        '''
        return len(self.array) - self.extension

    def __getitem__(self, ago):
        '''
        重载索引访问运算符，使用相对于当前索引的偏移量访问数据。
        
        参数：
            ago (int): 相对于当前索引的偏移量，正值表示过去的数据，负值表示未来的数据
            
        返回：
            对应位置的数据值
        '''
        return self.array[self.idx + ago]

    def get(self, ago=0, size=1):
        ''' 
        返回相对于*ago*的数组切片。
        
        参数：
            ago (int): 数组中相对于当前索引的点，size将从这里开始计算
            size (int): 要返回的切片大小，可以是正数或负数
        
        说明：
            如果size为正数，*ago*标记切片的结束；
            如果size为负数，*ago*标记切片的开始。
        
        返回：
            底层缓冲区的一个切片
        '''
        if self.useislice:
            start = self.idx + ago - size + 1
            end = self.idx + ago + 1
            return list(islice(self.array, start, end))

        return self.array[self.idx + ago - size + 1:self.idx + ago + 1]

    def getzeroval(self, idx=0):
        ''' 
        返回相对于缓冲区真实零点的单个值。
        
        参数：
            idx (int): 相对于缓冲区真实起始位置的偏移量
        
        返回：
            指定位置的单个值
        '''
        return self.array[idx]

    def getzero(self, idx=0, size=1):
        ''' 
        返回相对于缓冲区真实零点的切片。
        
        参数：
            idx (int): 相对于缓冲区真实起始位置的偏移量
            size (int): 要返回的切片大小
        
        返回：
            底层缓冲区的一个切片
        '''
        if self.useislice:
            return list(islice(self.array, idx, idx + size))

        return self.array[idx:idx + size]

    def __setitem__(self, ago, value):
        ''' 
        重载索引赋值运算符，在位置"ago"设置值并执行所有关联的绑定。
        
        参数：
            ago (int): 相对于当前索引的偏移量
            value (变量): 要设置的值
        '''
        self.array[self.idx + ago] = value
        for binding in self.bindings:
            binding[ago] = value

    def set(self, value, ago=0):
        ''' 
        在位置"ago"设置值并执行所有关联的绑定。
        
        参数：
            value (变量): 要设置的值
            ago (int): 相对于当前索引的偏移量
        '''
        self.array[self.idx + ago] = value
        for binding in self.bindings:
            binding[ago] = value

    def home(self):
        ''' 
        将逻辑索引倒回到起始位置。
        
        说明：
            底层缓冲区保持不变，实际长度可以通过buflen获取。
            这个操作重置了索引和计数，通常用于重新开始处理。
        '''
        self.idx = -1
        self.lencount = 0

    def forward(self, value=NAN, size=1):
        ''' 
        向前移动逻辑索引并根据需要扩展缓冲区。
        
        参数：
            value (变量): 在新位置设置的值，默认为NAN(非数字)
            size (int): 扩展缓冲区的额外位置数量
            
        说明：
            此方法同时增加索引和数据长度计数，然后向数组添加新值。
        '''
        self.idx += size
        self.lencount += size

        for i in range(size):
            self.array.append(value)

    def backwards(self, size=1, force=False):
        ''' 
        向后移动逻辑索引并根据需要减少缓冲区。
        
        参数：
            size (int): 回退和减少缓冲区的位置数量
            force (bool): 是否强制回退，即使超出了QBuffer模式的限制
            
        说明：
            此方法减少索引和数据长度计数，然后从数组中移除值。
        '''
        self.set_idx(self._idx - size, force=force)
        self.lencount -= size
        for i in range(size):
            self.array.pop()

    def rewind(self, size=1):
        '''
        回退逻辑索引。
        
        参数：
            size (int): 要回退的位置数量
            
        说明：
            与backwards不同，rewind只减少索引和计数，不修改底层数组。
        '''
        self.idx -= size
        self.lencount -= size

    def advance(self, size=1):
        ''' 
        前进逻辑索引，不触及底层缓冲区。
        
        参数：
            size (int): 要前进的位置数量
            
        说明：
            与forward不同，advance只增加索引和计数，不向数组添加新值。
            这通常用于跳过某些数据点。
        '''
        self.idx += size
        self.lencount += size

    def extend(self, value=NAN, size=0):
        ''' 
        扩展底层数组，添加索引不会到达的位置。
        
        参数：
            value (变量): 在新位置设置的值，默认为NAN
            size (int): 扩展缓冲区的额外位置数量
            
        说明：
            目的是允许前瞻操作或能够在缓冲区"未来"设置值。
            此方法增加扩展计数并向数组添加值，但不移动逻辑索引。
        '''
        self.extension += size
        for i in range(size):
            self.array.append(value)

    def addbinding(self, binding):
        ''' 
        添加另一个行绑定。
        
        参数：
            binding (LineBuffer): 当此行设置值时必须同步设置值的另一行
            
        说明：
            绑定允许多个LineBuffer同步更新，确保数据一致性。
            同时也会更新绑定对象的最小周期，确保不早于自身的最小周期。
        '''
        self.bindings.append(binding)
        binding.updateminperiod(self._minperiod)

    def plot(self, idx=0, size=None):
        ''' 
        返回相对于缓冲区真实零点的切片，主要用于绘图。
        
        参数：
            idx (int): 相对于缓冲区真实起始位置的偏移量
            size (int): 要返回的切片大小，如果为None则返回整个缓冲区
        
        说明：
            这是getzero的变种，如果不指定size，则返回整个缓冲区，
            这通常是绘图的目的（所有数据都要绘制）。
            
        返回：
            底层缓冲区的一个切片
        '''
        return self.getzero(idx, size or len(self))

    def plotrange(self, start, end):
        '''
        返回指定范围的数据，用于绘图。
        
        参数：
            start (int): 起始索引
            end (int): 结束索引
            
        返回：
            指定范围内的数据切片
        '''
        if self.useislice:
            return list(islice(self.array, start, end))

        return self.array[start:end]

    def oncebinding(self):
        '''
        在"once"模式下执行绑定。
        
        说明：
            "once"模式是一次性处理整个数据集的模式，
            此方法将当前数组的值复制到所有绑定的数组中。
        '''
        larray = self.array
        blen = self.buflen()
        for binding in self.bindings:
            binding.array[0:blen] = larray[0:blen]

    def bind2lines(self, binding=0):
        '''
        存储与另一行的绑定。binding可以是索引或名称。
        
        参数：
            binding (int或str): 要绑定的行的索引或名称
            
        返回：
            self: 允许方法链式调用
        '''
        if isinstance(binding, string_types):
            line = getattr(self._owner.lines, binding)
        else:
            line = self._owner.lines[binding]

        self.addbinding(line)

        return self

    bind2line = bind2lines

    def __call__(self, ago=None):
        '''
        重载调用运算符，返回自身的延迟版本(LineDelay对象)或时间框架适配版本。
        
        参数：
            ago (默认: None): 
              - 如果ago为None或LineRoot实例，返回LineCoupler实例
              - 如果ago是其他值，假定为int，返回LineDelay对象
              
        返回：
            LineCoupler或LineDelay实例
        '''
        from .lineiterator import LineCoupler
        if ago is None or isinstance(ago, LineRoot):
            return LineCoupler(self, ago)

        return LineDelay(self, ago)

    def _makeoperation(self, other, operation, r=False, _ownerskip=None):
        '''
        创建双操作数操作。
        
        参数：
            other: 操作的另一个操作数
            operation: 要执行的操作函数
            r (bool): 是否为反向操作
            _ownerskip: 查找所有者时要跳过的对象
            
        返回：
            LinesOperation实例
        '''
        return LinesOperation(self, other, operation, r=r,
                              _ownerskip=_ownerskip)

    def _makeoperationown(self, operation, _ownerskip=None):
        '''
        创建单操作数操作。
        
        参数：
            operation: 要执行的操作函数
            _ownerskip: 查找所有者时要跳过的对象
            
        返回：
            LineOwnOperation实例
        '''
        return LineOwnOperation(self, operation, _ownerskip=_ownerskip)

    def _settz(self, tz):
        '''
        设置时区。
        
        参数：
            tz: 时区对象
        '''
        self._tz = tz

    def datetime(self, ago=0, tz=None, naive=True):
        '''
        返回指定时间点的日期时间对象。
        
        参数：
            ago (int): 相对于当前索引的偏移量
            tz: 时区，如果为None则使用self._tz
            naive (bool): 是否返回不包含时区信息的日期时间
            
        返回：
            datetime对象
        '''
        return num2date(self.array[self.idx + ago],
                        tz=tz or self._tz, naive=naive)

    def date(self, ago=0, tz=None, naive=True):
        '''
        返回指定时间点的日期部分。
        
        参数：
            ago (int): 相对于当前索引的偏移量
            tz: 时区，如果为None则使用self._tz
            naive (bool): 是否返回不包含时区信息的日期
            
        返回：
            date对象
        '''
        return num2date(self.array[self.idx + ago],
                        tz=tz or self._tz, naive=naive).date()

    def time(self, ago=0, tz=None, naive=True):
        '''
        返回指定时间点的时间部分。
        
        参数：
            ago (int): 相对于当前索引的偏移量
            tz: 时区，如果为None则使用self._tz
            naive (bool): 是否返回不包含时区信息的时间
            
        返回：
            time对象
        '''
        return num2date(self.array[self.idx + ago],
                        tz=tz or self._tz, naive=naive).time()

    def dt(self, ago=0):
        '''
        返回日期时间浮点数的数值日期部分。
        
        参数：
            ago (int): 相对于当前索引的偏移量
            
        返回：
            数值日期部分(整数)
        '''
        return math.trunc(self.array[self.idx + ago])

    def tm_raw(self, ago=0):
        '''
        返回日期时间浮点数的原始数值时间部分。
        
        参数：
            ago (int): 相对于当前索引的偏移量
            
        说明：
            此函数命名为raw是因为它获取小数部分时不将其转换为time对象，
            以避免日计数(编码的整数部分)的影响。
            
        返回：
            原始数值时间部分(小数)
        '''
        return math.modf(self.array[self.idx + ago])[0]

    def tm(self, ago=0):
        '''
        返回日期时间浮点数的数值时间部分。
        
        参数：
            ago (int): 相对于当前索引的偏移量
            
        说明：
            为避免精度错误，此函数将小数部分转换为datetime.time对象后再返回，
            这有助于避免比较时的精度错误。
            
        返回：
            数值时间部分
        '''
        return time2num(num2date(self.array[self.idx + ago]).time())

    def tm_lt(self, other, ago=0):
        '''
        比较当前时间是否小于指定时间。
        
        参数：
            other: 要比较的时间值
            ago (int): 相对于当前索引的偏移量
            
        说明：
            为了比较原始"tm"部分(编码日期时间的小数部分)与当前日期时间的tm，
            原始"tm"必须与当前"日"计数(整数部分)同步。
            
        返回：
            布尔值，表示比较结果
        '''
        dtime = self.array[self.idx + ago]
        tm, dt = math.modf(dtime)

        return dtime < (dt + other)

    def tm_le(self, other, ago=0):
        '''
        比较当前时间是否小于等于指定时间。
        
        参数：
            other: 要比较的时间值
            ago (int): 相对于当前索引的偏移量
            
        说明：
            同tm_lt，但比较运算符为小于等于。
            
        返回：
            布尔值，表示比较结果
        '''
        dtime = self.array[self.idx + ago]
        tm, dt = math.modf(dtime)

        return dtime <= (dt + other)

    def tm_eq(self, other, ago=0):
        '''
        比较当前时间是否等于指定时间。
        
        参数：
            other: 要比较的时间值
            ago (int): 相对于当前索引的偏移量
            
        说明：
            同tm_lt，但比较运算符为等于。
            
        返回：
            布尔值，表示比较结果
        '''
        dtime = self.array[self.idx + ago]
        tm, dt = math.modf(dtime)

        return dtime == (dt + other)

    def tm_gt(self, other, ago=0):
        '''
        比较当前时间是否大于指定时间。
        
        参数：
            other: 要比较的时间值
            ago (int): 相对于当前索引的偏移量
            
        说明：
            同tm_lt，但比较运算符为大于。
            
        返回：
            布尔值，表示比较结果
        '''
        dtime = self.array[self.idx + ago]
        tm, dt = math.modf(dtime)

        return dtime > (dt + other)

    def tm_ge(self, other, ago=0):
        '''
        比较当前时间是否大于等于指定时间。
        
        参数：
            other: 要比较的时间值
            ago (int): 相对于当前索引的偏移量
            
        说明：
            同tm_lt，但比较运算符为大于等于。
            
        返回：
            布尔值，表示比较结果
        '''
        dtime = self.array[self.idx + ago]
        tm, dt = math.modf(dtime)

        return dtime >= (dt + other)

    def tm2dtime(self, tm, ago=0):
        '''
        将给定的tm值转换为(ago个bar之前)数据时间的框架。
        
        参数：
            tm: 时间值
            ago (int): 相对于当前索引的偏移量
            
        说明：
            用于外部比较以避免精度错误。
            返回整数日期部分加上传入的时间值。
            
        返回：
            日期时间值
        '''
        return int(self.array[self.idx + ago]) + tm

    def tm2datetime(self, tm, ago=0):
        '''
        将给定的tm值转换为(ago个bar之前)数据时间的框架，并返回datetime对象。
        
        参数：
            tm: 时间值
            ago (int): 相对于当前索引的偏移量
            
        说明：
            用于外部比较以避免精度错误。
            将整数日期部分加上传入的时间值，然后转换为datetime对象。
            
        返回：
            datetime对象
        '''
        return num2date(int(self.array[self.idx + ago]) + tm)


class MetaLineActions(LineBuffer.__class__):
    '''
    Metaclass for Lineactions

    Scans the instance before init for LineBuffer (or parentclass LineSingle)
    instances to calculate the minperiod for this instance

    postinit it registers the instance to the owner (remember that owner has
    been found in the base Metaclass for LineRoot)
    '''
    _acache = dict()
    _acacheuse = False

    @classmethod
    def cleancache(cls):
        cls._acache = dict()

    @classmethod
    def usecache(cls, onoff):
        cls._acacheuse = onoff

    def __call__(cls, *args, **kwargs):
        if not cls._acacheuse:
            return super(MetaLineActions, cls).__call__(*args, **kwargs)

        # implement a cache to avoid duplicating lines actions
        ckey = (cls, tuple(args), tuple(kwargs.items()))  # tuples hashable
        try:
            return cls._acache[ckey]
        except TypeError:  # something not hashable
            return super(MetaLineActions, cls).__call__(*args, **kwargs)
        except KeyError:
            pass  # hashable but not in the cache

        _obj = super(MetaLineActions, cls).__call__(*args, **kwargs)
        return cls._acache.setdefault(ckey, _obj)

    def dopreinit(cls, _obj, *args, **kwargs):
        _obj, args, kwargs = \
            super(MetaLineActions, cls).dopreinit(_obj, *args, **kwargs)

        _obj._clock = _obj._owner  # default setting

        if isinstance(args[0], LineRoot):
            _obj._clock = args[0]

        # Keep a reference to the datas for buffer adjustment purposes
        _obj._datas = [x for x in args if isinstance(x, LineRoot)]

        # Do not produce anything until the operation lines produce something
        _minperiods = [x._minperiod for x in args if isinstance(x, LineSingle)]

        mlines = [x.lines[0] for x in args if isinstance(x, LineMultiple)]
        _minperiods += [x._minperiod for x in mlines]

        _minperiod = max(_minperiods or [1])

        # update own minperiod if needed
        _obj.updateminperiod(_minperiod)

        return _obj, args, kwargs

    def dopostinit(cls, _obj, *args, **kwargs):
        _obj, args, kwargs = \
            super(MetaLineActions, cls).dopostinit(_obj, *args, **kwargs)

        # register with _owner to be kicked later
        _obj._owner.addindicator(_obj)

        return _obj, args, kwargs


class PseudoArray(object):
    def __init__(self, wrapped):
        self.wrapped = wrapped

    def __getitem__(self, key):
        return self.wrapped

    @property
    def array(self):
        return self


class LineActions(with_metaclass(MetaLineActions, LineBuffer)):
    '''
    Base class derived from LineBuffer intented to defined the
    minimum interface to make it compatible with a LineIterator by
    providing operational _next and _once interfaces.

    The metaclass does the dirty job of calculating minperiods and registering
    '''

    _ltype = LineBuffer.IndType

    def getindicators(self):
        return []

    def qbuffer(self, savemem=0):
        super(LineActions, self).qbuffer(savemem=savemem)
        for data in self._datas:
            data.minbuffer(size=self._minperiod)

    @staticmethod
    def arrayize(obj):
        if isinstance(obj, LineRoot):
            if not isinstance(obj, LineSingle):
                obj = obj.lines[0]  # get 1st line from multiline
        else:
            obj = PseudoArray(obj)

        return obj

    def _next(self):
        clock_len = len(self._clock)
        if clock_len > len(self):
            self.forward()

        if clock_len > self._minperiod:
            self.next()
        elif clock_len == self._minperiod:
            # only called for the 1st value
            self.nextstart()
        else:
            self.prenext()

    def _once(self):
        self.forward(size=self._clock.buflen())
        self.home()

        self.preonce(0, self._minperiod - 1)
        self.oncestart(self._minperiod - 1, self._minperiod)
        self.once(self._minperiod, self.buflen())

        self.oncebinding()


def LineDelay(a, ago=0, **kwargs):
    if ago <= 0:
        return _LineDelay(a, ago, **kwargs)

    return _LineForward(a, ago, **kwargs)


def LineNum(num):
    return LineDelay(PseudoArray(num))


class _LineDelay(LineActions):
    '''
    Takes a LineBuffer (or derived) object and stores the value from
    "ago" periods effectively delaying the delivery of data
    '''
    def __init__(self, a, ago):
        super(_LineDelay, self).__init__()
        self.a = a
        self.ago = ago

        # Need to add the delay to the period. "ago" is 0 based and therefore
        # we need to pass and extra 1 which is the minimum defined period for
        # any data (which will be substracted inside addminperiod)
        self.addminperiod(abs(ago) + 1)

    def next(self):
        self[0] = self.a[self.ago]

    def once(self, start, end):
        # cache python dictionary lookups
        dst = self.array
        src = self.a.array
        ago = self.ago

        for i in range(start, end):
            dst[i] = src[i + ago]


class _LineForward(LineActions):
    '''
    Takes a LineBuffer (or derived) object and stores the value from
    "ago" periods from the future
    '''
    def __init__(self, a, ago):
        super(_LineForward, self).__init__()
        self.a = a
        self.ago = ago

        # Need to add the delay to the period. "ago" is 0 based and therefore
        # we need to pass and extra 1 which is the minimum defined period for
        # any data (which will be substracted inside addminperiod)
        # self.addminperiod(abs(ago) + 1)
        if ago > self.a._minperiod:
            self.addminperiod(ago - self.a._minperiod + 1)

    def next(self):
        self[-self.ago] = self.a[0]

    def once(self, start, end):
        # cache python dictionary lookups
        dst = self.array
        src = self.a.array
        ago = self.ago

        for i in range(start, end):
            dst[i - ago] = src[i]


class LinesOperation(LineActions):

    '''
    Holds an operation that operates on a two operands. Example: mul

    It will "next"/traverse the array applying the operation on the
    two operands and storing the result in self.

    To optimize the operations and avoid conditional checks the right
    next/once is chosen using the operation direction (normal or reversed)
    and the nature of the operands (LineBuffer vs non-LineBuffer)

    In the "once" operations "map" could be used as in:

        operated = map(self.operation, srca[start:end], srcb[start:end])
        self.array[start:end] = array.array(str(self.typecode), operated)

    No real execution time benefits were appreciated and therefore the loops
    have been kept in place for clarity (although the maps are not really
    unclear here)
    '''

    def __init__(self, a, b, operation, r=False):
        super(LinesOperation, self).__init__()

        self.operation = operation
        self.a = a  # always a linebuffer
        self.b = b

        self.r = r
        self.bline = isinstance(b, LineBuffer)
        self.btime = isinstance(b, datetime.time)
        self.bfloat = not self.bline and not self.btime

        if r:
            self.a, self.b = b, a

    def next(self):
        if self.bline:
            self[0] = self.operation(self.a[0], self.b[0])
        elif not self.r:
            if not self.btime:
                self[0] = self.operation(self.a[0], self.b)
            else:
                self[0] = self.operation(self.a.time(), self.b)
        else:
            self[0] = self.operation(self.a, self.b[0])

    def once(self, start, end):
        if self.bline:
            self._once_op(start, end)
        elif not self.r:
            if not self.btime:
                self._once_val_op(start, end)
            else:
                self._once_time_op(start, end)
        else:
            self._once_val_op_r(start, end)

    def _once_op(self, start, end):
        # cache python dictionary lookups
        dst = self.array
        srca = self.a.array
        srcb = self.b.array
        op = self.operation

        for i in range(start, end):
            dst[i] = op(srca[i], srcb[i])

    def _once_time_op(self, start, end):
        # cache python dictionary lookups
        dst = self.array
        srca = self.a.array
        srcb = self.b
        op = self.operation
        tz = self._tz

        for i in range(start, end):
            dst[i] = op(num2date(srca[i], tz=tz).time(), srcb)

    def _once_val_op(self, start, end):
        # cache python dictionary lookups
        dst = self.array
        srca = self.a.array
        srcb = self.b
        op = self.operation

        for i in range(start, end):
            dst[i] = op(srca[i], srcb)

    def _once_val_op_r(self, start, end):
        # cache python dictionary lookups
        dst = self.array
        srca = self.a
        srcb = self.b.array
        op = self.operation

        for i in range(start, end):
            dst[i] = op(srca, srcb[i])


class LineOwnOperation(LineActions):
    '''
    Holds an operation that operates on a single operand. Example: abs

    It will "next"/traverse the array applying the operation and storing
    the result in self
    '''
    def __init__(self, a, operation):
        super(LineOwnOperation, self).__init__()

        self.operation = operation
        self.a = a

    def next(self):
        self[0] = self.operation(self.a[0])

    def once(self, start, end):
        # cache python dictionary lookups
        dst = self.array
        srca = self.a.array
        op = self.operation

        for i in range(start, end):
            dst[i] = op(srca[i])
