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

定义基类 LineRoot 和基类 LineSingle/LineMultiple 用于定义实际操作类的接口和层次结构

.. moduleauthor:: Daniel Rodriguez

'''
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import operator

from .utils.py3 import range, with_metaclass

from . import metabase


class MetaLineRoot(metabase.MetaParams):
    '''
    元类 `MetaLineRoot` 用于在对象创建时自动寻找并设置该对象的“所有者”。
    使用案例：
    - 当一个类继承了 `MetaLineRoot` 时，在实例化该类时，会通过 `donew` 方法自动找到并设置 `_owner` 属性。
    - 例如：
        class MyLine(LineRoot):
            pass

        my_line = MyLine()
        # 在实例化过程中，`_owner` 会被自动设置为 `LineMultiple` 或其他指定的类。

    过程解释：
    1. `donew` 方法在对象创建时被调用。
    2. 它通过 `metabase.findowner` 方法找到对象的所有者。
    3. 将找到的所有者存储在 `_owner` 属性中。
    '''

    def donew(cls, *args, **kwargs):
        # 调用父类的 `donew` 方法，创建对象并返回对象、args 和 kwargs
        _obj, args, kwargs = super(MetaLineRoot, cls).donew(*args, **kwargs)

        # 从 kwargs 中提取 `_ownerskip` 参数，用于跳过指定的调用栈层级
        ownerskip = kwargs.pop('_ownerskip', None)

        # 使用 `metabase.findowner` 方法找到对象的所有者
        # `_OwnerCls` 默认为 `LineMultiple`，如果未指定则使用 `LineMultiple`
        _obj._owner = metabase.findowner(
            _obj,  # 当前对象
            _obj._OwnerCls or LineMultiple,  # 所有者的类
            skip=ownerskip  # 跳过的调用栈层级
        )

        # 返回创建的对象以及剩余的 args 和 kwargs
        return _obj, args, kwargs


class LineRoot(with_metaclass(MetaLineRoot, object)):
    '''
    LineRoot 类是 backtrader 中所有线对象(Line Objects)的基类，定义了线对象的通用接口和功能。
    主要提供以下功能：
    - 周期管理：控制数据计算所需的最小周期
    - 迭代管理：定义数据处理的迭代方法
    - 操作管理：提供各种操作符的重载实现，支持单操作数和双操作数
    - 比较操作符：实现富比较操作
    '''
    # 持有该线对象的所有者类，默认为 None，会在实例化时由元类 MetaLineRoot 设置
    _OwnerCls = None
    # 最小周期，默认为 1，表示至少需要 1 个数据点才能进行计算
    _minperiod = 1
    # 操作阶段，默认为 1，用于区分不同阶段的操作行为
    _opstage = 1

    # 定义指标类型、策略类型和观察器类型的枚举值
    IndType, StratType, ObsType = range(3)

    def _stage1(self):
        '''
        将操作阶段设置为 1：阶段 1 通常用于初始化阶段，这时线对象正在构建中，操作会创建新的指标或线对象。
        '''
        self._opstage = 1

    def _stage2(self):
        '''
        将操作阶段设置为 2：阶段 2 通常是运行时阶段，这时线对象已经构建完成，操作会直接返回数值结果。
        '''
        self._opstage = 2

    def _operation(self, other, operation, r=False, intify=False):
        '''
        根据当前操作阶段，执行相应的操作处理。
        
        参数:
            other: 操作的另一个对象
            operation: 要执行的操作函数
            r: 是否为反向操作
            intify: 是否将结果转换为整数
            
        返回:
            根据当前阶段，调用相应的操作处理方法
        '''
        if self._opstage == 1:
            return self._operation_stage1(
                other, operation, r=r, intify=intify)

        return self._operation_stage2(other, operation, r=r)

    def _operationown(self, operation):
        '''
        处理单操作数操作（如 abs, neg 等）。
        根据当前阶段调用相应的单操作数处理方法。
        
        参数:operation: 要执行的单操作数操作函数
            
        返回:根据当前阶段，调用相应的单操作数操作方法
        '''
        if self._opstage == 1:
            return self._operationown_stage1(operation)

        return self._operationown_stage2(operation)

    def qbuffer(self, savemem=0):
        '''
        修改线对象以实现最小尺寸队列缓冲区方案。
        
        参数: savemem: 是否节省内存
            
        注意:这是一个抽象方法，需要由子类实现。
        '''
        raise NotImplementedError

    def minbuffer(self, size):
        '''
        接收缓冲区最小大小的通知。
        
        参数: size: 缓冲区的最小大小
            
        注意: 这是一个抽象方法，需要由子类实现。
        '''
        raise NotImplementedError

    def setminperiod(self, minperiod):
        '''
        直接设置最小周期。例如，可以被策略使用，避免等待所有指标生成值。
        
        参数: minperiod: 要设置的最小周期值
        '''
        self._minperiod = minperiod

    def updateminperiod(self, minperiod):
        '''
        如果需要，更新最小周期。如果传入的最小周期大于当前值，则进行更新。
        
        参数: minperiod: 要与当前最小周期比较的值
        '''
        self._minperiod = max(self._minperiod, minperiod)

    def addminperiod(self, minperiod):
        '''
        添加最小周期到自身的最小周期。
        
        参数: minperiod: 要添加的最小周期值
            
        注意: 这是一个抽象方法，需要由子类实现。
        '''
        raise NotImplementedError

    def incminperiod(self, minperiod):
        '''
        直接增加最小周期，没有其他考虑因素。
        
        参数: minperiod: 要增加的最小周期值
            
        注意: 这是一个抽象方法，需要由子类实现。
        '''
        raise NotImplementedError

    def prenext(self):
        '''
        在最小周期阶段的迭代过程中被调用。
        即在数据点数量未达到最小周期要求时调用。
        默认不执行任何操作。
        '''
        pass

    def nextstart(self):
        '''
        当最小周期阶段结束，对第一个后期值调用。
        只被调用一次，默认自动调用 next 方法。
        '''
        self.next()

    def next(self):
        '''
        当数据点数量已经满足最小周期要求时，用于计算值。
        默认不执行任何操作，由子类实现具体计算逻辑。
        '''
        pass

    def preonce(self, start, end):
        '''
        在 "once" 迭代的最小周期阶段被调用。
        用于一次性批量处理的预处理阶段。
        
        参数:
            start: 起始索引
            end: 结束索引
        '''
        pass

    def oncestart(self, start, end):
        '''
        当 "once" 最小周期阶段结束后，对第一个后期值调用。
        只被调用一次，默认自动调用 once 方法。
        
        参数:
            start: 起始索引
            end: 结束索引
        '''
        self.once(start, end)

    def once(self, start, end):
        '''
        当最小周期要求满足后，用于一次性批量计算值。
        
        参数:
            start: 起始索引
            end: 结束索引
        '''
        pass

    # 算术运算符相关方法
    def _makeoperation(self, other, operation, r=False, _ownerskip=None):
        '''
        创建与其他对象的双操作数操作。
        
        参数:
            other: 操作的另一个对象
            operation: 要执行的操作函数
            r: 是否为反向操作
            _ownerskip: 查找所有者时要跳过的对象
            
        注意:
            这是一个抽象方法，需要由子类实现。
        '''
        raise NotImplementedError

    def _makeoperationown(self, operation, _ownerskip=None):
        '''
        创建单操作数操作。
        
        参数:
            operation: 要执行的操作函数
            _ownerskip: 查找所有者时要跳过的对象
            
        注意:
            这是一个抽象方法，需要由子类实现。
        '''
        raise NotImplementedError

    def _operationown_stage1(self, operation):
        '''
        阶段1中的单操作数操作，操作数是"self"。
        
        参数:
            operation: 要执行的操作函数
            
        返回:
            调用 _makeoperationown 创建的操作结果
        '''
        return self._makeoperationown(operation, _ownerskip=self)

    def _roperation(self, other, operation, intify=False):
        '''
        反向操作的处理，依赖于 self._operation 并传递 r=True 
        来定义一个反向操作。
        
        参数:
            other: 操作的另一个对象
            operation: 要执行的操作函数
            intify: 是否将结果转换为整数
            
        返回:
            反向操作的结果
        '''
        return self._operation(other, operation, r=True, intify=intify)

    def _operation_stage1(self, other, operation, r=False, intify=False):
        '''
        阶段1中的双操作数操作。扫描另一个操作数以确定它是直接作为
        操作数还是其子项作为操作数。
        
        参数:
            other: 操作的另一个对象
            operation: 要执行的操作函数
            r: 是否为反向操作
            intify: 是否将结果转换为整数
            
        返回:
            调用 _makeoperation 创建的操作结果
        '''
        if isinstance(other, LineMultiple):
            other = other.lines[0]

        return self._makeoperation(other, operation, r, self)

    def _operation_stage2(self, other, operation, r=False):
        '''
        阶段2中的双操作数操作，主要用于富比较运算符。
        扫描另一个操作数并返回直接与其他操作数的操作结果或其子项的操作结果。
        
        参数:
            other: 操作的另一个对象
            operation: 要执行的操作函数
            r: 是否为反向操作
            
        返回:
            操作的结果
        '''
        if isinstance(other, LineRoot):
            other = other[0]

        # operation(float, other) ... expecting other to be a float
        if r:
            return operation(other, self[0])

        return operation(self[0], other)

    def _operationown_stage2(self, operation):
        '''
        阶段2中的单操作数操作。
        
        参数:
            operation: 要执行的操作函数
            
        返回:
            对 self[0] 执行 operation 的结果
        '''
        return operation(self[0])

    # 以下是各种操作符的重载实现
    
    def __add__(self, other):
        '''重载加法运算符 +'''
        return self._operation(other, operator.__add__)

    def __radd__(self, other):
        '''重载反向加法运算符，当 other + self 而 other 不支持加法时被调用'''
        return self._roperation(other, operator.__add__)

    def __sub__(self, other):
        '''重载减法运算符 -'''
        return self._operation(other, operator.__sub__)

    def __rsub__(self, other):
        '''重载反向减法运算符，当 other - self 而 other 不支持减法时被调用'''
        return self._roperation(other, operator.__sub__)

    def __mul__(self, other):
        '''重载乘法运算符 *'''
        return self._operation(other, operator.__mul__)

    def __rmul__(self, other):
        '''重载反向乘法运算符，当 other * self 而 other 不支持乘法时被调用'''
        return self._roperation(other, operator.__mul__)

    def __div__(self, other):
        '''重载除法运算符 /，用于 Python 2'''
        return self._operation(other, operator.__div__)

    def __rdiv__(self, other):
        '''重载反向除法运算符，用于 Python 2'''
        return self._roperation(other, operator.__div__)

    def __floordiv__(self, other):
        '''重载整除运算符 //'''
        return self._operation(other, operator.__floordiv__)

    def __rfloordiv__(self, other):
        '''重载反向整除运算符'''
        return self._roperation(other, operator.__floordiv__)

    def __truediv__(self, other):
        '''重载真除法运算符，用于 Python 3'''
        return self._operation(other, operator.__truediv__)

    def __rtruediv__(self, other):
        '''重载反向真除法运算符，用于 Python 3'''
        return self._roperation(other, operator.__truediv__)

    def __pow__(self, other):
        '''重载幂运算符 **'''
        return self._operation(other, operator.__pow__)

    def __rpow__(self, other):
        '''重载反向幂运算符'''
        return self._roperation(other, operator.__pow__)

    def __abs__(self):
        '''重载绝对值函数 abs()'''
        return self._operationown(operator.__abs__)

    def __neg__(self):
        '''重载负号运算符 -x'''
        return self._operationown(operator.__neg__)

    # 以下是富比较运算符的重载实现
    
    def __lt__(self, other):
        '''重载小于运算符 <'''
        return self._operation(other, operator.__lt__)

    def __gt__(self, other):
        '''重载大于运算符 >'''
        return self._operation(other, operator.__gt__)

    def __le__(self, other):
        '''重载小于等于运算符 <='''
        return self._operation(other, operator.__le__)

    def __ge__(self, other):
        '''重载大于等于运算符 >='''
        return self._operation(other, operator.__ge__)

    def __eq__(self, other):
        '''重载等于运算符 =='''
        return self._operation(other, operator.__eq__)

    def __ne__(self, other):
        '''重载不等于运算符 !='''
        return self._operation(other, operator.__ne__)

    def __nonzero__(self):
        '''
        重载 bool 转换，在 Python 2 中用于 if x: 这样的条件判断
        '''
        return self._operationown(bool)

    # 将 __nonzero__ 赋值给 __bool__，用于 Python 3 的 bool 转换
    __bool__ = __nonzero__

    # Python 3 中如果类重定义了 __eq__，则必须显式实现 __hash__
    # 这里直接使用 object 的 hash 实现
    __hash__ = object.__hash__


class LineMultiple(LineRoot):
    '''
    持有多条数据线的LineXXX实例的基类
    
    这个类是管理多个时间序列数据（如价格、成交量等）的基础类。
    在backtrader系统中，"line"通常代表一个时间序列数据。LineMultiple
    提供了管理多条数据线的通用功能，如重置状态、设置最小周期、
    调整缓冲区大小以及处理操作符等。
    '''
    def reset(self):
        '''
        重置对象状态为初始状态
        
        这个方法执行两个操作：
        1. 调用_stage1()将对象设置为操作阶段1
        2. 调用lines.reset()重置所有数据线
        '''
        self._stage1()
        self.lines.reset()

    def _stage1(self):
        '''
        将对象及其所有数据线设置为操作阶段1
        
        操作阶段1通常是对象构建阶段，此方法会：
        1. 调用父类(LineRoot)的_stage1方法
        2. 遍历所有数据线并调用它们的_stage1方法
        '''
        super(LineMultiple, self)._stage1()
        for line in self.lines:
            line._stage1()

    def _stage2(self):
        '''
        将对象及其所有数据线设置为操作阶段2
        
        操作阶段2通常是运行时阶段，此方法会：
        1. 调用父类(LineRoot)的_stage2方法
        2. 遍历所有数据线并调用它们的_stage2方法
        '''
        super(LineMultiple, self)._stage2()
        for line in self.lines:
            line._stage2()

    def addminperiod(self, minperiod):
        '''
        为所有数据线添加最小周期
        
        将传入的最小周期值传递给所有数据线，确保每条线都满足
        指定的最小周期要求。这通常用于确保有足够的历史数据来
        计算技术指标。
        
        参数:
            minperiod: 要添加的最小周期值
        '''
        # pass it down to the lines
        for line in self.lines:
            line.addminperiod(minperiod)

    def incminperiod(self, minperiod):
        '''
        为所有数据线增加最小周期
        
        直接增加所有数据线的最小周期值，不考虑重叠期。与addminperiod
        不同，这个方法通常用于无条件地延长数据准备周期。
        
        参数:
            minperiod: 要增加的最小周期值
        '''
        # pass it down to the lines
        for line in self.lines:
            line.incminperiod(minperiod)

    def _makeoperation(self, other, operation, r=False, _ownerskip=None):
        '''
        创建与其他对象的双操作数操作
        
        这个方法将操作委托给第一条数据线(lines[0])处理，用于支持
        算术运算符的重载（如+、-、*、/等）。
        
        参数:
            other: 操作的另一个对象
            operation: 要执行的操作函数
            r: 是否为反向操作
            _ownerskip: 查找所有者时要跳过的对象
            
        返回:
            由第一条数据线创建的操作结果
        '''
        return self.lines[0]._makeoperation(other, operation, r, _ownerskip)

    def _makeoperationown(self, operation, _ownerskip=None):
        '''
        创建单操作数操作
        
        这个方法将操作委托给第一条数据线(lines[0])处理，用于支持
        单操作数运算符的重载（如abs()、负号等）。
        
        参数:
            operation: 要执行的操作函数
            _ownerskip: 查找所有者时要跳过的对象
            
        返回:
            由第一条数据线创建的操作结果
        '''
        return self.lines[0]._makeoperationown(operation, _ownerskip)

    def qbuffer(self, savemem=0):
        '''
        为所有数据线设置队列缓冲区
        
        实现最小尺寸队列缓冲区方案，用于优化内存使用。
        对于LineMultiple，它会为每条数据线调用qbuffer方法，
        并强制启用内存节省模式。
        
        参数:
            savemem: 是否节省内存，本类中忽略此参数，总是传递1给各数据线
        '''
        for line in self.lines:
            line.qbuffer(savemem=1)

    def minbuffer(self, size):
        '''
        设置所有数据线的最小缓冲区大小
        
        告知所有数据线需要的最小缓冲区大小，确保它们有足够的空间
        存储历史数据。
        
        参数:
            size: 缓冲区的最小大小
        '''
        for line in self.lines:
            line.minbuffer(size)


class LineSingle(LineRoot):
    '''
    持有单条线的 LineXXX 实例的基类。
    
    这个类是处理单一数据线的基类，与 LineMultiple（多线基类）相对。
    在 backtrader 中，单线对象通常用于表示单一数据序列（如单一指标或单一价格序列）。
    LineSingle 实现了针对单线对象特定的最小周期管理方法。
    '''
    def addminperiod(self, minperiod):
        '''
        添加最小周期（减去重叠的1个最小周期）。
        
        这个方法增加对象的最小周期，但会减去一个重叠期。这是因为在多个数据源组合时，
        每个数据源都需要一个初始点，但它们的组合只需要一个共享的初始点。
        
        参数:
            minperiod: 要添加的最小周期值
            
        实现细节: 在当前最小周期上加上 minperiod - 1，减1是为了避免重复计算初始点。
        '''
        self._minperiod += minperiod - 1

    def incminperiod(self, minperiod):
        '''
        无条件增加最小周期。
        
        与 addminperiod 不同，此方法直接增加最小周期，不考虑重叠期。
        当需要无条件延长数据准备周期时使用。
        
        参数:
            minperiod: 要增加的最小周期值
            
        实现细节:直接将参数值加到当前最小周期上，不做任何调整。
        '''
        self._minperiod += minperiod
