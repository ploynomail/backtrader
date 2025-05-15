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

from backtrader.utils.py3 import filter, string_types, integer_types

from backtrader import date2num
import backtrader.feed as feed


class PandasDirectData(feed.DataBase):
    '''
    使用Pandas DataFrame作为数据源，直接迭代通过"itertuples"返回的元组。
    
    这意味着所有与行相关的参数必须具有数值，作为元组的索引。
    
    该类直接处理pandas.DataFrame的行，通过itertuples()方法获取每行数据，
    使得数据访问更加高效。每个itertuples返回的行都是一个命名元组，可以通过索引访问。
    
    注意：
    
      - ``dataname`` 参数是一个Pandas DataFrame对象
      
      - 任何Data行参数中的负值表示该列在DataFrame中不存在
    '''

    params = (
        # 以下参数定义了如何从DataFrame的行元组中获取各字段数据
        ('datetime', 0),  # 日期时间字段在元组中的索引位置，默认为0
        ('open', 1),      # 开盘价在元组中的索引位置，默认为1
        ('high', 2),      # 最高价在元组中的索引位置，默认为2
        ('low', 3),       # 最低价在元组中的索引位置，默认为3
        ('close', 4),     # 收盘价在元组中的索引位置，默认为4
        ('volume', 5),    # 成交量在元组中的索引位置，默认为5
        ('openinterest', 6),  # 未平仓合约数在元组中的索引位置，默认为6
    )

    datafields = [
        # 定义了所有可能的数据字段名称
        'datetime', 'open', 'high', 'low', 'close', 'volume', 'openinterest'
    ]

    def start(self):
        '''
        启动数据源处理。
        
        在每次启动时重置行迭代器，确保从头开始读取数据。
        这个方法会在回测或实盘中数据源被激活时调用。
        '''
        super(PandasDirectData, self).start()

        # 重置行迭代器，p.dataname是Pandas DataFrame对象
        self._rows = self.p.dataname.itertuples()

    def _load(self):
        '''
        加载并处理一行数据。
        
        此方法尝试从DataFrame迭代器获取下一行，并将该行的各个字段值
        赋给对应的数据线。如果没有更多数据可加载，则返回False。
        
        工作流程：
        1. 尝试获取下一行数据
        2. 处理除datetime外的所有标准数据字段
        3. 特别处理datetime字段，将其转换为backtrader内部格式
        
        返回：
          成功加载数据返回True，否则返回False
        '''
        try:
            # 尝试获取下一行数据
            row = next(self._rows)
        except StopIteration:
            # 没有更多数据，返回False
            return False

        # 设置标准数据字段 - 除datetime外
        for datafield in self.getlinealiases():
            if datafield == 'datetime':
                # datetime需要特殊处理，跳过
                continue

            # 获取列索引
            colidx = getattr(self.params, datafield)

            if colidx < 0:
                # 列不存在 -- 跳过
                continue

            # 获取要设置的数据线
            line = getattr(self.lines, datafield)

            # pandas索引：先列，后行
            line[0] = row[colidx]

        # 处理datetime
        colidx = getattr(self.params, 'datetime')
        tstamp = row[colidx]

        # 通过datetime转换为float并存储
        dt = tstamp.to_pydatetime()
        dtnum = date2num(dt)

        # 获取要设置的行
        line = getattr(self.lines, 'datetime')
        line[0] = dtnum

        # 完成...返回
        return True


class PandasData(feed.DataBase):
    '''
    使用Pandas DataFrame作为数据源，使用列名的索引(可以是"数字")。
    
    与PandasDirectData不同，此类通过列名或索引访问DataFrame的数据，
    而不是直接迭代行元组。这提供了更灵活的数据访问方式，特别是当
    DataFrame的列名与backtrader期望的字段名不完全匹配时。
    
    参数：
      - nocase (默认 *True*) 列名匹配时不区分大小写
    
    注意：
      - dataname 参数是一个Pandas DataFrame对象
    
      - datetime可能的值：
        - None：索引包含datetime
        - -1：无索引，自动检测列
        - >= 0 或字符串：特定列标识符
    
      - 其他行参数的可能值：  
        - None：列不存在
        - -1：自动检测
        - >= 0 或字符串：特定列标识符
    '''

    params = (
        ('nocase', True),  # 不区分大小写匹配列名，默认为True
        
        # datetime的可能值(必须始终存在)
        #  None：datetime是Pandas Dataframe中的"索引"
        #  -1：自动检测位置或不区分大小写的相等名称
        #  >= 0：pandas dataframe中列的数字索引
        #  string：pandas dataframe中的列名(作为索引)
        ('datetime', None),
        
        # 以下参数的可能值：
        #  None：列不存在
        #  -1：自动检测位置或不区分大小写的相等名称
        #  >= 0：pandas dataframe中列的数字索引
        #  string：pandas dataframe中的列名(作为索引)
        ('open', -1),      # 开盘价列，默认自动检测
        ('high', -1),      # 最高价列，默认自动检测
        ('low', -1),       # 最低价列，默认自动检测
        ('close', -1),     # 收盘价列，默认自动检测
        ('volume', -1),    # 成交量列，默认自动检测
        ('openinterest', -1),  # 未平仓合约数列，默认自动检测
    )

    datafields = [
        # 定义了所有可能的数据字段名称
        'datetime', 'open', 'high', 'low', 'close', 'volume', 'openinterest'
    ]

    def __init__(self):
        '''
        初始化PandasData对象。
        
        此方法完成以下任务：
        1. 调用父类初始化
        2. 获取DataFrame的列名
        3. 自动检测列是否都是数字类型
        4. 建立字段名到列的映射关系
        '''
        super(PandasData, self).__init__()

        # 这些"colnames"可以是字符串或数字类型
        colnames = list(self.p.dataname.columns.values)
        if self.p.datetime is None:
            # datetime预期为索引列，因此不返回
            pass

        # 尝试自动检测是否所有列都是数字
        cstrings = filter(lambda x: isinstance(x, string_types), colnames)
        colsnumeric = not len(list(cstrings))

        # 每个数据字段在哪里找到其值的映射
        self._colmapping = dict()

        # 预先构建列映射到内部字段
        for datafield in self.getlinealiases():
            defmapping = getattr(self.params, datafield)

            if isinstance(defmapping, integer_types) and defmapping < 0:
                # 请求自动检测
                for colname in colnames:
                    if isinstance(colname, string_types):
                        if self.p.nocase:
                            # 不区分大小写比较
                            found = datafield.lower() == colname.lower()
                        else:
                            # 区分大小写比较
                            found = datafield == colname

                        if found:
                            # 找到匹配的列名，添加到映射
                            self._colmapping[datafield] = colname
                            break

                if datafield not in self._colmapping:
                    # 请求自动检测但未找到
                    self._colmapping[datafield] = None
                    continue
            else:
                # 所有其他情况 -- 使用给定的索引
                self._colmapping[datafield] = defmapping

    def start(self):
        '''
        启动数据源处理。
        
        此方法完成以下任务：
        1. 调用父类start方法
        2. 重置索引
        3. 将列名转换为索引，便于使用iloc访问
        
        这个方法会在回测或实盘中数据源被激活时调用。
        '''
        super(PandasData, self).start()

        # 每次启动时重置长度
        self._idx = -1

        # 将名称(对.ix有效)转换为索引(对.iloc有效)
        if self.p.nocase:
            # 不区分大小写模式下，将所有列名转为小写
            colnames = [x.lower() for x in self.p.dataname.columns.values]
        else:
            # 区分大小写模式下，直接使用列名
            colnames = [x for x in self.p.dataname.columns.values]

        # 处理每个字段的映射
        for k, v in self._colmapping.items():
            if v is None:
                continue  # datetime的特殊标记
            if isinstance(v, string_types):
                try:
                    if self.p.nocase:
                        # 不区分大小写查找列索引
                        v = colnames.index(v.lower())
                    else:
                        # 区分大小写查找列索引
                        v = colnames.index(v)
                except ValueError as e:
                    # 处理未找到列的情况
                    defmap = getattr(self.params, k)
                    if isinstance(defmap, integer_types) and defmap < 0:
                        # 如果是自动检测且未找到，设为None
                        v = None
                    else:
                        # 否则抛出异常，通知用户出错
                        raise e

            # 更新映射中的值为列索引
            self._colmapping[k] = v

    def _load(self):
        '''
        加载并处理一行数据。
        
        此方法尝试从DataFrame获取下一行，并将该行的各个字段值
        赋给对应的数据线。如果没有更多数据可加载，则返回False。
        
        工作流程：
        1. 增加索引指针
        2. 检查是否还有数据可用
        3. 处理除datetime外的所有标准数据字段
        4. 特别处理datetime字段，可能来自索引或特定列
        
        返回：
          成功加载数据返回True，否则返回False
        '''
        self._idx += 1

        if self._idx >= len(self.p.dataname):
            # 已用尽所有行
            return False

        # 设置标准数据字段
        for datafield in self.getlinealiases():
            if datafield == 'datetime':
                # datetime需要特殊处理，跳过
                continue

            colindex = self._colmapping[datafield]
            if colindex is None:
                # 数据字段在流中标记为缺失：跳过
                continue

            # 获取要设置的行
            line = getattr(self.lines, datafield)

            # pandas索引：先列，后行
            line[0] = self.p.dataname.iloc[self._idx, colindex]

        # datetime转换
        coldtime = self._colmapping['datetime']

        if coldtime is None:
            # datetime在标准索引中
            tstamp = self.p.dataname.index[self._idx]
        else:
            # datetime在不同的列中...使用标准列索引
            tstamp = self.p.dataname.iloc[self._idx, coldtime]

        # 通过datetime转换为float并存储
        dt = tstamp.to_pydatetime()
        dtnum = date2num(dt)
        self.lines.datetime[0] = dtnum

        # 完成...返回
        return True
