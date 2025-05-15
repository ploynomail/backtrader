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


import bisect
import collections
from datetime import date, datetime, timedelta
from itertools import islice

from .feed import AbstractDataBase
from .metabase import MetaParams
from .utils import date2num, num2date
from .utils.py3 import integer_types, range, with_metaclass
from .utils import TIME_MAX


__all__ = ['SESSION_TIME', 'SESSION_START', 'SESSION_END', 'Timer']  # 定义此模块对外暴露的对象名称列表

SESSION_TIME, SESSION_START, SESSION_END = range(3)  # 定义三个常量，分别表示会话时间、会话开始和会话结束


class Timer(with_metaclass(MetaParams, object)):
    """定时器类，用于在指定时间触发事件"""
    params = (
        ('tid', None),           # 定时器ID，用于识别不同的定时器实例
        ('owner', None),         # 定时器的所有者，通常是策略实例
        ('strats', False),       # 是否将定时器事件传递给所有策略，而不仅仅是所有者
        ('when', None),          # 指定何时触发定时器（可以是时间、日期时间或SESSION_*常量）
        ('offset', timedelta()), # 时间偏移量，用于调整触发时间
        ('repeat', timedelta()), # 重复间隔，如果设置，定时器将重复触发
        ('weekdays', []),        # 限制定时器只在特定工作日触发的列表
        ('weekcarry', False),    # 如果True，当本周的日期已过，会使定时器在下一个有效工作日触发
        ('monthdays', []),       # 限制定时器只在特定月份日期触发的列表
        ('monthcarry', True),    # 如果True，当本月的日期已过，会使定时器在下一个有效月日触发
        ('allow', None),         # 自定义回调函数，用于进一步控制定时器是否触发
        ('tzdata', None),        # 时区数据源，用于时间转换
        ('cheat', False),        # 作弊模式，如果为True，定时器将在实际时间之前触发
    )

    SESSION_TIME, SESSION_START, SESSION_END = range(3)  # 在类内部也定义这些常量，与模块级常量相同

    def __init__(self, *args, **kwargs):
        """初始化定时器实例，存储传入的参数"""
        self.args = args         # 存储位置参数
        self.kwargs = kwargs     # 存储关键字参数

    def start(self, data):
        """启动定时器，与数据源关联并初始化"""
        # 记录'reset when'值，确定定时器触发的时间点
        if not isinstance(self.p.when, integer_types):  # 如果when不是整数类型（即不是SESSION_*常量）
            self._rstwhen = self.p.when       # 直接使用when参数值作为触发时间
            self._tzdata = self.p.tzdata      # 使用指定的时区数据
        else:
            # 如果when是整数类型（即SESSION_*常量），则需要从数据源获取触发时间
            self._tzdata = data if self.p.tzdata is None else self.p.tzdata  # 确定时区数据源

            if self.p.when == SESSION_START:  # 如果是会话开始
                self._rstwhen = self._tzdata.p.sessionstart  # 使用数据源的会话开始时间
            elif self.p.when == SESSION_END:  # 如果是会话结束
                self._rstwhen = self._tzdata.p.sessionend    # 使用数据源的会话结束时间

        self._isdata = isinstance(self._tzdata, AbstractDataBase)  # 检查时区数据是否是数据源实例
        self._reset_when()  # 重置定时器触发时间

        self._nexteos = datetime.min  # 初始化下一个会话结束时间为最小时间
        self._curdate = date.min      # 初始化当前日期为最小日期

        self._curmonth = -1  # 初始化当前月份为-1（不存在的月份）
        self._monthmask = collections.deque()  # 初始化月份掩码为空队列

        self._curweek = -1  # 初始化当前周为-1（不存在的周）
        self._weekmask = collections.deque()  # 初始化周掩码为空队列

    def _reset_when(self, ddate=datetime.min):
        """重置定时器的触发时间和状态"""
        self._when = self._rstwhen  # 重置触发时间为初始设置
        self._dtwhen = self._dwhen = None  # 清空日期时间和日期触发点

        self._lastcall = ddate  # 记录最后一次调用的日期

    def _check_month(self, ddate):
        """检查给定日期是否满足月份条件"""
        if not self.p.monthdays:
            return True  # 如果没有设置月份日期限制，则总是返回True

        mask = self._monthmask  # 获取月份掩码
        daycarry = False  # 初始化日期延续标志为False
        dmonth = ddate.month  # 获取目标日期的月份
        if dmonth != self._curmonth:  # 如果月份发生变化
            self._curmonth = dmonth  # 记录新的月份
            daycarry = self.p.monthcarry and bool(mask)  # 如果设置了monthcarry并且mask不为空，则设置daycarry
            self._monthmask = mask = collections.deque(self.p.monthdays)  # 重新初始化月份掩码

        dday = ddate.day  # 获取目标日期的日
        dc = bisect.bisect_left(mask, dday)  # 在掩码中查找当前日期的位置（查找小于dday的天数）
        daycarry = daycarry or (self.p.monthcarry and dc > 0)  # 更新daycarry标志
        if dc < len(mask):  # 如果在掩码范围内找到了位置
            curday = bisect.bisect_right(mask, dday, lo=dc) > 0  # 检查当前日期是否在掩码中
            dc += curday  # 增加dc计数
        else:
            curday = False  # 当前日期不在掩码中

        while dc:  # 移除已经过去的日期
            mask.popleft()
            dc -= 1

        return daycarry or curday  # 返回是否满足条件

    def _check_week(self, ddate=date.min):
        """检查给定日期是否满足星期条件"""
        if not self.p.weekdays:
            return True  # 如果没有设置星期限制，则总是返回True

        _, dweek, dwkday = ddate.isocalendar()  # 获取ISO日历格式的周和工作日

        mask = self._weekmask  # 获取星期掩码
        daycarry = False  # 初始化日期延续标志为False
        if dweek != self._curweek:  # 如果周发生变化
            self._curweek = dweek  # 记录新的周
            daycarry = self.p.weekcarry and bool(mask)  # 如果设置了weekcarry并且mask不为空，则设置daycarry
            self._weekmask = mask = collections.deque(self.p.weekdays)  # 重新初始化星期掩码

        dc = bisect.bisect_left(mask, dwkday)  # 在掩码中查找当前工作日的位置
        daycarry = daycarry or (self.p.weekcarry and dc > 0)  # 更新daycarry标志
        if dc < len(mask):  # 如果在掩码范围内找到了位置
            curday = bisect.bisect_right(mask, dwkday, lo=dc) > 0  # 检查当前工作日是否在掩码中
            dc += curday  # 增加dc计数
        else:
            curday = False  # 当前工作日不在掩码中

        while dc:  # 移除已经过去的工作日
            mask.popleft()
            dc -= 1

        return daycarry or curday  # 返回是否满足条件

    def check(self, dt):
        """检查定时器是否应该触发"""
        d = num2date(dt)  # 将数值时间转换为日期时间对象
        ddate = d.date()  # 获取日期部分
        if self._lastcall == ddate:  # 如果同一天已经调用过，不再重复触发
            return False  # 不重复，等待日期变化

        if d > self._nexteos:  # 如果超过了下一个会话结束时间
            if self._isdata:  # 如果时区数据是数据源实例
                nexteos, _ = self._tzdata._getnexteos()  # 从数据源获取下一个会话结束时间
            else:  # 否则使用通用会话结束时间
                nexteos = datetime.combine(ddate, TIME_MAX)  # 当天的最大时间作为会话结束
            self._nexteos = nexteos  # 更新下一个会话结束时间
            self._reset_when()  # 重置触发时间

        if ddate > self._curdate:  # 如果日期发生变化
            self._curdate = ddate  # 更新当前日期
            ret = self._check_month(ddate)  # 检查月份条件
            if ret:  # 如果月份条件满足
                ret = self._check_week(ddate)  # 继续检查星期条件
            if ret and self.p.allow is not None:  # 如果前面的条件都满足且设置了allow回调
                ret = self.p.allow(ddate)  # 调用allow回调进一步检查

            if not ret:  # 如果任何条件不满足
                self._reset_when(ddate)  # 重置触发时间，并标记当前日期已调用
                return False  # 定时器目标未满足

        # 无日期变化或通过了月、周和allow过滤条件
        dwhen = self._dwhen  # 获取触发时间的日期时间对象
        dtwhen = self._dtwhen  # 获取触发时间的数值表示
        if dtwhen is None:  # 如果还没有计算触发时间
            dwhen = datetime.combine(ddate, self._when)  # 组合当前日期和触发时间
            if self.p.offset:  # 如果设置了偏移量
                dwhen += self.p.offset  # 应用偏移量

            self._dwhen = dwhen  # 保存触发时间的日期时间对象

            if self._isdata:  # 如果时区数据是数据源实例
                self._dtwhen = dtwhen = self._tzdata.date2num(dwhen)  # 使用数据源将日期时间转换为数值
            else:  # 否则使用通用转换
                self._dtwhen = dtwhen = date2num(dwhen, tz=self._tzdata)  # 使用指定时区转换

        if dt < dtwhen:  # 如果当前时间早于触发时间
            return False  # 定时器目标未满足

        self.lastwhen = dwhen  # 记录最后一次触发的时间

        if not self.p.repeat:  # 如果不重复触发
            self._reset_when(ddate)  # 重置触发时间并标记当前日期已调用
        else:  # 如果需要重复触发
            if d > self._nexteos:  # 如果超过了下一个会话结束时间
                if self._isdata:  # 如果时区数据是数据源实例
                    nexteos, _ = self._tzdata._getnexteos()  # 获取下一个会话结束时间
                else:  # 否则使用通用会话结束时间
                    nexteos = datetime.combine(ddate, TIME_MAX)  # 当天的最大时间

                self._nexteos = nexteos  # 更新下一个会话结束时间
            else:
                nexteos = self._nexteos  # 使用已有的下一个会话结束时间

            while True:  # 计算下一个触发点
                dwhen += self.p.repeat  # 增加重复间隔
                if dwhen > nexteos:  # 如果下一个触发点超过了会话结束时间
                    self._reset_when(ddate)  # 重置到原始触发点
                    break  # 中断循环

                if dwhen > d:  # 如果下一个触发点已经超过了当前时间
                    self._dtwhen = dtwhen = date2num(dwhen)  # 更新触发时间的数值表示
                    # 获取本地化的预期下一次触发时间
                    if self._isdata:  # 如果时区数据是数据源实例
                        self._dwhen = self._tzdata.num2date(dtwhen)  # 使用数据源转换
                    else:  # 假设兼容pytz或None
                        self._dwhen = num2date(dtwhen, tz=self._tzdata)  # 使用指定时区转换

                    break  # 中断循环

        return True  # 定时器目标已满足，触发事件
