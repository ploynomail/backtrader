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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)


from datetime import datetime, timedelta, time

from .metabase import MetaParams
from backtrader.utils.py3 import string_types, with_metaclass
from backtrader.utils import UTC

__all__ = ['TradingCalendarBase', 'TradingCalendar', 'PandasMarketCalendar']

# Imprecission in the full time conversion to float would wrap over to next day
# if microseconds is 999999 as defined in time.max
_time_max = time(hour=23, minute=59, second=59, microsecond=999990)


MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY = range(7)
(ISONODAY, ISOMONDAY, ISOTUESDAY, ISOWEDNESDAY, ISOTHURSDAY, ISOFRIDAY,
 ISOSATURDAY, ISOSUNDAY) = range(8)

WEEKEND = [SATURDAY, SUNDAY]
ISOWEEKEND = [ISOSATURDAY, ISOSUNDAY]
ONEDAY = timedelta(days=1)


class TradingCalendarBase(with_metaclass(MetaParams, object)):
    def _nextday(self, day):
        '''
        Returns the next trading day (datetime/date instance) after ``day``
        (datetime/date instance) and the isocalendar components

        The return value is a tuple with 2 components: (nextday, (y, w, d))
        '''
        raise NotImplementedError

    def schedule(self, day):
        '''
        Returns a tuple with the opening and closing times (``datetime.time``)
        for the given ``date`` (``datetime/date`` instance)
        '''
        raise NotImplementedError

    def nextday(self, day):
        '''
        Returns the next trading day (datetime/date instance) after ``day``
        (datetime/date instance)
        '''
        return self._nextday(day)[0]  # 1st ret elem is next day

    def nextday_week(self, day):
        '''
        Returns the iso week number of the next trading day, given a ``day``
        (datetime/date) instance
        '''
        self._nextday(day)[1][1]  # 2 elem is isocal / 0 - y, 1 - wk, 2 - day

    def last_weekday(self, day):
        '''
        Returns ``True`` if the given ``day`` (datetime/date) instance is the
        last trading day of this week
        '''
        # Next day must be greater than day. If the week changes is enough for
        # a week change even if the number is smaller (year change)
        return day.isocalendar()[1] != self._nextday(day)[1][1]

    def last_monthday(self, day):
        '''
        Returns ``True`` if the given ``day`` (datetime/date) instance is the
        last trading day of this month
        '''
        # Next day must be greater than day. If the week changes is enough for
        # a week change even if the number is smaller (year change)
        return day.month != self._nextday(day)[0].month

    def last_yearday(self, day):
        '''
        Returns ``True`` if the given ``day`` (datetime/date) instance is the
        last trading day of this month
        '''
        # Next day must be greater than day. If the week changes is enough for
        # a week change even if the number is smaller (year change)
        return day.year != self._nextday(day)[0].year


class TradingCalendar(TradingCalendarBase):
    '''
    Wrapper of ``pandas_market_calendars`` for a trading calendar. The package
    ``pandas_market_calendar`` must be installed

    Params:

      - ``open`` (default ``time.min``)

        Regular start of the session

      - ``close`` (default ``time.max``)

        Regular end of the session

      - ``holidays`` (default ``[]``)

        List of non-trading days (``datetime.datetime`` instances)

      - ``earlydays`` (default ``[]``)

        List of tuples determining the date and opening/closing times of days
        which do not conform to the regular trading hours where each tuple has
        (``datetime.datetime``, ``datetime.time``, ``datetime.time`` )

      - ``offdays`` (default ``ISOWEEKEND``)

        A list of weekdays in ISO format (Monday: 1 -> Sunday: 7) in which the
        market doesn't trade. This is usually Saturday and Sunday and hence the
        default

    '''
    params = (
        ('open', time.min),
        ('close', _time_max),
        ('holidays', []),  # list of non trading days (date)
        ('earlydays', []),  # list of tuples (date, opentime, closetime)
        ('offdays', ISOWEEKEND),  # list of non trading (isoweekdays)
    )

    def __init__(self):
        self._earlydays = [x[0] for x in self.p.earlydays]  # speed up searches

    def _nextday(self, day):
        '''
        Returns the next trading day (datetime/date instance) after ``day``
        (datetime/date instance) and the isocalendar components

        The return value is a tuple with 2 components: (nextday, (y, w, d))
        '''
        while True:
            day += ONEDAY
            isocal = day.isocalendar()
            if isocal[2] in self.p.offdays or day in self.p.holidays:
                continue

            return day, isocal

    def schedule(self, day, tz=None):
        '''
        Returns the opening and closing times for the given ``day``. If the
        method is called, the assumption is that ``day`` is an actual trading
        day

        The return value is a tuple with 2 components: opentime, closetime
        '''
        while True:
            dt = day.date()
            try:
                i = self._earlydays.index(dt)
                o, c = self.p.earlydays[i][1:]
            except ValueError:  # not found
                o, c = self.p.open, self.p.close

            closing = datetime.combine(dt, c)
            if tz is not None:
                closing = tz.localize(closing).astimezone(UTC)
                closing = closing.replace(tzinfo=None)

            if day > closing:  # current time over eos
                day += ONEDAY
                continue

            opening = datetime.combine(dt, o)
            if tz is not None:
                opening = tz.localize(opening).astimezone(UTC)
                opening = opening.replace(tzinfo=None)

            return opening, closing


class PandasMarketCalendar(TradingCalendarBase):
    '''
    ``pandas_market_calendars``包的交易日历封装。必须安装
    ``pandas_market_calendar``包

    参数:

      - ``calendar`` (默认 ``None``)

        参数``calendar``接受以下内容:

        - 字符串: 支持的日历名称之一，例如
          `NYSE`。封装器将尝试获取日历实例

        - 日历实例: 如通过``get_calendar('NYSE')``返回的实例

      - ``cachesize`` (默认 ``365``)

        预先缓存用于查找的天数

    另见:

      - https://github.com/rsheftel/pandas_market_calendars

      - http://pandas-market-calendars.readthedocs.io/

    '''
    params = (
        ('calendar', None),  # pandas_market_calendars实例或交易所名称
        ('cachesize', 365),  # 预先缓存的天数
    )

    def __init__(self):
        self._calendar = self.p.calendar  # 从参数获取日历

        if isinstance(self._calendar, string_types):  # 使用传入的市场名称
            import pandas_market_calendars as mcal  # 导入pandas_market_calendars
            self._calendar = mcal.get_calendar(self._calendar)  # 获取日历实例

        import pandas as pd  # 由于pandas_market_calendars的存在，pandas一定可用
        self.dcache = pd.DatetimeIndex([0.0])  # 初始化日期缓存
        self.idcache = pd.DataFrame(index=pd.DatetimeIndex([0.0]))  # 初始化索引缓存
        self.csize = timedelta(days=self.p.cachesize)  # 计算缓存大小的时间间隔

    def _nextday(self, day):
        '''
        返回给定``day``(datetime/date实例)之后的下一个交易日
        (datetime/date实例)及其iso日历组件

        返回值是一个包含2个组件的元组: (nextday, (y, w, d))
        '''
        day += ONEDAY  # 增加一天
        while True:  # 持续循环直到找到下一个交易日
            i = self.dcache.searchsorted(day)  # 搜索日期在缓存中的位置
            if i == len(self.dcache):  # 如果日期超出缓存范围
                # 保持一年的缓存以加速搜索
                self.dcache = self._calendar.valid_days(day, day + self.csize)  # 更新缓存
                continue  # 重新搜索

            d = self.dcache[i].to_pydatetime()  # 转换为datetime对象
            return d, d.isocalendar()  # 返回日期和其isocalendar组件

    def schedule(self, day, tz=None):
        '''
        返回给定``day``的开盘和收盘时间。如果调用此方法，
        假设``day``是一个实际的交易日

        返回值是一个包含2个组件的元组: opentime, closetime
        '''
        while True:  # 持续循环直到找到有效数据
            i = self.idcache.index.searchsorted(day.date())  # 在索引缓存中搜索日期
            if i == len(self.idcache):  # 如果日期超出缓存范围
                # 保持一年的缓存以加速搜索
                self.idcache = self._calendar.schedule(day, day + self.csize)  # 更新缓存
                continue  # 重新搜索

            st = (x.tz_localize(None) for x in self.idcache.iloc[i, 0:2])  # 获取无时区时间
            opening, closing = st  # 获取UTC无时区时间
            if day > closing:  # 如果传入的时间超过了交易时段结束
                day += ONEDAY  # 转到下一天
                continue  # 重新搜索

            return opening.to_pydatetime(), closing.to_pydatetime()  # 返回开盘和收盘时间
