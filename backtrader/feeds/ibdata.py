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

import datetime

import backtrader as bt
from backtrader.feed import DataBase
from backtrader import TimeFrame, date2num, num2date
from backtrader.utils.py3 import (integer_types, queue, string_types,
                                  with_metaclass)
from backtrader.metabase import MetaParams
from backtrader.stores import ibstore


class MetaIBData(DataBase.__class__):
    def __init__(cls, name, bases, dct):
        '''Class has already been created ... register'''
        # 元类初始化方法，用于在类创建后进行注册
        # 调用父类初始化方法
        super(MetaIBData, cls).__init__(name, bases, dct)

        # 将当前类注册到IBStore中，使IBStore能够创建此类的实例
        ibstore.IBStore.DataCls = cls


class IBData(with_metaclass(MetaIBData, DataBase)):
    '''Interactive Brokers Data Feed.
    # Interactive Brokers数据源。

    Supports the following contract specifications in parameter ``dataname``:
    # 在参数``dataname``中支持以下合约规格:

          - TICKER  # Stock type and SMART exchange
          # TICKER  # 股票类型和SMART交易所（默认路由）

          - TICKER-STK  # Stock and SMART exchange
          # TICKER-STK  # 股票和SMART交易所

          - TICKER-STK-EXCHANGE  # Stock
          # TICKER-STK-EXCHANGE  # 在特定交易所的股票

          - TICKER-STK-EXCHANGE-CURRENCY  # Stock
          # TICKER-STK-EXCHANGE-CURRENCY  # 特定货币的股票

          - TICKER-CFD  # CFD and SMART exchange
          # TICKER-CFD  # 差价合约(CFD)和SMART交易所

          - TICKER-CFD-EXCHANGE  # CFD
          # TICKER-CFD-EXCHANGE  # 特定交易所的差价合约

          - TICKER-CDF-EXCHANGE-CURRENCY  # Stock
          # TICKER-CDF-EXCHANGE-CURRENCY  # 特定货币的差价合约

          - TICKER-IND-EXCHANGE  # Index
          # TICKER-IND-EXCHANGE  # 指数

          - TICKER-IND-EXCHANGE-CURRENCY  # Index
          # TICKER-IND-EXCHANGE-CURRENCY  # 特定货币的指数

          - TICKER-YYYYMM-EXCHANGE  # Future
          # TICKER-YYYYMM-EXCHANGE  # 期货（年月格式）

          - TICKER-YYYYMM-EXCHANGE-CURRENCY  # Future
          # TICKER-YYYYMM-EXCHANGE-CURRENCY  # 特定货币的期货

          - TICKER-YYYYMM-EXCHANGE-CURRENCY-MULT  # Future
          # TICKER-YYYYMM-EXCHANGE-CURRENCY-MULT  # 带乘数的期货

          - TICKER-FUT-EXCHANGE-CURRENCY-YYYYMM-MULT # Future
          # TICKER-FUT-EXCHANGE-CURRENCY-YYYYMM-MULT # 另一种期货格式

          - TICKER-YYYYMM-EXCHANGE-CURRENCY-STRIKE-RIGHT  # FOP
          # TICKER-YYYYMM-EXCHANGE-CURRENCY-STRIKE-RIGHT  # 期货期权(FOP)

          - TICKER-YYYYMM-EXCHANGE-CURRENCY-STRIKE-RIGHT-MULT  # FOP
          # TICKER-YYYYMM-EXCHANGE-CURRENCY-STRIKE-RIGHT-MULT  # 带乘数的期货期权

          - TICKER-FOP-EXCHANGE-CURRENCY-YYYYMM-STRIKE-RIGHT # FOP
          # TICKER-FOP-EXCHANGE-CURRENCY-YYYYMM-STRIKE-RIGHT # 另一种期货期权格式

          - TICKER-FOP-EXCHANGE-CURRENCY-YYYYMM-STRIKE-RIGHT-MULT # FOP
          # TICKER-FOP-EXCHANGE-CURRENCY-YYYYMM-STRIKE-RIGHT-MULT # 另一种带乘数的期货期权格式

          - CUR1.CUR2-CASH-IDEALPRO  # Forex
          # CUR1.CUR2-CASH-IDEALPRO  # 外汇（如EUR.USD）

          - TICKER-YYYYMMDD-EXCHANGE-CURRENCY-STRIKE-RIGHT  # OPT
          # TICKER-YYYYMMDD-EXCHANGE-CURRENCY-STRIKE-RIGHT  # 期权（年月日格式）

          - TICKER-YYYYMMDD-EXCHANGE-CURRENCY-STRIKE-RIGHT-MULT  # OPT
          # TICKER-YYYYMMDD-EXCHANGE-CURRENCY-STRIKE-RIGHT-MULT  # 带乘数的期权

          - TICKER-OPT-EXCHANGE-CURRENCY-YYYYMMDD-STRIKE-RIGHT # OPT
          # TICKER-OPT-EXCHANGE-CURRENCY-YYYYMMDD-STRIKE-RIGHT # 另一种期权格式

          - TICKER-OPT-EXCHANGE-CURRENCY-YYYYMMDD-STRIKE-RIGHT-MULT # OPT
          # TICKER-OPT-EXCHANGE-CURRENCY-YYYYMMDD-STRIKE-RIGHT-MULT # 另一种带乘数的期权格式

    Params:
    # 参数：

      - ``sectype`` (default: ``STK``)
      # - ``sectype`` (默认值: ``STK``)

        Default value to apply as *security type* if not provided in the
        ``dataname`` specification
        # 如果在``dataname``规格中未提供，则应用作为*证券类型*的默认值

      - ``exchange`` (default: ``SMART``)
      # - ``exchange`` (默认值: ``SMART``)

        Default value to apply as *exchange* if not provided in the
        ``dataname`` specification
        # 如果在``dataname``规格中未提供，则应用作为*交易所*的默认值

      - ``currency`` (default: ``''``)
      # - ``currency`` (默认值: ``''``)

        Default value to apply as *currency* if not provided in the
        ``dataname`` specification
        # 如果在``dataname``规格中未提供，则应用作为*货币*的默认值

      - ``historical`` (default: ``False``)
      # - ``historical`` (默认值: ``False``)

        If set to ``True`` the data feed will stop after doing the first
        download of data.
        # 如果设置为``True``，数据源将在首次下载数据后停止。

        The standard data feed parameters ``fromdate`` and ``todate`` will be
        used as reference.
        # 标准数据源参数``fromdate``和``todate``将被用作参考。

        The data feed will make multiple requests if the requested duration is
        larger than the one allowed by IB given the timeframe/compression
        chosen for the data.
        # 如果请求的时间段大于IB允许的时间段（基于选择的时间帧/压缩），数据源将发出多个请求。

      - ``what`` (default: ``None``)
      # - ``what`` (默认值: ``None``)

        If ``None`` the default for different assets types will be used for
        historical data requests:
        # 如果为``None``，则不同资产类型的历史数据请求将使用默认值：

          - 'BID' for CASH assets
          # - 'BID'用于现金资产（如外汇）

          - 'TRADES' for any other
          # - 'TRADES'用于任何其他资产类型

        Use 'ASK' for the Ask quote of cash assets
        # 使用'ASK'获取现金资产的卖出报价
        
        Check the IB API docs if another value is wished
        # 如果需要其他值，请查看IB API文档

      - ``rtbar`` (default: ``False``)
      # - ``rtbar`` (默认值: ``False``)

        If ``True`` the ``5 Seconds Realtime bars`` provided by Interactive
        Brokers will be used as the smalles tick. According to the
        documentation they correspond to real-time values (once collated and
        curated by IB)
        # 如果为``True``，将使用Interactive Brokers提供的``5秒实时K线``作为最小tick。
        # 根据文档，这些对应于实时值（一旦由IB整理和处理）

        If ``False`` then the ``RTVolume`` prices will be used, which are based
        on receiving ticks. In the case of ``CASH`` assets (like for example
        EUR.JPY) ``RTVolume`` will always be used and from it the ``bid`` price
        (industry de-facto standard with IB according to the literature
        scattered over the Internet)
        # 如果为``False``，则将使用``RTVolume``价格，这些价格基于接收的tick。
        # 对于``CASH``资产（例如EUR.JPY），将始终使用``RTVolume``，
        # 并从中获取``bid``价格（根据互联网上分散的文献，这是与IB一起使用的行业事实上的标准）

        Even if set to ``True``, if the data is resampled/kept to a
        timeframe/compression below Seconds/5, no real time bars will be used,
        because IB doesn't serve them below that level
        # 即使设置为``True``，如果数据被重新采样/保持在低于5秒的时间帧/压缩级别，
        # 也不会使用实时K线，因为IB不提供低于该级别的实时K线

      - ``qcheck`` (default: ``0.5``)
      # - ``qcheck`` (默认值: ``0.5``)

        Time in seconds to wake up if no data is received to give a chance to
        resample/replay packets properly and pass notifications up the chain
        # 如果没有收到数据，唤醒的时间（秒），以便有机会正确地重新采样/重播数据包并将通知传递给链

      - ``backfill_start`` (default: ``True``)
      # - ``backfill_start`` (默认值: ``True``)

        Perform backfilling at the start. The maximum possible historical data
        will be fetched in a single request.
        # 在开始时执行回填。将在单个请求中获取最大可能的历史数据。

      - ``backfill`` (default: ``True``)
      # - ``backfill`` (默认值: ``True``)

        Perform backfilling after a disconnection/reconnection cycle. The gap
        duration will be used to download the smallest possible amount of data
        # 在断开连接/重新连接周期后执行回填。将使用间隔持续时间下载尽可能少的数据

      - ``backfill_from`` (default: ``None``)
      # - ``backfill_from`` (默认值: ``None``)

        An additional data source can be passed to do an initial layer of
        backfilling. Once the data source is depleted and if requested,
        backfilling from IB will take place. This is ideally meant to backfill
        from already stored sources like a file on disk, but not limited to.
        # 可以传递一个额外的数据源来做初始层的回填。一旦数据源耗尽且如果请求，
        # 将从IB进行回填。这理想地是为了从已经存储的源（如磁盘上的文件）进行回填，但不限于此。

      - ``latethrough`` (default: ``False``)
      # - ``latethrough`` (默认值: ``False``)

        If the data source is resampled/replayed, some ticks may come in too
        late for the already delivered resampled/replayed bar. If this is
        ``True`` those ticks will bet let through in any case.
        # 如果数据源被重新采样/重播，一些tick可能会来得太晚，无法纳入已交付的重采样/重播K线。
        # 如果设为``True``，这些tick在任何情况下都会被放行。

        Check the Resampler documentation to see who to take those ticks into
        account.
        # 查看Resampler文档以了解如何将这些tick纳入考虑范围。

        This can happen especially if ``timeoffset`` is set to ``False``  in
        the ``IBStore`` instance and the TWS server time is not in sync with
        that of the local computer
        # 特别是当``IBStore``实例中的``timeoffset``设置为``False``
        # 且TWS服务器时间与本地计算机时间不同步时，这种情况可能发生

      - ``tradename`` (default: ``None``)
      # - ``tradename`` (默认值: ``None``)
        Useful for some specific cases like ``CFD`` in which prices are offered
        by one asset and trading happens in a different onel
        # 对某些特定情况很有用，如``CFD``，其中价格由一种资产提供，交易发生在另一种资产上

        - SPY-STK-SMART-USD -> SP500 ETF (will be specified as ``dataname``)
        # - SPY-STK-SMART-USD -> 标普500ETF（将被指定为``dataname``）

        - SPY-CFD-SMART-USD -> which is the corresponding CFD which offers not
          price tracking but in this case will be the trading asset (specified
          as ``tradename``)
        # - SPY-CFD-SMART-USD -> 这是相应的CFD，不提供价格跟踪，但在这种情况下将是交易资产（指定为``tradename``）

    The default values in the params are the to allow things like ```TICKER``,
    to which the parameter ``sectype`` (default: ``STK``) and ``exchange``
    (default: ``SMART``) are applied.
    # 参数中的默认值允许使用类似```TICKER``这样的简写形式，
    # 其中参数``sectype``（默认：``STK``）和``exchange``（默认：``SMART``）会被自动应用。

    Some assets like ``AAPL`` need full specification including ``currency``
    (default: '') whereas others like ``TWTR`` can be simply passed as it is.
    # 某些资产如``AAPL``需要完整规格，包括``currency``（默认：''），
    # 而其他如``TWTR``可以简单地按原样传递。

      - ``AAPL-STK-SMART-USD`` would be the full specification for dataname
      # - ``AAPL-STK-SMART-USD``是dataname的完整规格

        Or else: ``IBData`` as ``IBData(dataname='AAPL', currency='USD')``
        which uses the default values (``STK`` and ``SMART``) and overrides
        the currency to be ``USD``
        # 或者：``IBData``作为``IBData(dataname='AAPL', currency='USD')``，
        # 它使用默认值（``STK``和``SMART``）并将货币覆盖为``USD``
    '''
    # 类参数定义，用于配置Interactive Brokers数据源的行为
    params = (
        ('sectype', 'STK'),  # 证券类型，默认为股票
        ('exchange', 'SMART'),  # 交易所，默认为SMART路由
        ('currency', ''),  # 货币，默认为空
        ('rtbar', False),  # 是否使用实时5秒K线数据，默认否
        ('historical', False),  # 是否仅下载历史数据，默认否
        ('what', None),  # 历史数据类型，默认根据资产类型自动选择
        ('useRTH', False),  # 是否只下载常规交易时间的数据
        ('qcheck', 0.5),  # 检查事件的超时时间（秒）
        ('backfill_start', True),  # 开始时是否回填历史数据
        ('backfill', True),  # 重连后是否回填历史数据
        ('backfill_from', None),  # 额外的回填数据源
        ('latethrough', False),  # 是否允许延迟的tick通过
        ('tradename', None),  # 用于交易的不同资产名称
    )

    # 关联的存储类
    _store = ibstore.IBStore

    # 实时K线支持的最小时间帧大小（秒级，最少5秒）
    RTBAR_MINSIZE = (TimeFrame.Seconds, 5)

    # _load方法中使用的有限状态机的状态常量
    _ST_FROM, _ST_START, _ST_LIVE, _ST_HISTORBACK, _ST_OVER = range(5)

    def _timeoffset(self):
        """返回IB连接的时间偏移"""
        # 返回IB时间偏移量，用于时间同步
        return self.ib.timeoffset()

    def _gettz(self):
        """
        获取时区对象，如果用户未提供且能通过合约详情获取，
        则尝试从pytz获取。IB返回的时区规格似乎是pytz能理解的缩写，
        但IB可能返回的完整时区列表未文档化，可能有些缩写会失败。
        """
        # 检查是否已提供自定义时区对象或时区字符串
        tzstr = isinstance(self.p.tz, string_types)
        if self.p.tz is not None and not tzstr:
            return bt.utils.date.Localizer(self.p.tz)

        # 如果没有合约详情，无法获取时区信息
        if self.contractdetails is None:
            return None  # 无法获取时区信息

        try:
            # 尝试导入pytz库，保持导入的局部范围
            import pytz
        except ImportError:
            # 如果pytz不可用，则无法获取时区信息
            return None  # 无法获取时区信息

        # 获取时区字符串，优先使用用户提供的，否则使用合约详情中的
        tzs = self.p.tz if tzstr else self.contractdetails.m_timeZoneId

        # IB报告的CST与pytz不兼容，修复为CST6CDT
        if tzs == 'CST':  # reported by TWS, not compatible with pytz. patch it
            tzs = 'CST6CDT'

        try:
            # 尝试获取pytz时区对象
            tz = pytz.timezone(tzs)
        except pytz.UnknownTimeZoneError:
            # 如果时区名称未知，无法获取时区信息
            return None  # 无法获取时区信息

        # 合约详情存在，导入成功，时区找到，返回时区对象
        return tz

    def islive(self):
        '''返回`True`通知`Cerebro`应禁用预加载和单次运行模式'''
        # 如果不是纯历史模式，则返回True，表示这是一个实时数据源
        return not self.p.historical

    def __init__(self, **kwargs):
        """初始化IB数据源对象"""
        # 创建IB商店实例
        self.ib = self._store(**kwargs)
        # 解析主数据源的合约信息
        self.precontract = self.parsecontract(self.p.dataname)
        # 解析交易资产的合约信息（如果有）
        self.pretradecontract = self.parsecontract(self.p.tradename)

    def setenvironment(self, env):
        '''接收环境(cerebro)并将其传递给所属的store'''
        # 调用父类的setenvironment方法
        super(IBData, self).setenvironment(env)
        # 将IB商店添加到环境中
        env.addstore(self.ib)

    def parsecontract(self, dataname):
        '''解析dataname生成默认合约'''
        # 如果dataname为空，直接返回None
        if dataname is None:
            return None

        # 设置可选令牌的默认值
        exch = self.p.exchange  # 默认交易所
        curr = self.p.currency  # 默认货币
        expiry = ''  # 默认到期日
        strike = 0.0  # 默认行权价
        right = ''  # 默认权利（看涨/看跌）
        mult = ''  # 默认乘数

        # 分割ticker字符串
        tokens = iter(dataname.split('-'))

        # 获取股票代码，这是必须的
        symbol = next(tokens)
        try:
            # 尝试获取证券类型，这也是必须的
            sectype = next(tokens)
        except StopIteration:
            # 如果没有指定证券类型，使用默认值
            sectype = self.p.sectype

        # 检查证券类型是否为日期（针对期货和期权）
        if sectype.isdigit():
            expiry = sectype  # 保存到期日

            # 根据长度判断是期货还是期权
            if len(sectype) == 6:  # YYYYMM 格式，表示期货
                sectype = 'FUT'
            else:  # 假设是期权 - YYYYMMDD 格式
                sectype = 'OPT'

        # 如果是外汇，需要处理货币对
        if sectype == 'CASH':  # need to address currency for Forex
            symbol, curr = symbol.split('.')

        # 尝试获取可选参数
        try:
            # 获取交易所
            exch = next(tokens)  # 如果异常则使用默认值
            # 获取货币
            curr = next(tokens)  # 如果异常则使用默认值

            # 处理期货特有参数
            if sectype == 'FUT':
                # 如果之前没有设置到期日，获取到期日
                if not expiry:
                    expiry = next(tokens)
                # 获取乘数
                mult = next(tokens)

                # 尝试获取权利，看是否是期货期权(FOP)
                right = next(tokens)
                # 如果代码执行到这里，说明这是FOP而不是FUT
                sectype = 'FOP'
                strike, mult = float(mult), ''  # 将乘数赋值给行权价，并清空乘数

                # 再次尝试获取乘数
                mult = next(tokens)

            # 处理期权特有参数
            elif sectype == 'OPT':
                # 如果之前没有设置到期日，获取到期日
                if not expiry:
                    expiry = next(tokens)
                # 获取行权价
                strike = float(next(tokens))  # 如果异常则使用默认值
                # 获取权利（看涨/看跌）
                right = next(tokens)  # 如果异常则使用默认值

                # 获取乘数（对期权可能无用，但不会造成伤害）
                mult = next(tokens)

        except StopIteration:
            # 如果参数不够，忽略错误
            pass

        # 创建初始合约对象
        precon = self.ib.makecontract(
            symbol=symbol, sectype=sectype, exch=exch, curr=curr,
            expiry=expiry, strike=strike, right=right, mult=mult)

        # 返回创建的合约对象
        return precon

    def start(self):
        '''启动IB连接并获取实际合约和详细信息（如果存在）'''
        # 调用父类的start方法
        super(IBData, self).start()
        # 启动store并获取等待队列
        self.qlive = self.ib.start(data=self)
        self.qhist = None  # 历史数据队列初始化为None

        # 根据参数和时间帧确定是否使用RTVolume还是RealTimeBars
        self._usertvol = not self.p.rtbar  # 如果不使用rtbar，则使用RTVolume
        tfcomp = (self._timeframe, self._compression)
        if tfcomp < self.RTBAR_MINSIZE:
            # 如果请求的时间帧/压缩不被rtbars支持，强制使用RTVolume
            self._usertvol = True

        # 初始化合约相关变量
        self.contract = None  # 实际合约
        self.contractdetails = None  # 合约详情
        self.tradecontract = None  # 交易合约
        self.tradecontractdetails = None  # 交易合约详情

        # 如果指定了额外的回填数据源，则从该数据源开始
        if self.p.backfill_from is not None:
            self._state = self._ST_FROM  # 设置状态为FROM
            self.p.backfill_from.setenvironment(self._env)  # 设置环境
            self.p.backfill_from._start()  # 启动回填数据源
        else:
            self._state = self._ST_START  # 初始状态设为START
        
        # 初始化其他状态变量
        self._statelivereconn = False  # 实时状态重连标志
        self._subcription_valid = False  # 订阅状态
        self._storedmsg = dict()  # 存储待处理的实时消息

        # 如果IB未连接，直接返回
        if not self.ib.connected():
            return

        # 发送连接通知
        self.put_notification(self.CONNECTED)
        # 获取真实合约详情
        cds = self.ib.getContractDetails(self.precontract, maxcount=1)
        if cds is not None:
            # 成功获取合约详情
            cdetails = cds[0]
            self.contract = cdetails.contractDetails.m_summary  # 设置实际合约
            self.contractdetails = cdetails.contractDetails  # 设置合约详情
        else:
            # 找不到合约（或找到多个）
            self.put_notification(self.DISCONNECTED)
            return

        # 处理交易合约
        if self.pretradecontract is None:
            # 如果没有指定不同的交易资产，使用标准资产
            self.tradecontract = self.contract
            self.tradecontractdetails = self.contractdetails
        else:
            # 如果指定了不同的目标资产（典型的一些CDS产品）
            # 获取另一组详情
            cds = self.ib.getContractDetails(self.pretradecontract, maxcount=1)
            if cds is not None:
                # 成功获取交易合约详情
                cdetails = cds[0]
                self.tradecontract = cdetails.contractDetails.m_summary
                self.tradecontractdetails = cdetails.contractDetails
            else:
                # 找不到合约（或找到多个）
                self.put_notification(self.DISCONNECTED)
                return

        # 如果当前状态为START，完成初始化并执行START状态的逻辑
        if self._state == self._ST_START:
            self._start_finish()  # 完成初始化
            self._st_start()  # 执行START状态的逻辑

    def stop(self):
        '''停止数据源并通知商店停止'''
        # 调用父类的stop方法
        super(IBData, self).stop()
        # 停止IB连接
        self.ib.stop()

    def reqdata(self):
        '''请求实时数据。检查现金/非现金类型和useRT参数'''
        # 如果没有合约或订阅已经有效，直接返回
        if self.contract is None or self._subcription_valid:
            return

        # 根据用户配置选择请求方式
        if self._usertvol:
            # 使用市场数据（tick数据）
            self.qlive = self.ib.reqMktData(self.contract, self.p.what)
        else:
            # 使用实时K线数据
            self.qlive = self.ib.reqRealTimeBars(self.contract)

        # 标记订阅状态为有效
        self._subcription_valid = True
        return self.qlive

    def canceldata(self):
        '''取消市场数据订阅，检查资产类型和rtbar参数'''
        # 如果没有合约，直接返回
        if self.contract is None:
            return

        # 根据用户配置选择取消方式
        if self._usertvol:
            # 取消市场数据订阅
            self.ib.cancelMktData(self.qlive)
        else:
            # 取消实时K线订阅
            self.ib.cancelRealTimeBars(self.qlive)

    def haslivedata(self):
        """检查是否有实时数据可用"""
        # 如果存在已存储的消息或实时队列有效，返回True
        return bool(self._storedmsg or self.qlive)

    def _load(self):
        """
        加载数据的核心方法，实现为一个有限状态机
        处理不同状态下的数据获取逻辑
        """
        # 如果没有合约或状态为OVER，无法进行操作
        if self.contract is None or self._state == self._ST_OVER:
            return False  # 无法执行任何操作

        # 无限循环，直到返回或遇到return语句
        while True:
            # 处理实时数据状态
            if self._state == self._ST_LIVE:
                try:
                    # 尝试获取存储的消息或从队列获取实时消息，设置超时
                    msg = (self._storedmsg.pop(None, None) or
                           self.qlive.get(timeout=self._qcheck))
                except queue.Empty:
                    # 队列超时，表示当前没有新数据
                    if True:
                        return None

                    # 以下代码已被禁用，直到进行进一步检查
                    if not self._statelivereconn:
                        return None  # 表示超时情况

                    # 等待数据但什么都没收到 - 补充数据直到现在
                    dtend = self.num2date(date2num(datetime.datetime.utcnow()))
                    dtbegin = None
                    if len(self) > 1:
                        dtbegin = self.num2date(self.datetime[-1])

                    # 请求历史数据进行补充
                    self.qhist = self.ib.reqHistoricalDataEx(
                        contract=self.contract,
                        enddate=dtend, begindate=dtbegin,
                        timeframe=self._timeframe,
                        compression=self._compression,
                        what=self.p.what, useRTH=self.p.useRTH, tz=self._tz,
                        sessionend=self.p.sessionend)

                    # 如果上一个状态不是延迟，发送延迟通知
                    if self._laststatus != self.DELAYED:
                        self.put_notification(self.DELAYED)

                    # 切换到历史回填状态
                    self._state = self._ST_HISTORBACK

                    # 重置重连状态并继续循环
                    self._statelivereconn = False
                    continue  # 重新进入循环并处理历史回填状态

                # 处理连接中断情况
                if msg is None:  # 历史/回填期间连接中断
                    # 标记订阅无效
                    self._subcription_valid = False
                    # 发送连接中断通知
                    self.put_notification(self.CONNBROKEN)
                    # 尝试重新连接
                    if not self.ib.reconnect(resub=True):
                        # 重连失败，发送断开连接通知
                        self.put_notification(self.DISCONNECTED)
                        return False  # 失败

                    # 设置重连状态并继续循环
                    self._statelivereconn = self.p.backfill
                    continue

                # 处理未订阅错误
                if msg == -354:
                    self.put_notification(self.NOTSUBSCRIBED)
                    return False

                # 处理连接中断错误
                elif msg == -1100:  # 连接中断
                    # 标记订阅无效并设置重连状态
                    self._subcription_valid = False
                    self._statelivereconn = self.p.backfill
                    continue

                # 处理连接中断但tickerId保留的情况
                elif msg == -1102:  # 连接中断/恢复，tickerId保留
                    # 消息可能重复
                    if not self._statelivereconn:
                        # 设置重连状态
                        self._statelivereconn = self.p.backfill
                    continue

                # 处理连接中断且tickerId丢失的情况
                elif msg == -1101:  # 连接中断/恢复，tickerId丢失
                    # 消息可能重复
                    self._subcription_valid = False
                    if not self._statelivereconn:
                        # 设置重连状态并重新订阅
                        self._statelivereconn = self.p.backfill
                        self.reqdata()  # 重新订阅
                    continue

                # 处理撤销事件，当前订阅失效
                elif msg == -10225:  # 发生撤销事件，当前订阅被停用
                    self._subcription_valid = False
                    if not self._statelivereconn:
                        # 设置重连状态并重新订阅
                        self._statelivereconn = self.p.backfill
                        self.reqdata()  # 重新订阅
                    continue

                # 处理意外的整型消息
                elif isinstance(msg, integer_types):
                    # 跳过历史数据的意外通知
                    # 可能是"尚未处理的未连接"
                    self.put_notification(self.UNKNOWN, msg)
                    continue

                # 根据预期的返回类型处理消息
                if not self._statelivereconn:
                    # 如果不在重连状态中
                    if self._laststatus != self.LIVE:
                        # 如果上一个状态不是LIVE
                        if self.qlive.qsize() <= 1:  # 实时队列很短
                            # 发送LIVE通知
                            self.put_notification(self.LIVE)

                    # 根据数据类型加载数据
                    if self._usertvol:
                        # 加载RTVolume数据
                        ret = self._load_rtvolume(msg)
                    else:
                        # 加载RTBar数据
                        ret = self._load_rtbar(msg)
                    if ret:
                        # 如果加载成功，返回True
                        return True

                    # 无法加载K线，继续获取新的
                    continue

                # 处理重连 - 尝试回填
                # 保存消息
                self._storedmsg[None] = msg  # 保存消息

                # 执行回填操作
                if self._laststatus != self.DELAYED:
                    # 如果上一个状态不是延迟，发送延迟通知
                    self.put_notification(self.DELAYED)

                # 确定历史数据的开始时间
                dtend = None
                if len(self) > 1:
                    # 长度为1...第一次转发
                    # 获取UTC格式的开始日期，类似msg.datetime
                    dtbegin = num2date(self.datetime[-1])
                elif self.fromdate > float('-inf'):
                    # 使用用户指定的开始日期
                    dtbegin = num2date(self.fromdate)
                else:  # 第一个K线且没有设置开始日期
                    # 传递None以在一个请求中获取最大可能的数据
                    dtbegin = None

                # 确定历史数据的结束时间
                dtend = msg.datetime if self._usertvol else msg.time

                # 请求历史数据
                self.qhist = self.ib.reqHistoricalDataEx(
                    contract=self.contract, enddate=dtend, begindate=dtbegin,
                    timeframe=self._timeframe, compression=self._compression,
                    what=self.p.what, useRTH=self.p.useRTH, tz=self._tz,
                    sessionend=self.p.sessionend)

                # 切换到历史回填状态
                self._state = self._ST_HISTORBACK
                # 重置重连状态
                self._statelivereconn = False  # 不再处于实时连接恢复状态
                continue

            # 处理历史回填状态
            elif self._state == self._ST_HISTORBACK:
                # 从历史队列获取消息
                msg = self.qhist.get()
                # 处理连接中断情况
                if msg is None:  # 历史/回填期间连接中断
                    # 情况未管理，直接退出
                    self._subcription_valid = False
                    self.put_notification(self.DISCONNECTED)
                    return False  # 错误管理取消了队列

                # 处理未订阅数据错误
                elif msg == -354:  # 数据未订阅
                    self._subcription_valid = False
                    self.put_notification(self.NOTSUBSCRIBED)
                    return False

                # 处理无权限错误
                elif msg == -420:  # 没有数据权限
                    self._subcription_valid = False
                    self.put_notification(self.NOTSUBSCRIBED)
                    return False

                # 处理意外的整型消息
                elif isinstance(msg, integer_types):
                    # 跳过历史数据的意外通知
                    # 可能是"尚未处理的未连接"
                    self.put_notification(self.UNKNOWN, msg)
                    continue

                # 处理有效的历史数据
                if msg.date is not None:
                    # 尝试加载历史数据
                    if self._load_rtbar(msg, hist=True):
                        return True  # 加载成功

                    # 日期来自重叠的历史请求
                    continue

                # 历史数据结束
                if self.p.historical:  # 仅历史模式
                    # 发送断开连接通知
                    self.put_notification(self.DISCONNECTED)
                    return False  # 历史数据结束

                # 如果需要实时数据，切换到实时状态
                self._state = self._ST_LIVE
                continue

            # 处理从外部源回填状态
            elif self._state == self._ST_FROM:
                # 检查额外数据源是否已耗尽
                if not self.p.backfill_from.next():
                    # 额外数据源已消耗完
                    self._state = self._ST_START  # 切换到START状态
                    continue

                # 复制相同名称的线
                for alias in self.lines.getlinealiases():
                    # 获取源数据线
                    lsrc = getattr(self.p.backfill_from.lines, alias)
                    # 获取目标数据线
                    ldst = getattr(self.lines, alias)
                    # 复制数据
                    ldst[0] = lsrc[0]

                # 数据复制成功，返回True
                return True

            # 处理初始状态
            elif self._state == self._ST_START:
                # 执行START状态的逻辑
                if not self._st_start():
                    return False

    def _st_start(self):
        """处理START状态的逻辑"""
        # 如果是历史模式
        if self.p.historical:
            # 发送延迟通知
            self.put_notification(self.DELAYED)
            
            # 确定历史数据的结束时间
            dtend = None
            if self.todate < float('inf'):
                dtend = num2date(self.todate)

            # 确定历史数据的开始时间
            dtbegin = None
            if self.fromdate > float('-inf'):
                dtbegin = num2date(self.fromdate)

            # 请求历史数据
            self.qhist = self.ib.reqHistoricalDataEx(
                contract=self.contract, enddate=dtend, begindate=dtbegin,
                timeframe=self._timeframe, compression=self._compression,
                what=self.p.what, useRTH=self.p.useRTH, tz=self._tz,
                sessionend=self.p.sessionend)

            # 切换到历史回填状态
            self._state = self._ST_HISTORBACK
            return True  # 继续执行

        # 如果需要实时数据
        # 尝试重新连接
        if not self.ib.reconnect(resub=True):
            # 连接失败，发送断开连接通知
            self.put_notification(self.DISCONNECTED)
            # 设置状态为OVER
            self._state = self._ST_OVER
            return False  # 失败

        # 设置重连状态为是否需要初始回填
        self._statelivereconn = self.p.backfill_start
        # 如果需要初始回填，发送延迟通知
        if self.p.backfill_start:
            self.put_notification(self.DELAYED)

        # 设置状态为LIVE
        self._state = self._ST_LIVE
        return True  # 没有return语句，隐式继续

    def _load_rtbar(self, rtbar, hist=False):
        """
        加载实时K线数据(RTBars)
        一个完整的5秒K线由实时tick构成，包含开盘/最高/最低/收盘/成交量价格
        历史数据具有相同的数据，但使用'date'而不是'time'作为日期时间
        """
        # 转换日期时间
        dt = date2num(rtbar.time if not hist else rtbar.date)
        # 如果日期早于已交付的日期且不允许延迟通过，返回失败
        if dt < self.lines.datetime[-1] and not self.p.latethrough:
            return False  # 不能交付早于已交付的数据

        # 设置日期时间
        self.lines.datetime[0] = dt
        # 将tick放入K线
        self.lines.open[0] = rtbar.open  # 设置开盘价
        self.lines.high[0] = rtbar.high  # 设置最高价
        self.lines.low[0] = rtbar.low  # 设置最低价
        self.lines.close[0] = rtbar.close  # 设置收盘价
        self.lines.volume[0] = rtbar.volume  # 设置成交量
        self.lines.openinterest[0] = 0  # 设置未平仓量为0

        # 加载成功
        return True

    def _load_rtvolume(self, rtvol):
        """
        加载实时成交量数据(RTVolume)
        交付单个tick并用于整个价格集
        包含开盘/最高/最低/收盘/成交量价格
        """
        # 日期时间转换
        dt = date2num(rtvol.datetime)
        # 如果日期早于已交付的日期且不允许延迟通过，返回失败
        if dt < self.lines.datetime[-1] and not self.p.latethrough:
            return False  # 不能交付早于已交付的数据

        # 设置日期时间
        self.lines.datetime[0] = dt

        # 将tick放入K线
        tick = rtvol.price  # 获取价格
        self.lines.open[0] = tick  # 设置开盘价
        self.lines.high[0] = tick  # 设置最高价
        self.lines.low[0] = tick  # 设置最低价
        self.lines.close[0] = tick  # 设置收盘价
        self.lines.volume[0] = rtvol.size  # 设置成交量
        self.lines.openinterest[0] = 0  # 设置未平仓量为0

        # 加载成功
        return True
