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

from collections import OrderedDict
import itertools
import sys

import backtrader as bt
from .utils.py3 import zip, string_types, with_metaclass


def findbases(kls, topclass):
    """
    递归查找指定类的所有符合条件的基类
    
    该函数会查找一个类的继承树中所有继承自特定顶级类的基类，按继承顺序返回
    
    参数:
        kls: 要查找基类的目标类
        topclass: 用于筛选的顶级类，只返回其子类
        
    返回:
        包含所有符合条件基类的列表，按照继承层次从远到近排序
    示例：
    假设以下类继承关系：

    class TopClass: pass
    class A(TopClass): pass
    class B(A): pass
    class C(B): pass
    调用 findbases(C, TopClass) 的执行步骤分解：
    ​​处理 C 的基类 B​​
        B 是 TopClass 的子类，触发递归：findbases(B, TopClass)。
            递归处理 B 的基类 A：
                A 是 TopClass 的子类，继续递归：findbases(A, TopClass)。
                处理 A 的基类 TopClass：
                    TopClass 是 TopClass 的子类，递归其基类（如 object，不满足条件）。
                    结果列表为空，添加 TopClass → retval = [TopClass]。
                合并递归结果 [TopClass]，并添加 A → retval = [TopClass, A]。
            合并递归结果 [TopClass, A]，并添加 B → retval = [TopClass, A, B]。
    ​​最终返回结果​​
    findbases(C, TopClass) 返回 [TopClass, A, B]，表示 C 的继承链中所有继承自 TopClass 的基类，按从远到近排序。
    """
    # 初始化返回值列表
    retval = list()
    
    # 遍历类的所有直接基类
    for base in kls.__bases__:
        # 检查当前基类是否是topclass的子类
        if issubclass(base, topclass):
            # 递归获取此基类的所有符合条件的基类，并添加到结果列表中
            retval.extend(findbases(base, topclass))
            # 将当前基类添加到结果列表，保证基类按从远到近的顺序排列
            retval.append(base)

    # 返回找到的所有符合条件的基类
    return retval


def findowner(owned, cls, startlevel=2, skip=None):
    """
    在调用栈中查找符合条件的对象所有者
    
    此函数通过检查调用栈中的局部变量，寻找特定类型的对象实例，
    常用于在backtrader中查找指标、数据源或其他组件的所有者（如策略对象）。
    
    使用案例:
    ```python
    # 在指标中查找所属的策略对象
    class MyIndicator(bt.Indicator):
        def __init__(self):
            # 查找拥有此指标的策略实例
            self.strategy = findowner(self, bt.Strategy)
            if self.strategy is not None:
                # 使用策略对象的信息
                print(f"Found owner strategy: {self.strategy.__class__.__name__}")
    ```
    
    使用场景:
    - 指标需要访问其所属策略的参数或方法
    - 数据源需要查找创建它的系统组件
    - 在复杂组件层次中建立对象间的引用关系
    
    参数:
        owned: 被拥有的对象，即要查找其所有者的对象
        cls: 所有者应该属于的类
        startlevel: 开始搜索的调用栈层级（2表示跳过当前函数和直接调用者）
        skip: 在搜索中需要跳过的对象
        
    返回:
        找到的所有者对象，如果未找到则返回None
    """
    # 从指定层级开始遍历调用栈帧，跳过当前函数和直接调用者
    for framelevel in itertools.count(startlevel):
        try:
            # 获取特定层级的栈帧： _getframe() 方法是返回调用堆栈中指定深度（depth）的​​帧对象（frame object）​​，包含当前执行的代码上下文信息：
                                # ​​depth=0​​（默认）：返回当前函数的帧对象。
                                # ​​depth=1​​：返回调用当前函数的上一级帧对象（调用者）。
                                # 若 depth 超过堆栈深度，抛出 ValueError。
            frame = sys._getframe(framelevel)
        except ValueError:
            # 如果超出了调用栈的最大深度，说明没有找到符合条件的所有者，终止循环
            break

        # 在常规代码中查找名为'self'的局部变量，这通常是对象实例
        self_ = frame.f_locals.get('self', None)
        # 检查获取的self_对象是否满足条件：
        # 1. 不是要跳过的对象
        # 2. 不是被查找所有者的对象本身
        # 3. 是指定类的实例
        if skip is not self_:
            if self_ is not owned and isinstance(self_, cls):
                # 找到符合条件的所有者，返回
                return self_

        # 在元类方法中查找名为'_obj'的局部变量
        # 这是元编程中可能存在的对象引用
        obj_ = frame.f_locals.get('_obj', None)
        # 对'_obj'执行与'self'相同的检查
        if skip is not obj_:
            if obj_ is not owned and isinstance(obj_, cls):
                # 找到符合条件的所有者，返回
                return obj_

    # 遍历完所有栈帧仍未找到符合条件的所有者，返回None
    return None


class MetaBase(type):
    def doprenew(cls, *args, **kwargs):
        """
        实例化过程的第一步：预创建阶段
        此方法在实际创建类实例前被调用，可以修改类本身或传入的参数
        参数:
            cls: 调用此元类的类
            args, kwargs: 实例化时传入的参数
        返回:
            可能被修改的类和参数元组(cls, args, kwargs)
        """
        return cls, args, kwargs

    def donew(cls, *args, **kwargs):
        """
        实例化过程的第二步：创建实例
        调用类的__new__方法创建对象实例
        参数:
            cls: 调用此元类的类
            args, kwargs: 实例化时传入的参数
        返回:
            新创建的对象和可能被修改的参数元组(_obj, args, kwargs)
        """
        _obj = cls.__new__(cls, *args, **kwargs)
        return _obj, args, kwargs

    def dopreinit(cls, _obj, *args, **kwargs):
        """
        实例化过程的第三步：初始化前处理
        在调用__init__前对对象进行处理
        参数:
            cls: 调用此元类的类
            _obj: 新创建的对象
            args, kwargs: 实例化时传入的参数
        返回:
            对象和可能被修改的参数元组(_obj, args, kwargs)
        """
        return _obj, args, kwargs

    def doinit(cls, _obj, *args, **kwargs):
        """
        实例化过程的第四步：初始化
        调用对象的__init__方法进行初始化
        参数:
            cls: 调用此元类的类
            _obj: 新创建的对象
            args, kwargs: 实例化时传入的参数
        返回:
            初始化后的对象和可能被修改的参数元组(_obj, args, kwargs)
        """
        if args:
            # 如果args不为空，调用__init__方法进行初始化
            _obj.__init__(*args)
        else:
            _obj.__init__(*args, **kwargs)
        return _obj, args, kwargs

    def dopostinit(cls, _obj, *args, **kwargs):
        """
        实例化过程的第五步：初始化后处理
        在对象初始化完成后进行额外处理
        参数:
            cls: 调用此元类的类
            _obj: 已初始化的对象
            args, kwargs: 实例化时传入的参数
        返回:
            最终处理后的对象和参数元组(_obj, args, kwargs)
        """
        return _obj, args, kwargs

    def __call__(cls, *args, **kwargs):
        """
        元类的调用方法，控制类实例化的完整流程
        按顺序调用上述五个方法，完成从类到实例的创建过程
        参数:
            cls: 调用此元类的类
            args, kwargs: 实例化时传入的参数
        返回:
            创建并初始化完成的对象实例
        """
        cls, args, kwargs = cls.doprenew(*args, **kwargs)
        _obj, args, kwargs = cls.donew(*args, **kwargs)
        _obj, args, kwargs = cls.dopreinit(_obj, *args, **kwargs)
        # _obj, args, kwargs = cls.doinit(_obj, **args, **kwargs)
        if args:
        # 如果args是元组，不要尝试解包它
            _obj, args, kwargs = cls.doinit(_obj, *args, **kwargs)
        else:
        # 没有位置参数，只有关键字参数
            _obj, args, kwargs = cls.doinit(_obj, **kwargs)
        _obj, args, kwargs = cls.dopostinit(_obj, *args, **kwargs)
        return _obj


class AutoInfoClass(object):
    """
    自动信息类，用于管理和存储配置参数信息
    作为backtrader中参数管理的基础设施，支持参数值的查询和配置类的派生
    """
    _getpairsbase = classmethod(lambda cls: OrderedDict())  # 返回基础参数字典
    _getpairs = classmethod(lambda cls: OrderedDict())      # 返回当前参数字典
    _getrecurse = classmethod(lambda cls: False)            # 是否递归创建子参数

    @classmethod
    def _derive(cls, name, info, otherbases, recurse=False):
        """
        派生出一个新的信息类
        根据基类和提供的信息创建一个新类，合并各种来源的参数
        
        参数:
            name: 新类的名称后缀
            info: 要添加的新参数信息
            otherbases: 其他要合并参数的基类列表
            recurse: 是否递归处理参数
        返回:
            派生的新类
        """
        # collect the 3 set of infos
        # info = OrderedDict(info)
        baseinfo = cls._getpairs().copy()
        obasesinfo = OrderedDict()
        for obase in otherbases:
            if isinstance(obase, (tuple, dict)):
                obasesinfo.update(obase)
            else:
                obasesinfo.update(obase._getpairs())

        # update the info of this class (base) with that from the other bases
        baseinfo.update(obasesinfo)

        # The info of the new class is a copy of the full base info
        # plus and update from parameter
        clsinfo = baseinfo.copy()
        clsinfo.update(info)

        # The new items to update/set are those from the otherbase plus the new
        info2add = obasesinfo.copy()
        info2add.update(info)

        clsmodule = sys.modules[cls.__module__]
        newclsname = str(cls.__name__ + '_' + name)  # str - Python 2/3 compat

        # This loop makes sure that if the name has already been defined, a new
        # unique name is found. A collision example is in the plotlines names
        # definitions of bt.indicators.MACD and bt.talib.MACD. Both end up
        # definining a MACD_pl_macd and this makes it impossible for the pickle
        # module to send results over a multiprocessing channel
        namecounter = 1
        while hasattr(clsmodule, newclsname):
            newclsname += str(namecounter)
            namecounter += 1

        newcls = type(newclsname, (cls,), {})
        setattr(clsmodule, newclsname, newcls)

        setattr(newcls, '_getpairsbase',
                classmethod(lambda cls: baseinfo.copy()))
        setattr(newcls, '_getpairs', classmethod(lambda cls: clsinfo.copy()))
        setattr(newcls, '_getrecurse', classmethod(lambda cls: recurse))

        for infoname, infoval in info2add.items():
            if recurse:
                recursecls = getattr(newcls, infoname, AutoInfoClass)
                infoval = recursecls._derive(name + '_' + infoname,
                                             infoval,
                                             [])

            setattr(newcls, infoname, infoval)

        return newcls

    def isdefault(self, pname):
        """
        检查参数是否为默认值
        
        参数:
            pname: 要检查的参数名
        返回:
            如果参数值等于默认值则返回True，否则返回False
        """
        return self._get(pname) == self._getkwargsdefault()[pname]

    def notdefault(self, pname):
        """
        检查参数是否不为默认值
        
        参数:
            pname: 要检查的参数名
        返回:
            如果参数值不等于默认值则返回True，否则返回False
        """
        return self._get(pname) != self._getkwargsdefault()[pname]

    def _get(self, name, default=None):
        """
        获取属性值，支持默认值
        
        参数:
            name: 属性名称
            default: 如果属性不存在时返回的默认值
        返回:
            属性值或默认值
        """
        return getattr(self, name, default)

    @classmethod
    def _getkwargsdefault(cls):
        """
        获取默认参数字典
        
        返回:
            包含所有参数默认值的有序字典
        """
        return cls._getpairs()

    @classmethod
    def _getkeys(cls):
        """
        获取所有参数名称
        
        返回:
            参数名列表
        """
        return cls._getpairs().keys()

    @classmethod
    def _getdefaults(cls):
        """
        获取所有参数的默认值
        
        返回:
            包含所有默认值的列表
        """
        return list(cls._getpairs().values())

    @classmethod
    def _getitems(cls):
        """
        获取参数名称和值的键值对
        
        返回:
            参数字典的items视图
        """
        return cls._getpairs().items()

    @classmethod
    def _gettuple(cls):
        """
        获取参数键值对的元组
        
        返回:
            包含所有参数键值对的元组
        """
        return tuple(cls._getpairs().items())

    def _getkwargs(self, skip_=False):
        """
        获取实例的当前参数值作为kwargs字典
        
        参数:
            skip_: 如果为True，则跳过以下划线开头的参数
        返回:
            包含参数名和当前值的有序字典
        """
        l = [
            (x, getattr(self, x))
            for x in self._getkeys() if not skip_ or not x.startswith('_')]
        return OrderedDict(l)

    def _getvalues(self):
        """
        获取所有参数的当前值列表
        
        返回:
            包含所有参数当前值的列表
        """
        return [getattr(self, x) for x in self._getkeys()]

    def __new__(cls, *args, **kwargs):
        """
        创建类实例时的处理
        如果启用了递归，会自动为属性创建子对象实例
        
        参数:
            args, kwargs: 传递给父类__new__的参数
        返回:
            新创建的对象实例
        """
        obj = super(AutoInfoClass, cls).__new__(cls, *args, **kwargs)

        if cls._getrecurse():
            for infoname in obj._getkeys():
                recursecls = getattr(cls, infoname)
                setattr(obj, infoname, recursecls())

        return obj


class MetaParams(MetaBase):
    """
    参数元类，继承自MetaBase
    用于创建和管理带参数的类。这个元类负责：
    1. 收集和组织类的参数
    2. 处理包导入机制
    3. 在实例化时设置参数
    
    使用此元类的类将能够通过params/p属性访问其参数
    """
    def __new__(meta, name, bases, dct):
        """
        创建新类时调用的方法
        处理参数定义、包导入声明，并创建最终的类
        
        参数:
            meta: 元类自身
            name: 正在创建的类的名称
            bases: 正在创建的类的基类元组
            dct: 类的命名空间字典，包含类的属性和方法
            
        返回:
            创建好的新类
        """
        # 从类定义中移除params属性，以避免继承导致的参数重复
        newparams = dct.pop('params', ())

        # 定义包属性的名称
        packs = 'packages'
        # 从类定义中移除packages属性，并转换为元组
        newpackages = tuple(dct.pop(packs, ()))  # 在创建类前移除

        # 定义frompackages属性的名称
        fpacks = 'frompackages'
        # 从类定义中移除frompackages属性，并转换为元组
        fnewpackages = tuple(dct.pop(fpacks, ()))  # 在创建类前移除

        # 创建新类 - 这一步会继承预定义的"params"
        cls = super(MetaParams, meta).__new__(meta, name, bases, dct)

        # 获取类的params属性 - 默认为空的AutoInfoClass
        params = getattr(cls, 'params', AutoInfoClass)

        # 获取类的packages属性 - 默认为空元组
        packages = tuple(getattr(cls, packs, ()))
        # 获取类的frompackages属性 - 默认为空元组
        fpackages = tuple(getattr(cls, fpacks, ()))

        # 获取右侧基类(除第一个基类外)中具有params属性的类的参数
        morebasesparams = [x.params for x in bases[1:] if hasattr(x, 'params')]

        # 获取基类中的packages属性，并添加到packages中
        for y in [x.packages for x in bases[1:] if hasattr(x, packs)]:
            packages += tuple(y)

        # 获取基类中的frompackages属性，并添加到fpackages中
        for y in [x.frompackages for x in bases[1:] if hasattr(x, fpacks)]:
            fpackages += tuple(y)

        # 更新类的packages和frompackages属性
        cls.packages = packages + newpackages
        cls.frompackages = fpackages + fnewpackages

        # 创建派生的参数类并存储到新类中
        cls.params = params._derive(name, newparams, morebasesparams)

        # 返回创建好的新类
        return cls

    def donew(cls, *args, **kwargs):
        """
        重写MetaBase.donew方法，在创建实例时执行
        主要负责：
        1. 导入指定的包
        2. 处理参数值
        3. 创建实例并设置参数
        
        参数:
            cls: 调用此元类的类
            args, kwargs: 实例化时传入的参数
            
        返回:
            新创建的对象实例和可能修改后的参数元组
        """
        # 获取类所在模块
        clsmod = sys.modules[cls.__module__]
        
        # 导入类指定的packages
        for p in cls.packages:
            # 检查是否提供了包的别名
            if isinstance(p, (tuple, list)):
                p, palias = p  # 解构包名和别名
            else:
                palias = p  # 没有别名，使用包名作为别名

            # 导入包
            pmod = __import__(p)

            # 处理带点的包名，如'os.path'
            plevels = p.split('.')
            # 处理没有别名且包含多级的情况
            if p == palias and len(plevels) > 1:  # 'os.path'未设置别名
                setattr(clsmod, pmod.__name__, pmod)  # 在模块中设置'os'

            else:  # 处理有别名或单级包名的情况
                # 递归获取子模块
                for plevel in plevels[1:]:
                    pmod = getattr(pmod, plevel)

                # 设置别名到模块
                setattr(clsmod, palias, pmod)

        # 从指定包中导入 - 第2部分可以是字符串或可迭代对象
        for p, frompackage in cls.frompackages:
            # 确保frompackage是元组
            if isinstance(frompackage, string_types):
                frompackage = (frompackage,)  # 转换为元组

            # 处理frompackage中的每个导入项
            for fp in frompackage:
                # 检查是否提供了别名
                if isinstance(fp, (tuple, list)):
                    fp, falias = fp  # 解构导入项和别名
                else:
                    fp, falias = fp, fp  # 默认别名与导入项相同

                # 导入指定模块和属性
                # 使用str(fp)避免Python 2/3字符串类型差异
                pmod = __import__(p, fromlist=[str(fp)])
                # 获取导入的属性
                pattr = getattr(pmod, fp)
                # 设置属性到当前模块
                setattr(clsmod, falias, pattr)
                # 同时设置到所有基类的模块中
                for basecls in cls.__bases__:
                    setattr(sys.modules[basecls.__module__], falias, pattr)

        # 创建参数实例
        params = cls.params()
        # 从kwargs中获取参数值并设置到params实例
        for pname, pdef in cls.params._getitems():
            # 使用pop从kwargs中移除参数，如果不存在则使用默认值
            setattr(params, pname, kwargs.pop(pname, pdef))

        # 创建对象实例
        _obj, args, kwargs = super(MetaParams, cls).donew(*args, **kwargs)
        # 设置参数到对象
        _obj.params = params
        _obj.p = params  # 为方便使用添加shorter别名

        # 参数值已在__init__之前设置
        return _obj, args, kwargs


class ParamsBase(with_metaclass(MetaParams, object)):
    pass  # stub to allow easy subclassing without metaclasses


class ItemCollection(object):
    '''
    项目集合类，用于管理可通过索引和名称访问的项目集合。
    
    简单使用案例:
    ```python
    # 创建集合
    collection = ItemCollection()
    
    # 添加项目
    collection.append(item1, "第一项")
    collection.append(item2, "第二项")
    
    # 通过索引访问
    first_item = collection[0]
    
    # 通过名称访问
    second_item = collection.getbyname("第二项")
    
    # 获取所有名称
    all_names = collection.getnames()
    
    # 获取所有项目及其名称
    for name, item in collection.getitems():
        print(f"{name}: {item}")
    ```
    
    Holds a collection of items that can be reached by

      - Index
      - Name (if set in the append operation)
    '''
    def __init__(self):
        """
        初始化项目集合
        创建两个空列表分别用于存储项目和对应的名称
        """
        self._items = list()
        self._names = list()

    def __len__(self):
        """
        返回集合中项目的数量
        
        返回:
            集合中项目的数量
        """
        return len(self._items)

    def append(self, item, name=None):
        """
        将项目添加到集合中，并可选择性地为其指定名称
        
        参数:
            item: 要添加的项目
            name: 项目的名称，如果提供则可以通过该名称访问项目
        
        如果指定了名称，该项目可通过 `collection.name` 直接访问
        """
        setattr(self, name, item)
        self._items.append(item)
        if name:
            self._names.append(name)

    def __getitem__(self, key):
        """
        通过索引访问项目
        
        参数:
            key: 项目的索引位置
        返回:
            索引位置对应的项目
            
        允许使用 collection[index] 语法访问项目
        """
        return self._items[key]

    def getnames(self):
        """
        获取所有项目的名称列表
        
        返回:
            包含所有已命名项目名称的列表
        """
        return self._names

    def getitems(self):
        """
        获取名称和项目的配对
        
        返回:
            名称和项目的zip迭代器，可用于循环遍历所有命名项目
        """
        return zip(self._names, self._items)

    def getbyname(self, name):
        """
        通过名称查找并返回项目
        
        参数:
            name: 项目的名称
        返回:
            与该名称关联的项目
            
        如果名称不存在，会引发ValueError异常
        """
        idx = self._names.index(name)
        return self._items[idx]
