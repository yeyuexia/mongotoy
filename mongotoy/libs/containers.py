# coding: utf8

import copy

from queue import push_flush_queue


def modified(func):
    def _(self, *args):
        origin = copy.copy(self)
        ret = func(*args)
        if origin != self:
            push_flush_queue(self.__lord__)
        return ret
    return _


class ToyList(list):
    def __init__(self, lord, l):
        super(ToyList, self).__init__(l)
        self.__lord__ = lord

    @modified
    def __delitem__(self, index):
        super(ToyList, self).__delitem__(index)

    @modified
    def __imul__(self, multiply):
        super(ToyList, self).__imul__(multiply)

    @modified
    def append(self, obj):
        super(ToyList, self).append(obj)

    @modified
    def extend(self, objs):
        super(ToyList, self).extend(objs)

    @modified
    def insert(self, index, obj):
        super(ToyList, self).insert(index, obj)

    @modified
    def reverse(self):
        super(ToyList, self).reverse()


class ToySet(set):
    def __init__(self, lord, s):
        super(ToySet, self).__init__(s)
        self.__lord__ = lord

    @modified
    def __delitem__(self, obj):
        super(ToySet, self).__delitem__(obj)

    @modified
    def __iand__(self, s):
        super(ToySet, self).__iand__(s)

    @modified
    def __ior__(self, s):
        super(ToySet, self).__ior__(s)

    @modified
    def __isub__(self, s):
        super(ToySet, self).__isub__(s)

    @modified
    def __ixor__(self, s):
        super(ToySet, self).__ixor__(s)

    @modified
    def add(self, obj):
        super(ToySet, self).add(obj)

    @modified
    def clear(self):
        super(ToySet, self).clear()

    @modified
    def update(self, s):
        super(ToySet, self).update(s)
