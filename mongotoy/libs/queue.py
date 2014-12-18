# coding: utf8

import threading

_queue_lock = threading.Lock()


class Queue(object):
    """use for store the models need to update"""
    def __init__(self):
        self.obj_ids = set()
        self.objs = set()

    def push(self, obj):
        if not (obj._id and obj._id in self.obj_ids):
            if obj._id:
                self.obj_ids.add(obj._id)
            self.objs.add(obj)

    def pop(self, obj):
        if obj._id and obj._id in self.obj_ids:
            self.obj_ids.remove(obj)
        if self.objs:
            self.objs.remove(obj)

    def exists(self, obj):
        return (obj._id and obj._id in self.obj_ids) or (obj in self.objs)

    def get_all(self):
        return self.objs

    def clear(self):
        self.obj_ids = set()
        self.objs = set()


flush_queue = Queue()


def pop_from_queue(model):
    model = _get_parent(model)
    if model is not None:
        if flush_queue.exists(model):
            with _queue_lock:
                if flush_queue.exists(model):
                    flush_queue.pop(model)


def push_flush_queue(model):
    model = _get_parent(model)
    if model is not None:
        if not flush_queue.exists(model):
            with _queue_lock:
                flush_queue.push(model)


def _get_parent(model):
    while hasattr(model, "__parent__"):
        model = model.__parent__
    return model


def get_lock():
    return _queue_lock
