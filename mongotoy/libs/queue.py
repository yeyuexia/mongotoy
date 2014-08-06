# coding: utf8


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
