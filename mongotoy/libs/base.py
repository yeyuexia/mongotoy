# coding: utf8

from .fields import Field
from .util import get_collection_name


class BaseMetaClass(type):
    def __new__(cls, name, bases, attrs):
        for attr_key, attr_value in attrs.iteritems():
            if isinstance(attr_value, Field):
                attr_value.__field_name__ = attr_key
        new_class = super(BaseMetaClass, cls).__new__(cls, name, bases, attrs)
        for attr_key, attr_value in attrs.iteritems():
            if isinstance(attr_value, Field):
                attr_value.__parent__ = new_class
        return new_class


class MetaClass(BaseMetaClass):
    def __new__(cls, name, bases, attrs):
        if "__collection__" not in attrs:
            attrs["__collection__"] = get_collection_name(name)
        return super(MetaClass, cls).__new__(cls, name, bases, attrs)
