# coding: utf8

from bson.objectid import ObjectId

from .queue import push_flush_queue, pop_from_queue
from .session import get_session
from .fields import Field, ListModelField
from .query import BaseQuery, Query, QueryOne
from .base import MetaClass, BaseMetaClass
from .consts import (
    INSERT, ID, DELETE
)
from .operators import QueryOperator, LogicalOperator, Not


class BaseModel(object):
    __persistence__ = False

    def __init__(self, *args, **kwargs):
        self.__spec__ = set()
        self.__persistence__ = kwargs.get("__persistence__", False)
        for arg in args:
            pass
        for field, field_instance in self.__class__.__dict__.iteritems():
            if isinstance(field_instance, Field):
                if field in kwargs:
                    v = kwargs.pop(field)
                else:
                    v = field_instance.get_default(self, self.__persistence__)
                setattr(self, field, v)
        self.__persistence__ = False

    def __setattr__(self, key, value):
        if key in self.__class__.__dict__:
            field = self.__class__.__dict__[key]
            if isinstance(field, Field):
                try:
                    value = field.normalize_value(
                        value, self, self.__persistence__
                    )
                except Exception:
                    raise TypeError("%s.%s, value:%s, except %s" % (
                        self.__class__.__name__,
                        key, value, field.__f_type__
                    ))
                super(BaseModel, self).__setattr__(key, value)
                if not self.__persistence__:
                    self.__spec__.add(key)
                    self.push_flush_queue(self)
            else:
                # TODO:

                super(BaseModel, self).__setattr__(key, value)
        else:
            super(BaseModel, self).__setattr__(key, value)

    def push_flush_queue(self, model):
        push_flush_queue(model)

    @classmethod
    def assert_valid_field_name(cls, key):
        if not (hasattr(cls, key) and isinstance(getattr(cls, key), Field)):
            raise KeyError("Model %s does not has field named %s" % (
                cls.__name__, key
            ))

    @classmethod
    def assert_valid_field(cls, key):
        if key.find('.') == -1:
            cls.assert_valid_field_name(key)
        else:
            key, field_key = key.split(".", 1)
            cls.assert_valid_field_name(key)
            getattr(cls, key).assert_valid_field(field_key)
        return True

    @classmethod
    def _generate_query_context(cls, *args, **kwargs):
        """
        build query command
        """
        # TODO: need to make API more simple
        spec = dict()
        for arg in args:
            if not isinstance(arg, (QueryOperator, LogicalOperator)):
                raise ValueError("not registered attribute: %s" % arg)
            for key, value in arg.compile().iteritems():
                if isinstance(arg, (QueryOperator, Not)):
                    cls.assert_valid_field(key)
                if key in spec:
                    spec[key].update(value)
                else:
                    spec.update([(key, value)])

        for key, value in kwargs.iteritems():
            cls.assert_valid_field(key)
            field = getattr(cls, key)
            if isinstance(value, BaseQuery):
                value.parent = getattr(cls, key)
            else:
                try:
                    value = field.normalize_value(
                        value, cls, False
                    )
                except Exception:
                    raise TypeError(
                        "ValueTypeError: %s.%s  value: %s, except %s" % (
                            cls.__name__, key, value, field.__f_type__
                        )
                    )
            if isinstance(value, list) and len(value) == 1:
                spec[key] = value[0]
            else:
                spec[key] = value
        return spec

    @classmethod
    def query(cls, *args, **kwargs):
        """
        Query the database.
        """
        return Query(cls, spec=cls._generate_query_context(*args, **kwargs))

    @classmethod
    def get_by(cls, *args, **kwargs):
        """
        get a model instance by the pass arguments
        """
        return QueryOne(cls, spec=cls._generate_query_context(
            *args, **kwargs
        ))()

    def update(self, **kwargs):
        """
        update model
        """
        for key, value in kwargs.iteritems():
            setattr(self, key, value)

    def to_dict(self):
        """
        gather all field values into a dict
        """
        res = dict()
        for field, v in self.__class__.__dict__.iteritems():
            if isinstance(v, ListModelField):
                res[field] = [
                    value.to_dict() for value in getattr(self, field)
                ]
            elif isinstance(v, Field):
                value = getattr(self, field)
                if issubclass(v.__f_type__, SubModel):
                    res[field] = value.to_dict()
                else:
                    res[field] = value
        return res


class Model(BaseModel):
    """
    you need declear a model class for you mongo collection before you can use mongotoy.
    eg:

        class TestModel(Model):
            __collectionname__ = "test_model"

            class SubModel1(SubModel):
                sub_field1 = Field(str, "unknown")
                sub_field2 = Field(dict, {})
            field1 = Field(float, 1)
            field2 = Field(list, None)
            field3 = Field(SubModel)
    """

    __collectionname__ = None
    __metaclass__ = MetaClass
    __engine__ = None
    _id = Field(ObjectId, None)

    def __new__(cls, **kwargs):
        return super(Model, cls).__new__(cls)

    def __init__(self, *args, **kwargs):
        value = kwargs.pop(ID, self._id.get_default())
        self._id = self._id.normalize_value(
            value, self, self.__persistence__
        )
        super(Model, self).__init__(*args, **kwargs)

    @classmethod
    def get(cls, model_id):
        """
        get a model instance by id
        """
        return QueryOne(cls, spec=cls._generate_query_context(
            _id=model_id
        ))()

    @classmethod
    def get_by(cls, *args, **kwargs):
        return super(Model, cls).get_by(*args, **kwargs)

    @classmethod
    def query(cls, *args, **kwargs):
        return super(Model, cls).query(*args, **kwargs)

    @classmethod
    def get_by_or_create(cls, **kwargs):
        """
        get or create a model instance by the pass arguments.
        """
        model = cls.get_by(**kwargs)
        if model is None:
            model = cls(**kwargs)
            model.save()
        return model

    def save(self):
        pop_from_queue(self)
        command = self.to_dict()
        del command[ID]
        _id = get_session().execute(
            self.__collectionname__, INSERT, dict(doc_or_docs=command)
        )
        self._id = _id

    def delete(self):
        get_session().execute(
            self.__collectionname__, DELETE, dict(spec_or_id={ID: self._id})
        )

    def to_dict(self):
        res = super(Model, self).to_dict()
        res[ID] = str(self._id) if self._id else None
        return res


class SubModel(BaseModel):
    __parent__ = None
    __fieldname__ = None

    __metaclass__ = BaseMetaClass

    def __init__(self, *args, **kwargs):
        self.__parent__ = kwargs.pop("__parent__", None)
        self.__fieldname__ = kwargs.pop("__fieldname__", None)
        super(SubModel, self).__init__(*args, **kwargs)

    def push_flush_queue(self, model):
        if self.__parent__ is None:
            return
        model = self
        while isinstance(model, SubModel) and model.__parent__:
            model = model.__parent__
        super(SubModel, self).push_flush_queue(model)
