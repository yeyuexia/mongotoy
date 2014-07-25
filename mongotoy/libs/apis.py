# coding: utf8

import copy
import types
import threading
import datetime

from bson.objectid import ObjectId

from .base import generate_field, get_collection_name
from .queue import flush_queue
from .session import get_session
from .consts import (
    QUERY_FIND, QUERY_FIND_ONE, INSERT,
    UPDATE, ID, ASCENDING, DESCENDING
)
from operators import Element, Logical, Comparison

_queue_lock = threading.Lock()


def _push_flush_queue(model):
    if isinstance(model, SubModel):
        if model.__parent__ is None:
            return
        while isinstance(model, SubModel) and model.__parent__:
            model = model.__parent__
    if not flush_queue.exists(model):
        with _queue_lock:
            flush_queue.push(model)


def flush():
    """
    push all change to mongo db. It is a block operator so would be takes some time.
    """
    with _queue_lock:
        session = get_session()
        insert_models = {}
        update_models = []
        for model in flush_queue.get_all():
            if model._id:
                update_models.append(model)
            else:
                collection_name = model.__collectionname__
                if collection_name not in insert_models:
                    insert_models[collection_name] = []
                insert_models[collection_name].append(model)
        for collection_name, contexts in insert_models.iteritems():
            command = [m.to_dict() for m in contexts]
            for c in command:
                del c[ID]
            ids = session.execute(
                collection_name, INSERT, command
            )
            for model, model_id in zip(contexts, ids):
                model._id = model_id
        for model in update_models:
            update_command = model.to_dict()
            del update_command[ID]
            session.execute(
                model.__collectionname__, UPDATE,
                dict(spec={ID: model._id},
                     document=update_command)
            )
        flush_queue.clear()


class Field(object):
    def __init__(self, field_type, default=None, value=None):
        assert isinstance(field_type, type), "unknown type for field"
        self.field_type = field_type
        self.default = default
        self.value = value

    def __desc__(self):
        return "Field %s, type: %s, default: %s" % (
            self.__name__, self.field_type, self.default
        )

    def __mapper__(self):
        return self.value


class BaseModel(object):
    def __init__(self, **kwargs):
        self.__persistence__ = kwargs.get("__persistence__", False)
        for field, value in self.__class__.__dict__.iteritems():
            if isinstance(value, Field):
                if field in kwargs:
                    v = kwargs.pop(field)
                else:
                    if issubclass(value.field_type, SubModel):
                        v = value.field_type(
                            __parent__=self, __fieldname__=field,
                            __persistence__=self.__persistence__
                        )
                    else:
                        if isinstance(value.default, types.FunctionType):
                            v = value.default()
                        else:
                            v = copy.deepcopy(value.default)
                setattr(self, field, v)
        self.__persistence__ = False

    def __setattr__(self, key, value):
        if key in self.__class__.__dict__:
            field = self.__class__.__dict__[key]
            if isinstance(field, Field):
                value = self._normalize_field_value(field, value)
                super(BaseModel, self).__setattr__(key, value)
                if not self.__persistence__:
                    _push_flush_queue(self)
            else:
                # TODO:

                super(BaseModel, self).__setattr__(key, value)
        else:
            super(BaseModel, self).__setattr__(key, value)

    def _normalize_field_value(self, field, value):
        if value is not None and not isinstance(value, field.field_type):
            if field.field_type is str and isinstance(value, unicode):
                value = value.encode("utf8", "ignore")
            elif field.field_type is unicode and isinstance(value, str):
                value = value.decode("utf8", "ignore")
            elif field.field_type is datetime.datetime:
                value = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
            elif issubclass(field.field_type, SubModel):
                value = field.field_type(
                    __parent__=self, __fieldname__=field,
                    __persistence__=self.__persistence__,
                    **value
                )
            else:
                try:
                    value = field.field_type(value)
                except:
                    raise ValueError("ValueError:%s value:%s" % (
                        self.__class__.__name__, value
                    ))
        return value

    @classmethod
    def assert_vaild_field(cls, key):
        if key not in cls.__dict__:
            raise KeyError("Model %s does not has field named %s" % (
                cls.__class__.__name__, key
            ))
        return True

    @classmethod
    def _generate_query_context(cls, *args, **kwargs):
        query = dict()
        for key, value in kwargs.iteritems():
            cls.assert_vaild_field(key)
            field_type = cls.__dict__[key].field_type
            if field_type is not list and not isinstance(value, field_type):
                if isinstance(value, Comparison) or isinstance(value, Element):
                    value = value.compile()
                elif (
                    issubclass(field_type, SubModel) and isinstance(value, Query)
                ):
                    value.__parent__ = cls.__class__
                    value.__fieldname__ = key
                else:
                    try:
                        value = field_type(value)
                    except:
                        raise ValueError("ValueError: %s.%s value:%s" % (
                            cls.__class__.__name__, key, value
                        ))
            query[key] = value
        for arg in args:
            if isinstance(arg, Logical):
                arg.value = [cls._generate_query_context(v) for v in arg.value]
            else:
                raise ValueError("not registered attribute: %s" % arg)
        return query

    @classmethod
    def query(cls, *args, **kwargs):
        """
        Query the database.
        """
        return Query(cls, query=cls._generate_query_context(*args, **kwargs))

    @classmethod
    def get_by(cls, *args, **kwargs):
        """
        get a model instance by the pass arguments
        """
        return QueryOne(cls, query=cls._generate_query_context(
            *args, **kwargs
        ))()

    def update(self, **kwargs):
        """
        update model
        """
        for key, value in kwargs.iteritems():
            if value:
                setattr(self, key, value)

    def to_dict(self):
        """
        gather all field values into a dict
        """
        res = dict()
        for field, v in self.__class__.__dict__.iteritems():
            if isinstance(v, Field):
                value = getattr(self, field)
                if issubclass(v.field_type, SubModel):
                    res[field] = value.to_dict()
                elif field == "_id":
                    res[field] = str(value)
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
    __engine__ = None

    def __new__(cls, **kwargs):
        setattr(cls, "_id", Field(ObjectId, None))
        if cls.__collectionname__ is None:
            cls.__collectionname__ = get_collection_name(cls.__name__)
        return super(Model, cls).__new__(cls, **kwargs)

    @classmethod
    def get(cls, model_id):
        """
        get a model instance by id
        """
        if cls.__collectionname__ is None:
            cls.__collectionname__ = get_collection_name(cls.__name__)
        return QueryOne(cls, query=cls._generate_query_context(
            _id=model_id
        ))()

    @classmethod
    def get_by(cls, *args, **kwargs):
        if cls.__collectionname__ is None:
            cls.__collectionname__ = get_collection_name(cls.__name__)
        return super(Model, cls).get_by(*args, **kwargs)

    @classmethod
    def query(cls, *args, **kwargs):
        if cls.__collectionname__ is None:
            cls.__collectionname__ = get_collection_name(cls.__name__)
        return super(Model, cls).query(*args, **kwargs)


class SubModel(BaseModel):
    __parent__ = None
    __fieldname__ = None

    def __init__(self, **kwargs):
        self.__parent__ = kwargs.pop("__parent__")
        self.__fieldname__ = kwargs.pop("__fieldname__")
        super(SubModel, self).__init__(**kwargs)


class BaseQuery(object):

    def __init__(self, model, **kwargs):
        self.parent = None
        self.model = model
        self.commands = dict()
        self.commands["spec"] = kwargs.get("query")
        self.commands["sort"] = kwargs.get("sort")
        self.commands["fields"] = kwargs.get("filter")
        try:
            self.commands["skip"] = int(kwargs.get("skip", 0))
        except ValueError:
            raise ValueError("skip must be int")

    def __repr__(self):
        collection, command, query = self.compile_context()
        return "%s.%s(%s)" % (
            collection, command,
            ", ".join(
                ["%s=%s" % (key, value) for key, value in query.iteritems()]
            )
        )

    def get_commands(self):
        commands = copy.deepcopy(self.commands)
        if self.parent:
            for key in ("spec", "fields", "sort"):
                values = commands[key]
                if values:
                    if isinstance(values, (list, tuple)):
                        commands[key] = [
                            generate_field(self.__fieldname__, v) for v in values
                        ]
                    elif isinstance(values, dict):
                        new_values = dict()
                        for k, v in values.iteritems():
                            k = generate_field(self.__fieldname__, k)
                            new_values[k] = v
                        commands[key] = new_values
                    else:
                        commands[key] = generate_field(
                            self.__fieldname__, values
                        )
        else:
            commands["timeout"] = False
        return commands

    def compile_context(self):
        raise NotImplementedError()

    def execute_context(self, context):
        raise NotImplementedError()

    def sort(self, **kwargs):
        """
        Sorts this cursor’s results.

        Pass a field name and a direction, either ASCENDING or DESCENDING:

        query().sort(field_one=ASCENDING, field_two=DESCENDING, ...)
        """
        for key, value in kwargs.iteritems():
            self.model.assert_vaild_field(key)
            if self.value not in (DESCENDING, ASCENDING):
                raise ValueError("sort, invaild key")
        self.commands["sort"] = kwargs
        return self

    def skip(self, num):
        """
        Skips the first skip results of this cursor.

        Raises TypeError if skip is not an integer.
        Raises ValueError if skip is less than 0.
        Raises InvalidOperation if this Cursor has already been used.
        The last skip applied to this cursor takes precedence.

        eg:
            query().skip(number)
        """
        try:
            self.commands["skip"] = int(num)
        except ValueError:
            raise ValueError("skip must be int")

    def filter(self, *args):
        pass


class Query(BaseQuery):
    def __init__(self, model, **kwargs):
        super(Query, self).__init__(model, **kwargs)
        self.commands["limit"] = kwargs.get("limit", 0)

    def __getitem__(self, item):
        pass

    def __iter__(self):
        context = self._compile_context()
        return self.execute_context(context)

    def compile_context(self, operation=QUERY_FIND):
        collection = self.model.__collectionname__
        return (collection, operation, self.get_commands())

    def execute_context(self, context):
        for record in get_session().execute(*context):
            yield self.model.__class__(__persistence__=True, **record)

    def all(self):
        """
        Return a list of all object
        """
        return list(self)

    def limit(self, count):
        """
        query().limit(count)
        """
        assert isinstance(count, int), "limit access int argument"
        self.commands["limit"] = count
        return self

    def update(self, if_not_create=False, **kwargs):
        """
        update model
        """
        update = Update(self.model,
                        spec=self.get_commands()["spec"],
                        document=kwargs,
                        multi=True,
                        upsert=if_not_create)
        return get_session().execute(*update.compile_context())


class QueryOne(BaseQuery):

    def compile_context(self):
        collection = self.model.__collectionname__
        return (collection, QUERY_FIND, self.get_commands())

    def execute_context(self, context):
        for result in get_session().execute(*context).limit(-1):
            return self.model(__persistence__=True, **result)

    def __call__(self):
        return self.execute_context(self.compile_context())


class Update(object):
    """
    Update object, wrap the params of find method in pymongo
    params:
        model:
        query:
        update:
        multi:
        upsert:
    """
    def __init__(self, model, spec, document, multi, upsert):
        self.model = model
        self.spec = spec
        self.document = document
        self.multi = multi
        self.upsert = upsert

    def _generate_commands(self, model, **kwargs):
        def normalize_key_value(key, value):
            cmds = dict()
            field = getattr(model, key)
            if issubclass(field.field_type, Model):
                if isinstance(value, dict):
                    sub_commands = self._generate_commands(field.field_type, **value)
                    for k, v in sub_commands.iteritems():
                        new_key = generate_field(key, k)
                        cmds[new_key] = v
            else:
                cmds[key] = Model._normalize_field_value(field, value)

        commands = dict()
        for key, value in kwargs.iteritems():
            model.assert_vaild_field(key)
            commands.update(normalize_key_value(key, value))
        return commands

    def get_commands(self):
        return dict(
            spec=self.spec,
            document=self._generate_commands(self.model, **self.update),
            multi=self.multi,
            upsert=self.upsert
        )

    def compile_context(self):
        collection = self.model.__collectionname__
        return (collection, UPDATE, self.get_commands())
