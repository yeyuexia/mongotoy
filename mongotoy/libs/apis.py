# coding: utf8

import copy
import types
import numbers
import threading
import datetime

from bson.objectid import ObjectId

from .base import generate_field, get_collection_name
from .queue import flush_queue
from .session import get_session
from .consts import (
    QUERY_FIND, INSERT, UPDATE, ID,
    ASCENDING, DESCENDING, DELETE
)
from .operators import (
    Set, QueryOperator,
    query_operator_compiler,
    update_operator_compiler
)

_queue_lock = threading.Lock()


def _pop_from_queue(model):
    if flush_queue.exists(model):
        with _queue_lock:
            flush_queue.pop(model)


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
            commands = [m.to_dict() for m in contexts]
            for command in commands:
                del command[ID]
            ids = session.execute(
                collection_name, INSERT, dict(doc_or_docs=commands)
            )
            for model, model_id in zip(contexts, ids):
                model._id = model_id
        for model in update_models:
            update_command = model.to_dict()
            del update_command[ID]
            session.execute(
                model.__collectionname__, UPDATE,
                dict(spec={ID: model._id},
                     document=Set(update_command).compile())
            )
        flush_queue.clear()


def _normalize_field_value(field_type, value):
    if field_type is str and isinstance(value, unicode):
        value = value.encode("utf8", "ignore")
    elif field_type is unicode and isinstance(value, str):
        value = value.decode("utf8", "ignore")
    elif field_type is datetime.datetime and isinstance(value, basestring):
        if value.find("T") != -1:
            value = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
        else:
            value = datetime.datetime.strptime(value, "%Y-%m-%d")
    elif field_type is datetime.datetime and isinstance(value, numbers.Number):
        value = datetime.datetime.fromtimestamp(value)
    elif field_type is datetime.date and isinstance(value, basestring):
        value = datetime.datetime.strptime(value, "%Y-%m-%d")
    else:
        value = field_type(value)
    return value


class Field(object):
    def __init__(self, field_type, default=None):
        assert isinstance(field_type, type), "unknown type for field"
        self.field_type = field_type
        self.default = default

    def __desc__(self):
        return "Field %s, type: %s, default: %s" % (
            self.__name__, self.field_type, self.default
        )


class BaseModel(object):
    __persistence__ = False

    def __init__(self, *args, **kwargs):
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
                        if isinstance(value.default, types.FunctionType) or isinstance(value.default, types.BuiltinFunctionType):
                            v = value.default()
                        else:
                            v = copy.deepcopy(value.default)
                setattr(self, field, v)
        self.__persistence__ = False
        for arg in kwargs:
            pass

    def __setattr__(self, key, value):
        if key in self.__class__.__dict__:
            field = self.__class__.__dict__[key]
            if isinstance(field, Field):
                value = self.normalize_field_value(field, value)
                super(BaseModel, self).__setattr__(key, value)
                if not self.__persistence__:
                    _push_flush_queue(self)
            else:
                # TODO:

                super(BaseModel, self).__setattr__(key, value)
        else:
            super(BaseModel, self).__setattr__(key, value)

    def normalize_field_value(self, field, value):
        """
        convert value to field type instance.
        """
        if value is not None and not isinstance(value, field.field_type):
            if issubclass(field.field_type, SubModel):
                if isinstance(value, dict):
                    value = field.field_type(
                        __parent__=self, __fieldname__=field,
                        __persistence__=self.__persistence__,
                        **value
                    )
                elif isinstance(value, SubModel):
                    value.__parent__ = self
                    value.__fieldname__ = field
            else:
                try:
                    value = _normalize_field_value(field.field_type, value)
                except Exception:
                    raise ValueError("ValueError:%s value:%s" % (
                        self.__class__.__name__, value
                    ))
        return value

    @classmethod
    def assert_vaild_field(cls, key):
        if not (hasattr(cls, key) and isinstance(getattr(cls, key), Field)):
            raise KeyError("Model %s does not has field named %s" % (
                cls.__name__, key
            ))
        return True

    @classmethod
    def _generate_query_context(cls, *args, **kwargs):
        """
        build query command
        """
        # TODO: need to make API more simple
        spec = dict()
        for arg in args:
            if isinstance(arg, QueryOperator):
                for key, value in query_operator_compiler(cls, arg).iteritems():
                    if key in spec:
                        spec[key].update(value)
                    else:
                        spec.update([(key, value)])
            else:
                raise ValueError("not registered attribute: %s" % arg)

        for key, value in kwargs.iteritems():
            cls.assert_vaild_field(key)
            field_type = getattr(cls, key).field_type
            if field_type is not list and not isinstance(value, field_type):
                if issubclass(field_type, SubModel):
                    if isinstance(value, Query):
                        value.__parent__ = cls
                        value.__fieldname__ = key
                    elif isinstance(value, dict):
                        value = Query(cls, cls._generate_query_context(**value))
                else:
                    try:
                        value = _normalize_field_value(field_type, value)
                    except Exception:
                        raise ValueError("ValueError: %s.%s value:%s" % (
                            cls.__name__, key, value
                        ))
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
            if isinstance(v, Field):
                value = getattr(self, field)
                if issubclass(v.field_type, SubModel):
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
    __engine__ = None
    _id = Field(ObjectId, None)

    def __new__(cls, **kwargs):
        if cls.__collectionname__ is None:
            cls.__collectionname__ = get_collection_name(cls.__name__)
        return super(Model, cls).__new__(cls)

    def __init__(self, *args, **kwargs):
        value = kwargs.pop(ID, self._id.default)
        self._id = self.normalize_field_value(self._id, value)
        super(Model, self).__init__(*args, **kwargs)

    @classmethod
    def get(cls, model_id):
        """
        get a model instance by id
        """
        if cls.__collectionname__ is None:
            cls.__collectionname__ = get_collection_name(cls.__name__)
        return QueryOne(cls, spec=cls._generate_query_context(
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
        _pop_from_queue(self)
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

    def __init__(self, *args, **kwargs):
        self.__parent__ = kwargs.pop("__parent__", None)
        self.__fieldname__ = kwargs.pop("__fieldname__", None)
        super(SubModel, self).__init__(*args, **kwargs)


class BaseQuery(object):

    def __init__(self, model, **kwargs):
        self.parent = None
        self.model = model
        self.commands = dict()
        self.commands["spec"] = kwargs.get("spec")
        self.commands["sort"] = kwargs.get("sort")
        self.commands["fields"] = kwargs.get("fields")
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
                        new_values = []
                        for v in values:
                            if isinstance(v, tuple):
                                new_v = [generate_field(self.__fieldname__, v[0])]
                                new_v.extend(list(v[1:]))
                                new_values.append(new_v)
                            else:
                                new_values.append(
                                    generate_field(self.__fieldname__, v)
                                )
                        commands[key] = new_values
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

    def _compile_context(self):
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

    def filters(self, fields):
        """
        """
        for field in fields:
            self.model.assert_vaild_field(field)
        self.commands["fields"] = fields


class Query(BaseQuery):
    def __init__(self, model, **kwargs):
        super(Query, self).__init__(model, **kwargs)
        self.cursor = None
        self.commands["limit"] = kwargs.get("limit", 0)

    def __getitem__(self, item):
        pass

    def __iter__(self):
        if self.cursor is None:
            context = self._compile_context()
            self.execute_context(context)
        return self

    def next(self):
        if self.cursor is None:
            context = self._compile_context()
            self.execute_context(context)
        record = next(self.cursor)
        return self.model(__persistence__=True, **record)

    def _compile_context(self, operation=QUERY_FIND):
        collection = self.model.__collectionname__
        return (collection, operation, self.get_commands())

    def execute_context(self, context):
        self.cursor = get_session().execute(*context)

    def all(self):
        """
        Return a list of all object
        """
        return list(self)

    def limit(self, capicity):
        """
        query().limit(count)
        """
        assert isinstance(capicity, int), "limit access int argument"
        self.commands["limit"] = capicity
        return self

    def update(self, if_not_create=False, multi=True, *args, **kwargs):
        """
        update model
        """
        args = list(args)
        if kwargs and not any(isinstance(x, Set) for x in args):
            args.append(Set(kwargs))
        update = Update(self.model,
                        spec=self.get_commands()["spec"],
                        document=args,
                        multi=multi,
                        upsert=if_not_create)
        return get_session().execute(*update.compile_context())

    def delete(self):
        collection = self.model.__collectionname__
        get_session().execute(
            collection, DELETE, dict(spec_or_id=self.get_commands()["spec"])
        )

    def count(self):
        """
        get the records count in this query operator
        """
        if self.cursor is None:
            context = self._compile_context()
            self.execute_context(context)
        return self.cursor.count()


class QueryOne(BaseQuery):

    def _compile_context(self):
        collection = self.model.__collectionname__
        return (collection, QUERY_FIND, self.get_commands())

    def execute_context(self, context):
        for result in get_session().execute(*context).limit(-1):
            return self.model(__persistence__=True, **result)

    def __call__(self):
        return self.execute_context(self._compile_context())


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

    def _generate_commands(self):

        commands = dict()
        for expression in self.document:
            commands.update(update_operator_compiler(self.model, expression))
        return commands

    def get_commands(self):
        return dict(
            spec=self.spec,
            document=self._generate_commands(),
            multi=self.multi,
            upsert=self.upsert
        )

    def compile_context(self):
        collection = self.model.__collectionname__
        return (collection, UPDATE, self.get_commands())
