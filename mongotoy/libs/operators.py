# coding: utf8

import numbers
import datetime


class Operator(object):

    def compile(self):
        raise NotImplementedError("")


class QueryOperator(Operator):
    def __init__(self, key, value):
        self.key = key
        self.value = value


class UpdateOperator(Operator):
    def __init__(self, value):
        self.value = value


class Logical(QueryOperator):
    pass


class Element(QueryOperator):
    pass


class Comparison(QueryOperator):
    pass


class Evaluation(QueryOperator):
    pass


class Array(QueryOperator):
    pass


class Geospatial(QueryOperator):
    pass


class Set(UpdateOperator):

    def compile(self):
        return {"$set": self.value}


class Unset(UpdateOperator):

    def compile(self):
        return {"$unset": self.value}


class Inc(UpdateOperator):

    def compile(self):
        return {"$inc": self.value}


class Mul(UpdateOperator):

    def compile(self):
        return {"$mul": self.value}


class Rename(UpdateOperator):

    def compile(self):
        return {"$rename": self.value}


class SetOnInsert(UpdateOperator):

    def compile(self):
        return {"$setOnInsert": self.value}


class Min(UpdateOperator):

    def compile(self):
        return {"$min": self.value}


class Max(UpdateOperator):

    def compile(self):
        return {"$max": self.value}


class CurrentDate(UpdateOperator):

    def compile(self):
        return {"$currentDate": self.value}


class Gt(Comparison):

    def compile(self):
        return {self.key: {"$gt": self.value}}


class Gte(Comparison):

    def compile(self):
        return {self.key: {"$gte": self.value}}


class Lt(Comparison):

    def compile(self):
        return {self.key: {"$lt": self.value}}


class Lte(Comparison):

    def compile(self):
        return {self.key: {"$lte": self.value}}


class Ne(Comparison):

    def compile(self):
        return {self.key: {"$ne": self.value}}


class In(Comparison):

    def compile(self):
        return {self.key: {"$in": self.value}}


class Nin(Comparison):

    def compile(self):
        return {self.key: {"$nin": self.value}}


class Or(Logical):

    def __init__(self, *args, **kwargs):
        super(Or, self).__init__(None, (args, kwargs))

    def compile(self):
        return {"$or": self.value}


class And(Logical):

    def __init__(self, *args, **kwargs):
        super(And, self).__init__(None, (args, kwargs))

    def compile(self):
        return {"$and": self.value}


class Not(Logical):

    def __init__(self, key, value):
        assert isinstance(value, Operator), "value must be expression"
        super(Nor, self).__init__(key, value)

    def compile(self):
        return {self.key: {"$not": self.value}}


class Nor(Logical):

    def __init__(self, *args, **kwargs):
        super(Nor, self).__init__(None, (args, kwargs))

    def compile(self):
        return {"$nor": self.value}


class Exists(Element):
    def __init__(self, value):
        assert isinstance(value, bool), "invalid value"
        super(Exists, self).__init__(value)

    def compile(self):
        return {self.key: {"$exists": self.value}}


class Type(Element):
    Double = 1
    String = 2
    Object = 3
    Array = 4
    Binary_data = 5
    Undefined = 6
    Object_id = 7
    Boolean = 8
    Date = 9
    Null = 10
    Regular = 11
    Javascript = 13
    Symbol = 14
    Javascript_with_scope = 15
    Integer_32bit = 16
    Timestamp = 17
    integer_64bit = 18
    Min_key = 255
    Max_key = 127

    def __init__(self, key, value):
        assert value in (
            self.Double, self.String, self.Object,
            self.Array, self.Binary_data, self.Undefined,
            self.Object_id, self.Boolean, self.Date,
            self.Null, self.Regular, self.Javascript,
            self.Symbol, self.Javascript_with_scope,
            self.Integer_32bit, self.Timestamp,
            self.integer_64bit, self.Min_key, self.Max_key
        ), "invalid value"
        super(Type, self).__init__(key, value)

    def compile(self):
        return {self.key: {"$type": self.value}}


class Where(Evaluation):
    pass


class Text(Evaluation):
    pass


class Regex(Evaluation):
    pass


class Mod(Evaluation):
    pass


class GeoWithin(Geospatial):
    pass


class GeoIntersects(Geospatial):
    pass


class Near(Geospatial):
    pass


class NearSphere(Geospatial):
    pass


class All(Array):
    pass


class ElemMatch(Array):
    pass


class Size(Array):
    pass


def _assert_vaild_field(model, key):
    fields = key.split(".")
    submodel = model
    for field in fields:
        if hasattr(model, field):
            submodel = getattr(submodel, field)
        else:
            raise KeyError(
                "%s doesn't has field named %s" % (submodel.__name__, field)
            )


def _get_field_type(model, key):
    fields = key.split(".")
    real_field = model
    for field in fields:
        real_field = getattr(real_field, key)
    return real_field.field_type


def query_operator_compiler(model, instance):
    if isinstance(instance, Logical):
        if isinstance(instance.value, Operator):
            _assert_vaild_field(model, instance.key)
            instance.value = instance.value.compile()
        else:
            v_args, v_kwargs = instance.value
            values = []
            for value in v_args:
                values.append(query_operator_compiler(model, value))
            for key, value in v_kwargs.iteritems():
                values.append({key: value})
            instance.value = values
    else:
        _assert_vaild_field(model, instance.key)
    return instance.compile()


def update_operator_compiler(model, instance):
    for key, value in instance.value.iteritems():
        _assert_vaild_field(model, key)
        field_type = _get_field_type(model, key)
        if (
            isinstance(instance, (Inc, Mul)) and not isinstance(field_type, numbers.Number)
        ) or (
            isinstance(instance, (Min, Max)) and not isinstance(
                field_type, (numbers.Number, datetime.datetime, datetime.date)
            )
        ) or (
            isinstance(instance, CurrentDate) and not isinstance(
                field_type, (datetime.datetime, datetime.date)
            )
        ) or (
            isinstance(instance, Rename) and not isinstance(value, basestring)
        ) or (
            isinstance(instance, Set) and not isinstance(value, field_type)
        ):
            raise SyntaxError(
                "%s can not use operator %s" % (
                    getattr(model, key).field_type.__name__,
                    instance.__class__.__name__
                )
            )
    return instance.compile()
