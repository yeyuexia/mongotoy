# coding: utf8

import numbers
import datetime


class Operator(object):
    def __init__(self, value):
        self._value_check(value)
        self.value = value

    def compile(self):
        raise NotImplementedError("")

    def _value_check(self, value):
        pass


class QueryOperator(Operator):
    def __init__(self, field=None, value=None):
        if field is not None:
            self.field = field
        super(QueryOperator, self).__init__(value)


class UpdateOperator(Operator):
    def __init__(self, value):
        super(UpdateOperator, self).__init__(value)


class LogicalOperator(Operator):
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
        return {self.field: {"$gt": self.value}}


class Gte(Comparison):

    def compile(self):
        return {self.field: {"$gte": self.value}}


class Lt(Comparison):

    def compile(self):
        return {self.field: {"$lt": self.value}}


class Lte(Comparison):

    def compile(self):
        return {self.field: {"$lte": self.value}}


class Ne(Comparison):

    def compile(self):
        return {self.field: {"$ne": self.value}}


class In(Comparison):

    def compile(self):
        return {self.field: {"$in": self.value}}


class Nin(Comparison):

    def compile(self):
        return {self.field: {"$nin": self.value}}


class Or(LogicalOperator):

    def __init__(self, *args, **kwargs):
        values = []
        for arg in args:
            values.append(arg.compile())
        values.extend(kwargs)
        super(Or, self).__init__(values)

    def compile(self):
        return {"$or": self.value}


class And(LogicalOperator):

    def __init__(self, *args, **kwargs):
        values = []
        for arg in args:
            values.append(arg.compile())
        values.update(kwargs)
        super(And, self).__init__(values)

    def compile(self):
        return {"$and": self.value}


class Not(LogicalOperator):

    def __init__(self, field, value):
        self.field = field
        super(Not, self).__init__(value)

    def _value_check(self, value):
        assert isinstance(value, Operator), "value must be expression"

    def compile(self):
        return {self.field: {"$not": self.value}}


class Nor(LogicalOperator):

    def __init__(self, *args, **kwargs):
        values = []
        for arg in args:
            values.append(arg.compile())
        values.update(kwargs)
        super(Nor, self).__init__(values)

    def compile(self):
        return {"$nor": self.value}


class Exists(Element):

    def _value_check(self, value):
        assert isinstance(value, bool), "invalid value"

    def compile(self):
        return {self.field: {"$exists": self.value}}


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

    def _value_check(self, value):
        assert value in (
            self.Double, self.String, self.Object,
            self.Array, self.Binary_data, self.Undefined,
            self.Object_id, self.Boolean, self.Date,
            self.Null, self.Regular, self.Javascript,
            self.Symbol, self.Javascript_with_scope,
            self.Integer_32bit, self.Timestamp,
            self.integer_64bit, self.Min_key, self.Max_key
        ), "invalid value"

    def compile(self):
        return {self.field: {"$type": self.value}}


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


def update_operator_compiler(model, instance):
    for key, value in instance.command.iteritems():
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
        ):
            raise SyntaxError(
                "%s can not use operator %s" % (
                    field_type.__name__,
                    instance.__class__.__name__
                )
            )
        if isinstance(instance, Set) and not isinstance(value, field_type):
            try:
                instance[key] = field_type.normalize_vlaue(value)
            except:
                raise SyntaxError(
                    "%s.%s, wrong field type. %s expected, %s gived" % (
                        model.__name__, key, field_type.__name__, type(value)
                    )
                )
    return instance.compile()
