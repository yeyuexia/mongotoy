# coding: utf8


class Operator(object):

    def compile(self):
        raise NotImplementedError("")


class Logical(Operator):
    def __init__(self, *value):
        self.value = value


class Element(Operator):
    def __init__(self, value):
        self.value = value


class Comparison(Operator):
    def __init__(self, value):
        self.value = value


class Evaluation(Operator):
    def __init__(self, value):
        self.value = value


class Array(Operator):
    def __init__(self, value):
        self.value = value


class Update(Operator):
    def __init__(self, value):
        self.value = value


class Geospatial(Operator):
    def __init__(self, value):
        self.value = value


class Set(Update):

    def compile(self):
        return {"$set": self.value}


class Unset(Update):

    def compile(self):
        return {"$unset": self.value}


class Inc(Update):

    def compile(self):
        return {"$inc": self.value}


class Mul(Update):

    def compile(self):
        return {"$mul": self.value}


class Gt(Comparison):

    def compile(self):
        return {"$gt": self.value}


class Gte(Comparison):

    def compile(self):
        return {"$gte": self.value}


class Lt(Comparison):

    def compile(self):
        return {"$lt": self.value}


class Lte(Comparison):

    def compile(self):
        return {"$lte": self.value}


class Ne(Comparison):

    def compile(self):
        return {"$ne": self.value}


class In(Comparison):

    def compile(self):
        return {"$in": self.value}


class Nin(Comparison):

    def compile(self):
        return {"$nin": self.value}


class Or(Logical):

    def compile(self):
        return {"$or": self.value}


class And(Logical):

    def compile(self):
        return {"$and": self.value}


class Not(Logical):

    def compile(self):
        return {"$not": self.value}


class Nor(Logical):

    def compile(self):
        return {"$nor": self.value}


class Exists(Element):
    def __init__(self, value):
        assert isinstance(value, bool), "invalid value"
        super(Exists, self).__init__(value)

    def compile(self):
        return {"$exists": self.value}


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

    def __init__(self, value):
        assert value in (
            self.Double, self.String, self.Object,
            self.Array, self.Binary_data, self.Undefined,
            self.Object_id, self.Boolean, self.Date,
            self.Null, self.Regular, self.Javascript,
            self.Symbol, self.Javascript_with_scope,
            self.Integer_32bit, self.Timestamp,
            self.integer_64bit, self.Min_key, self.Max_key
        ), "invalid value"
        super(Type, self).__init__(value)

    def compile(self):
        return {"$type": self.value}


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
