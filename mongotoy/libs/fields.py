# coding: utf8

import copy
import numbers
import inspect
import datetime

from .containers import ToyList


class Field(object):

    __f_type__ = None

    def __init__(self, f_type=None, default=None, allow_none=True):
        if f_type is not None:
            self.__f_type__ = f_type

        assert inspect.isclass(self.__f_type__), "unknown type for field"
        self._allow_none = allow_none
        self.__field_name__ = None
        self.__lord__ = None
        if not (inspect.isroutine(default) or
                isinstance(default, self.__f_type__)):
            self.default = self._convert_to_f_type(default, self.__lord__)
        else:
            self.default = default

    def _return_none(self):
        if not self._allow_none:
            raise ValueError(
                "field %s could not be None" % self.__field_name__
            )

    def get_default(self, lord=None, persistence=False):
        if inspect.isroutine(self.default):
            value = self.default()
        else:
            value = copy.deepcopy(self.default)
        return value

    def normalize_value(self, value, lord=None, presistence=False):
        if isinstance(value, self.__f_type__):
            return value
        return self._convert_to_f_type(value, lord, presistence)

    def _convert_to_f_type(self, value, lord=None, persistence=False):
        if value is None:
            return self._return_none()
        return self.__f_type__(value)

    def __desc__(self):
        return "Field %s, type: %s, default: %s" % (
            self.__name__, self.__f_type__, self.default
        )


class IntField(Field):

    __f_type__ = int

    def __init__(self, default=None, allow_none=True):
        super(IntField, self).__init__(default=default, allow_none=allow_none)


class FloatField(Field):

    __f_type__ = float

    def __init__(self, default=None, allow_none=True):
        super(FloatField, self).__init__(
            default=default, allow_none=allow_none
        )


class StrField(Field):

    __f_type__ = str

    def __init__(self, default=None, allow_none=True):
        super(StrField, self).__init__(default=default, allow_none=allow_none)

    def _convert_to_f_type(self, value, lord=None, persistence=False):
        if isinstance(value, unicode):
            return value.encode("utf8", "ignore")
        else:
            return super(StrField, self)._convert_to_f_type(
                value, lord, persistence
            )


class UnicodeField(Field):

    __f_type__ = unicode

    def __init__(self, default=None, allow_none=True):
        super(UnicodeField, self).__init__(
            default=default, allow_none=allow_none
        )

    def _convert_to_f_type(self, value, lord=None, persistence=False):
        if isinstance(value, str):
            return value.decode("utf8", "ignore")
        else:
            return super(UnicodeField, self)._convert_to_f_type(
                value, lord, persistence
            )


class DateField(Field):

    __f_type__ = datetime.date

    def __init__(self, default=None, allow_none=True):
        super(DateField, self).__init__(default=default, allow_none=allow_none)

    def _convert_to_f_type(self, value, lord=None, persistence=False):
        if isinstance(value, numbers.Number):
            return datetime.datetime.fromtimestamp(value).date()
        elif isinstance(value, basestring):
            return datetime.datetime.strptime(value, "%Y-%m-%d").date()
        else:
            return super(DateField, self)._convert_to_f_type(
                value, lord, persistence
            )


class DateTimeField(Field):

    __f_type__ = datetime.datetime

    def __init__(self, default=None, allow_none=True):
        super(DateTimeField, self).__init__(
            default=default, allow_none=allow_none
        )

    def _convert_to_f_type(self, value, lord=None, persistence=False):
        if isinstance(value, basestring):
            if value.find("T") != -1:
                value = datetime.datetime.strptime(
                    value, "%Y-%m-%dT%H:%M:%S"
                )
            else:
                value = datetime.datetime.strptime(value, "%Y-%m-%d")
        elif isinstance(value, numbers.Number):
            value = datetime.datetime.fromtimestamp(value)
        else:
            value = super(DateTimeField, self)._convert_to_f_type(
                value, lord, persistence
            )
        return value


class ListField(Field):

    __f_type__ = ToyList

    def __init__(self, default=None, allow_none=True):
        super(ListField, self).__init__(default=default, allow_none=allow_none)

    def _convert_to_f_type(self, value, lord=None, persistence=False):
        if value is None:
            return self._return_none()
        elif isinstance(value, (tuple, set, list)):
            return ToyList(lord, value)
        return ToyList(lord, [value])


class SetField(Field):

    __f_type__ = set

    def normalize_value(self, value, lord=None, presistence=False):
        if isinstance(value, set):
            return ToyList(lord, value)
        return ToyList(lord, self._convert_to_f_type(value, lord, presistence))


class ModelField(Field):

    def __init__(self, submodel, default=None, allow_none=True):
        super(ModelField, self).__init__(submodel, default, allow_none)

    def get_default(self, lord, persistence):
        if self.default:
            value = copy.deepcopy(self.default)
            value.__lord__ = lord
        else:
            value = self.__f_type__(
                __lord__=lord, __persistence__=persistence,
                __fieldname__=self.__field_name__
            )
        return value

    def _convert_to_f_type(self, value, lord, persistence=False):
        if isinstance(value, dict):
            return self.__f_type__(
                __lord__=lord, __persistence__=persistence,
                __fieldname__=self.__field_name__, **value
            )
        else:
            return super(ModelField, self)._convert_to_f_type(
                value, lord, persistence
            )


class ListModelField(ListField):
    def __init__(self, submodel, default=None, allow_none=True):
        self.submodel = submodel
        super(ListModelField, self).__init__(default, allow_none)

    def get_default(self, lord, persistence):
        if self.default:
            value = self._convert_to_f_type(self.default, lord, persistence)
        else:
            value = [self.submodel(
                __lord__=lord, __persistence__=persistence,
                __fieldname__=self.__field_name__
            )]
        return value

    def _convert_to_f_type(self, value, lord, persistence=False):
        if value is None:
            return self._return_none()
        values = ToyList(lord, [])
        for model_instance in super(ListModelField, self)._convert_to_f_type(
            value, lord, persistence
        ):
            if not isinstance(model_instance, self.submodel):
                if isinstance(model_instance, dict):
                    model_instance = self.submodel(
                        __lord__=lord, __persistence__=persistence,
                        __fieldname__=self.__field_name__, **model_instance
                    )
                else:
                    raise ValueError()
            values.append(model_instance)
        return values
