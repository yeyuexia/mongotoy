# coding: utf8

import unittest
import datetime

#import mock

from mongotoy.libs.fields import (
    Field, IntField, FloatField, StrField, UnicodeField,
    DateField, DateTimeField, ListField, SetField
)


class BaseTestField(unittest.TestCase):
    _field = None
    _field_type = None
    _default_value = None
    _allow_none = True
    _test_values = None

    def setUp(self):
        if self._field is not None:
            self.field = self._field(
                default=self._default_value, allow_none=self._allow_none
            )

    def test_type(self):
        if self._field is not None:
            self.assertEqual(self._field_type, self.field.__f_type__)

    def test_get_default(self):
        if self._field is not None:
            self.assertEqual(self._default_value, self.field.get_default())

    def test_normalize_value(self):
        if self._field is not None:
            for test_value, normalized_value in self._test_values:
                self.assertEqual(
                    normalized_value, self.field.normalize_value(test_value)
                )


class TestField(BaseTestField):
    _field = Field
    _field_type = int
    _default_value = 5
    _allow_none = True
    _test_values = [(20.5, 20), (40.0, 40)]

    def setUp(self):
        self.field = self._field(
            self._field_type, self._default_value, self._allow_none
        )


class TestIntField(BaseTestField):
    _field = IntField
    _field_type = int
    _default_value = 5
    _test_values = [(5.3, 5)]


class TestFloatField(BaseTestField):
    _field = FloatField
    _field_type = float
    _default_value = 3.2
    _test_values = [("3.2", 3.2)]


class TestStrField(BaseTestField):
    _field = StrField
    _field_type = str
    _default_value = "test"
    _test_values = [(3.2, "3.2"), (u"aaa", u"aaa".encode("utf8"))]


class TestUnicodeField(BaseTestField):
    _field = UnicodeField
    _field_type = unicode
    _default_value = u"test"
    _test_values = [(3.2, u"3.2"), ("aaa", "aaa".decode("utf8"))]


class TestDateField(BaseTestField):
    _field = DateField
    _field_type = datetime.date
    _default_value = datetime.datetime.now
    _test_values = [("2014-10-27", datetime.date(2014, 10, 27))]

    def test_get_default(self):
        pass


class TestListField(BaseTestField):
    _field = ListField
    _field_type = list
    _default_value = [2, 3]
    _test_values = [(set([1, 2, 3]), [1, 2, 3]), ((2, 4, 5), [2, 4, 5])]


class TestSetField(BaseTestField):
    _field = SetField
    _field_type = set
    _default_value = set([1])
    _test_values = [((11, 11, 22), [11, 22])]


if __name__ == "__main__":
    unittest.main()
