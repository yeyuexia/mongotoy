# coding: utf8

import sys
sys.path.append("/Users/zhuoliu/works/mongotoy")
import unittest
import datetime

#import mock

from mongotoy.libs.models import Model, SubModel
from mongotoy.libs.query import Query
from mongotoy.libs.fields import (
    Field, StrField, ListModelField, IntField,
    FloatField, ListField, ModelField, DateTimeField
)
from mongotoy.libs.operators import And, Or, Gt, Lt, Type


class TestModel(Model):

    class SubModel1(SubModel):

        class SubModel2(SubModel):
            sub_field1 = IntField(10)

        sub_field1 = StrField("unknown")
        sub_field2 = ListModelField(SubModel2)
    field1 = FloatField(1)
    field2 = ListField(None)
    field3 = ModelField(SubModel1)
    field4 = DateTimeField(datetime.datetime(2014, 8, 4))


class TestFieldApi(unittest.TestCase):
    def test_field(self):
        field = IntField(20)
        self.assertTrue(field.__f_type__, int)
        self.assertTrue(field.default, 20)

    def test_field_type(self):
        with self.assertRaises(AssertionError):
            Field(1)
        with self.assertRaises(AssertionError):
            Field(514.1)


class TestModelApi(unittest.TestCase):

    def test_collection_name(self):
        test_model = TestModel()
        self.assertEqual(test_model.__collection__, "test_model")

    def test_default_to_dict(self):
        test_model = TestModel()
        self.assertEqual(
            test_model.to_dict(),
            {"field1": 1.0, "field2": None, "_id": None,
             "field3": dict(
                 sub_field1="unknown", sub_field2=[{"sub_field1": 10}]
             ),
             "field4": datetime.datetime(2014, 8, 4)}
        )

    def test_to_dict(self):
        now = datetime.datetime.now()
        test_model = TestModel(
            field1=4.6, field2=[1, 25, 1.4, 3],
            field3=TestModel.SubModel1(sub_field1="test",
                                       sub_field2={"sub_field1": 4}),
            field4=now
        )
        self.assertEqual(
            test_model.to_dict(),
            {"field1": 4.6, "field2": [1, 25, 1.4, 3], "_id": None,
             "field3": dict(sub_field1="test", sub_field2=[{"sub_field1": 4}]),
             "field4": now}
        )

    def test_wrong_field_value(self):
        with self.assertRaises(TypeError):
            TestModel(field1="aasda")
        test_model = TestModel()
        with self.assertRaises(TypeError):
            test_model.field1 = "str"
        with self.assertRaises(TypeError):
            test_model.field4 = datetime.date.today()

    def test_assert_vaild_field(self):
        with self.assertRaises(KeyError):
            TestModel.assert_valid_field("field5")
        with self.assertRaises(KeyError):
            TestModel.assert_valid_field("__collection__")

    def test_generate_query_context(self):
        now = datetime.datetime.now()
        res = TestModel._generate_query_context(
            Or(Type(TestModel.field1, 1), Gt(TestModel.field4, now), field1=2.0), field2=55
        )
        self.assertEqual(
            res["$or"].sort(),
            [{"field1": 2.0}, {"field1": {"$type": 1}}, {"field4": {"$gt": now}}].sort()
        )
        self.assertEqual(res["field2"], 55)

    def test_update(self):
        t = TestModel(field1=1)
        pass


class TestQuery(unittest.TestCase):
    def test_query_get_command(self):
        query = Query(TestModel, spec=dict(field1=2), sort=dict(field1=1), filter=["field3"])
        #self.assertTrue(query.compile_context(), dd


if __name__ == "__main__":
    unittest.main()
