# Welcome to Mongotoy

Mongotoy is a simple Python Mongo toolkit and Object Relational Mapper that makes mongo collections more clear.

## Usage

* Declare collection named `test_model`.

```

from mongotoy.libs.models import Model, SubModel
from mongotoy.libs.fields import IntField, FloatField, ModelField, DateTimeField, SetField


class TestModel(Model):

	class SubField(SubModel):
		field1 = FloatField(0)
		field2 = SetField(set())

	field1 = IntField(0)
	field2 = ModelField(SubField)
	field3 = DateTimeField(datetime.datetime.now)

```
* Build connection with Mongo DB with host and port, and register TestModel

```
from mongotoy.libs.session import create_session

collection_mapper = {"db1": ["TestModel"]}

create_session(host, port=port, collection_mapper=collection_mapper)
```

collection_mapper is a dict and its key is db name and key is the model name db, you can alse use

```
from mongotoy.libs.session import loads_db_mapper

loads_db_mapper(collection_mapper)
```

to load or reload mapper.

* Create a model instance

```
record = TestModel(field1=10, field2={1: 2},
				   field3=TestModel.SubField(field1=3.0, field2=set([1,2,3])))
```
* Save record

```
flush()
```
Notes: all change need invoke flush method when you need to submit it to mongo server.

* Query

```
cursor = TestModel.query(field1=1)
for model_instance in cursor:
	pass
```
* Get model instance

```
model = TestModel.get_by(field1=1)
```
* Get model instance by id

```
model = TestModel.get(_id)
```
* Convert to dict

```
record = model.to_dict()
```
* Update

```
TestModel.query(field1=2).update(field1=1)
```

* Delete instance

```
instance.delete()
```
* Batch delete

```
TestModel.query(field1=1).delete()
```
Notes: the method `delete()` and `update()` would invoke immediately, you don't need call `flush()` after invoke them.

## Fields
In `mongotoy`, Field name would auto map to the field name in mongo collection. 

all field class has same initialize method like

```
Field(default=None, allow_none=True)
``` 

except `ModelField` and `ListModelField`:

```
ModelField(submodel, default=None, allow_none=True)

ListModelField(submodel, default=None, allow_none=True)
```
## Operators
You can use mongo operators in mongotoy

```
from mongotoy.libs.operators import Or, And, Type

cursor = TestModel.query(Or(And(field1=1, field2=2), Type("field2", float), field1=5))
```
 and in update

```
from mongotoy.libs.operators import Unset, Set

TestModel.query(field1=1).update(Set(field2=1, field1=5), Unset(field4=""))
```
Now mongotoy support all logical, comparison, element and update operators.

## Advance Usage
* Custom collection name

```
class Test(Model):
	__collection__ = "test_collection"
	pass
	
```
The attribute `__collection__` used for reset collection name. 
Notes: when you custom the collection name, you must declear collection name instead Model name in collection mapper.

```
mapper = dict(db1=["test_collection", ..])
...
```
